package scala.scalanative.sandbox.streamio.http2

import java.io.ByteArrayOutputStream
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import munit.FunSuite

import scala.scalanative.meta.LinktimeInfo
import scala.scalanative.sandbox.streamio.StreamIoDebug
import scala.scalanative.sandbox.streamio.transport.{
  AcceptOverloadStrategy,
  ByteQueue,
  ConnectionFailure,
  ConnectionHandler,
  Reactor,
  ServerHandlerFactory,
  TcpConnection,
  TcpConnectionOptions,
  TcpServer,
  TcpServerOptions
}

class Http2CoreSuite extends FunSuite {
  private val serialTestExecutor = Executors.newSingleThreadExecutor()

  override def munitIgnore: Boolean = LinktimeInfo.isWindows
  override val munitTimeout: Duration = 60.seconds
  override def munitExecutionContext: ExecutionContext =
    ExecutionContext.fromExecutor(serialTestExecutor)

  override def afterAll(): Unit =
    serialTestExecutor.shutdown()

  test("loopback client round-trips echoed body") {
    StreamIoDebug.log("core-test", "loopback start")
    withServer(new Http2Handler {
      override def onRequest(stream: Http2Stream): Unit = {
        StreamIoDebug.log("core-test", s"loopback onRequest stream=${stream.id}")
        stream.sendResponseHeaders(
          200,
          Seq("content-type" -> "text/plain")
        )

        stream.requestBody.subscribe(new Http2RequestBodyHandler {
          override def onData(
              stream: Http2Stream,
              data: Array[Byte],
              releaseWindow: () => Unit
          ): Boolean = {
            StreamIoDebug.log("core-test", s"loopback server onData bytes=${data.length}")
            stream.sendData(data)
            releaseWindow()
            true
          }

          override def onEnd(stream: Http2Stream): Unit = {
            StreamIoDebug.log("core-test", s"loopback server onEnd stream=${stream.id}")
            stream.sendData(Array.emptyByteArray, endStream = true)
          }
        })
      }
    }) { (reactor, server) =>
      val responseHeaders = new AtomicReference(Vector.empty[(String, String)])
      val responseBody = new ByteArrayOutputStream()
      val failure = new AtomicReference[Throwable](null)
      val done = new CountDownLatch(1)

      val client = Http2Client.connect(
        reactor,
        "127.0.0.1",
        server.port,
        new Http2ClientLifecycleHandler {
          override def onReady(client: Http2Client): Unit = {
            val stream = client.openStream(
              headers = requestHeaders(server.port, "/echo"),
              handler = new Http2ClientStreamHandler {
                override def onHeaders(
                    stream: Http2ClientStream,
                    headers: Vector[(String, String)],
                    endStream: Boolean
                ): Unit = {
                  responseHeaders.set(headers)
                  if (endStream) done.countDown()
                }

                override def onData(
                    stream: Http2ClientStream,
                    data: Array[Byte],
                    endStream: Boolean,
                    releaseWindow: () => Unit
                ): Boolean = {
                  responseBody.write(data)
                  releaseWindow()
                  if (endStream) done.countDown()
                  true
                }

                override def onComplete(stream: Http2ClientStream): Unit =
                  done.countDown()

                override def onError(
                    stream: Http2ClientStream,
                    cause: Throwable
                ): Unit = {
                  failure.compareAndSet(null, cause)
                  done.countDown()
                }
              },
              endStream = false
            )
            stream.sendData("chunk-1".getBytes(StandardCharsets.UTF_8))
            stream.sendData(
              "chunk-2".getBytes(StandardCharsets.UTF_8),
              endStream = true
            )
          }

          override def onError(client: Http2Client, cause: Throwable): Unit = {
            failure.compareAndSet(null, cause)
            done.countDown()
          }

          override def onClosed(client: Http2Client): Unit =
            done.countDown()
        }
      )

      try {
        StreamIoDebug.log("core-test", "loopback awaiting response")
        assert(done.await(5, TimeUnit.SECONDS), "timed out waiting for response")
        StreamIoDebug.log("core-test", "loopback response finished")
        val cause = failure.get()
        if (cause != null) throw cause
        assertEquals(responseHeaders.get().collectFirst {
          case (":status", value) => value
        }, Some("200"))
        assertEquals(responseBody.toString(StandardCharsets.UTF_8.name()), "chunk-1chunk-2")
      } finally client.close()
    }
  }

  test("transport rejects excess connections when maxOpenConnections is reached") {
    withReactor { reactor =>
      val acceptedCount = new AtomicInteger(0)
      val firstAccepted = new CountDownLatch(1)
      val extraAccepted = new CountDownLatch(1)
      val server = reactor.listen(
        port = 0,
        factory = ServerHandlerFactory(_ =>
          new ConnectionHandler {
            override def onConnected(connection: TcpConnection): Unit = {
              val seen = acceptedCount.incrementAndGet()
              if (seen == 1) firstAccepted.countDown()
              else extraAccepted.countDown()
            }

            override def onReadable(
                connection: scala.scalanative.sandbox.streamio.transport.TcpConnection,
                inbound: ByteQueue
            ): Unit =
              if (inbound.readableBytes > 0)
                inbound.discard(inbound.readableBytes)
          }
        ),
        host = "127.0.0.1",
        options = TcpServerOptions(
          maxOpenConnections = 1,
          overloadStrategy = AcceptOverloadStrategy.RejectAccepted
        )
      )

      try {
        val first = new Socket("127.0.0.1", server.port)
        try {
          assert(
            firstAccepted.await(5, TimeUnit.SECONDS),
            "timed out waiting for first accepted connection"
          )
          val second = new Socket("127.0.0.1", server.port)
          try {
            second.getOutputStream.write(1)
            second.getOutputStream.flush()
            assert(
              !extraAccepted.await(500, TimeUnit.MILLISECONDS),
              "server accepted an excess connection while at capacity"
            )
          } finally second.close()

          assertEquals(acceptedCount.get(), 1)
        } finally {
          first.close()
          assert(
            !extraAccepted.await(500, TimeUnit.MILLISECONDS),
            "server promoted an excess connection instead of rejecting it"
          )
          assertEquals(acceptedCount.get(), 1)
        }
      } finally server.close()
    }
  }

  test("transport reports outbound buffer overflow as a value failure") {
    StreamIoDebug.log("core-test", "overflow test start")
    withReactor { reactor =>
      val failureRef = new AtomicReference[ConnectionFailure](null)
      val failureSeen = new CountDownLatch(1)
      val server = reactor.listen(
        port = 0,
        factory = ServerHandlerFactory(_ =>
          new ConnectionHandler {
            override def onConnected(connection: TcpConnection): Unit = {
              StreamIoDebug.log("core-test", s"overflow onConnected fd=${connection.fd}")
              connection.write(new Array[Byte](32))
            }

            override def onReadable(
                connection: TcpConnection,
                inbound: ByteQueue
            ): Unit =
              if (inbound.readableBytes > 0)
                inbound.discard(inbound.readableBytes)

            override def onConnectionFailure(
                connection: TcpConnection,
                cause: ConnectionFailure
            ): Unit = {
              StreamIoDebug.log(
                "core-test",
                s"overflow onConnectionFailure fd=${connection.fd} cause=${cause.message}"
              )
              failureRef.set(cause)
              failureSeen.countDown()
            }
          }
        ),
        host = "127.0.0.1",
        options = TcpServerOptions(
          childConnectionOptions = TcpConnectionOptions(maxQueuedWriteBytes = 8)
        )
      )

      try {
        StreamIoDebug.log("core-test", "overflow creating client socket")
        val client = new Socket("127.0.0.1", server.port)
        try {
          StreamIoDebug.log("core-test", "overflow awaiting failure")
          assert(
            failureSeen.await(5, TimeUnit.SECONDS),
            "timed out waiting for connection failure"
          )
          StreamIoDebug.log("core-test", "overflow failure observed")
          val failure = failureRef.get()
          assert(failure != null)
          failure match {
            case transport: scala.scalanative.sandbox.streamio.transport.TransportFailure =>
              assertEquals(
                transport.code,
                scala.scalanative.sandbox.streamio.transport.TransportError.OutboundBufferOverflow
              )
              assert(transport.message.contains("outbound buffer overflow"))
            case other =>
              fail(s"expected TransportFailure but received $other")
          }
        } finally client.close()
      } finally server.close()
    }
  }

  private def withServer(
      handler: Http2Handler,
      maxConcurrentStreams: Int = 16384
  )(body: (Reactor, Http2Server) => Unit): Unit =
    withReactor { reactor =>
      val server = Http2Server.bind(
        reactor,
        port = 0,
        handler = handler,
        maxConcurrentStreams = maxConcurrentStreams
      )
      try body(reactor, server)
      finally server.close()
    }

  private def withReactor(body: Reactor => Unit): Unit = {
    val reactor = Reactor.polling()
    val thread = new Thread(new Runnable {
      override def run(): Unit = reactor.run()
    })
    thread.setDaemon(true)
    thread.start()
    try body(reactor)
    finally {
      reactor.close()
      thread.join(1000L)
    }
  }

  private def requestHeaders(
      port: Int,
      path: String,
      method: String = "POST"
  ): Seq[(String, String)] =
    Seq(
      ":method" -> method,
      ":scheme" -> "http",
      ":path" -> path,
      ":authority" -> s"127.0.0.1:${port}"
    )
}
