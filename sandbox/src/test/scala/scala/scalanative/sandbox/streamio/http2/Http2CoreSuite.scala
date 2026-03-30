package scala.scalanative.sandbox.streamio.http2

import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

import munit.FunSuite

import scala.scalanative.meta.LinktimeInfo
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
  override def munitIgnore: Boolean = LinktimeInfo.isWindows
  override val munitTimeout: Duration = 60.seconds

  test("loopback client round-trips echoed body") {
    withServer(new Http2Handler {
      override def onRequest(stream: Http2Stream): Unit = {
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
            stream.sendData(data)
            releaseWindow()
            true
          }

          override def onEnd(stream: Http2Stream): Unit =
            stream.sendData(Array.emptyByteArray, endStream = true)
        })
      }
    }) { (_, server) =>
      val client = new LoopbackHttp2Client("127.0.0.1", server.port)
      try {
        val response = client.request(
          headers = requestHeaders(server.port, "/echo"),
          dataFrames = Seq(
            "chunk-1".getBytes(StandardCharsets.UTF_8),
            "chunk-2".getBytes(StandardCharsets.UTF_8)
          )
        )

        assertEquals(response.header(":status"), Some("200"))
        assertEquals(response.bodyUtf8, "chunk-1chunk-2")
      } finally client.close()
    }
  }

  test("transport rejects excess connections when maxOpenConnections is reached") {
    withReactor { reactor =>
      val server = reactor.listen(
        port = 0,
        factory = ServerHandlerFactory(_ =>
          new ConnectionHandler {
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
        first.setSoTimeout(1000)
        try {
          val second = new Socket("127.0.0.1", server.port)
          second.setSoTimeout(1000)
          try {
            second.getOutputStream.write(1)
            second.getOutputStream.flush()
            val rejected =
              try second.getInputStream.read() == -1
              catch {
                case _: java.io.IOException => true
              }
            assert(rejected)
          } finally second.close()
        } finally first.close()
      } finally server.close()
    }
  }

  test("transport reports outbound buffer overflow as a value failure") {
    withReactor { reactor =>
      val failures = new java.util.concurrent.LinkedBlockingQueue[ConnectionFailure]()
      val server = reactor.listen(
        port = 0,
        factory = ServerHandlerFactory(_ =>
          new ConnectionHandler {
            override def onConnected(connection: TcpConnection): Unit =
              connection.write(new Array[Byte](32))

            override def onReadable(
                connection: TcpConnection,
                inbound: ByteQueue
            ): Unit =
              if (inbound.readableBytes > 0)
                inbound.discard(inbound.readableBytes)

            override def onConnectionFailure(
                connection: TcpConnection,
                cause: ConnectionFailure
            ): Unit =
              failures.offer(cause)
          }
        ),
        host = "127.0.0.1",
        options = TcpServerOptions(
          childConnectionOptions = TcpConnectionOptions(maxQueuedWriteBytes = 8)
        )
      )

      try {
        val client = new Socket("127.0.0.1", server.port)
        try {
          val failure = failures.poll(5, TimeUnit.SECONDS)
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

  private def requestHeaders(port: Int, path: String): Seq[(String, String)] =
    Seq(
      ":method" -> "GET",
      ":scheme" -> "http",
      ":path" -> path,
      ":authority" -> s"127.0.0.1:${port}"
    )
}
