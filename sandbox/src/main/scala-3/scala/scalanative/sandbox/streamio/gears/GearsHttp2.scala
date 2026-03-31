package scala.scalanative.sandbox.streamio.gears

import java.io.{ByteArrayOutputStream, IOException}
import java.nio.charset.StandardCharsets
import java.util.ArrayDeque
import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import scala.scalanative.sandbox.streamio.http2._
import scala.scalanative.sandbox.streamio.transport.{Reactor, TcpServerOptions}

import gears.async.{
  Async, BufferedChannel, Cancellable, ChannelClosedException, Future,
  ReadableChannel, UnboundedChannel
}

private object ReactorSubmission {
  def future(
      submit: Runnable => Unit,
      onCancel: () => Unit = () => ()
  )(body: => Unit): Future[Unit] =
    Future.withResolver { resolver =>
      val cancelled = new AtomicBoolean(false)
      resolver.onCancel(() => {
        if (cancelled.compareAndSet(false, true))
          onCancel()
      })
      submit(new Runnable {
        override def run(): Unit =
          if (!cancelled.get())
            try {
              body
              resolver.resolve(())
            } catch {
              case NonFatal(t) =>
                resolver.reject(t)
            }
      })
    }

  def run(
      submit: Runnable => Unit,
      onCancel: () => Unit = () => ()
  )(body: => Unit)(using Async): Unit =
    future(submit, onCancel)(body).await
}

private object GearsHttp2Internal {
  final val CancelErrorCode = 8
}

final class GearsByteStream private[gears] (
    capacityChunks: Int,
    onChunkConsumed: () => Unit,
    onAbandoned: Throwable => Unit
) extends AutoCloseable {
  import GearsByteStream._

  private final class Waiter(val resolver: Future.Resolver[StreamEvent])

  private val chunks = new ArrayDeque[Chunk]()
  private val waiters = new ArrayDeque[Waiter]()
  private var failed: Throwable = _
  private var finished = false
  private var abandoned = false

  def read(using Async): Option[Array[Byte]] =
    nextEvent() match {
      case Chunk(bytes, releaseWindow) =>
        releaseWindow()
        onChunkConsumed()
        Some(bytes)
      case End =>
        None
      case Failed(cause) =>
        throw cause
    }

  def bufferAll(using Async): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    var done = false
    while (!done) {
      read match {
        case Some(bytes) =>
          out.write(bytes, 0, bytes.length)
        case None =>
          done = true
      }
    }
    out.toByteArray
  }

  def isTerminated: Boolean = synchronized {
    failed != null || finished || abandoned
  }

  override def close(): Unit =
    abandon(new IOException("body stream closed"))

  private[gears] def offer(
      bytes: Array[Byte],
      releaseWindow: () => Unit = () => ()
  ): Boolean = {
    val waiter = synchronized {
      if (failed != null || finished || abandoned) {
        releaseWindow()
        null
      } else if (!waiters.isEmpty) {
        waiters.removeFirst()
      } else if (chunks.size() < capacityChunks) {
        chunks.addLast(Chunk(bytes, releaseWindow))
        null
      } else return false
    }
    if (waiter != null)
      waiter.resolver.resolve(Chunk(bytes, releaseWindow))
    true
  }

  private[gears] def finish(): Unit = {
    val toResolve = synchronized {
      if (finished || failed != null || abandoned) Nil
      else {
        finished = true
        if (chunks.isEmpty) drainWaiters() else Nil
      }
    }
    toResolve.foreach(_.resolver.resolve(End))
  }

  private[gears] def fail(cause: Throwable): Unit = {
    val (queued, toResolve) = synchronized {
      if (failed != null || finished || abandoned) (Nil, Nil)
      else {
        failed = cause
        (drainChunks(), drainWaiters())
      }
    }
    releaseDroppedChunks(queued)
    toResolve.foreach(_.resolver.resolve(Failed(cause)))
  }

  private def nextEvent()(using Async): StreamEvent = {
    val pending = synchronized {
      if (!chunks.isEmpty) Left(chunks.removeFirst())
      else if (failed != null) Left(Failed(failed))
      else if (finished) Left(End)
      else Right(awaitEvent)
    }

    pending match {
      case Left(event)   => event
      case Right(future) => future.await
    }
  }

  private[gears] def abandon(cause: Throwable): Unit = {
    val (queued, toResolve, notify) = synchronized {
      if (failed != null || finished || abandoned) (Nil, Nil, false)
      else {
        abandoned = true
        failed = cause
        (drainChunks(), drainWaiters(), true)
      }
    }
    releaseDroppedChunks(queued)
    toResolve.foreach(_.resolver.resolve(Failed(cause)))
    if (notify)
      onAbandoned(cause)
  }

  private def awaitEvent(using Async): Future[StreamEvent] =
    Future.withResolver { resolver =>
      val waiter = new Waiter(resolver)
      val resolvedNow = synchronized {
        if (!chunks.isEmpty) Some(chunks.removeFirst())
        else if (failed != null) Some(Failed(failed))
        else if (finished) Some(End)
        else {
          waiters.addLast(waiter)
          None
        }
      }
      resolver.onCancel(() => cancelWaiter(waiter))
      resolvedNow.foreach(resolver.resolve)
    }

  private def cancelWaiter(waiter: Waiter): Unit = {
    val shouldAbandon = synchronized {
      waiters.remove(waiter) && failed == null && !finished && !abandoned
    }
    if (shouldAbandon)
      abandon(new CancellationException("body stream read cancelled"))
  }

  private def drainWaiters(): List[Waiter] = {
    val drained = List.newBuilder[Waiter]
    while (!waiters.isEmpty) drained += waiters.removeFirst()
    drained.result()
  }

  private def drainChunks(): List[Chunk] = {
    val drained = List.newBuilder[Chunk]
    while (!chunks.isEmpty) drained += chunks.removeFirst()
    drained.result()
  }

  private def releaseDroppedChunks(chunks: List[Chunk]): Unit =
    chunks.foreach(_.releaseWindow())
}

private object GearsByteStream {
  sealed trait StreamEvent
  final case class Chunk(bytes: Array[Byte], releaseWindow: () => Unit)
      extends StreamEvent
  case object End extends StreamEvent
  final case class Failed(cause: Throwable) extends StreamEvent

  def bounded(
      capacityChunks: Int = 32,
      onChunkConsumed: () => Unit = () => (),
      onAbandoned: Throwable => Unit = _ => ()
  ): GearsByteStream =
    new GearsByteStream(
      math.max(1, capacityChunks),
      onChunkConsumed,
      onAbandoned
    )
}

private final class ServerStreamLifecycle(stream: Http2Stream) {
  private val aborted = new AtomicBoolean(false)

  def abort(message: String): Unit =
    if (aborted.compareAndSet(false, true))
      stream.submit(new Runnable {
        override def run(): Unit =
          stream.reset(message, GearsHttp2Internal.CancelErrorCode)
      })
}

final class GearsHttp2Request private[gears] (
    val stream: Http2Stream,
    val headers: Vector[(String, String)],
    val body: GearsByteStream,
    val response: GearsHttp2Response
) {
  def header(name: String): Option[String] =
    headers.collectFirst { case (`name`, value) => value }

  def method: Option[String] = header(":method")
  def path: Option[String] = header(":path")
  def authority: Option[String] = header(":authority")
  def bodyBytes(using Async): Array[Byte] = body.bufferAll

  private[gears] def completeScope(): Unit = {
    if (!body.isTerminated)
      body.abandon(
        new IOException(
          "request handler completed before request body was fully consumed"
        )
      )
    if (!response.isCompleted)
      response.abort(
        "request handler completed before response was fully completed"
      )
  }
}

final class GearsHttp2Response private[gears] (
    stream: Http2Stream,
    lifecycle: ServerStreamLifecycle
) {
  private val completed = new AtomicBoolean(false)

  def isCompleted: Boolean =
    completed.get()

  def sendHeaders(
      status: Int,
      headers: Seq[(String, String)] = Nil,
      endStream: Boolean = false
  )(using Async): Unit =
    ReactorSubmission.run(
      stream.submit,
      onCancel = () => abort("response header send cancelled")
    ) {
      stream.sendResponseHeaders(status, headers, endStream)
      if (endStream) completed.set(true)
    }

  def sendData(
      bytes: Array[Byte],
      endStream: Boolean = false
  )(using Async): Unit =
    ReactorSubmission.run(
      stream.submit,
      onCancel = () => abort("response body send cancelled")
    ) {
      stream.sendData(bytes, endStream)
      if (endStream) completed.set(true)
    }

  def writeUtf8(
      value: String,
      endStream: Boolean = false
  )(using Async): Unit =
    ReactorSubmission.run(
      stream.submit,
      onCancel = () => abort("response body send cancelled")
    ) {
      stream.writeUtf8(value, endStream)
      if (endStream) completed.set(true)
    }

  private[gears] def abort(message: String): Unit =
    if (!completed.get()) {
      completed.set(true)
      lifecycle.abort(message)
    }
}

trait GearsHttp2Handler {
  def onRequest(request: GearsHttp2Request)(using Async): Unit

  def onFailure(request: GearsHttp2Request, cause: Throwable)(using
      Async
  ): Unit = {
    request.response.sendHeaders(500, Seq("content-type" -> "text/plain"))
    request.response.writeUtf8(
      Option(cause.getMessage).getOrElse("internal error"),
      endStream = true
    )
  }
}

final class GearsHttp2Listener private[gears] (
    val server: Http2Server,
    private val requestsChannel: BufferedChannel[GearsHttp2Request]
) extends AutoCloseable {
  private var serveLoop: Cancellable = _

  def port: Int = server.port
  def requests: ReadableChannel[GearsHttp2Request] = requestsChannel.asReadable

  private[gears] def attachServeLoop(loop: Cancellable): Unit =
    serveLoop = loop

  override def close(): Unit = {
    val loop = serveLoop
    if (loop != null) loop.cancel()
    requestsChannel.close()
    server.close()
  }
}

object GearsHttp2Server {
  def listen(
      reactor: Reactor,
      port: Int,
      host: String = "0.0.0.0",
      requestQueueCapacity: Int = 1024,
      maxConcurrentStreams: Int = 16384,
      transportOptions: TcpServerOptions = TcpServerOptions()
  ): GearsHttp2Listener = {
    val requests = BufferedChannel[GearsHttp2Request](requestQueueCapacity)
    val server = Http2Server.bind(
      reactor,
      port,
      new Adapter(requests),
      host,
      maxConcurrentStreams,
      transportOptions
    )
    new GearsHttp2Listener(server, requests)
  }

  def serve(
      listener: GearsHttp2Listener,
      handler: GearsHttp2Handler
  )(using Async.Spawn): Future[Unit] =
    Future {
      var done = false
      while (!done) {
        listener.requests.read() match {
          case Right(request) =>
            Future {
              try handler.onRequest(request)
              catch {
                case NonFatal(cause) =>
                  handler.onFailure(request, cause)
              } finally {
                request.completeScope()
              }
            }
          case Left(_) =>
            done = true
        }
      }
    }

  def bind(
      reactor: Reactor,
      port: Int,
      handler: GearsHttp2Handler,
      host: String = "0.0.0.0",
      requestQueueCapacity: Int = 1024,
      maxConcurrentStreams: Int = 16384,
      transportOptions: TcpServerOptions = TcpServerOptions()
  )(using Async.Spawn): GearsHttp2Listener = {
    val listener =
      listen(
        reactor,
        port,
        host,
        requestQueueCapacity,
        maxConcurrentStreams,
        transportOptions
      )
    listener.attachServeLoop(serve(listener, handler))
    listener
  }

  private final class Adapter(requests: BufferedChannel[GearsHttp2Request])
      extends Http2Handler {
    override def onRequest(stream: Http2Stream): Unit = {
      val lifecycle = new ServerStreamLifecycle(stream)
      val bodyStream =
        GearsByteStream.bounded(
          capacityChunks = 32,
          onChunkConsumed = () => stream.requestBody.requestDrain(),
          onAbandoned = _ => lifecycle.abort("request body abandoned")
        )
      stream.requestBody.subscribe(new Http2RequestBodyHandler {
        override def onData(
            stream: Http2Stream,
            data: Array[Byte],
            releaseWindow: () => Unit
        ): Boolean =
          if (data.isEmpty) true
          else bodyStream.offer(data, releaseWindow)

        override def onEnd(stream: Http2Stream): Unit =
          bodyStream.finish()

        override def onFailure(
            stream: Http2Stream,
            cause: Throwable
        ): Unit =
          bodyStream.fail(cause)
      })
      val request =
        new GearsHttp2Request(
          stream = stream,
          headers = stream.requestHeaders,
          body = bodyStream,
          response = new GearsHttp2Response(stream, lifecycle)
        )
      requests.sendSource(request).poll() match {
        case Some(Right(_)) =>
        case Some(Left(_))  =>
          bodyStream.abandon(new ChannelClosedException)
          stream.reset("server unavailable")
        case None =>
          bodyStream.abandon(
            new java.io.IOException("server request queue full")
          )
          stream.reset("server request queue full")
      }
    }
  }
}

final case class GearsHttp2ClientResponse(
    headers: Vector[(String, String)],
    body: Array[Byte]
) {
  def header(name: String): Option[String] =
    headers.collectFirst { case (`name`, value) => value }

  def bodyUtf8: String =
    new String(body, StandardCharsets.UTF_8)
}

final case class GearsHttp2StreamingResponse(
    headers: Vector[(String, String)],
    body: GearsByteStream
) extends AutoCloseable {
  def header(name: String): Option[String] =
    headers.collectFirst { case (`name`, value) => value }

  override def close(): Unit =
    body.close()
}

private final class ClientStreamLifecycle(stream: Http2ClientStream) {
  private val aborted = new AtomicBoolean(false)

  def abort(message: String): Unit =
    if (aborted.compareAndSet(false, true))
      stream.submit(new Runnable {
        override def run(): Unit =
          stream.reset(message, GearsHttp2Internal.CancelErrorCode)
      })
}

final class GearsHttp2RequestBody private[gears] (
    stream: Http2ClientStream,
    lifecycle: ClientStreamLifecycle
) extends AutoCloseable {
  private val completed = new AtomicBoolean(false)

  def sendData(
      bytes: Array[Byte],
      endStream: Boolean = false
  )(using Async): Unit =
    ReactorSubmission.run(
      stream.submit,
      onCancel = () => abort("request body send cancelled")
    ) {
      stream.sendData(bytes, endStream)
      if (endStream) completed.set(true)
    }

  def writeUtf8(
      value: String,
      endStream: Boolean = false
  )(using Async): Unit =
    sendData(value.getBytes(StandardCharsets.UTF_8), endStream)

  def finish()(using Async): Unit =
    sendData(Array.empty[Byte], endStream = true)

  override def close(): Unit =
    abort("request body closed")

  private[gears] def abort(message: String): Unit =
    if (!completed.get()) {
      completed.set(true)
      lifecycle.abort(message)
    }
}

final class GearsHttp2ClientExchange private[gears] (
    val requestBody: GearsHttp2RequestBody,
    private val responseFuture: Future[GearsHttp2StreamingResponse]
) extends AutoCloseable {
  def awaitResponse(using Async): GearsHttp2StreamingResponse =
    responseFuture.await

  override def close(): Unit =
    requestBody.close()
}

final class GearsHttp2Client private[gears] (client: Http2Client)
    extends AutoCloseable {
  def openRequest(
      headers: Seq[(String, String)]
  )(using Async): GearsHttp2ClientExchange =
    openRequestFuture(headers).await

  def streamRequest(
      headers: Seq[(String, String)],
      dataFrames: Seq[Array[Byte]] = Nil
  )(using Async): GearsHttp2StreamingResponse = {
    val exchange = openRequest(headers)
    try {
      if (dataFrames.isEmpty) exchange.requestBody.finish()
      else
        dataFrames.zipWithIndex.foreach {
          case (bytes, idx) =>
            exchange.requestBody.sendData(
              bytes,
              endStream = idx == dataFrames.length - 1
            )
        }
      exchange.awaitResponse
    } catch {
      case t: Throwable =>
        exchange.close()
        throw t
    }
  }

  def request(
      headers: Seq[(String, String)],
      dataFrames: Seq[Array[Byte]] = Nil
  )(using Async): GearsHttp2ClientResponse = {
    val response = streamRequest(headers, dataFrames)
    try GearsHttp2ClientResponse(response.headers, response.body.bufferAll)
    finally response.close()
  }

  override def close(): Unit =
    client.close()

  private def openRequestFuture(
      headers: Seq[(String, String)]
  ): Future[GearsHttp2ClientExchange] =
    Future.withResolver { resolver =>
      val cancelled = new AtomicBoolean(false)
      var exchange: GearsHttp2ClientExchange = null
      resolver.onCancel(() => {
        cancelled.set(true)
        if (exchange != null)
          exchange.close()
      })
      client.submit(new Runnable {
        override def run(): Unit = {
          if (!cancelled.get()) {
            val responseState = new ResponseState
            try {
              val stream = client.openStream(
                headers,
                responseState.handler,
                endStream = false
              )
              val lifecycle = new ClientStreamLifecycle(stream)
              responseState.bind(lifecycle)
              exchange = new GearsHttp2ClientExchange(
                requestBody = new GearsHttp2RequestBody(stream, lifecycle),
                responseFuture = responseState.future
              )
              if (cancelled.get()) exchange.close()
              else resolver.resolve(exchange)
            } catch {
              case NonFatal(t) =>
                responseState.fail(t)
                resolver.reject(t)
            }
          }
        }
      })
    }

  private final class ResponseState {
    @volatile private var lifecycle: ClientStreamLifecycle = _
    private val body = GearsByteStream.bounded(
      capacityChunks = 64,
      onAbandoned = _ => abort("response body abandoned")
    )
    private var resolverRef: Future.Resolver[GearsHttp2StreamingResponse] = _
    private var responseHeaders = Vector.empty[(String, String)]
    private var resolved = false

    val future: Future[GearsHttp2StreamingResponse] =
      Future.withResolver { resolver =>
        resolverRef = resolver
        resolver.onCancel(() => abort("response await cancelled"))
      }

    def bind(nextLifecycle: ClientStreamLifecycle): Unit =
      lifecycle = nextLifecycle

    val handler: Http2ClientStreamHandler = new Http2ClientStreamHandler {
      override def onHeaders(
          stream: Http2ClientStream,
          headers: Vector[(String, String)],
          endStream: Boolean
      ): Unit = {
        responseHeaders = headers
        resolve()
        if (endStream) body.finish()
      }

      override def onData(
          stream: Http2ClientStream,
          data: Array[Byte],
          endStream: Boolean,
          releaseWindow: () => Unit
      ): Boolean =
        if (data.isEmpty) {
          if (endStream) body.finish()
          true
        } else {
          val accepted = body.offer(data, releaseWindow)
          if (accepted && endStream) body.finish()
          accepted
        }

      override def onComplete(stream: Http2ClientStream): Unit = {
        resolve()
        body.finish()
      }

      override def onError(
          stream: Http2ClientStream,
          cause: Throwable
      ): Unit =
        fail(cause)
    }

    def fail(cause: Throwable): Unit = {
      body.fail(cause)
      if (!resolved) {
        resolved = true
        resolverRef.reject(cause)
      }
    }

    private def resolve(): Unit =
      if (!resolved) {
        resolved = true
        resolverRef.resolve(GearsHttp2StreamingResponse(responseHeaders, body))
      }

    private def abort(message: String): Unit = {
      val current = lifecycle
      if (current != null)
        current.abort(message)
    }
  }
}

object GearsHttp2Client {
  def connect(
      reactor: Reactor,
      host: String,
      port: Int
  )(using Async): GearsHttp2Client =
    connectFuture(reactor, host, port).await

  private def connectFuture(
      reactor: Reactor,
      host: String,
      port: Int
  ): Future[GearsHttp2Client] =
    Future.withResolver { resolver =>
      try {
        var completed = false
        var current: GearsHttp2Client = null

        resolver.onCancel(() => {
          if (current != null)
            current.close()
        })

        def resolve(client: Http2Client): Unit =
          if (!completed) {
            completed = true
            resolver.resolve(current)
          }

        def reject(cause: Throwable): Unit =
          if (!completed) {
            completed = true
            resolver.reject(cause)
          }

        val rawClient = Http2Client.connect(
          reactor,
          host,
          port,
          new Http2ClientLifecycleHandler {
            override def onReady(client: Http2Client): Unit =
              resolve(client)

            override def onError(
                client: Http2Client,
                cause: Throwable
            ): Unit =
              reject(cause)
          }
        )
        current = new GearsHttp2Client(rawClient)
      } catch {
        case NonFatal(t) =>
          resolver.reject(t)
      }
    }
}
