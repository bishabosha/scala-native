package scala.scalanative.sandbox.streamio.gears

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.util.ArrayDeque

import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import scala.scalanative.sandbox.streamio.http2._
import scala.scalanative.sandbox.streamio.transport.Reactor

import gears.async.{
    Async,
    ChannelClosedException,
    Future,
    ReadableChannel,
    UnboundedChannel
  }

private object ReactorSubmission {
  def future(submit: Runnable => Unit)(body: => Unit): Future[Unit] =
    Future.withResolver { resolver =>
      submit(new Runnable {
        override def run(): Unit =
          try {
            body
            resolver.resolve(())
          } catch {
            case NonFatal(t) =>
              resolver.reject(t)
          }
      })
    }

  def run(submit: Runnable => Unit)(body: => Unit)(using Async): Unit =
    future(submit)(body).await
}

final class GearsByteStream private[gears] (
    capacityChunks: Int,
    onChunkConsumed: () => Unit
) {
  import GearsByteStream._

  private val chunks = new ArrayDeque[Chunk]()
  private val waiters = new ArrayDeque[Future.Promise[StreamEvent]]()
  private var failed: Throwable = _
  private var finished = false

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

  private[gears] def offer(
      bytes: Array[Byte],
      releaseWindow: () => Unit = () => ()
  ): Boolean =
    synchronized {
      if (failed != null || finished) false
      else if (!waiters.isEmpty) {
        waiters.removeFirst().complete(Success(Chunk(bytes, releaseWindow)))
        true
      } else if (chunks.size() < capacityChunks) {
        chunks.addLast(Chunk(bytes, releaseWindow))
        true
      } else false
    }

  private[gears] def finish(): Unit = {
    val toResolve = synchronized {
      if (finished || failed != null) Nil
      else {
        finished = true
        if (chunks.isEmpty) drainWaiters(End) else Nil
      }
    }
    toResolve.foreach(_.complete(Success(End)))
  }

  private[gears] def fail(cause: Throwable): Unit = {
    val toResolve = synchronized {
      if (failed != null || finished) Nil
      else {
        failed = cause
        if (chunks.isEmpty) drainWaiters(Failed(cause)) else Nil
      }
    }
    toResolve.foreach(_.complete(Success(Failed(cause))))
  }

  private def nextEvent()(using Async): StreamEvent = {
    val pending = synchronized {
      if (!chunks.isEmpty) Left(chunks.removeFirst())
      else if (failed != null) Left(Failed(failed))
      else if (finished) Left(End)
      else {
        val promise = Future.Promise[StreamEvent]()
        waiters.addLast(promise)
        Right(promise.asFuture)
      }
    }

    pending match {
      case Left(event)   => event
      case Right(future) => future.await
    }
  }

  private def drainWaiters(event: StreamEvent): List[Future.Promise[StreamEvent]] = {
    val drained = List.newBuilder[Future.Promise[StreamEvent]]
    while (!waiters.isEmpty) drained += waiters.removeFirst()
    drained.result()
  }
}

private object GearsByteStream {
  sealed trait StreamEvent
  final case class Chunk(bytes: Array[Byte], releaseWindow: () => Unit)
      extends StreamEvent
  case object End extends StreamEvent
  final case class Failed(cause: Throwable) extends StreamEvent

  def bounded(
      capacityChunks: Int = 32,
      onChunkConsumed: () => Unit = () => ()
  ): GearsByteStream =
    new GearsByteStream(math.max(1, capacityChunks), onChunkConsumed)
}

final case class GearsHttp2Request(
    stream: Http2Stream,
    headers: Vector[(String, String)],
    body: GearsByteStream,
    response: GearsHttp2Response
) {
  def header(name: String): Option[String] =
    headers.collectFirst { case (`name`, value) => value }

  def method: Option[String] = header(":method")
  def path: Option[String] = header(":path")
  def authority: Option[String] = header(":authority")
  def bodyBytes(using Async): Array[Byte] = body.bufferAll
}

final class GearsHttp2Response private[gears] (stream: Http2Stream) {
  def sendHeaders(
      status: Int,
      headers: Seq[(String, String)] = Nil,
      endStream: Boolean = false
  )(using Async): Unit =
    ReactorSubmission.run(stream.submit) {
      stream.sendResponseHeaders(status, headers, endStream)
    }

  def sendData(
      bytes: Array[Byte],
      endStream: Boolean = false
  )(using Async): Unit =
    ReactorSubmission.run(stream.submit) {
      stream.sendData(bytes, endStream)
    }

  def writeUtf8(
      value: String,
      endStream: Boolean = false
  )(using Async): Unit =
    ReactorSubmission.run(stream.submit) {
      stream.writeUtf8(value, endStream)
    }
}

trait GearsHttp2Handler {
  def onRequest(request: GearsHttp2Request)(using Async): Unit

  def onFailure(request: GearsHttp2Request, cause: Throwable)(using Async): Unit = {
    request.response.sendHeaders(500, Seq("content-type" -> "text/plain"))
    request.response.writeUtf8(
      Option(cause.getMessage).getOrElse("internal error"),
      endStream = true
    )
  }
}

final class GearsHttp2Listener private[gears] (
    val server: Http2Server,
    private val requestsChannel: UnboundedChannel[GearsHttp2Request]
) extends AutoCloseable {
  def port: Int = server.port
  def requests: ReadableChannel[GearsHttp2Request] = requestsChannel.asReadable

  override def close(): Unit = {
    requestsChannel.close()
    server.close()
  }
}

object GearsHttp2Server {
  def listen(
      reactor: Reactor,
      port: Int,
      host: String = "0.0.0.0"
  ): GearsHttp2Listener = {
    val requests = UnboundedChannel[GearsHttp2Request]()
    val server = Http2Server.bind(
      reactor,
      port,
      new Adapter(requests),
      host
    )
    new GearsHttp2Listener(server, requests)
  }

  def serve(
      listener: GearsHttp2Listener,
      handler: GearsHttp2Handler
  )(using Async.Spawn): Unit =
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
      host: String = "0.0.0.0"
  )(using Async.Spawn): GearsHttp2Listener = {
    val listener = listen(reactor, port, host)
    serve(listener, handler)
    listener
  }

  private final class Adapter(requests: UnboundedChannel[GearsHttp2Request])
      extends Http2Handler {
    override def onRequest(stream: Http2Stream): Unit = {
      val bodyStream =
        GearsByteStream.bounded(
          capacityChunks = 32,
          onChunkConsumed = () => stream.requestBody.requestDrain()
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
        GearsHttp2Request(
          stream = stream,
          headers = stream.requestHeaders,
          body = bodyStream,
          response = new GearsHttp2Response(stream)
        )
      try requests.sendImmediately(request)
      catch {
        case _: ChannelClosedException =>
          bodyStream.finish()
          stream.sendResponseHeaders(
            503,
            Seq("content-type" -> "text/plain")
          )
          stream.writeUtf8("server unavailable", endStream = true)
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
) {
  def header(name: String): Option[String] =
    headers.collectFirst { case (`name`, value) => value }
}

final class GearsHttp2RequestBody private[gears] (
    stream: Http2ClientStream
) {
  def sendData(
      bytes: Array[Byte],
      endStream: Boolean = false
  )(using Async): Unit =
    ReactorSubmission.run(stream.submit) {
      stream.sendData(bytes, endStream)
    }

  def writeUtf8(
      value: String,
      endStream: Boolean = false
  )(using Async): Unit =
    sendData(value.getBytes(StandardCharsets.UTF_8), endStream)

  def finish()(using Async): Unit =
    sendData(Array.empty[Byte], endStream = true)
}

final class GearsHttp2ClientExchange private[gears] (
    val requestBody: GearsHttp2RequestBody,
    private val responseFuture: Future[GearsHttp2StreamingResponse]
) {
  def awaitResponse(using Async): GearsHttp2StreamingResponse =
    responseFuture.await
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
  }

  def request(
      headers: Seq[(String, String)],
      dataFrames: Seq[Array[Byte]] = Nil
  )(using Async): GearsHttp2ClientResponse = {
    val response = streamRequest(headers, dataFrames)
    GearsHttp2ClientResponse(response.headers, response.body.bufferAll)
  }

  override def close(): Unit =
    client.close()

  private def openRequestFuture(
      headers: Seq[(String, String)]
  ): Future[GearsHttp2ClientExchange] =
    Future.withResolver { resolver =>
      client.submit(new Runnable {
        override def run(): Unit = {
          val responseState = new ResponseState
          try {
            val stream = client.openStream(
              headers,
              responseState.handler,
              endStream = false
            )
            resolver.resolve(
              new GearsHttp2ClientExchange(
                requestBody = new GearsHttp2RequestBody(stream),
                responseFuture = responseState.future
              )
            )
          } catch {
            case NonFatal(t) =>
              responseState.fail(t)
              resolver.reject(t)
          }
        }
      })
    }

  private final class ResponseState {
    private val body = GearsByteStream.bounded(capacityChunks = 64)
    private val promise = Future.Promise[GearsHttp2StreamingResponse]()
    private var responseHeaders = Vector.empty[(String, String)]
    private var resolved = false

    val future: Future[GearsHttp2StreamingResponse] = promise.asFuture

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
        promise.complete(Failure(cause))
      }
    }

    private def resolve(): Unit =
      if (!resolved) {
        resolved = true
        promise.complete(
          Success(GearsHttp2StreamingResponse(responseHeaders, body))
        )
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

        def resolve(client: Http2Client): Unit =
          if (!completed) {
            completed = true
            resolver.resolve(new GearsHttp2Client(client))
          }

        def reject(cause: Throwable): Unit =
          if (!completed) {
            completed = true
            resolver.reject(cause)
          }

        Http2Client.connect(
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
      } catch {
        case NonFatal(t) =>
          resolver.reject(t)
      }
    }
}
