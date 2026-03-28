package scala.scalanative.sandbox.streamio.gears

import java.io.{ByteArrayOutputStream, IOException}
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import scala.scalanative.sandbox.streamio.http2._
import scala.scalanative.sandbox.streamio.transport.Reactor

import gears.async.{Async, AsyncSupport, Future}

final case class GearsHttp2Request(
    stream: Http2Stream,
    headers: Vector[(String, String)],
    body: Future[Array[Byte]],
    response: GearsHttp2Response
) {
  def header(name: String): Option[String] =
    headers.collectFirst { case (`name`, value) => value }

  def method: Option[String] = header(":method")
  def path: Option[String] = header(":path")
  def authority: Option[String] = header(":authority")
}

final class GearsHttp2Response private[gears] (stream: Http2Stream) {
  def sendHeaders(
      status: Int,
      headers: Seq[(String, String)] = Nil,
      endStream: Boolean = false
  ): Future[Unit] =
    onReactor {
      stream.sendResponseHeaders(status, headers, endStream)
    }

  def sendData(
      bytes: Array[Byte],
      endStream: Boolean = false
  ): Future[Unit] =
    onReactor {
      stream.sendData(bytes, endStream)
    }

  def writeUtf8(
      value: String,
      endStream: Boolean = false
  ): Future[Unit] =
    onReactor {
      stream.writeUtf8(value, endStream)
    }

  private def onReactor(body: => Unit): Future[Unit] =
    Future.withResolver { resolver =>
      stream.submit(new Runnable {
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
}

trait GearsHttp2Handler {
  def onRequest(request: GearsHttp2Request)(using Async): Unit

  def onFailure(request: GearsHttp2Request, cause: Throwable): Unit =
    request.response
      .sendHeaders(500, Seq("content-type" -> "text/plain"))
      .onComplete(gears.async.Listener {
        case (_, _) =>
          request.response.writeUtf8(
            Option(cause.getMessage).getOrElse("internal error"),
            endStream = true
          )
          ()
      })
}

object GearsHttp2Server {
  def bind[S <: AsyncSupport](
      reactor: Reactor,
      port: Int,
      handler: GearsHttp2Handler,
      host: String = "0.0.0.0"
  )(using
      support: S,
      scheduler: support.Scheduler
  ): Http2Server =
    Http2Server.bind(
      reactor,
      port,
      new Adapter(handler, support, scheduler),
      host
    )

  private final class Adapter[S <: AsyncSupport](
      handler: GearsHttp2Handler,
      support: S,
      scheduler: support.Scheduler
  ) extends Http2Handler {
    private final class RequestState(
        val request: GearsHttp2Request,
        val promise: Future.Promise[Array[Byte]],
        val bodyBuffer: ByteArrayOutputStream,
        var bodyComplete: Boolean
    )

    private val requests = mutable.HashMap.empty[Int, RequestState]

    override def onHeaders(
        stream: Http2Stream,
        headers: Vector[(String, String)],
        endStream: Boolean
    ): Unit = {
      val promise = Future.Promise[Array[Byte]]()
      val request =
        GearsHttp2Request(
          stream = stream,
          headers = headers,
          body = promise.asFuture,
          response = new GearsHttp2Response(stream)
        )
      val state =
        new RequestState(
          request,
          promise,
          new ByteArrayOutputStream(),
          endStream
        )
      requests(stream.id) = state
      if (endStream) {
        promise.complete(Success(Array.empty[Byte]))
      }

      scheduler.execute(new Runnable {
        override def run(): Unit =
          try
            Async.blocking {
              handler.onRequest(request)
            }(using support, scheduler)
          catch {
            case NonFatal(t) =>
              handler.onFailure(request, t)
          }
      })
    }

    override def onData(
        stream: Http2Stream,
        data: Array[Byte],
        endStream: Boolean
    ): Unit =
      requests.get(stream.id).foreach { state =>
        if (data.nonEmpty)
          state.bodyBuffer.write(data, 0, data.length)
        if (endStream) {
          state.bodyComplete = true
          state.promise.complete(Success(state.bodyBuffer.toByteArray))
        }
      }

    override def onStreamClosed(stream: Http2Stream): Unit =
      requests.remove(stream.id).foreach { state =>
        if (!state.bodyComplete)
          state.promise.complete(
            Failure(
              new IOException("request body stream closed before endStream")
            )
          )
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

final class GearsByteStream private[gears] () {
  private val chunks = new java.util.ArrayDeque[Option[Array[Byte]]]()
  private val waiters =
    new java.util.ArrayDeque[Future.Promise[Option[Array[Byte]]]]()
  private var closed = false
  private var failure: Throwable = _

  def read(): Future[Option[Array[Byte]]] = synchronized {
    if (failure != null) failedFuture(failure)
    else if (!chunks.isEmpty) successfulFuture(chunks.removeFirst())
    else if (closed) successfulFuture(None)
    else {
      val promise = Future.Promise[Option[Array[Byte]]]()
      waiters.addLast(promise)
      promise.asFuture
    }
  }

  def bufferAll(using Async): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    var done = false
    while (!done) {
      read().await match {
        case Some(bytes) =>
          out.write(bytes, 0, bytes.length)
        case None =>
          done = true
      }
    }
    out.toByteArray
  }

  private[gears] def push(bytes: Array[Byte]): Unit = synchronized {
    if (!closed && failure == null) {
      if (!waiters.isEmpty)
        waiters.removeFirst().complete(Success(Some(bytes)))
      else chunks.addLast(Some(bytes))
    }
  }

  private[gears] def finish(): Unit = synchronized {
    if (!closed && failure == null) {
      closed = true
      if (!waiters.isEmpty) {
        val completed = new java.util.ArrayList[Future.Promise[Option[Array[Byte]]]](
          waiters.size()
        )
        while (!waiters.isEmpty)
          completed.add(waiters.removeFirst())
        var idx = 0
        while (idx < completed.size()) {
          completed.get(idx).complete(Success(None))
          idx += 1
        }
      }
    }
  }

  private[gears] def fail(cause: Throwable): Unit = synchronized {
    if (failure == null) {
      failure = cause
      val completed = new java.util.ArrayList[Future.Promise[Option[Array[Byte]]]](
        waiters.size()
      )
      while (!waiters.isEmpty)
        completed.add(waiters.removeFirst())
      var idx = 0
      while (idx < completed.size()) {
        completed.get(idx).complete(Failure(cause))
        idx += 1
      }
    }
  }

  private def successfulFuture(
      value: Option[Array[Byte]]
  ): Future[Option[Array[Byte]]] =
    Future.withResolver(_.resolve(value))

  private def failedFuture(
      cause: Throwable
  ): Future[Option[Array[Byte]]] =
    Future.withResolver(_.reject(cause))
}

final case class GearsHttp2StreamingResponse(
    headers: Vector[(String, String)],
    body: GearsByteStream
) {
  def header(name: String): Option[String] =
    headers.collectFirst { case (`name`, value) => value }
}

final class GearsHttp2Client private[gears] (client: Http2Client)
    extends AutoCloseable {
  def streamRequest(
      headers: Seq[(String, String)],
      dataFrames: Seq[Array[Byte]] = Nil
  ): Future[GearsHttp2StreamingResponse] =
    Future.withResolver { resolver =>
      client.submit(new Runnable {
        override def run(): Unit =
          try {
            val body = new GearsByteStream()
            var responseHeaders = Vector.empty[(String, String)]
            var resolved = false

            def resolve(): Unit =
              if (!resolved) {
                resolved = true
                resolver.resolve(
                  GearsHttp2StreamingResponse(responseHeaders, body)
                )
              }

            def reject(cause: Throwable): Unit = {
              body.fail(cause)
              if (!resolved) {
                resolved = true
                resolver.reject(cause)
              }
            }

            val stream = client.openStream(
              headers,
              new Http2ClientStreamHandler {
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
                    endStream: Boolean
                ): Unit = {
                  if (data.nonEmpty)
                    body.push(data)
                  if (endStream) body.finish()
                }

                override def onComplete(stream: Http2ClientStream): Unit = {
                  resolve()
                  body.finish()
                }

                override def onError(
                    stream: Http2ClientStream,
                    cause: Throwable
                ): Unit =
                  reject(cause)
              },
              endStream = dataFrames.isEmpty
            )

            dataFrames.zipWithIndex.foreach {
              case (bytes, idx) =>
                stream.sendData(
                  bytes,
                  endStream = idx == dataFrames.length - 1
                )
            }
          } catch {
            case NonFatal(t) =>
              resolver.reject(t)
          }
      })
    }

  def request(
      headers: Seq[(String, String)],
      dataFrames: Seq[Array[Byte]] = Nil
  ): Future[GearsHttp2ClientResponse] =
    Future.withResolver { resolver =>
      client.submit(new Runnable {
        override def run(): Unit =
          try {
            val body = new ByteArrayOutputStream()
            var responseHeaders = Vector.empty[(String, String)]
            var completed = false

            def resolve(): Unit =
              if (!completed) {
                completed = true
                resolver.resolve(
                  GearsHttp2ClientResponse(responseHeaders, body.toByteArray)
                )
              }

            def reject(cause: Throwable): Unit =
              if (!completed) {
                completed = true
                resolver.reject(cause)
              }

            val stream = client.openStream(
              headers,
              new Http2ClientStreamHandler {
                override def onHeaders(
                    stream: Http2ClientStream,
                    headers: Vector[(String, String)],
                    endStream: Boolean
                ): Unit = {
                  responseHeaders = headers
                  if (endStream) resolve()
                }

                override def onData(
                    stream: Http2ClientStream,
                    data: Array[Byte],
                    endStream: Boolean
                ): Unit = {
                  if (data.nonEmpty)
                    body.write(data, 0, data.length)
                  if (endStream) resolve()
                }

                override def onComplete(stream: Http2ClientStream): Unit =
                  resolve()

                override def onError(
                    stream: Http2ClientStream,
                    cause: Throwable
                ): Unit =
                  reject(cause)
              },
              endStream = dataFrames.isEmpty
            )

            dataFrames.zipWithIndex.foreach {
              case (bytes, idx) =>
                stream.sendData(
                  bytes,
                  endStream = idx == dataFrames.length - 1
                )
            }
          } catch {
            case NonFatal(t) =>
              resolver.reject(t)
          }
      })
    }

  override def close(): Unit =
    client.close()
}

object GearsHttp2Client {
  def connect(
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
