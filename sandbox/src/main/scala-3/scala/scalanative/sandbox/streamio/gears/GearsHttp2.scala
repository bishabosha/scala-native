package scala.scalanative.sandbox.streamio.gears

import java.io.ByteArrayOutputStream
import java.io.IOException

import scala.collection.mutable
import scala.util.Failure
import scala.util.Success
import scala.util.control.NonFatal

import gears.async.{Async, AsyncSupport, Future}

import scala.scalanative.sandbox.streamio.http2._
import scala.scalanative.sandbox.streamio.transport.Reactor

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
      .onComplete(gears.async.Listener { case (_, _) =>
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
        new RequestState(request, promise, new ByteArrayOutputStream(), endStream)
      requests(stream.id) = state
      if (endStream) {
        promise.complete(Success(Array.empty[Byte]))
      }

      scheduler.execute(new Runnable {
        override def run(): Unit =
          try Async.blocking {
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
            Failure(new IOException("request body stream closed before endStream"))
          )
      }
  }
}
