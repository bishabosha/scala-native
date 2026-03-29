package scala.scalanative.sandbox.streamio.gears

import java.io.{ByteArrayOutputStream, IOException}
import java.nio.charset.StandardCharsets
import java.util.concurrent.CancellationException

import scala.collection.mutable
import scala.compiletime.uninitialized
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

import scala.scalanative.sandbox.streamio.http2._
import scala.scalanative.sandbox.streamio.transport.Reactor

import gears.async.{
  Async,
  AsyncSupport,
  Cancellable,
  CompletionGroup,
  Future,
  Listener
}

private object GearsAsyncRuntime {
  def execute[S <: AsyncSupport](
      body: Async ?=> Unit
  )(onFailure: Throwable => Unit)(using
      support: S,
      scheduler: support.Scheduler
  ): Unit =
    scheduler.execute(new Runnable {
      override def run(): Unit =
        support.boundary[Unit] {
          val label = summon[support.Label[Unit]]
          val async =
            new ScheduledAsync[S](CompletionGroup.Unlinked)(using
              support,
              scheduler,
              label
            )
          try body(using async)
          catch {
            case NonFatal(t) =>
              onFailure(t)
          }
        }
    })

  private final class ScheduledAsync[S <: AsyncSupport](val group: CompletionGroup)(
      using
      support0: S,
      scheduler0: support0.Scheduler,
      label0: support0.Label[Unit]
  ) extends Async(using support0, scheduler0) {
    override def await[T](src: Async.Source[T]): T = {
      final class CancelSuspension extends Cancellable {
        var suspension: support.Suspension[Try[T], Unit] = uninitialized
        var listener: Listener[T] = uninitialized
        private var completed = false

        def markCompleted(): Boolean = synchronized {
          val completedBefore = completed
          completed = true
          completedBefore
        }

        override def cancel(): Unit = {
          val completedBefore = markCompleted()
          if (!completedBefore) {
            src.dropListener(listener)
            resumeAsync(
              suspension,
              Failure(new CancellationException())
            )
          }
        }
      }

      if (group.isCancelled) throw new CancellationException()

      src.poll().getOrElse {
        val cancellable = new CancelSuspension
        val result = support.suspend[Try[T], Unit](
          { suspension =>
            val listener = Listener.acceptingListener[T] { (item, _) =>
              val completedBefore = cancellable.markCompleted()
              if (!completedBefore)
                resumeAsync(suspension, Success(item))
            }
            cancellable.suspension = suspension
            cancellable.listener = listener
            cancellable.link(group)
            src.onComplete(listener)
          }
        )(using label0.asInstanceOf[support.Label[Unit]])
        cancellable.unlink()
        result.get
      }
    }

    override def withGroup(group: CompletionGroup): Async =
      new ScheduledAsync[S](group)(using support0, scheduler0, label0)

    private def resumeAsync[U](
        suspension: support.Suspension[U, Unit],
        value: U
    ): Unit =
      scheduler.execute(new Runnable {
        override def run(): Unit =
          suspension.resume(value)
      })
  }
}

private object ReactorSubmission {
  def apply(submit: Runnable => Unit)(body: => Unit): Future[Unit] =
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
        val completed =
          new java.util.ArrayList[Future.Promise[Option[Array[Byte]]]](
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
      val completed =
        new java.util.ArrayList[Future.Promise[Option[Array[Byte]]]](
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
    ReactorSubmission(stream.submit)(body)
}

trait GearsHttp2Handler {
  def onRequest(request: GearsHttp2Request)(using Async): Unit

  def onFailure(request: GearsHttp2Request, cause: Throwable): Unit =
    request.response
      .sendHeaders(500, Seq("content-type" -> "text/plain"))
      .onComplete(Listener {
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
      new Adapter(handler),
      host
    )

  private final class Adapter[S <: AsyncSupport](handler: GearsHttp2Handler)(
      using
      support: S,
      scheduler: support.Scheduler
  ) extends Http2Handler {
    private final class RequestState(
        val request: GearsHttp2Request,
        val bodyStream: GearsByteStream,
        var bodyComplete: Boolean
    )

    private val requests = mutable.HashMap.empty[Int, RequestState]

    override def onHeaders(
        stream: Http2Stream,
        headers: Vector[(String, String)],
        endStream: Boolean
    ): Unit = {
      val bodyStream = new GearsByteStream()
      if (endStream) bodyStream.finish()
      val request =
        GearsHttp2Request(
          stream = stream,
          headers = headers,
          body = bodyStream,
          response = new GearsHttp2Response(stream)
        )
      requests(stream.id) =
        new RequestState(request, bodyStream, bodyComplete = endStream)

      GearsAsyncRuntime.execute[S] {
        handler.onRequest(request)
      } { cause =>
        handler.onFailure(request, cause)
      }
    }

    override def onData(
        stream: Http2Stream,
        data: Array[Byte],
        endStream: Boolean
    ): Unit =
      requests.get(stream.id).foreach { state =>
        if (data.nonEmpty)
          state.bodyStream.push(data)
        if (endStream) {
          state.bodyComplete = true
          state.bodyStream.finish()
        }
      }

    override def onStreamClosed(stream: Http2Stream): Unit =
      requests.remove(stream.id).foreach { state =>
        if (!state.bodyComplete)
          state.bodyStream.fail(
            new IOException("request body stream closed before endStream")
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
  ): Future[Unit] =
    onReactor {
      stream.sendData(bytes, endStream)
    }

  def writeUtf8(
      value: String,
      endStream: Boolean = false
  ): Future[Unit] =
    sendData(value.getBytes(StandardCharsets.UTF_8), endStream)

  def finish(): Future[Unit] =
    sendData(Array.empty[Byte], endStream = true)

  private def onReactor(body: => Unit): Future[Unit] =
    ReactorSubmission(stream.submit)(body)
}

final case class GearsHttp2ClientExchange(
    requestBody: GearsHttp2RequestBody,
    response: Future[GearsHttp2StreamingResponse]
)

final class GearsHttp2Client private[gears] (client: Http2Client)
    extends AutoCloseable {
  def openRequest(
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
              GearsHttp2ClientExchange(
                requestBody = new GearsHttp2RequestBody(stream),
                response = responseState.future
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

  def streamRequest(
      headers: Seq[(String, String)],
      dataFrames: Seq[Array[Byte]] = Nil
  ): Future[GearsHttp2StreamingResponse] =
    Future.withResolver { resolver =>
      client.submit(new Runnable {
        override def run(): Unit = {
          val responseState = new ResponseState
          try {
            val stream = client.openStream(
              headers,
              responseState.handler,
              endStream = dataFrames.isEmpty
            )

            dataFrames.zipWithIndex.foreach {
              case (bytes, idx) =>
                stream.sendData(
                  bytes,
                  endStream = idx == dataFrames.length - 1
                )
            }

            responseState.future.onComplete(Listener {
              case (result, _) =>
                resolver.complete(result)
            })
          } catch {
            case NonFatal(t) =>
              responseState.fail(t)
              resolver.reject(t)
          }
        }
      })
    }

  def request(
      headers: Seq[(String, String)],
      dataFrames: Seq[Array[Byte]] = Nil
  ): Future[GearsHttp2ClientResponse] =
    Future.withResolver { resolver =>
      client.submit(new Runnable {
        override def run(): Unit = {
          val responseState = new BufferedResponseState(resolver)
          try {
            val stream = client.openStream(
              headers,
              responseState.handler,
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
        }
      })
    }

  override def close(): Unit =
    client.close()

  private final class ResponseState {
    private val body = new GearsByteStream()
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

  private final class BufferedResponseState(
      resolver: Future.Resolver[GearsHttp2ClientResponse]
  ) {
    private val body = new ByteArrayOutputStream()
    private var responseHeaders = Vector.empty[(String, String)]
    private var completed = false

    val handler: Http2ClientStreamHandler = new Http2ClientStreamHandler {
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
    }

    private def resolve(): Unit =
      if (!completed) {
        completed = true
        resolver.resolve(GearsHttp2ClientResponse(responseHeaders, body.toByteArray))
      }

    private def reject(cause: Throwable): Unit =
      if (!completed) {
        completed = true
        resolver.reject(cause)
      }
  }
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
