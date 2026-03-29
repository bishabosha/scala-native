package scala.scalanative.sandbox.streamio.gears

import java.io.{ByteArrayOutputStream, IOException}
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import scala.scalanative.sandbox.streamio.http2._
import scala.scalanative.sandbox.streamio.transport.Reactor

import gears.async.{Async, Future, Listener}

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

final class GearsByteStream private[gears] () {
  private val chunks = new java.util.ArrayDeque[Option[Array[Byte]]]()
  private val waiters =
    new java.util.ArrayDeque[Future.Promise[Option[Array[Byte]]]]()
  private var closed = false
  private var failure: Throwable = _

  def read(using Async): Option[Array[Byte]] =
    readFuture.await

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

  private[gears] def readFuture: Future[Option[Array[Byte]]] = synchronized {
    if (failure != null) failedFuture(failure)
    else if (!chunks.isEmpty) successfulFuture(chunks.removeFirst())
    else if (closed) successfulFuture(None)
    else {
      val promise = Future.Promise[Option[Array[Byte]]]()
      waiters.addLast(promise)
      promise.asFuture
    }
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

object GearsHttp2Server {
  def bind(
      reactor: Reactor,
      port: Int,
      handler: GearsHttp2Handler,
      host: String = "0.0.0.0"
  )(using Async.Spawn): Http2Server =
    Http2Server.bind(
      reactor,
      port,
      new Adapter(handler),
      host
    )

  private final class Adapter(handler: GearsHttp2Handler)(using Async.Spawn)
  extends Http2Handler {
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

      Future {
        handler.onRequest(request)
      }.onComplete(Listener {
        case (Failure(cause), _) =>
          Future {
            handler.onFailure(request, cause)
          }
          ()
        case _ =>
          ()
      })
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
