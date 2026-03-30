package scala.scalanative.sandbox.streamio.gears

import scala.util.Success
import scala.concurrent.duration.*

import munit.FunSuite

import scala.scalanative.meta.LinktimeInfo
import scala.scalanative.sandbox.streamio.http2.{
  Http2Handler,
  Http2RequestBodyHandler,
  Http2Server,
  Http2Stream
}

import gears.async.{Async, Future}
import gears.async.default.{DefaultSupport, given}

class GearsHttp2LimitsSuite extends FunSuite {
  override def munitIgnore: Boolean = LinktimeInfo.isWindows
  override val munitTimeout: Duration = 90.seconds

  test("gears server refuses streams above maxConcurrentStreams") {
    runAsync {
      val release = Future.Promise[Unit]()
      val reactor = GearsReactor.polling[DefaultSupport.type]()(using
        DefaultSupport,
        DefaultSupport
      )
      val listener =
        GearsHttp2Server.bind(
          reactor,
          port = 0,
          handler = new GearsHttp2Handler {
            override def onRequest(
                request: GearsHttp2Request
            )(using Async): Unit = {
              request.response.sendHeaders(200, Seq("content-type" -> "text/plain"))
              if (request.path.contains("/hold")) {
                release.asFuture.await
                request.response.sendData(Array.emptyByteArray, endStream = true)
              } else request.response.sendData(Array.emptyByteArray, endStream = true)
            }
          },
          maxConcurrentStreams = 1
        )

      try {
        val client = GearsHttp2Client.connect(reactor, "127.0.0.1", listener.port)
        try {
          val first = client.openRequest(requestHeaders(listener.port, "/hold"))
          first.requestBody.finish()
          val firstResponse = first.awaitResponse
          assertEquals(firstResponse.header(":status"), Some("200"))

          val second = client.openRequest(requestHeaders(listener.port, "/hold"))
          second.requestBody.finish()
          val failure = intercept[Throwable] {
            second.awaitResponse
          }
          assert(failure.getMessage.contains("RST_STREAM error=7"))

          release.complete(Success(()))
          assertEquals(firstResponse.body.bufferAll.length, 0)
        } finally client.close()
      } finally {
        listener.close()
        reactor.close()
      }
    }
  }

  test("gears listener refuses requests when bounded queue is full") {
    runAsync {
      val reactor = GearsReactor.polling[DefaultSupport.type]()(using
        DefaultSupport,
        DefaultSupport
      )
      val listener =
        GearsHttp2Server.listen(
          reactor,
          port = 0,
          requestQueueCapacity = 1
        )

      try {
        val client = GearsHttp2Client.connect(reactor, "127.0.0.1", listener.port)
        try {
          val first = client.openRequest(requestHeaders(listener.port, "/queued"))
          first.requestBody.finish()

          val second = client.openRequest(requestHeaders(listener.port, "/queued"))
          second.requestBody.finish()

          val failure = intercept[Throwable] {
            second.awaitResponse
          }
          assert(
            failure.getMessage.contains("RST_STREAM error=7") ||
              failure.getMessage.contains("server request queue full")
          )

          listener.requests.read() match {
            case Right(request) =>
              assertEquals(request.path, Some("/queued"))
            case Left(closed) =>
              fail("expected queued request but listener was closed: " + closed)
          }
        } finally client.close()
      } finally {
        listener.close()
        reactor.close()
      }
    }
  }

  test("closing a streaming response resets the server stream") {
    runAsync {
      val streamClosed = Future.Promise[Unit]()
      val reactor = GearsReactor.polling[DefaultSupport.type]()(using
        DefaultSupport,
        DefaultSupport
      )
      val server =
        Http2Server.bind(
          reactor,
          port = 0,
          handler = new Http2Handler {
            override def onRequest(stream: Http2Stream): Unit =
              stream.sendResponseHeaders(
                200,
                Seq("content-type" -> "text/plain"),
                endStream = false
              )

            override def onStreamClosed(stream: Http2Stream): Unit =
              streamClosed.complete(Success(()))
          }
        )

      try {
        val client = GearsHttp2Client.connect(reactor, "127.0.0.1", server.port)
        try {
          val response = client.streamRequest(requestHeaders(server.port, "/stream"))
          try {
            assertEquals(response.header(":status"), Some("200"))
            response.close()
            streamClosed.asFuture.await
          } finally response.close()
        } finally client.close()
      } finally {
        server.close()
        reactor.close()
      }
    }
  }

  test("closing an in-flight exchange fails the server request body") {
    runAsync {
      val requestStarted = Future.Promise[Unit]()
      val requestFailed = Future.Promise[String]()
      val reactor = GearsReactor.polling[DefaultSupport.type]()(using
        DefaultSupport,
        DefaultSupport
      )
      val server =
        Http2Server.bind(
          reactor,
          port = 0,
          handler = new Http2Handler {
            override def onRequest(stream: Http2Stream): Unit = {
              stream.requestBody.subscribe(new Http2RequestBodyHandler {
                override def onData(
                    stream: Http2Stream,
                    data: Array[Byte],
                    releaseWindow: () => Unit
                ): Boolean = {
                  releaseWindow()
                  true
                }

                override def onFailure(
                    stream: Http2Stream,
                    cause: Throwable
                ): Unit =
                  requestFailed.complete(
                    Success(
                      Option(cause.getMessage).getOrElse(
                        cause.getClass.getName
                      )
                    )
                  )
              })
              requestStarted.complete(Success(()))
            }
          }
        )

      try {
        val client = GearsHttp2Client.connect(reactor, "127.0.0.1", server.port)
        try {
          val exchange = client.openRequest(postHeaders(server.port, "/upload"))
          try {
            requestStarted.asFuture.await
            exchange.close()
            val message = requestFailed.asFuture.await
            assert(message.contains("reset by peer"))
          } finally exchange.close()
        } finally client.close()
      } finally {
        server.close()
        reactor.close()
      }
    }
  }

  test("gears server cleans up incomplete request lifecycle when handler returns") {
    runAsync {
      val reactor = GearsReactor.polling[DefaultSupport.type]()(using
        DefaultSupport,
        DefaultSupport
      )
      val listener =
        GearsHttp2Server.bind(
          reactor,
          port = 0,
          handler = new GearsHttp2Handler {
            override def onRequest(
                request: GearsHttp2Request
            )(using Async): Unit = ()
          }
        )

      try {
        val client = GearsHttp2Client.connect(reactor, "127.0.0.1", listener.port)
        try {
          val exchange = client.openRequest(postHeaders(listener.port, "/leak-check"))
          try {
            val failure = intercept[Throwable] {
              exchange.awaitResponse
            }
            assert(failure.getMessage.contains("RST_STREAM error=8"))
          } finally exchange.close()
        } finally client.close()
      } finally {
        listener.close()
        reactor.close()
      }
    }
  }

  private def runAsync(body: Async.Spawn ?=> Unit): Unit =
    Async.blocking {
      Async.group {
        body(using summon[Async.Spawn])
      }
    }(using DefaultSupport, DefaultSupport)

  private def requestHeaders(
      port: Int,
      path: String,
      method: String = "GET"
  ): Seq[(String, String)] =
    Seq(
      ":method" -> method,
      ":scheme" -> "http",
      ":path" -> path,
      ":authority" -> s"127.0.0.1:${port}"
    )

  private def postHeaders(port: Int, path: String): Seq[(String, String)] =
    requestHeaders(port, path, method = "POST")
}
