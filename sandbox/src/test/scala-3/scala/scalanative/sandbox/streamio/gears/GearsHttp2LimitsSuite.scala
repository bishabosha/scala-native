package scala.scalanative.sandbox.streamio.gears

import scala.util.Success
import scala.concurrent.duration.*

import munit.FunSuite

import scala.scalanative.meta.LinktimeInfo

import gears.async.{Async, Future}
import gears.async.default.{DefaultSupport, given}

class GearsHttp2LimitsSuite extends FunSuite {
  override def munitIgnore: Boolean = LinktimeInfo.isWindows
  override val munitTimeout: Duration = 60.seconds

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

  private def runAsync(body: Async.Spawn ?=> Unit): Unit =
    Async.blocking {
      Async.group {
        body(using summon[Async.Spawn])
      }
    }(using DefaultSupport, DefaultSupport)

  private def requestHeaders(port: Int, path: String): Seq[(String, String)] =
    Seq(
      ":method" -> "GET",
      ":scheme" -> "http",
      ":path" -> path,
      ":authority" -> s"127.0.0.1:${port}"
    )
}
