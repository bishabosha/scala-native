import java.nio.charset.StandardCharsets

import scala.scalanative.meta.LinktimeInfo
import scala.scalanative.sandbox.streamio.gears._
import scala.scalanative.sandbox.streamio.http2.{
  Http2Server, LoopbackHttp2Client
}
import scala.scalanative.sandbox.streamio.transport.Reactor

import gears.async.Async
import gears.async.default.{DefaultSupport, given}

object TestGears {
  def main(args: Array[String]): Unit = {
    if (LinktimeInfo.isWindows) {
      println("streamio sandbox smoke skipped on Windows")
      return
    }

    val serveOnly = args.contains("--serve")
    val port = readPort(args).getOrElse(if (serveOnly) 8080 else 0)
    val (reactor, server, thread) = startExampleServer(port)

    if (serveOnly) {
      println(s"h2c gears echo server listening on 127.0.0.1:${server.port}")
      thread.join()
    } else {
      val client = new LoopbackHttp2Client("127.0.0.1", server.port)
      try {
        val response = client.request(
          headers = Seq(
            ":method" -> "POST",
            ":scheme" -> "http",
            ":path" -> "/echo",
            ":authority" -> s"127.0.0.1:${server.port}"
          ),
          dataFrames = Seq(
            "chunk-1".getBytes(StandardCharsets.UTF_8),
            "chunk-2".getBytes(StandardCharsets.UTF_8)
          )
        )
        println("status=" + response.header(":status").getOrElse("?"))
        println(response.bodyUtf8)
      } finally {
        client.close()
        server.close()
        reactor.close()
      }
    }
  }

  def startExampleServer(
      port: Int = 0
  ): (Reactor, Http2Server, Thread) = {
    val reactor = new Reactor()
    val server = GearsHttp2Server.bind[DefaultSupport.type](
      reactor,
      port,
      new GearsHttp2Handler {
        override def onRequest(
            request: GearsHttp2Request
        )(using Async): Unit = {
          val path = request.path.getOrElse("/")
          val body = request.body.await
          request.response
            .sendHeaders(200, Seq("content-type" -> "text/plain"))
            .await

          if (body.isEmpty && path != "/echo")
            request.response
              .writeUtf8("hello from scala-native h2", endStream = true)
              .await
          else {
            val payload =
              if (path == "/echo") body
              else "body=".getBytes(StandardCharsets.UTF_8) ++ body
            request.response.sendData(payload, endStream = true).await
          }
        }
      }
    )(using DefaultSupport, DefaultSupport)
    val thread = new Thread(() => reactor.run())
    thread.setDaemon(true)
    thread.start()
    (reactor, server, thread)
  }

  private def readPort(args: Array[String]): Option[Int] = {
    def parse(value: String): Option[Int] =
      try Some(value.toInt)
      catch {
        case _: NumberFormatException => None
      }

    args.iterator.zipWithIndex.collectFirst {
      case (arg, idx) if arg == "--port" && idx + 1 < args.length =>
        parse(args(idx + 1))
      case (arg, _) if arg.startsWith("--port=") =>
        parse(arg.stripPrefix("--port="))
    }.flatten
  }
}
