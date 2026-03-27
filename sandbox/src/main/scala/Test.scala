import java.nio.charset.StandardCharsets

import scala.scalanative.meta.LinktimeInfo
import scala.scalanative.sandbox.streamio.http2._
import scala.scalanative.sandbox.streamio.transport._

object Test {
  def main(args: Array[String]): Unit = {
    if (LinktimeInfo.isWindows) {
      println("streamio sandbox smoke skipped on Windows")
      return
    }

    val serveOnly = args.contains("--serve")
    val port = readPort(args).getOrElse(if (serveOnly) 8080 else 0)
    val reactor = new Reactor()
    val server = Http2Server.bind(
      reactor,
      port = port,
      handler = new Http2Handler {
        override def onHeaders(
            stream: Http2Stream,
            headers: Vector[(String, String)],
            endStream: Boolean
        ): Unit = {
          stream.sendResponseHeaders(
            status = 200,
            headers = Seq("content-type" -> "text/plain"),
            endStream = false
          )
          if (endStream) {
            stream.writeUtf8("hello from scala-native h2", endStream = true)
          }
        }

        override def onData(
            stream: Http2Stream,
            data: Array[Byte],
            endStream: Boolean
        ): Unit = {
          val prefix =
            if (stream.requestHeaders.exists(_ == (":path", "/echo")))
              Array.empty[Byte]
            else "body=".getBytes(StandardCharsets.UTF_8)
          val payload =
            if (prefix.isEmpty) data else prefix ++ data
          stream.sendData(payload, endStream = endStream)
        }
      }
    )

    val thread = new Thread(new Runnable {
      override def run(): Unit = reactor.run()
    })
    thread.setDaemon(true)
    thread.start()

    if (serveOnly) {
      println(s"h2c echo server listening on 127.0.0.1:${server.port}")
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
