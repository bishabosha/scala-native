import gears.async.Async
import gears.async.default.DefaultSupport
import gears.async.default.given

import scala.scalanative.sandbox.streamio.gears._
import scala.scalanative.sandbox.streamio.transport.Reactor

object TestGears {
  def startExampleServer(port: Int = 0): (Reactor, scala.scalanative.sandbox.streamio.http2.Http2Server) = {
    val reactor = new Reactor()
    val server = GearsHttp2Server.bind[DefaultSupport.type](
      reactor,
      port,
      new GearsHttp2Handler {
        override def onRequest(request: GearsHttp2Request)(using Async): Unit = {
          val body = request.body.await
          request.response.sendHeaders(200, Seq("content-type" -> "text/plain")).await
          request.response
            .writeUtf8(
              s"${request.method.getOrElse("GET")} ${request.path.getOrElse("/")} ${body.length}",
              endStream = true
            )
            .await
        }
      }
    )(using DefaultSupport, DefaultSupport)
    val thread = new Thread(() => reactor.run())
    thread.setDaemon(true)
    thread.start()
    (reactor, server)
  }
}
