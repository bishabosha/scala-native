import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}

import scala.collection.mutable.ArrayBuffer
import scala.util.Success
import scala.util.control.NonFatal

import scala.scalanative.meta.LinktimeInfo
import scala.scalanative.sandbox.streamio.gears._
import scala.scalanative.sandbox.streamio.transport.Reactor

import gears.async.default.{DefaultSupport, given}
import gears.async.{Async, Future}

object TestGears {
  private final case class BenchResult(
      concurrency: Int,
      successes: Long,
      failures: Long,
      bodyBytes: Long,
      elapsedNanos: Long
  ) {
    def requestsPerSecond: Double =
      if (elapsedNanos == 0L) 0.0
      else successes.toDouble * 1000000000.0 / elapsedNanos.toDouble

    def mibPerSecond: Double =
      if (elapsedNanos == 0L) 0.0
      else
        bodyBytes.toDouble * 1000000000.0 /
          elapsedNanos.toDouble / (1024.0 * 1024.0)

    def approxMeanLatencyMillis: Double =
      if (requestsPerSecond == 0.0) 0.0
      else concurrency.toDouble * 1000.0 / requestsPerSecond
  }

  private final case class OpenStreamsResult(
      targetStreams: Int,
      openedStreams: Long,
      failures: Long,
      openElapsedNanos: Long,
      totalElapsedNanos: Long
  ) {
    def openMillis: Double =
      openElapsedNanos.toDouble / 1000000.0

    def totalMillis: Double =
      totalElapsedNanos.toDouble / 1000000.0
  }

  private final case class OpenConnectionsResult(
      targetConnections: Int,
      openedConnections: Long,
      failures: Long,
      openElapsedNanos: Long,
      totalElapsedNanos: Long
  ) {
    def openMillis: Double =
      openElapsedNanos.toDouble / 1000000.0

    def totalMillis: Double =
      totalElapsedNanos.toDouble / 1000000.0
  }

  private object HoldRegistry {
    private val nextId = new AtomicLong()
    private val sessions = new ConcurrentHashMap[String, HoldSession]()

    def create(): HoldSession = {
      val session = new HoldSession(nextId.incrementAndGet().toString)
      sessions.put(session.id, session)
      session
    }

    def get(id: String): Option[HoldSession] =
      Option(sessions.get(id))

    def remove(id: String): Unit =
      sessions.remove(id)
  }

  private final class HoldSession(val id: String) {
    private val released = new AtomicBoolean(false)
    private val releasePromise = Future.Promise[Unit]()

    def awaitRelease(using Async): Unit =
      releasePromise.asFuture.await

    def release(): Unit =
      if (released.compareAndSet(false, true))
        releasePromise.complete(Success(()))
  }

  def main(args: Array[String]): Unit = {
    if (LinktimeInfo.isWindows) {
      println("streamio sandbox smoke skipped on Windows")
      return
    }

    val serveOnly = args.contains("--serve")
    val benchClientOnly = args.contains("--bench-client")
    val benchMode = args.contains("--bench") || benchClientOnly
    val host = readStringArg(args, "--host").getOrElse("127.0.0.1")
    val port =
      readPort(args).getOrElse(if (serveOnly || benchClientOnly) 8080 else 0)
    val reactorEvents =
      readIntArg(args, "--server-reactor-events")
        .orElse(readIntArg(args, "--reactor-events"))
        .getOrElse(1024)
    Async.blocking {
      Async.group {
        if (benchClientOnly) {
          runBenchmark(host, port, args)
        } else {
          val (reactor, listener) = startExampleServer(port, reactorEvents)
          if (serveOnly) {
            println(
              s"h2c gears echo server listening on 127.0.0.1:${listener.port}"
            )
            new CountDownLatch(1).await()
          } else if (benchMode) {
            try runBenchmark("127.0.0.1", listener.port, args)
            finally {
              listener.close()
              reactor.close()
            }
          } else {
            try {
              val client =
                GearsHttp2Client.connect(reactor, "127.0.0.1", listener.port)
              try {
                val exchange = client
                  .openRequest(
                    headers = Seq(
                      ":method" -> "POST",
                      ":scheme" -> "http",
                      ":path" -> "/echo",
                      ":authority" -> s"127.0.0.1:${listener.port}"
                    )
                  )
                exchange.requestBody.writeUtf8("chunk-1")
                exchange.requestBody.writeUtf8("chunk-2")
                exchange.requestBody.finish()
                val response = exchange.awaitResponse
                println("status=" + response.header(":status").getOrElse("?"))
                println(
                  new String(response.body.bufferAll, StandardCharsets.UTF_8)
                )
              } finally client.close()
            } finally {
              listener.close()
              reactor.close()
            }
          }
        }
      }
    }(using DefaultSupport, DefaultSupport)
  }

  def startExampleServer(
      port: Int = 0,
      reactorMaxEvents: Int = 1024
  )(using Async.Spawn): (Reactor, GearsHttp2Listener) = {
    val reactor = GearsReactor.polling[DefaultSupport.type](
      maxEvents = reactorMaxEvents
    )(using
      DefaultSupport,
      DefaultSupport
    )
    val listener = GearsHttp2Server.listen(reactor, port)
    GearsHttp2Server.serve(
      listener,
      new GearsHttp2Handler {
        override def onRequest(
            request: GearsHttp2Request
        )(using Async): Unit = {
          val path = request.path.getOrElse("/")
          request.response.sendHeaders(200, Seq("content-type" -> "text/plain"))

          if (path == "/echo") {
            var done = false
            while (!done) {
              request.body.read match {
                case Some(bytes) =>
                  request.response.sendData(bytes)
                case None =>
                  done = true
              }
            }
            request.response.sendData(Array.empty[Byte], endStream = true)
          } else if (path == "/hold") {
            request.response.sendHeaders(
              200,
              Seq("content-type" -> "text/plain")
            )
            request
              .header("x-streamio-hold")
              .flatMap(HoldRegistry.get)
              .foreach(_.awaitRelease)
            request.response.sendData(Array.empty[Byte], endStream = true)
          } else {
            val body = request.body.bufferAll
            if (body.isEmpty)
              request.response.writeUtf8(
                "hello from scala-native h2",
                endStream = true
              )
            else {
              val payload = "body=".getBytes(StandardCharsets.UTF_8) ++ body
              request.response.sendData(payload, endStream = true)
            }
          }
        }
      }
    )
    (reactor, listener)
  }

  private def runBenchmark(
      host: String,
      port: Int,
      args: Array[String]
  )(using Async.Spawn): Unit = {
    val levels =
      readIntListArg(args, "--bench-levels")
        .getOrElse(Seq(1, 2, 4, 8, 16, 32, 64, 128))
    val warmupSeconds = readIntArg(args, "--bench-warmup").getOrElse(1)
    val measureSeconds = readIntArg(args, "--bench-seconds").getOrElse(2)
    val path = readStringArg(args, "--bench-path").getOrElse("/")
    val style = readStringArg(args, "--bench-style").getOrElse("connections")
    val clientCount = math.max(
      1,
      readIntArg(args, "--bench-clients").getOrElse(
        if (style == "multiplex") 1 else 1
      )
    )
    val clientReactorEvents =
      readIntArg(args, "--bench-client-reactor-events")
        .orElse(readIntArg(args, "--bench-reactor-events"))
        .orElse(readIntArg(args, "--reactor-events"))
        .getOrElse(1024)
    val openTimeoutSeconds =
      readIntArg(args, "--bench-open-timeout").getOrElse(30)
    val openBatchSize =
      math.max(1, readIntArg(args, "--bench-open-batch").getOrElse(256))
    val openBatchPauseMillis =
      math.max(0, readIntArg(args, "--bench-open-pause-millis").getOrElse(10))

    val clientReactor = GearsReactor.polling[DefaultSupport.type](
      maxEvents = clientReactorEvents
    )(using
      DefaultSupport,
      DefaultSupport
    )

    try {
      println(
        s"benchmarking h2c target ${host}:${port}, path=$path"
      )
      println(
        s"warmup=${warmupSeconds}s measure=${measureSeconds}s levels=${levels.mkString(",")}"
      )
      println(
        s"style=$style clientPool=$clientCount clientReactorEvents=$clientReactorEvents"
      )

      val requestHeaders = Seq(
        ":method" -> "GET",
        ":scheme" -> "http",
        ":path" -> path,
        ":authority" -> s"${host}:${port}"
      )

      if (style == "open-streams") {
        println(
          s"openTimeout=${openTimeoutSeconds}s openBatch=${openBatchSize} openPause=${openBatchPauseMillis}ms"
        )
        val results = levels.map { targetStreams =>
          val result =
            benchmarkOpenStreams(
              clientReactor,
              host,
              port,
              targetStreams,
              clientCount,
              openTimeoutSeconds,
              openBatchSize,
              openBatchPauseMillis
            )
          println(
            f"streams=${result.targetStreams}%5d  opened=${result.openedStreams}%5d  failures=${result.failures}%d  open=${result.openMillis}%.1f ms  total=${result.totalMillis}%.1f ms"
          )
          result
        }
        val best = results.maxBy(_.openedStreams)
        println(
          f"best open-streams=${best.targetStreams}%d  opened=${best.openedStreams}%d  failures=${best.failures}%d"
        )
      } else if (style == "open-connections") {
        println(
          s"openTimeout=${openTimeoutSeconds}s openBatch=${openBatchSize} openPause=${openBatchPauseMillis}ms"
        )
        val results = levels.map { targetConnections =>
          val result =
            benchmarkOpenConnections(
              clientReactor,
              host,
              port,
              targetConnections,
              openTimeoutSeconds,
              openBatchSize,
              openBatchPauseMillis
            )
          println(
            f"connections=${result.targetConnections}%5d  opened=${result.openedConnections}%5d  failures=${result.failures}%d  open=${result.openMillis}%.1f ms  total=${result.totalMillis}%.1f ms"
          )
          result
        }
        val best = results.maxBy(_.openedConnections)
        println(
          f"best open-connections=${best.targetConnections}%d  opened=${best.openedConnections}%d  failures=${best.failures}%d"
        )
      } else {
        val results = levels.map { concurrency =>
          if (warmupSeconds > 0)
            benchmarkLevel(
              clientReactor,
              host,
              port,
              concurrency,
              warmupSeconds,
              requestHeaders,
              style,
              clientCount
            )
          val result =
            benchmarkLevel(
              clientReactor,
              host,
              port,
              concurrency,
              measureSeconds,
              requestHeaders,
              style,
              clientCount
            )
          println(
            f"concurrency=${result.concurrency}%3d  req/s=${result.requestsPerSecond}%.1f  failures=${result.failures}%d  body=${result.mibPerSecond}%.2f MiB/s  mean~=${result.approxMeanLatencyMillis}%.2f ms"
          )
          result
        }

        val best = results.maxBy(_.requestsPerSecond)
        println(
          f"best concurrency=${best.concurrency}%d  throughput=${best.requestsPerSecond}%.1f req/s  failures=${best.failures}%d"
        )
      }
    } finally {
      clientReactor.close()
    }
  }

  private def benchmarkLevel(
      clientReactor: Reactor,
      host: String,
      port: Int,
      concurrency: Int,
      seconds: Int,
      requestHeaders: Seq[(String, String)],
      style: String,
      clientCount: Int
  )(using Async.Spawn): BenchResult = {
    style match {
      case "connections" =>
        benchmarkConnectionStorm(
          clientReactor,
          host,
          port,
          concurrency,
          seconds,
          requestHeaders
        )
      case "multiplex" =>
        benchmarkMultiplexed(
          clientReactor,
          host,
          port,
          concurrency,
          seconds,
          requestHeaders,
          clientCount
        )
      case other =>
        throw new IllegalArgumentException(
          s"unsupported bench style: $other"
        )
    }
  }

  private def benchmarkConnectionStorm(
      clientReactor: Reactor,
      host: String,
      port: Int,
      concurrency: Int,
      seconds: Int,
      requestHeaders: Seq[(String, String)]
  )(using Async.Spawn): BenchResult = {
    val successes = new AtomicLong()
    val failures = new AtomicLong()
    val bodyBytes = new AtomicLong()
    val deadline = System.nanoTime() + seconds.toLong * 1000000000L
    val startedAt = System.nanoTime()

    val workers = (0 until concurrency).map { _ =>
      Future {
        var client: GearsHttp2Client = null
        try {
          while (System.nanoTime() < deadline) {
            if (client == null) {
              try {
                client = GearsHttp2Client.connect(clientReactor, host, port)
              } catch {
                case NonFatal(_) =>
                  failures.incrementAndGet()
              }
            } else
              try {
                val response = client.request(requestHeaders)
                if (response.header(":status").contains("200")) {
                  successes.incrementAndGet()
                  bodyBytes.addAndGet(response.body.length.toLong)
                } else {
                  failures.incrementAndGet()
                }
              } catch {
                case NonFatal(_) =>
                  failures.incrementAndGet()
                  try client.close()
                  catch {
                    case NonFatal(_) =>
                  }
                  client = null
              }
          }
        } finally {
          if (client != null)
            try client.close()
            catch {
              case NonFatal(_) =>
            }
        }
      }
    }

    workers.awaitAll
    BenchResult(
      concurrency = concurrency,
      successes = successes.get(),
      failures = failures.get(),
      bodyBytes = bodyBytes.get(),
      elapsedNanos = System.nanoTime() - startedAt
    )
  }

  private def benchmarkMultiplexed(
      clientReactor: Reactor,
      host: String,
      port: Int,
      concurrency: Int,
      seconds: Int,
      requestHeaders: Seq[(String, String)],
      clientCount: Int
  )(using Async.Spawn): BenchResult = {
    val successes = new AtomicLong()
    val failures = new AtomicLong()
    val bodyBytes = new AtomicLong()

    val clients = (0 until clientCount).map { _ =>
      GearsHttp2Client.connect(clientReactor, host, port)
    }

    try {
      val deadline = System.nanoTime() + seconds.toLong * 1000000000L
      val startedAt = System.nanoTime()
      val workers = (0 until concurrency).map { workerIndex =>
        val client = clients(workerIndex % clients.length)
        Future {
          while (System.nanoTime() < deadline) {
            try {
              val response = client.request(requestHeaders)
              if (response.header(":status").contains("200")) {
                successes.incrementAndGet()
                bodyBytes.addAndGet(response.body.length.toLong)
              } else {
                failures.incrementAndGet()
              }
            } catch {
              case NonFatal(_) =>
                failures.incrementAndGet()
            }
          }
        }
      }

      workers.awaitAll
      BenchResult(
        concurrency = concurrency,
        successes = successes.get(),
        failures = failures.get(),
        bodyBytes = bodyBytes.get(),
        elapsedNanos = System.nanoTime() - startedAt
      )
    } finally {
      clients.foreach { client =>
        try client.close()
        catch {
          case NonFatal(_) =>
        }
      }
    }
  }

  private def benchmarkOpenStreams(
      clientReactor: Reactor,
      host: String,
      port: Int,
      targetStreams: Int,
      clientCount: Int,
      openTimeoutSeconds: Int,
      openBatchSize: Int,
      openBatchPauseMillis: Int
  )(using Async.Spawn): OpenStreamsResult = {
    val opened = new AtomicLong()
    val failures = new AtomicLong()
    val clients = (0 until clientCount).map { _ =>
      GearsHttp2Client.connect(clientReactor, host, port)
    }
    val session = HoldRegistry.create()
    val openedLatch = new CountDownLatch(targetStreams)
    val startedAt = System.nanoTime()
    val headers = Seq(
      ":method" -> "GET",
      ":scheme" -> "http",
      ":path" -> "/hold",
      ":authority" -> s"${host}:${port}",
      "x-streamio-hold" -> session.id
    )

    try {
      val workers = ArrayBuffer.empty[Future[Unit]]
      var launched = 0
      while (launched < targetStreams) {
        val batchSize = math.min(openBatchSize, targetStreams - launched)
        val batch = (0 until batchSize).map { idx =>
          val client = clients((launched + idx) % clients.length)
          Future {
            try {
              val exchange = client.openRequest(headers)
              exchange.requestBody.finish()
              val response = exchange.awaitResponse
              if (response.header(":status").contains("200")) {
                opened.incrementAndGet()
                openedLatch.countDown()
                response.body.bufferAll
              } else {
                failures.incrementAndGet()
              }
            } catch {
              case NonFatal(_) =>
                failures.incrementAndGet()
            }
            ()
          }
        }
        workers ++= batch
        launched += batchSize
        if (launched < targetStreams && openBatchPauseMillis > 0)
          pauseMillis(openBatchPauseMillis)
      }

      openedLatch.await(openTimeoutSeconds.toLong, TimeUnit.SECONDS)
      val openedAt = System.nanoTime()
      session.release()
      workers.toSeq.awaitAll
      OpenStreamsResult(
        targetStreams = targetStreams,
        openedStreams = opened.get(),
        failures = failures.get(),
        openElapsedNanos = openedAt - startedAt,
        totalElapsedNanos = System.nanoTime() - startedAt
      )
    } finally {
      HoldRegistry.remove(session.id)
      clients.foreach { client =>
        try client.close()
        catch {
          case NonFatal(_) =>
        }
      }
    }
  }

  private def benchmarkOpenConnections(
      clientReactor: Reactor,
      host: String,
      port: Int,
      targetConnections: Int,
      openTimeoutSeconds: Int,
      openBatchSize: Int,
      openBatchPauseMillis: Int
  )(using Async.Spawn): OpenConnectionsResult = {
    val opened = new AtomicLong()
    val failures = new AtomicLong()
    val session = HoldRegistry.create()
    val openedLatch = new CountDownLatch(targetConnections)
    val startedAt = System.nanoTime()
    val headers = Seq(
      ":method" -> "GET",
      ":scheme" -> "http",
      ":path" -> "/hold",
      ":authority" -> s"${host}:${port}",
      "x-streamio-hold" -> session.id
    )

    try {
      val workers = ArrayBuffer.empty[Future[Unit]]
      var launched = 0
      while (launched < targetConnections) {
        val batchSize = math.min(openBatchSize, targetConnections - launched)
        val batch = (0 until batchSize).map { _ =>
          Future {
            var client: GearsHttp2Client = null
            try {
              client = GearsHttp2Client.connect(clientReactor, host, port)
              val exchange = client.openRequest(headers)
              exchange.requestBody.finish()
              val response = exchange.awaitResponse
              if (response.header(":status").contains("200")) {
                opened.incrementAndGet()
                openedLatch.countDown()
                response.body.bufferAll
              } else {
                failures.incrementAndGet()
              }
            } catch {
              case NonFatal(_) =>
                failures.incrementAndGet()
            } finally {
              if (client != null)
                try client.close()
                catch {
                  case NonFatal(_) =>
                }
            }
            ()
          }
        }
        workers ++= batch
        launched += batchSize
        if (launched < targetConnections && openBatchPauseMillis > 0)
          pauseMillis(openBatchPauseMillis)
      }

      openedLatch.await(openTimeoutSeconds.toLong, TimeUnit.SECONDS)
      val openedAt = System.nanoTime()
      session.release()
      workers.toSeq.awaitAll
      OpenConnectionsResult(
        targetConnections = targetConnections,
        openedConnections = opened.get(),
        failures = failures.get(),
        openElapsedNanos = openedAt - startedAt,
        totalElapsedNanos = System.nanoTime() - startedAt
      )
    } finally {
      HoldRegistry.remove(session.id)
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

  private def readIntArg(args: Array[String], name: String): Option[Int] = {
    def parse(value: String): Option[Int] =
      try Some(value.toInt)
      catch {
        case _: NumberFormatException => None
      }

    args.iterator.zipWithIndex.collectFirst {
      case (arg, idx) if arg == name && idx + 1 < args.length =>
        parse(args(idx + 1))
      case (arg, _) if arg.startsWith(name + "=") =>
        parse(arg.stripPrefix(name + "="))
    }.flatten
  }

  private def readStringArg(
      args: Array[String],
      name: String
  ): Option[String] =
    args.iterator.zipWithIndex.collectFirst {
      case (arg, idx) if arg == name && idx + 1 < args.length =>
        Some(args(idx + 1))
      case (arg, _) if arg.startsWith(name + "=") =>
        Some(arg.stripPrefix(name + "="))
    }.flatten

  private def readIntListArg(
      args: Array[String],
      name: String
  ): Option[Seq[Int]] =
    readStringArg(args, name)
      .map(_.split(',').toSeq.flatMap { part =>
        try Some(part.trim.toInt)
        catch {
          case _: NumberFormatException => None
        }
      })
      .filter(_.nonEmpty)

  private def pauseMillis(millis: Int): Unit =
    if (millis > 0) {
      val deadline = System.nanoTime() + millis.toLong * 1000000L
      while (System.nanoTime() < deadline)
        Thread.`yield`()
    }
}
