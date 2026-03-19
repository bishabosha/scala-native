import scala.noinline
import scala.util.boundary
import scala.util.boundary.Label
import scala.util.boundary.break

object BoundaryBenchmark {
  @volatile private var sink = 0
  @volatile private var observedExceptions = 0

  final case class Settings(warmupIterations: Int, measureIterations: Int)

  def main(args: Array[String]): Unit = {
    val settings = parseArgs(args)
    println(
      s"Boundary benchmark: warmup=${settings.warmupIterations}, measure=${settings.measureIterations}"
    )
    println(
      "Build twice to compare the same program:"
    )
    println(
      "  1. default nativeConfig (optimizeBoundaryBreaks = true)"
    )
    println(
      "  2. nativeConfig ~= (_.withOptimizeBoundaryBreaks(false))"
    )
    println(
      "Direct cases should show the optimization. Collection cases are current negative controls."
    )
    println()

    run("direct-break", settings) { i =>
      directBreak(i)
    }
    run("direct-no-break", settings) { i =>
      directNoBreak(i)
    }
    run("collection-break", settings) { i =>
      transparentBreak(i)
    }
    run("collection-fallback-break", settings) { i =>
      fallbackBreak(i)
    }
    run("collection-no-break", settings) { i =>
      transparentNoBreak(i)
    }
    run("collection-fallback-no-break", settings) { i =>
      fallbackNoBreak(i)
    }

    println()
    println(s"observedExceptions=$observedExceptions sink=$sink")
  }

  private def run(
      name: String,
      settings: Settings
  )(body: Int => Int): Unit = {
    var acc = 0
    var i = 0
    while (i < settings.warmupIterations) {
      acc += body(i)
      i += 1
    }

    val startedAt = System.nanoTime()
    acc = 0
    i = 0
    while (i < settings.measureIterations) {
      acc += body(i)
      i += 1
    }
    val elapsedNanos = System.nanoTime() - startedAt
    sink = acc

    val nsPerOp = elapsedNanos.toDouble / settings.measureIterations.toDouble
    val ms = elapsedNanos.toDouble / 1000000.0
    println(
      f"$name%-22s total=${ms}%.3f ms  ns/op=${nsPerOp}%.1f  acc=$acc"
    )
  }

  private def parseArgs(args: Array[String]): Settings = {
    val defaultWarmup = 100000
    val defaultMeasure = 1000000

    args.toList match {
      case Nil =>
        Settings(defaultWarmup, defaultMeasure)
      case warmup :: measure :: Nil =>
        Settings(warmup.toInt, measure.toInt)
      case _ =>
        throw new IllegalArgumentException(
          "Usage: BoundaryBenchmark [warmupIterations measureIterations]"
        )
    }
  }

  private def transparentBreak(seed: Int): Int =
    boundary[Int] {
      transparentPassthrough(seed, stop = 18)
    }

  private def fallbackBreak(seed: Int): Int =
    boundary[Int] {
      fallbackPassthrough(seed, stop = 18)
    }

  private def transparentNoBreak(seed: Int): Int =
    boundary[Int] {
      transparentPassthrough(seed, stop = -1)
    }

  private def fallbackNoBreak(seed: Int): Int =
    boundary[Int] {
      fallbackPassthrough(seed, stop = -1)
    }

  private def directBreak(seed: Int): Int =
    boundary[Int] {
      directLayer1(seed, breakNow = true)
    }

  private def directNoBreak(seed: Int): Int =
    boundary[Int] {
      directLayer1(seed, breakNow = false)
    }

  @noinline
  private def directLayer1(seed: Int, breakNow: Boolean)(using
      outer: Label[Int]
  ): Int =
    boundary[Int] {
      directLayer2(seed + 1, breakNow)(using outer)
    }

  @noinline
  private def directLayer2(value: Int, breakNow: Boolean)(using
      outer: Label[Int]
  ): Int =
    boundary[Int] {
      directLayer3(value * 2, breakNow)(using outer)
    }

  @noinline
  private def directLayer3(value: Int, breakNow: Boolean)(using
      outer: Label[Int]
  ): Int =
    directLayer4(value - 1, breakNow)

  @noinline
  private def directLayer4(value: Int, breakNow: Boolean)(using
      label: Label[Int]
  ): Int =
    if (breakNow) break(value)
    else value

  @noinline
  private def transparentPassthrough(seed: Int, stop: Int)(using
      outer: Label[Int]
  ): Int =
    boundary[Int] {
      nestedOptionCollectionPipeline(seed, stop)(using outer)
    }

  @noinline
  private def fallbackPassthrough(seed: Int, stop: Int)(using
      outer: Label[Int]
  ): Int =
    boundary[Int] {
      nestedOptionCollectionPipelineWithObservedCatch(seed, stop)(using outer)
    }

  @noinline
  private def nestedOptionCollectionPipeline(seed: Int, stop: Int)(using
      label: Label[Int]
  ): Int =
    Option(seed & 15)
      .map(v => hopOption(v, stop))
      .getOrElse(0)

  @noinline
  private def nestedOptionCollectionPipelineWithObservedCatch(
      seed: Int,
      stop: Int
  )(using label: Label[Int]): Int =
    try nestedOptionCollectionPipeline(seed, stop)
    catch {
      case t: Throwable =>
        observedExceptions += 1
        throw t
    }

  @noinline
  private def hopOption(value: Int, stop: Int)(using label: Label[Int]): Int =
    Option.when(value >= 0)(value + 1)
      .map(hopList(_, stop))
      .getOrElse(0)

  @noinline
  private def hopList(value: Int, stop: Int)(using label: Label[Int]): Int =
    List(value, value + 1, value + 2)
      .map(hopVector(_, stop))
      .sum

  @noinline
  private def hopVector(value: Int, stop: Int)(using label: Label[Int]): Int =
    Vector(value, value + 1)
      .map(hopBreak(_, stop))
      .sum

  @noinline
  private def hopBreak(value: Int, stop: Int)(using label: Label[Int]): Int =
    if (value == stop) break(value)
    else value
}
