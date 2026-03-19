package scala.scalanative.codegen

import java.nio.file.Files

import org.junit.Assert._
import org.junit.Test

import scala.scalanative.build.{Config, NativeConfig}
import scala.scalanative.linker.ReachabilityAnalysis
import scala.scalanative.nir

class BoundaryOptTest extends CodeGenSpec {

  private def generatedText(outfiles: Seq[java.nio.file.Path]): String =
    outfiles.iterator.map(Files.readString(_)).mkString("\n")

  private def hasCall(ir: String, name: String): Boolean =
    ir.linesIterator.exists { line =>
      line.contains("call") && (
        line.contains(s"@$name") || line.contains(s"""@"$name"""")
      )
    }

  private def optimizeAndPrepare(
      entry: String,
      sources: Map[String, String],
      setupConfig: NativeConfig => NativeConfig = identity
  )(
      f: (Config, ReachabilityAnalysis.Result, BoundaryOpt.Prepared) => Unit
  ): Unit =
    optimize(entry, sources, setupConfig.compose(_.withBaseName(entry))) {
      case (config, optimized) =>
        implicit val analysis: ReachabilityAnalysis.Result = optimized
        val arch = config.compilerConfig.configuredOrDetectedTriple.arch
        val supported = arch match {
          case "aarch64" | "arm64" if !config.targetsWindows => true
          case "x86_64"                                      => true
          case "x86" if !config.targetsWindows               => true
          case _                                             => false
        }
        f(config, optimized, BoundaryOpt.prepare(optimized.defns, supported))
    }

  private def boundaryFrameType(config: Config): nir.Type.StructValue = {
    val arch = config.compilerConfig.configuredOrDetectedTriple.arch
    val jmpBufWords = arch match {
      case "aarch64" | "arm64" if !config.targetsWindows => 24
      case "x86_64" if config.targetsWindows             => 32
      case "x86_64"                                      => 9
      case "x86"                                         => 8
      case other                                         =>
        throw new AssertionError(s"Unsupported arch in test: $other")
    }
    nir.Type.StructValue(
      Seq(
        nir.Type.Ptr,
        nir.Type.Ptr,
        nir.Type.Ptr,
        nir.Type.ArrayValue(nir.Type.Ptr, jmpBufWords)
      )
    )
  }

  private def hasRuntimeCall(
      defns: Seq[nir.Defn],
      name: nir.Global.Member
  ): Boolean =
    defns.exists {
      case defn: nir.Defn.Define =>
        defn.insts.exists {
          case nir.Inst.Let(
                _,
                nir.Op.Call(_, nir.Val.Global(`name`, _), _),
                _
              ) =>
            true
          case _ =>
            false
        }
      case _ =>
        false
    }

  private val transparentNestedSource =
    """|import scala.util.boundary, boundary.break
       |
       |object Main {
       |  def helper(x: Int)(using boundary.Label[Int]): Int =
       |    if (x < 0) break(1)
       |    x + 1
       |
       |  def passthrough(x: Int)(using outer: boundary.Label[Int]): Int =
       |    boundary[Int] {
       |      helper(x)(using outer)
       |    }
       |
       |  def foo(x: Int): Int =
       |    boundary[Int] {
       |      passthrough(x)
       |    }
       |
       |  def main(args: Array[String]): Unit =
       |    println(foo(-1))
       |}""".stripMargin

  private val transparentMultiHopSource =
    """|import scala.util.boundary, boundary.break
       |
       |object Main {
       |  def helper(x: Int)(using boundary.Label[Int]): Int =
       |    if (x < 0) break(1)
       |    x + 1
       |
       |  def hop1(x: Int)(using outer: boundary.Label[Int]): Int =
       |    boundary[Int] {
       |      helper(x)(using outer)
       |    }
       |
       |  def hop2(x: Int)(using outer: boundary.Label[Int]): Int =
       |    boundary[Int] {
       |      hop1(x)(using outer)
       |    }
       |
       |  def foo(x: Int): Int =
       |    boundary[Int] {
       |      hop2(x)
       |    }
       |
       |  def main(args: Array[String]): Unit =
       |    println(foo(-1))
       |}""".stripMargin

  @Test def rewritesTransparentNestedBoundaryPath(): Unit = codegen(
    entry = "Main",
    sources = Map("Main.scala" -> transparentNestedSource),
    setupConfig = _.withOptimize(false)
  ) {
    case (_, _, outfiles) =>
      val ir = generatedText(outfiles)
      assertTrue(hasCall(ir, "scalanative_boundary_setjmp"))
      assertTrue(hasCall(ir, "scalanative_boundary_break_fast"))
  }

  @Test def preparesTransparentNestedBoundaryPath(): Unit = optimizeAndPrepare(
    entry = "Main",
    sources = Map("Main.scala" -> transparentNestedSource),
    setupConfig = _.withOptimize(false)
  ) {
    case (_, _, prepared) =>
      assertFalse(prepared.activeBoundariesByMethod.isEmpty)
      assertFalse(prepared.safeBreakSitesByMethod.isEmpty)
  }

  @Test def rewritesTransparentNestedBoundaryPathAtNirLevel(): Unit =
    optimizeAndPrepare(
      entry = "Main",
      sources = Map("Main.scala" -> transparentNestedSource),
      setupConfig = _.withOptimize(false)
    ) {
      case (config, optimized, prepared) =>
        val frameTy = boundaryFrameType(config)
        val rewritten = optimized.defns.collect {
          case defn: nir.Defn.Define =>
            BoundaryOpt.rewrite(defn, prepared, frameTy)
        }
        assertTrue(hasRuntimeCall(rewritten, Lower.BoundarySetjmpName))
        assertTrue(hasRuntimeCall(rewritten, Lower.BoundaryBreakFastName))
    }

  @Test def keepsExceptionPathWhenIntermediateCatchCanIntercept(): Unit =
    codegen(
      entry = "Main",
      sources = Map(
        "Main.scala" ->
          """|import scala.util.boundary, boundary.break
             |
             |object Main {
             |  def helper(x: Int)(using boundary.Label[Int]): Int =
             |    try {
             |      if (x < 0) break(1)
             |      x + 1
             |    } catch {
             |      case _: Throwable => 0
             |    }
             |
             |  def foo(x: Int): Int =
             |    boundary[Int] {
             |      helper(x)
             |    }
             |
             |  def main(args: Array[String]): Unit =
             |    println(foo(-1))
             |}""".stripMargin
      ),
      setupConfig = _.withOptimize(false)
    ) {
      case (_, _, outfiles) =>
        val ir = generatedText(outfiles)
        assertFalse(hasCall(ir, "scalanative_boundary_setjmp"))
        assertFalse(hasCall(ir, "scalanative_boundary_break_fast"))
    }

  @Test def rewritesTransparentMultiHopBoundaryPath(): Unit = codegen(
    entry = "Main",
    sources = Map("Main.scala" -> transparentMultiHopSource),
    setupConfig = _.withOptimize(false)
  ) {
    case (_, _, outfiles) =>
      val ir = generatedText(outfiles)
      assertTrue(hasCall(ir, "scalanative_boundary_setjmp"))
      assertTrue(hasCall(ir, "scalanative_boundary_break_fast"))
  }

  @Test def keepsExceptionPathWhenIntermediateBreakHandlerIsNotTransparent()
      : Unit = codegen(
    entry = "Main",
    sources = Map(
      "Main.scala" ->
        """|import scala.util.boundary, boundary.break
           |
           |object Main {
           |  def helper(x: Int)(using boundary.Label[Int]): Int =
           |    if (x < 0) break(1)
           |    x + 1
           |
           |  def hop(x: Int)(using outer: boundary.Label[Int]): Int =
           |    try {
           |      boundary[Int] {
           |        helper(x)(using outer)
           |      }
           |    } catch {
           |      case ex: boundary.Break[Int] =>
           |        if (ex.isSameLabelAs(summon[boundary.Label[Int]])) 0
           |        else 42
           |    }
           |
           |  def foo(x: Int): Int =
           |    boundary[Int] {
           |      hop(x)
           |    }
           |
           |  def main(args: Array[String]): Unit =
           |    println(foo(-1))
           |}""".stripMargin
    ),
    setupConfig = _.withOptimize(false)
  ) {
    case (_, _, outfiles) =>
      val ir = generatedText(outfiles)
      assertFalse(hasCall(ir, "scalanative_boundary_setjmp"))
      assertFalse(hasCall(ir, "scalanative_boundary_break_fast"))
  }

  @Test def keepsExceptionPathWhenIntermediateFinallyIsPresent(): Unit =
    codegen(
      entry = "Main",
      sources = Map(
        "Main.scala" ->
          """|import scala.util.boundary, boundary.break
             |
             |object Main {
             |  var finalized: Int = 0
             |
             |  def helper(x: Int)(using boundary.Label[Int]): Int =
             |    try {
             |      if (x < 0) break(1)
             |      x + 1
             |    } finally {
             |      finalized += 1
             |    }
             |
             |  def foo(x: Int): Int =
             |    boundary[Int] {
             |      helper(x)
             |    }
             |
             |  def main(args: Array[String]): Unit =
             |    println(foo(-1))
             |}""".stripMargin
      ),
      setupConfig = _.withOptimize(false)
    ) {
      case (_, _, outfiles) =>
        val ir = generatedText(outfiles)
        assertFalse(hasCall(ir, "scalanative_boundary_setjmp"))
        assertFalse(hasCall(ir, "scalanative_boundary_break_fast"))
    }
}
