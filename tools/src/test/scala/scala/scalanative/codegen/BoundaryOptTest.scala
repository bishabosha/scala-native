package scala.scalanative.codegen

import java.nio.file.Files

import org.junit.Assert._
import org.junit.Test

class BoundaryOptTest extends CodeGenSpec {

  private def generatedText(outfiles: Seq[java.nio.file.Path]): String =
    outfiles.iterator.map(Files.readString(_)).mkString("\n")

  private def hasCall(ir: String, name: String): Boolean =
    ir.linesIterator.exists(line => line.contains("call") && line.contains(s"@$name"))

  @Test def rewritesTransparentNestedBoundaryPath(): Unit = codegen(
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
    ),
    setupConfig = _.withOptimize(false)
  ) {
    case (_, _, outfiles) =>
      val ir = generatedText(outfiles)
      assertTrue(hasCall(ir, "scalanative_boundary_setjmp"))
      assertTrue(hasCall(ir, "scalanative_boundary_break_fast"))
  }

  @Test def keepsExceptionPathWhenIntermediateCatchCanIntercept(): Unit = codegen(
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
}
