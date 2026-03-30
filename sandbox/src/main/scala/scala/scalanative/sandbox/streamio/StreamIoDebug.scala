package scala.scalanative.sandbox.streamio

import java.io.File
import java.util.concurrent.atomic.AtomicLong

private[streamio] object StreamIoDebug {
  private val enabledFlag = {
    val raw = System.getenv("STREAMIO_DEBUG")
    val envEnabled =
      raw != null &&
      raw.nonEmpty &&
      raw != "0" &&
      !raw.equalsIgnoreCase("false") &&
      !raw.equalsIgnoreCase("off")
    envEnabled || new File("/tmp/streamio-debug").isFile
  }

  private val sequence = new AtomicLong(0L)

  def enabled: Boolean = enabledFlag

  def log(area: String, message: => String): Unit =
    if (enabledFlag) {
      val id = sequence.incrementAndGet()
      val thread = Thread.currentThread().getName()
      System.err.println(s"[streamio:$id][$area][$thread] $message")
      System.err.flush()
    }
}
