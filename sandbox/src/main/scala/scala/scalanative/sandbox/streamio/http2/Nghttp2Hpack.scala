package scala.scalanative.sandbox.streamio.http2

import java.io.IOException
import java.nio.charset.StandardCharsets

import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

private object Nghttp2Hpack {
  import Api._

  final class Inflater extends AutoCloseable {
    private val handle: Ptr[nghttp2_hd_inflater] = Zone.acquire { implicit z =>
      val out = stackalloc[Ptr[nghttp2_hd_inflater]]()
      val rc = nghttp2.nghttp2_hd_inflate_new(out)
      if (rc != 0)
        throw new IOException(s"nghttp2_hd_inflate_new failed: $rc")
      !out
    }

    def decode(block: Array[Byte]): Vector[(String, String)] = {
      val headers = Vector.newBuilder[(String, String)]
      var offset = 0
      var finished = false

      while (offset <= block.length && !finished) {
        Zone.acquire { implicit z =>
          val nv = stackalloc[nghttp2_nv]()
          val flags = stackalloc[CInt]()
          !flags = 0

          val inPtr =
            if (offset == block.length) null.asInstanceOf[Ptr[Byte]]
            else block.at(offset)
          val consumed = nghttp2.nghttp2_hd_inflate_hd3(
            handle,
            nv,
            flags,
            inPtr,
            (block.length - offset).toCSize,
            1
          )
          val processed = consumed.toLong
          if (processed < 0L)
            throw new IOException(s"nghttp2_hd_inflate_hd3 failed: $processed")

          val inflateFlags = !flags
          if ((inflateFlags & NGHTTP2_HD_INFLATE_EMIT) != 0) {
            val name =
              fromCStringSlice(
                nv._1.asInstanceOf[CString],
                nv._3,
                StandardCharsets.US_ASCII
              )
            val value =
              fromCStringSlice(
                nv._2.asInstanceOf[CString],
                nv._4,
                StandardCharsets.UTF_8
              )
            headers += ((name, value))
          }

          if ((inflateFlags & NGHTTP2_HD_INFLATE_FINAL) != 0) {
            nghttp2.nghttp2_hd_inflate_end_headers(handle)
            finished = true
          }

          if (
            processed == 0L &&
            (inflateFlags & NGHTTP2_HD_INFLATE_EMIT) == 0 &&
            !finished
          ) {
            throw new IOException("nghttp2 HPACK inflater made no progress")
          }

          offset += processed.toInt
        }
      }

      if (!finished)
        throw new IOException("incomplete HPACK header block")

      headers.result()
    }

    override def close(): Unit =
      if (handle != null) nghttp2.nghttp2_hd_inflate_del(handle)
  }

  private object Api {
    final val NGHTTP2_HD_INFLATE_FINAL = 0x01
    final val NGHTTP2_HD_INFLATE_EMIT = 0x02

    type nghttp2_hd_inflater = CStruct0
    type nghttp2_nv = CStruct5[Ptr[Byte], Ptr[Byte], CSize, CSize, CUnsignedChar]

    @extern
    @link("nghttp2")
    object nghttp2 {
      def nghttp2_hd_inflate_new(
          inflater_ptr: Ptr[Ptr[nghttp2_hd_inflater]]
      ): CInt = extern

      def nghttp2_hd_inflate_del(
          inflater: Ptr[nghttp2_hd_inflater]
      ): Unit = extern

      def nghttp2_hd_inflate_hd3(
          inflater: Ptr[nghttp2_hd_inflater],
          nv_out: Ptr[nghttp2_nv],
          inflate_flags: Ptr[CInt],
          in: Ptr[Byte],
          inlen: CSize,
          in_final: CInt
      ): CSSize = extern

      def nghttp2_hd_inflate_end_headers(
          inflater: Ptr[nghttp2_hd_inflater]
      ): CInt = extern
    }
  }
}
