package scala.scalanative.sandbox.streamio.http2

import java.io.{ByteArrayOutputStream, EOFException, IOException, InputStream}
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.ArrayDeque
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.util.control.NonFatal

import scala.scalanative.sandbox.streamio.StreamIoDebug
import scala.scalanative.sandbox.streamio.transport._

trait Http2Handler {
  def onRequest(stream: Http2Stream): Unit

  def onStreamClosed(stream: Http2Stream): Unit = ()
}

final class Http2Server private (
    val transport: TcpServer
) extends AutoCloseable {
  def port: Int = transport.port
  override def close(): Unit = transport.close()
}

object Http2Server {
  def bind(
      reactor: Reactor,
      port: Int,
      handler: Http2Handler,
      host: String = "0.0.0.0",
      maxConcurrentStreams: Int = Http2Connection.DefaultMaxConcurrentStreams,
      transportOptions: TcpServerOptions = TcpServerOptions()
  ): Http2Server = {
    val tcpServer = reactor.listen(
      port,
      ServerHandlerFactory(connection =>
        new Http2Connection(connection, handler, maxConcurrentStreams)
      ),
      host,
      transportOptions
    )
    new Http2Server(tcpServer)
  }
}

trait Http2RequestBodyHandler {
  def onData(
      stream: Http2Stream,
      data: Array[Byte],
      releaseWindow: () => Unit
  ): Boolean
  def onEnd(stream: Http2Stream): Unit = ()
  def onFailure(stream: Http2Stream, cause: Throwable): Unit = ()
}

final class Http2RequestBody private[http2] (stream: Http2Stream) {
  import Http2RequestBody._

  private val events = new ArrayDeque[BodyEvent]()
  private var subscriber: Http2RequestBodyHandler = _
  private var terminated = false

  def subscribe(next: Http2RequestBodyHandler): Unit =
    stream.submit(new Runnable {
      override def run(): Unit = {
        StreamIoDebug.log("h2-server", s"stream=${stream.id} body.subscribe")
        attach(next)
      }
    })

  private def attach(next: Http2RequestBodyHandler): Unit = {
    if (subscriber != null)
      throw new IllegalStateException(
        s"request body for stream ${stream.id} already has a subscriber"
      )
    subscriber = next
    drain()
  }

  private[streamio] def requestDrain(): Unit =
    stream.submit(new Runnable {
      override def run(): Unit = drain()
    })

  private[http2] def push(bytes: Array[Byte]): Unit =
    if (!terminated && bytes.nonEmpty) {
      StreamIoDebug.log(
        "h2-server",
        s"stream=${stream.id} body.push bytes=${bytes.length} queued=${events.size()}"
      )
      events.addLast(Data(bytes, newRelease(bytes.length)))
      drain()
    }

  private[http2] def finish(): Unit =
    if (!terminated) {
      StreamIoDebug.log("h2-server", s"stream=${stream.id} body.finish")
      terminated = true
      events.addLast(End)
      drain()
    }

  private[http2] def fail(cause: Throwable): Unit =
    if (!terminated) {
      terminated = true
      events.addLast(Failure(cause))
      drain()
    }

  private[http2] def isTerminated: Boolean =
    terminated

  private def drain(): Unit =
    if (subscriber != null) {
      StreamIoDebug.log(
        "h2-server",
        s"stream=${stream.id} body.drain events=${events.size()}"
      )
      var continue = true
      while (continue && !events.isEmpty) {
        events.peekFirst() match {
          case Data(bytes, releaseWindow) =>
            if (safeOnData(bytes, releaseWindow)) events.removeFirst()
            else continue = false
          case End =>
            events.removeFirst()
            safeInvoke(subscriber.onEnd(stream))
          case Failure(cause) =>
            events.removeFirst()
            safeInvoke(subscriber.onFailure(stream, cause))
        }
      }
    }

  private def newRelease(size: Int): () => Unit = {
    val released = new AtomicBoolean(false)
    () =>
      if (released.compareAndSet(false, true))
        stream.submit(new Runnable {
          override def run(): Unit =
            stream.releaseConsumedBytes(size)
        })
  }

  private def safeOnData(
      bytes: Array[Byte],
      releaseWindow: () => Unit
  ): Boolean =
    try subscriber.onData(stream, bytes, releaseWindow)
    catch {
      case NonFatal(t) =>
        stream.failHandler(t)
        false
    }

  private def safeInvoke(body: => Unit): Unit =
    try body
    catch {
      case NonFatal(t) =>
        stream.failHandler(t)
    }
}

private object Http2RequestBody {
  sealed trait BodyEvent
  final case class Data(bytes: Array[Byte], releaseWindow: () => Unit)
      extends BodyEvent
  case object End extends BodyEvent
  final case class Failure(cause: Throwable) extends BodyEvent
}

final class Http2Stream private[http2] (
    private[http2] val state: Http2Connection.StreamState,
    private val owner: Http2Connection
) {
  def id: Int = state.id
  def requestHeaders: Vector[(String, String)] = state.requestHeaders
  def requestBody: Http2RequestBody = state.requestBody

  def sendResponseHeaders(
      status: Int,
      headers: Seq[(String, String)] = Nil,
      endStream: Boolean = false
  ): Unit =
    owner.sendResponseHeaders(this, status, headers, endStream)

  def sendData(bytes: Array[Byte], endStream: Boolean = false): Unit =
    owner.sendData(this, bytes, endStream)

  def writeUtf8(value: String, endStream: Boolean = false): Unit =
    sendData(value.getBytes(StandardCharsets.UTF_8), endStream)

  private[streamio] def submit(task: Runnable): Unit =
    owner.submit(task)

  private[http2] def releaseConsumedBytes(count: Int): Unit =
    owner.releaseConsumedBytes(this, count)

  private[http2] def failHandler(cause: Throwable): Unit =
    owner.failHandler(this, cause)

  private[streamio] def reset(
      message: String,
      errorCode: Int = Http2FrameCodec.ErrorCode.RefusedStream
  ): Unit =
    owner.reset(this, errorCode, message)
}

private final class Http2Connection(
    connection: TcpConnection,
    handler: Http2Handler,
    maxConcurrentStreams: Int
) extends ConnectionHandler {
  import Http2Connection._
  import Http2FrameCodec._

  private val decoder = new HpackDecoder
  private val encoder = new HpackEncoder
  private val streams = mutable.HashMap.empty[Int, StreamState]
  private val ignoredStreams = mutable.HashSet.empty[Int]
  private val ignoredStreamOrder = new ArrayDeque[Int]()
  private var nextLocalStreamId = 2
  private var lastRemoteStreamId = 0
  private var peerInitialWindowSize = DefaultWindowSize
  private var peerConnectionWindow = DefaultWindowSize
  private var peerMaxFrameSize = DefaultMaxFrameSize
  private var awaitingPreface = true
  private var pendingHeaderStreamId = 0
  private var pendingHeaderEndStream = false
  private val pendingHeaderBytes = new ByteArrayOutputStream()

  override def onConnected(connection: TcpConnection): Unit = {
    StreamIoDebug.log(
      "h2-server",
      s"fd=${connection.fd} onConnected send SETTINGS"
    )
    connection.writeOwned(
      encodeSettings(
        Seq(SettingId.MaxConcurrentStreams -> maxConcurrentStreams)
      )
    )
  }

  override def onReadable(
      connection: TcpConnection,
      inbound: ByteQueue
  ): Unit = {
    if (awaitingPreface) {
      if (inbound.readableBytes < ClientPreface.length) return
      if (!inbound.peekAscii(ClientPreface))
        protocolError(
          connection,
          ErrorCode.ProtocolError,
          "missing client preface"
        )
      else {
        inbound.discard(ClientPreface.length)
        awaitingPreface = false
      }
    }

    var continue = true
    while (continue && !connection.isClosed) {
      val header = tryDecodeFrameHeader(inbound)
      if (header eq null) continue = false
      else if (inbound.readableBytes < header.length) continue = false
      else {
        val payload = inbound.readBytes(header.length)
        StreamIoDebug.log(
          "h2-server",
          s"frame type=${header.tpe} stream=${header.streamId} flags=${header.flags} length=${header.length}"
        )
        handleFrame(connection, header, payload)
      }
    }
  }

  override def onClosed(connection: TcpConnection): Unit =
    try
      streams.values.foreach { state =>
        if (!state.remoteClosed)
          state.requestBody.fail(
            new IOException("request body stream closed before endStream")
          )
        handler.onStreamClosed(state.stream)
      }
    finally decoder.close()

  override def onError(
      connection: TcpConnection,
      cause: Throwable
  ): Unit =
    cause.printStackTrace()

  override def onSocketError(
      connection: TcpConnection,
      cause: SocketFailure
  ): Unit =
    if (!SocketError.isBenignDisconnect(cause))
      onError(connection, cause.toException)

  private[streamio] def submit(task: Runnable): Unit =
    connection.submit(task)

  def sendResponseHeaders(
      stream: Http2Stream,
      status: Int,
      headers: Seq[(String, String)],
      endStream: Boolean
  ): Unit = {
    StreamIoDebug.log(
      "h2-server",
      s"stream=${stream.id} sendResponseHeaders status=$status endStream=$endStream"
    )
    val block = encoder.encode((":status", status.toString) +: headers.toVector)
    val frames = encodeHeaders(
      stream.id,
      block,
      endStream = endStream,
      maxFrameSize = peerMaxFrameSize
    )
    writeFrames(frames)
    if (endStream) {
      stream.state.localClosed = true
      closeIfComplete(stream.state)
    }
  }

  def sendData(
      stream: Http2Stream,
      bytes: Array[Byte],
      endStream: Boolean
  ): Unit = {
    StreamIoDebug.log(
      "h2-server",
      s"stream=${stream.id} sendData bytes=${bytes.length} endStream=$endStream"
    )
    if (stream.state.localClosed) return
    if (bytes.isEmpty) {
      writeFrames(Vector(encodeData(stream.id, Array.empty[Byte], endStream)))
      if (endStream) {
        stream.state.localClosed = true
        closeIfComplete(stream.state)
      }
      return
    }

    var offset = 0
    var remaining = bytes.length
    while (remaining > 0) {
      val permitted =
        math.min(
          math.min(stream.state.sendWindow, peerConnectionWindow),
          peerMaxFrameSize
        )

      if (permitted <= 0) {
        val tail = java.util.Arrays.copyOfRange(bytes, offset, bytes.length)
        if (stream.state.pendingWriteBytes + tail.length > MaxPendingWriteBytes) {
          resetStream(
            stream.state,
            ErrorCode.InternalError,
            "stream outbound buffer overflow",
            failRequestBody = false
          )
          return
        }
        stream.state.pendingWrites.addLast(PendingWrite(tail, endStream))
        stream.state.pendingWriteBytes += tail.length
        return
      }

      val size = math.min(remaining, permitted)
      val chunk = java.util.Arrays.copyOfRange(bytes, offset, offset + size)
      val done = size == remaining
      writeFrames(
        Vector(encodeData(stream.id, chunk, endStream && done))
      )
      stream.state.sendWindow -= size
      peerConnectionWindow -= size
      offset += size
      remaining -= size
      if (done && endStream) {
        stream.state.localClosed = true
        closeIfComplete(stream.state)
      }
    }
  }

  private def handleFrame(
      connection: TcpConnection,
      header: FrameHeader,
      payload: Array[Byte]
  ): Unit =
    try {
      header.tpe match {
        case FrameType.Data    => onDataFrame(connection, header, payload)
        case FrameType.Headers => onHeadersFrame(connection, header, payload)
        case FrameType.Continuation =>
          onContinuationFrame(connection, header, payload)
        case FrameType.Settings => onSettingsFrame(connection, header, payload)
        case FrameType.WindowUpdate =>
          onWindowUpdateFrame(connection, header, payload)
        case FrameType.Ping      => onPingFrame(connection, header, payload)
        case FrameType.RstStream => onRstStreamFrame(header, payload)
        case FrameType.GoAway    => connection.closeWhenFlushed()
        case _                   =>
      }
    } catch {
      case NonFatal(t) =>
        protocolError(connection, ErrorCode.InternalError, t.getMessage)
    }

  private def onDataFrame(
      connection: TcpConnection,
      header: FrameHeader,
      payload: Array[Byte]
  ): Unit = {
    val stream = streams.get(header.streamId) match {
      case Some(found)                                        => found
      case None if shouldIgnoreUnknownStream(header.streamId) => return
      case None                                               =>
        throw new IOException(s"DATA on unknown stream ${header.streamId}")
    }
    val padded = (header.flags & Flag.Padded) != 0
    val padLength = if (padded) payload(0) & 0xff else 0
    val dataOffset = if (padded) 1 else 0
    val dataLength = payload.length - dataOffset - padLength
    val data =
      if (dataLength <= 0) Array.empty[Byte]
      else
        java.util.Arrays.copyOfRange(
          payload,
          dataOffset,
          dataOffset + dataLength
        )

    stream.remoteClosed = (header.flags & Flag.EndStream) != 0
    if (dataLength > 0)
      stream.requestBody.push(data)
    if (stream.remoteClosed)
      stream.requestBody.finish()
    closeIfComplete(stream)
  }

  private def onHeadersFrame(
      connection: TcpConnection,
      header: FrameHeader,
      payload: Array[Byte]
  ): Unit = {
    if (header.streamId <= 0 || (header.streamId & 1) == 0)
      protocolError(
        connection,
        ErrorCode.ProtocolError,
        "invalid client stream id"
      )

    if (!streams.contains(header.streamId) && shouldIgnoreUnknownStream(
          header.streamId
        ))
      return

    if (header.streamId > lastRemoteStreamId)
      lastRemoteStreamId = header.streamId

    val padded = (header.flags & Flag.Padded) != 0
    val priority = (header.flags & Flag.Priority) != 0
    val padLength = if (padded) payload(0) & 0xff else 0
    val start = (if (padded) 1 else 0) + (if (priority) 5 else 0)
    val length = payload.length - start - padLength
    val fragment =
      if (length <= 0) Array.empty[Byte]
      else java.util.Arrays.copyOfRange(payload, start, start + length)

    pendingHeaderStreamId = header.streamId
    pendingHeaderEndStream = (header.flags & Flag.EndStream) != 0
    pendingHeaderBytes.reset()
    pendingHeaderBytes.write(fragment)

    if ((header.flags & Flag.EndHeaders) != 0)
      finishHeaders(connection)
  }

  private def onContinuationFrame(
      connection: TcpConnection,
      header: FrameHeader,
      payload: Array[Byte]
  ): Unit = {
    if (pendingHeaderStreamId == 0 || pendingHeaderStreamId != header.streamId)
      if (shouldIgnoreUnknownStream(header.streamId)) return
      else
        protocolError(
          connection,
          ErrorCode.ProtocolError,
          "unexpected CONTINUATION"
        )

    pendingHeaderBytes.write(payload)
    if ((header.flags & Flag.EndHeaders) != 0)
      finishHeaders(connection)
  }

  private def finishHeaders(connection: TcpConnection): Unit = {
    val streamId = pendingHeaderStreamId
    val endStream = pendingHeaderEndStream
    pendingHeaderStreamId = 0
    pendingHeaderEndStream = false

    val headers = decoder.decode(pendingHeaderBytes.toByteArray)
    val stateOpt =
      streams.get(streamId).orElse {
        if (streams.size >= maxConcurrentStreams) {
          rememberIgnoredStream(streamId)
          writeFrames(
            Vector(encodeRstStream(streamId, ErrorCode.RefusedStream))
          )
          None
        } else {
          val created = newStream(streamId)
          streams(streamId) = created
          Some(created)
        }
      }

    stateOpt.foreach { state =>
      StreamIoDebug.log(
        "h2-server",
        s"stream=$streamId finishHeaders endStream=$endStream headers=${headers.mkString(",")}"
      )
      state.requestHeaders = headers
      state.remoteClosed = endStream
      if (endStream)
        state.requestBody.finish()
      safeInvoke(state.stream) {
        handler.onRequest(state.stream)
      }
      closeIfComplete(state)
    }
  }

  private def onSettingsFrame(
      connection: TcpConnection,
      header: FrameHeader,
      payload: Array[Byte]
  ): Unit = {
    if (header.streamId != 0)
      protocolError(connection, ErrorCode.ProtocolError, "SETTINGS on stream")

    if ((header.flags & Flag.Ack) != 0) {
      if (payload.nonEmpty)
        protocolError(
          connection,
          ErrorCode.FrameSizeError,
          "SETTINGS ack with payload"
        )
      return
    }

    var idx = 0
    while (idx + 6 <= payload.length) {
      val id = ((payload(idx) & 0xff) << 8) | (payload(idx + 1) & 0xff)
      val value =
        ((payload(idx + 2) & 0xff) << 24) |
          ((payload(idx + 3) & 0xff) << 16) |
          ((payload(idx + 4) & 0xff) << 8) |
          (payload(idx + 5) & 0xff)
      id match {
        case SettingId.InitialWindowSize =>
          val delta = value - peerInitialWindowSize
          peerInitialWindowSize = value
          streams.values.foreach(_.sendWindow += delta)
        case SettingId.MaxFrameSize =>
          peerMaxFrameSize = value
        case _ =>
      }
      idx += 6
    }

    writeFrames(Vector(encodeSettingsAck()))
  }

  private def onWindowUpdateFrame(
      connection: TcpConnection,
      header: FrameHeader,
      payload: Array[Byte]
  ): Unit = {
    if (payload.length != 4)
      protocolError(connection, ErrorCode.FrameSizeError, "WINDOW_UPDATE size")

    val increment =
      (((payload(0) & 0x7f) << 24) |
        ((payload(1) & 0xff) << 16) |
        ((payload(2) & 0xff) << 8) |
        (payload(3) & 0xff))

    if (increment == 0)
      protocolError(connection, ErrorCode.ProtocolError, "WINDOW_UPDATE zero")

    if (header.streamId == 0) peerConnectionWindow += increment
    else streams.get(header.streamId).foreach(_.sendWindow += increment)

    flushPendingWrites()
  }

  private def onPingFrame(
      connection: TcpConnection,
      header: FrameHeader,
      payload: Array[Byte]
  ): Unit = {
    if (payload.length != 8)
      protocolError(connection, ErrorCode.FrameSizeError, "PING size")
    else if ((header.flags & Flag.Ack) == 0)
      writeFrames(Vector(encodePingAck(payload)))
  }

  private def onRstStreamFrame(
      header: FrameHeader,
      payload: Array[Byte]
  ): Unit =
    streams.remove(header.streamId).foreach { state =>
      rememberIgnoredStream(state.id)
      state.localClosed = true
      state.remoteClosed = true
      state.requestBody.fail(
        new IOException(s"stream ${header.streamId} reset by peer")
      )
      handler.onStreamClosed(state.stream)
    }

  private def flushPendingWrites(): Unit =
    streams.values.foreach { state =>
      while (!state.pendingWrites.isEmpty &&
          state.sendWindow > 0 &&
          peerConnectionWindow > 0) {
        val next = state.pendingWrites.removeFirst()
        state.pendingWriteBytes -= next.bytes.length
        sendData(state.stream, next.bytes, next.endStream)
      }
    }

  private def writeFrames(frames: Seq[Array[Byte]]): Unit =
    frames.foreach(connection.writeOwned)

  private def protocolError(
      connection: TcpConnection,
      code: Int,
      message: String
  ): Unit = {
    writeFrames(Vector(encodeGoAway(lastRemoteStreamId, code, message)))
    connection.closeWhenFlushed()
  }

  private def newStream(id: Int): StreamState = {
    val state = new StreamState(id, peerInitialWindowSize)
    state.stream = new Http2Stream(state, this)
    state.requestBody = new Http2RequestBody(state.stream)
    state
  }

  private[http2] def releaseConsumedBytes(
      stream: Http2Stream,
      count: Int
  ): Unit =
    if (count > 0 && !connection.isClosed) {
      val frames = Vector.newBuilder[Array[Byte]]
      frames += encodeWindowUpdate(0, count)
      if (streams.contains(stream.id) && !stream.state.remoteClosed)
        frames += encodeWindowUpdate(stream.id, count)
      writeFrames(frames.result())
      stream.requestBody.requestDrain()
    }

  private def closeIfComplete(state: StreamState): Unit =
    if (state.localClosed && state.remoteClosed) {
      streams.remove(state.id)
      handler.onStreamClosed(state.stream)
    }

  private def resetStream(
      state: StreamState,
      code: Int,
      message: String,
      failRequestBody: Boolean
  ): Unit =
    if (streams.remove(state.id).nonEmpty) {
      rememberIgnoredStream(state.id)
      state.localClosed = true
      state.remoteClosed = true
      writeFrames(Vector(encodeRstStream(state.id, code)))
      if (failRequestBody && !state.requestBody.isTerminated)
        state.requestBody.fail(new IOException(message))
      handler.onStreamClosed(state.stream)
    }

  private[http2] def reset(
      stream: Http2Stream,
      code: Int,
      message: String
  ): Unit =
    resetStream(stream.state, code, message, failRequestBody = true)

  private[http2] def failHandler(
      stream: Http2Stream,
      cause: Throwable
  ): Unit =
    try {
      stream.sendResponseHeaders(500, Seq("content-type" -> "text/plain"))
      stream.writeUtf8(
        Option(cause.getMessage).getOrElse("internal error"),
        endStream = true
      )
    } catch {
      case _: Throwable =>
    }

  private def safeInvoke(stream: Http2Stream)(body: => Unit): Unit =
    try body
    catch {
      case NonFatal(t) =>
        failHandler(stream, t)
    }

  private def shouldIgnoreUnknownStream(streamId: Int): Boolean =
    ignoredStreams.contains(streamId)

  private def rememberIgnoredStream(streamId: Int): Unit =
    if (ignoredStreams.add(streamId)) {
      ignoredStreamOrder.addLast(streamId)
      if (ignoredStreamOrder.size() > MaxIgnoredStreams)
        ignoredStreams.remove(ignoredStreamOrder.removeFirst())
    }
}

private object Http2Connection {
  final val DefaultMaxConcurrentStreams = 16384
  final val DefaultWindowSize = 65535
  final val DefaultMaxFrameSize = 16384
  final val MaxIgnoredStreams = 1024

  final class StreamState(
      val id: Int,
      var sendWindow: Int
  ) {
    var requestHeaders = Vector.empty[(String, String)]
    var remoteClosed = false
    var localClosed = false
    var stream: Http2Stream = _
    var requestBody: Http2RequestBody = _
    val pendingWrites = new ArrayDeque[PendingWrite]()
    var pendingWriteBytes = 0
  }

  final val MaxPendingWriteBytes = 512 * 1024
  final case class PendingWrite(bytes: Array[Byte], endStream: Boolean)
}

private[http2] object Http2FrameCodec {
  final val ClientPreface =
    "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(StandardCharsets.US_ASCII)

  final case class FrameHeader(
      length: Int,
      tpe: Int,
      flags: Int,
      streamId: Int
  )

  object FrameType {
    final val Data = 0x0
    final val Headers = 0x1
    final val RstStream = 0x3
    final val Settings = 0x4
    final val Ping = 0x6
    final val GoAway = 0x7
    final val WindowUpdate = 0x8
    final val Continuation = 0x9
  }

  object Flag {
    final val Ack = 0x1
    final val EndStream = 0x1
    final val EndHeaders = 0x4
    final val Padded = 0x8
    final val Priority = 0x20
  }

  object SettingId {
    final val MaxConcurrentStreams = 0x3
    final val InitialWindowSize = 0x4
    final val MaxFrameSize = 0x5
  }

  object ErrorCode {
    final val NoError = 0
    final val ProtocolError = 1
    final val InternalError = 2
    final val FrameSizeError = 6
    final val RefusedStream = 7
    final val Cancel = 8
  }

  def tryDecodeFrameHeader(inbound: ByteQueue): FrameHeader =
    if (inbound.readableBytes < 9) null
    else {
      val length =
        (inbound.getByte(0) << 16) |
          (inbound.getByte(1) << 8) |
          inbound.getByte(2)
      val tpe = inbound.getByte(3)
      val flags = inbound.getByte(4)
      val streamId =
        ((inbound.getByte(5) & 0x7f) << 24) |
          (inbound.getByte(6) << 16) |
          (inbound.getByte(7) << 8) |
          inbound.getByte(8)
      inbound.discard(9)
      FrameHeader(length, tpe, flags, streamId)
    }

  def encodeSettings(settings: Seq[(Int, Int)]): Array[Byte] = {
    val payload = new Array[Byte](settings.length * 6)
    var offset = 0
    settings.foreach {
      case (id, value) =>
        payload(offset) = ((id >>> 8) & 0xff).toByte
        payload(offset + 1) = (id & 0xff).toByte
        payload(offset + 2) = ((value >>> 24) & 0xff).toByte
        payload(offset + 3) = ((value >>> 16) & 0xff).toByte
        payload(offset + 4) = ((value >>> 8) & 0xff).toByte
        payload(offset + 5) = (value & 0xff).toByte
        offset += 6
    }
    frame(FrameType.Settings, 0, 0, payload)
  }

  def encodeSettingsAck(): Array[Byte] =
    frame(FrameType.Settings, Flag.Ack, 0, Array.empty[Byte])

  def encodePingAck(payload: Array[Byte]): Array[Byte] =
    frame(FrameType.Ping, Flag.Ack, 0, payload)

  def encodeWindowUpdate(streamId: Int, increment: Int): Array[Byte] =
    frame(
      FrameType.WindowUpdate,
      0,
      streamId,
      Array[Byte](
        ((increment >>> 24) & 0x7f).toByte,
        ((increment >>> 16) & 0xff).toByte,
        ((increment >>> 8) & 0xff).toByte,
        (increment & 0xff).toByte
      )
    )

  def encodeRstStream(streamId: Int, errorCode: Int): Array[Byte] =
    frame(
      FrameType.RstStream,
      0,
      streamId,
      Array[Byte](
        ((errorCode >>> 24) & 0xff).toByte,
        ((errorCode >>> 16) & 0xff).toByte,
        ((errorCode >>> 8) & 0xff).toByte,
        (errorCode & 0xff).toByte
      )
    )

  def encodeGoAway(
      lastStreamId: Int,
      errorCode: Int,
      message: String
  ): Array[Byte] = {
    val debug = message.getBytes(StandardCharsets.UTF_8)
    val payload = new Array[Byte](8 + debug.length)
    payload(0) = ((lastStreamId >>> 24) & 0x7f).toByte
    payload(1) = ((lastStreamId >>> 16) & 0xff).toByte
    payload(2) = ((lastStreamId >>> 8) & 0xff).toByte
    payload(3) = (lastStreamId & 0xff).toByte
    payload(4) = ((errorCode >>> 24) & 0xff).toByte
    payload(5) = ((errorCode >>> 16) & 0xff).toByte
    payload(6) = ((errorCode >>> 8) & 0xff).toByte
    payload(7) = (errorCode & 0xff).toByte
    System.arraycopy(debug, 0, payload, 8, debug.length)
    frame(FrameType.GoAway, 0, 0, payload)
  }

  def encodeHeaders(
      streamId: Int,
      block: Array[Byte],
      endStream: Boolean,
      maxFrameSize: Int
  ): Vector[Array[Byte]] = {
    val frames = Vector.newBuilder[Array[Byte]]
    var offset = 0
    var first = true
    while (offset < block.length || (first && block.isEmpty)) {
      val remaining = block.length - offset
      val size = math.min(math.max(remaining, 0), maxFrameSize)
      val payload =
        if (size == 0) Array.empty[Byte]
        else java.util.Arrays.copyOfRange(block, offset, offset + size)
      val last = offset + size >= block.length
      val flags =
        (if (first && endStream) Flag.EndStream else 0) |
          (if (last) Flag.EndHeaders else 0)
      frames += frame(
        if (first) FrameType.Headers else FrameType.Continuation,
        flags,
        streamId,
        payload
      )
      offset += size
      first = false
    }
    frames.result()
  }

  def encodeData(
      streamId: Int,
      data: Array[Byte],
      endStream: Boolean
  ): Array[Byte] =
    frame(
      FrameType.Data,
      if (endStream) Flag.EndStream else 0,
      streamId,
      data
    )

  private def frame(
      tpe: Int,
      flags: Int,
      streamId: Int,
      payload: Array[Byte]
  ): Array[Byte] = {
    val frame = new Array[Byte](9 + payload.length)
    frame(0) = ((payload.length >>> 16) & 0xff).toByte
    frame(1) = ((payload.length >>> 8) & 0xff).toByte
    frame(2) = (payload.length & 0xff).toByte
    frame(3) = tpe.toByte
    frame(4) = flags.toByte
    frame(5) = ((streamId >>> 24) & 0x7f).toByte
    frame(6) = ((streamId >>> 16) & 0xff).toByte
    frame(7) = ((streamId >>> 8) & 0xff).toByte
    frame(8) = (streamId & 0xff).toByte
    System.arraycopy(payload, 0, frame, 9, payload.length)
    frame
  }
}

private[http2] final class HpackDecoder extends AutoCloseable {
  private val inflater = new Nghttp2Hpack.Inflater

  def decode(block: Array[Byte]): Vector[(String, String)] =
    inflater.decode(block)

  override def close(): Unit =
    inflater.close()
}

private[http2] final class HpackEncoder {
  import HpackTable._

  def encode(headers: Seq[(String, String)]): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    headers.foreach {
      case (name, value) =>
        exactIndex(name, value) match {
          case Some(index) =>
            writeInteger(out, 0x80, 7, index)
          case None =>
            nameIndex(name) match {
              case Some(index) =>
                writeInteger(out, 0x00, 4, index)
                writeString(out, value)
              case None =>
                out.write(0x00)
                writeString(out, name)
                writeString(out, value)
            }
        }
    }
    out.toByteArray
  }

  private def writeString(out: ByteArrayOutputStream, value: String): Unit = {
    val bytes = value.getBytes(StandardCharsets.UTF_8)
    writeInteger(out, 0x00, 7, bytes.length)
    out.write(bytes, 0, bytes.length)
  }

  private def writeInteger(
      out: ByteArrayOutputStream,
      prefixMask: Int,
      prefixBits: Int,
      value: Int
  ): Unit = {
    val maxPrefix = (1 << prefixBits) - 1
    if (value < maxPrefix) out.write(prefixMask | value)
    else {
      out.write(prefixMask | maxPrefix)
      var remaining = value - maxPrefix
      while (remaining >= 128) {
        out.write((remaining & 0x7f) | 0x80)
        remaining >>>= 7
      }
      out.write(remaining)
    }
  }
}

private object HpackTable {
  val StaticTable: Array[(String, String)] = Array(
    ":authority" -> "",
    ":method" -> "GET",
    ":method" -> "POST",
    ":path" -> "/",
    ":path" -> "/index.html",
    ":scheme" -> "http",
    ":scheme" -> "https",
    ":status" -> "200",
    ":status" -> "204",
    ":status" -> "206",
    ":status" -> "304",
    ":status" -> "400",
    ":status" -> "404",
    ":status" -> "500",
    "accept-charset" -> "",
    "accept-encoding" -> "gzip, deflate",
    "accept-language" -> "",
    "accept-ranges" -> "",
    "accept" -> "",
    "access-control-allow-origin" -> "",
    "age" -> "",
    "allow" -> "",
    "authorization" -> "",
    "cache-control" -> "",
    "content-disposition" -> "",
    "content-encoding" -> "",
    "content-language" -> "",
    "content-length" -> "",
    "content-location" -> "",
    "content-range" -> "",
    "content-type" -> "",
    "cookie" -> "",
    "date" -> "",
    "etag" -> "",
    "expect" -> "",
    "expires" -> "",
    "from" -> "",
    "host" -> "",
    "if-match" -> "",
    "if-modified-since" -> "",
    "if-none-match" -> "",
    "if-range" -> "",
    "if-unmodified-since" -> "",
    "last-modified" -> "",
    "link" -> "",
    "location" -> "",
    "max-forwards" -> "",
    "proxy-authenticate" -> "",
    "proxy-authorization" -> "",
    "range" -> "",
    "referer" -> "",
    "refresh" -> "",
    "retry-after" -> "",
    "server" -> "",
    "set-cookie" -> "",
    "strict-transport-security" -> "",
    "transfer-encoding" -> "",
    "user-agent" -> "",
    "vary" -> "",
    "via" -> "",
    "www-authenticate" -> ""
  )

  def exactIndex(name: String, value: String): Option[Int] = {
    var idx = 0
    while (idx < StaticTable.length) {
      val header = StaticTable(idx)
      if (header._1 == name && header._2 == value) return Some(idx + 1)
      idx += 1
    }
    None
  }

  def nameIndex(name: String): Option[Int] = {
    var idx = 0
    while (idx < StaticTable.length) {
      if (StaticTable(idx)._1 == name) return Some(idx + 1)
      idx += 1
    }
    None
  }
}

final case class LoopbackResponse(
    headers: Vector[(String, String)],
    body: Array[Byte]
) {
  def header(name: String): Option[String] =
    headers.collectFirst { case (`name`, value) => value }

  def bodyUtf8: String = new String(body, StandardCharsets.UTF_8)
}

final class LoopbackHttp2Client(host: String, port: Int) extends AutoCloseable {
  import Http2FrameCodec._

  private val socket = new Socket(host, port)
  private val in = socket.getInputStream
  private val out = socket.getOutputStream
  private val encoder = new HpackEncoder
  private val decoder = new HpackDecoder

  out.write(ClientPreface)
  out.write(encodeSettings(Nil))
  out.flush()
  expectServerSettings()

  def request(
      headers: Seq[(String, String)],
      dataFrames: Seq[Array[Byte]]
  ): LoopbackResponse = {
    val block = encoder.encode(headers)
    encodeHeaders(
      streamId = 1,
      block = block,
      endStream = dataFrames.isEmpty,
      maxFrameSize = 16384
    ).foreach(writeFrame)

    dataFrames.zipWithIndex.foreach {
      case (bytes, idx) =>
        writeFrame(
          encodeData(1, bytes, endStream = idx == dataFrames.length - 1)
        )
    }

    readResponse()
  }

  override def close(): Unit = socket.close()

  private def expectServerSettings(): Unit = {
    val frame = readFrame()
    if (frame._1.tpe != FrameType.Settings)
      throw new IOException("expected initial SETTINGS frame")
    writeFrame(encodeSettingsAck())
  }

  private def readResponse(): LoopbackResponse = {
    val body = new ByteArrayOutputStream()
    var responseHeaders = Vector.empty[(String, String)]
    var done = false
    while (!done) {
      val (header, payload) = readFrame()
      header.tpe match {
        case FrameType.Settings =>
          if ((header.flags & Flag.Ack) == 0) writeFrame(encodeSettingsAck())
        case FrameType.Headers =>
          responseHeaders = decoder.decode(payload)
          if ((header.flags & Flag.EndStream) != 0) done = true
        case FrameType.Data =>
          body.write(payload)
          if ((header.flags & Flag.EndStream) != 0) done = true
        case FrameType.GoAway =>
          done = true
        case _ =>
      }
    }
    LoopbackResponse(responseHeaders, body.toByteArray)
  }

  private def writeFrame(frame: Array[Byte]): Unit = {
    out.write(frame)
    out.flush()
  }

  private def readFrame(): (FrameHeader, Array[Byte]) = {
    val headerBytes = readFully(in, 9)
    val header =
      FrameHeader(
        ((headerBytes(0) & 0xff) << 16) |
          ((headerBytes(1) & 0xff) << 8) |
          (headerBytes(2) & 0xff),
        headerBytes(3) & 0xff,
        headerBytes(4) & 0xff,
        ((headerBytes(5) & 0x7f) << 24) |
          ((headerBytes(6) & 0xff) << 16) |
          ((headerBytes(7) & 0xff) << 8) |
          (headerBytes(8) & 0xff)
      )
    val payload =
      if (header.length == 0) Array.empty[Byte]
      else readFully(in, header.length)
    (header, payload)
  }

  private def readFully(in: InputStream, length: Int): Array[Byte] = {
    val bytes = new Array[Byte](length)
    var offset = 0
    while (offset < length) {
      val read = in.read(bytes, offset, length - offset)
      if (read < 0) throw new EOFException()
      offset += read
    }
    bytes
  }
}
