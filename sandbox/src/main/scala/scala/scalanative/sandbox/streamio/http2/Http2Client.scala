package scala.scalanative.sandbox.streamio.http2

import java.io.{ByteArrayOutputStream, IOException}
import java.nio.charset.StandardCharsets
import java.util.ArrayDeque
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.util.control.NonFatal

import scala.scalanative.sandbox.streamio.StreamIoDebug
import scala.scalanative.sandbox.streamio.transport._

trait Http2ClientLifecycleHandler {
  def onReady(client: Http2Client): Unit
  def onClosed(client: Http2Client): Unit = ()
  def onError(client: Http2Client, cause: Throwable): Unit = ()
}

trait Http2ClientStreamHandler {
  def onHeaders(
      stream: Http2ClientStream,
      headers: Vector[(String, String)],
      endStream: Boolean
  ): Unit

  def onData(
      stream: Http2ClientStream,
      data: Array[Byte],
      endStream: Boolean,
      releaseWindow: () => Unit
  ): Boolean

  def onComplete(stream: Http2ClientStream): Unit = ()
  def onError(stream: Http2ClientStream, cause: Throwable): Unit = ()
}

final class Http2Client private[http2] (
    private val transport: TcpConnection,
    private val owner: Http2ClientConnection
) extends AutoCloseable {
  def openStream(
      headers: Seq[(String, String)],
      handler: Http2ClientStreamHandler,
      endStream: Boolean = true
  ): Http2ClientStream =
    owner.openStream(headers, handler, endStream)

  private[streamio] def submit(task: Runnable): Unit =
    transport.submit(task)

  override def close(): Unit =
    transport.close()
}

object Http2Client {
  def connect(
      reactor: Reactor,
      host: String,
      port: Int,
      handler: Http2ClientLifecycleHandler
  ): Http2Client = {
    val clientHandler = new Http2ClientConnection(handler)
    val transport = reactor.connect(host, port, clientHandler)
    val client = new Http2Client(transport, clientHandler)
    clientHandler.attach(client)
    client
  }
}

final class Http2ClientStream private[http2] (
    private[http2] val state: Http2ClientConnection.StreamState,
    private val owner: Http2ClientConnection
) {
  def id: Int = state.id
  def requestHeaders: Vector[(String, String)] = state.requestHeaders

  def sendData(bytes: Array[Byte], endStream: Boolean = false): Unit =
    owner.sendData(this, bytes, endStream)

  def writeUtf8(value: String, endStream: Boolean = false): Unit =
    sendData(value.getBytes(StandardCharsets.UTF_8), endStream)

  private[streamio] def submit(task: Runnable): Unit =
    owner.submit(task)

  private[streamio] def reset(
      message: String,
      errorCode: Int = Http2FrameCodec.ErrorCode.Cancel
  ): Unit =
    owner.reset(this, errorCode, message)
}

private final class Http2ClientConnection(
    lifecycleHandler: Http2ClientLifecycleHandler
) extends ConnectionHandler {
  import Http2ClientConnection._
  import Http2FrameCodec._

  private val decoder = new HpackDecoder
  private val encoder = new HpackEncoder
  private val streams = mutable.HashMap.empty[Int, StreamState]
  private val ignoredStreams = mutable.HashSet.empty[Int]
  private val ignoredStreamOrder = new ArrayDeque[Int]()
  private var client: Http2Client = _
  private var ready = false
  private var lifecycleFailed = false
  private var nextLocalStreamId = 1
  private var peerInitialWindowSize = DefaultWindowSize
  private var peerConnectionWindow = DefaultWindowSize
  private var peerMaxFrameSize = DefaultMaxFrameSize
  private var pendingHeaderStreamId = 0
  private var pendingHeaderEndStream = false
  private val pendingHeaderBytes = new ByteArrayOutputStream()
  private var transport: TcpConnection = _

  def attach(client: Http2Client): Unit =
    this.client = client

  override def onConnected(connection: TcpConnection): Unit = {
    StreamIoDebug.log(
      "h2-client",
      s"fd=${connection.fd} onConnected send preface/settings"
    )
    transport = connection
    connection.writeOwned(ClientPreface)
    connection.writeOwned(encodeSettings(Nil))
  }

  override def onReadable(
      connection: TcpConnection,
      inbound: ByteQueue
  ): Unit = {
    var continue = true
    while (continue && !connection.isClosed) {
      val header = tryDecodeFrameHeader(inbound)
      if (header eq null) continue = false
      else if (inbound.readableBytes < header.length) continue = false
      else {
        val payload = inbound.readBytes(header.length)
        StreamIoDebug.log(
          "h2-client",
          s"frame type=${header.tpe} stream=${header.streamId} flags=${header.flags} length=${header.length}"
        )
        handleFrame(connection, header, payload)
      }
    }
  }

  override def onClosed(connection: TcpConnection): Unit =
    try {
      if (!ready && !lifecycleFailed && client != null)
        reportLifecycleError(
          new IOException("connection closed before HTTP/2 settings completed")
        )
      failOpenStreams(new IOException("HTTP/2 client connection closed"))
      if (client != null) safeLifecycle(_.onClosed(client))
    } finally decoder.close()

  override def onError(
      connection: TcpConnection,
      cause: Throwable
  ): Unit =
    if (client != null) reportLifecycleError(cause)

  override def onSocketError(
      connection: TcpConnection,
      cause: SocketFailure
  ): Unit =
    if (client != null) {
      val error =
        if (ready) cause.toException
        else
          new IOException(
            "connection rejected before HTTP/2 settings completed",
            cause.toException
          )
      reportLifecycleError(error)
    }

  private[streamio] def submit(task: Runnable): Unit =
    transport.submit(task)

  def openStream(
      headers: Seq[(String, String)],
      handler: Http2ClientStreamHandler,
      endStream: Boolean
  ): Http2ClientStream = {
    if (!ready)
      throw new IllegalStateException("HTTP/2 client not ready yet")

    val id = nextLocalStreamId
    nextLocalStreamId += 2
    val state = new StreamState(id, handler, peerInitialWindowSize)
    state.requestHeaders = headers.toVector
    state.stream = new Http2ClientStream(state, this)
    streams(id) = state
    StreamIoDebug.log(
      "h2-client",
      s"stream=$id openStream endStream=$endStream headers=${state.requestHeaders.mkString(",")}"
    )

    val block = encoder.encode(state.requestHeaders)
    writeFrames(
      encodeHeaders(
        streamId = id,
        block = block,
        endStream = endStream,
        maxFrameSize = peerMaxFrameSize
      )
    )
    if (endStream) {
      state.localClosed = true
      closeIfComplete(state)
    }
    state.stream
  }

  def sendData(
      stream: Http2ClientStream,
      bytes: Array[Byte],
      endStream: Boolean
  ): Unit = {
    StreamIoDebug.log(
      "h2-client",
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
            "client outbound stream buffer overflow"
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
        case FrameType.GoAway    => onGoAwayFrame(connection, payload)
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
    val state = streams.get(header.streamId) match {
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

    state.remoteClosed = (header.flags & Flag.EndStream) != 0
    StreamIoDebug.log(
      "h2-client",
      s"stream=${state.id} onDataFrame dataLength=$dataLength endStream=${state.remoteClosed}"
    )
    if (dataLength > 0 || state.remoteClosed) {
      state.pendingReads.addLast(
        PendingRead(data, state.remoteClosed, newRelease(state, dataLength))
      )
      drainPendingReads(state)
    } else closeIfComplete(state)
  }

  private def onHeadersFrame(
      connection: TcpConnection,
      header: FrameHeader,
      payload: Array[Byte]
  ): Unit = {
    val state = streams.get(header.streamId) match {
      case Some(found)                                        => found
      case None if shouldIgnoreUnknownStream(header.streamId) => return
      case None                                               =>
        throw new IOException(s"HEADERS on unknown stream ${header.streamId}")
    }

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
      finishHeaders(state)
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
    if ((header.flags & Flag.EndHeaders) != 0) {
      val state = streams.getOrElse(
        header.streamId,
        throw new IOException(
          s"CONTINUATION on unknown stream ${header.streamId}"
        )
      )
      finishHeaders(state)
    }
  }

  private def finishHeaders(state: StreamState): Unit = {
    val endStream = pendingHeaderEndStream
    pendingHeaderStreamId = 0
    pendingHeaderEndStream = false

    val headers = decoder.decode(pendingHeaderBytes.toByteArray)
    StreamIoDebug.log(
      "h2-client",
      s"stream=${state.id} finishHeaders endStream=$endStream headers=${headers.mkString(",")}"
    )
    state.responseHeaders = headers
    state.remoteClosed = endStream
    safeInvoke(state) {
      state.handler.onHeaders(state.stream, headers, endStream)
    }
    closeIfComplete(state)
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
    if (!ready && client != null) {
      ready = true
      StreamIoDebug.log("h2-client", "connection ready")
      safeLifecycle(_.onReady(client))
    }
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
  ): Unit = {
    val code =
      if (payload.length == 4)
        ((payload(0) & 0xff) << 24) |
          ((payload(1) & 0xff) << 16) |
          ((payload(2) & 0xff) << 8) |
          (payload(3) & 0xff)
      else ErrorCode.InternalError

    streams.remove(header.streamId).foreach { state =>
      rememberIgnoredStream(state.id)
      state.localClosed = true
      state.remoteClosed = true
      safeStreamError(state, new IOException(s"RST_STREAM error=$code"))
    }
  }

  private def onGoAwayFrame(
      connection: TcpConnection,
      payload: Array[Byte]
  ): Unit = {
    val message =
      if (payload.length <= 8) "GOAWAY"
      else new String(payload, 8, payload.length - 8, StandardCharsets.UTF_8)
    failOpenStreams(new IOException(message))
    connection.closeWhenFlushed()
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
    frames.foreach(transport.writeOwned)

  private def protocolError(
      connection: TcpConnection,
      code: Int,
      message: String
  ): Unit = {
    writeFrames(Vector(encodeGoAway(0, code, message)))
    connection.closeWhenFlushed()
  }

  private def closeIfComplete(state: StreamState): Unit =
    if (state.localClosed && state.remoteClosed && state.pendingReads.isEmpty) {
      streams.remove(state.id)
      safeStreamComplete(state)
    }

  private def drainPendingReads(state: StreamState): Unit = {
    if (!state.pendingReads.isEmpty)
      StreamIoDebug.log(
        "h2-client",
        s"stream=${state.id} drainPendingReads size=${state.pendingReads.size()}"
      )
    var continue = true
    while (continue && !state.pendingReads.isEmpty) {
      val next = state.pendingReads.peekFirst()
      if (safeOnData(
            state,
            next.bytes,
            next.endStream,
            next.releaseWindow
          )) {
        state.pendingReads.removeFirst()
        closeIfComplete(state)
      } else continue = false
    }
  }

  private def newRelease(
      state: StreamState,
      size: Int
  ): () => Unit = {
    val released = new AtomicBoolean(false)
    () =>
      if (released.compareAndSet(false, true))
        state.stream.submit(new Runnable {
          override def run(): Unit =
            releaseConsumedBytes(state, size)
        })
  }

  private def releaseConsumedBytes(
      state: StreamState,
      count: Int
  ): Unit = {
    if (transport != null && !transport.isClosed) {
      StreamIoDebug.log(
        "h2-client",
        s"stream=${state.id} releaseConsumedBytes count=$count"
      )
      if (count > 0) {
        val frames = Vector.newBuilder[Array[Byte]]
        frames += encodeWindowUpdate(0, count)
        if (streams.contains(state.id) && !state.remoteClosed)
          frames += encodeWindowUpdate(state.id, count)
        writeFrames(frames.result())
      }
      drainPendingReads(state)
    }
  }

  private def resetStream(
      state: StreamState,
      code: Int,
      message: String
  ): Unit =
    if (streams.remove(state.id).nonEmpty) {
      rememberIgnoredStream(state.id)
      state.localClosed = true
      state.remoteClosed = true
      writeFrames(Vector(encodeRstStream(state.id, code)))
      safeStreamError(state, new IOException(message))
    }

  private[http2] def reset(
      stream: Http2ClientStream,
      code: Int,
      message: String
  ): Unit =
    resetStream(stream.state, code, message)

  private def shouldIgnoreUnknownStream(streamId: Int): Boolean =
    ignoredStreams.contains(streamId)

  private def rememberIgnoredStream(streamId: Int): Unit =
    if (ignoredStreams.add(streamId)) {
      ignoredStreamOrder.addLast(streamId)
      if (ignoredStreamOrder.size() > MaxIgnoredStreams)
        ignoredStreams.remove(ignoredStreamOrder.removeFirst())
    }

  private def failOpenStreams(cause: Throwable): Unit = {
    val open = streams.values.toList
    streams.clear()
    open.foreach(safeStreamError(_, cause))
  }

  private def safeInvoke(state: StreamState)(body: => Unit): Unit =
    try body
    catch {
      case NonFatal(t) =>
        safeStreamError(state, t)
        transport.close()
    }

  private def safeOnData(
      state: StreamState,
      bytes: Array[Byte],
      endStream: Boolean,
      releaseWindow: () => Unit
  ): Boolean =
    try state.handler.onData(state.stream, bytes, endStream, releaseWindow)
    catch {
      case NonFatal(t) =>
        safeStreamError(state, t)
        transport.close()
        false
    }

  private def safeLifecycle(f: Http2ClientLifecycleHandler => Unit): Unit =
    try f(lifecycleHandler)
    catch {
      case _: Throwable =>
    }

  private def reportLifecycleError(cause: Throwable): Unit = {
    lifecycleFailed = true
    safeLifecycle(_.onError(client, cause))
  }

  private def safeStreamComplete(state: StreamState): Unit =
    try state.handler.onComplete(state.stream)
    catch {
      case _: Throwable =>
    }

  private def safeStreamError(state: StreamState, cause: Throwable): Unit =
    try state.handler.onError(state.stream, cause)
    catch {
      case _: Throwable =>
    }
}

private object Http2ClientConnection {
  final val DefaultWindowSize = 65535
  final val DefaultMaxFrameSize = 16384
  final val MaxIgnoredStreams = 1024

  final class StreamState(
      val id: Int,
      val handler: Http2ClientStreamHandler,
      var sendWindow: Int
  ) {
    var requestHeaders = Vector.empty[(String, String)]
    var responseHeaders = Vector.empty[(String, String)]
    var remoteClosed = false
    var localClosed = false
    var stream: Http2ClientStream = _
    val pendingWrites = new ArrayDeque[PendingWrite]()
    var pendingWriteBytes = 0
    val pendingReads = new ArrayDeque[PendingRead]()
  }

  final val MaxPendingWriteBytes = 512 * 1024
  final case class PendingWrite(bytes: Array[Byte], endStream: Boolean)
  final case class PendingRead(
      bytes: Array[Byte],
      endStream: Boolean,
      releaseWindow: () => Unit
  )
}
