package scala.scalanative.sandbox.streamio.transport

import java.io.IOException
import java.nio.charset.{Charset, StandardCharsets}
import java.util.ArrayDeque
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch}

import scala.collection.mutable
import scala.util.control.NonFatal

import scala.scalanative.bsd.{kevent => bsdKevent}
import scala.scalanative.linux.epoll
import scala.scalanative.meta.LinktimeInfo
import scala.scalanative.posix
import scala.scalanative.posix.arpa.inet
import scala.scalanative.posix.errno._
import scala.scalanative.posix.netinet.{in, inOps, tcp}
import scala.scalanative.posix.pollOps._
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.timeOps._
import scala.scalanative.posix.{fcntl, poll, time, unistd}
import scala.scalanative.sandbox.streamio.StreamIoDebug
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

final class ByteQueue(initialCapacity: Int = 8192) {
  private var buffer = new Array[Byte](math.max(256, initialCapacity))
  private var readPos = 0
  private var writePos = 0

  def readableBytes: Int = writePos - readPos

  def writableBytes: Int = buffer.length - writePos

  def clear(): Unit = {
    readPos = 0
    writePos = 0
  }

  def getByte(index: Int): Int = {
    require(index >= 0 && index < readableBytes, s"index=$index")
    buffer(readPos + index) & 0xff
  }

  def readByte(): Int = {
    val result = getByte(0)
    discard(1)
    result
  }

  def discard(count: Int): Unit = {
    require(count >= 0 && count <= readableBytes, s"count=$count")
    readPos += count
    if (readPos == writePos) clear()
  }

  def readBytes(count: Int): Array[Byte] = {
    require(count >= 0 && count <= readableBytes, s"count=$count")
    val bytes = new Array[Byte](count)
    System.arraycopy(buffer, readPos, bytes, 0, count)
    discard(count)
    bytes
  }

  def readString(
      count: Int,
      charset: Charset = StandardCharsets.UTF_8
  ): String =
    new String(readBytes(count), charset)

  def writeBytes(bytes: Array[Byte]): Unit =
    writeBytes(bytes, 0, bytes.length)

  def writeBytes(bytes: Array[Byte], offset: Int, length: Int): Unit = {
    require(offset >= 0 && length >= 0 && offset + length <= bytes.length)
    ensureWritable(length)
    System.arraycopy(bytes, offset, buffer, writePos, length)
    writePos += length
  }

  def writeAscii(value: String): Unit =
    writeBytes(value.getBytes(StandardCharsets.US_ASCII))

  def peekAscii(expected: Array[Byte]): Boolean = {
    if (readableBytes < expected.length) false
    else {
      var idx = 0
      var matches = true
      while (idx < expected.length && matches) {
        matches = buffer(readPos + idx) == expected(idx)
        idx += 1
      }
      matches
    }
  }

  private[transport] def ensureWritable(count: Int): Unit = {
    if (count <= writableBytes) return
    compact()
    if (count <= writableBytes) return

    val required = writePos + count
    var nextCapacity = buffer.length
    while (nextCapacity < required) nextCapacity *= 2

    val next = new Array[Byte](nextCapacity)
    val readable = readableBytes
    if (readable > 0)
      System.arraycopy(buffer, readPos, next, 0, readable)
    buffer = next
    readPos = 0
    writePos = readable
  }

  private[transport] def backingArray: Array[Byte] = buffer

  private[transport] def readerIndex: Int = readPos

  private[transport] def writerIndex: Int = writePos

  private[transport] def advanceWrite(count: Int): Unit = {
    require(count >= 0)
    writePos += count
  }

  private def compact(): Unit = {
    if (readPos == 0) ()
    else if (readPos == writePos) clear()
    else {
      val readable = readableBytes
      System.arraycopy(buffer, readPos, buffer, 0, readable)
      readPos = 0
      writePos = readable
    }
  }
}

trait ConnectionHandler {
  def onConnected(connection: TcpConnection): Unit = ()
  def onReadable(connection: TcpConnection, inbound: ByteQueue): Unit
  def onClosed(connection: TcpConnection): Unit = ()
  def onConnectionFailure(
      connection: TcpConnection,
      cause: ConnectionFailure
  ): Unit =
    cause match {
      case socket: SocketFailure =>
        onSocketError(connection, socket)
      case other =>
        onError(connection, other.toException)
    }
  def onSocketError(connection: TcpConnection, cause: SocketFailure): Unit =
    onError(connection, cause.toException)
  def onError(connection: TcpConnection, cause: Throwable): Unit = ()
}

sealed trait ConnectionFailure {
  def fd: Int
  def message: String
  def toException: IOException
}

final class SocketFailure private[transport] (
    val operation: String,
    val fd: Int,
    val errnoCode: Int
) extends ConnectionFailure {
  override def message: String =
    s"$operation(fd=$fd) failed, errno=$errnoCode"

  def toException: SocketIOException =
    new SocketIOException(operation, fd, errnoCode)
}

final class SocketIOException private[transport] (
    val operation: String,
    val fd: Int,
    val errnoCode: Int
) extends IOException(s"$operation(fd=$fd) failed, errno=$errnoCode") {
  override def fillInStackTrace(): Throwable = this
}

final class TransportFailure private[transport] (
    val code: String,
    val fd: Int,
    val detail: String
) extends ConnectionFailure {
  override def message: String = detail

  override def toException: TransportIOException =
    new TransportIOException(code, fd, detail)
}

final class TransportIOException private[transport] (
    val code: String,
    val fd: Int,
    detail: String
) extends IOException(detail) {
  override def fillInStackTrace(): Throwable = this
}

object SocketError {
  def recv(fd: Int, errnoCode: Int): SocketFailure =
    new SocketFailure("recv", fd, errnoCode)

  def send(fd: Int, errnoCode: Int): SocketFailure =
    new SocketFailure("send", fd, errnoCode)

  def connect(fd: Int, errnoCode: Int): SocketFailure =
    new SocketFailure("connect", fd, errnoCode)

  def isBenignDisconnect(cause: SocketFailure): Boolean =
    cause.errnoCode == ECONNRESET ||
      cause.errnoCode == EPIPE ||
      cause.errnoCode == ENOTCONN ||
      cause.errnoCode == ETIMEDOUT ||
      cause.errnoCode == ECONNABORTED

  def isBenignDisconnect(cause: Throwable): Boolean =
    cause match {
      case socket: SocketIOException =>
        isBenignDisconnect(
          new SocketFailure(socket.operation, socket.fd, socket.errnoCode)
        )
      case _ => false
    }

  def isConnectFailure(cause: SocketFailure): Boolean =
    cause.operation == "connect" &&
      (cause.errnoCode == ETIMEDOUT ||
      cause.errnoCode == ECONNREFUSED ||
      cause.errnoCode == ECONNRESET ||
      cause.errnoCode == ECONNABORTED ||
      cause.errnoCode == EHOSTUNREACH ||
      cause.errnoCode == ENETUNREACH)

  def isConnectFailure(cause: Throwable): Boolean =
    cause match {
      case socket: SocketIOException if socket.operation == "connect" =>
        isConnectFailure(
          new SocketFailure(socket.operation, socket.fd, socket.errnoCode)
        )
      case _ => false
    }
}

object TransportError {
  final val MissingFailure = "missing_failure_in_scope"
  final val OutboundBufferOverflow = "outbound_buffer_overflow"
  final val WakeupCreate = "wakeup_create_failed"
  final val WakeupSignal = "wakeup_signal_failed"
  final val WakeupDrain = "wakeup_drain_failed"
  final val SelectorCreate = "selector_create_failed"
  final val SelectorControl = "selector_control_failed"
  final val SelectorWait = "selector_wait_failed"
  final val PollWait = "poll_wait_failed"
  final val FcntlGetFlags = "fcntl_getfl_failed"
  final val FcntlSetFlags = "fcntl_setfl_failed"
  final val SocketCreate = "socket_create_failed"
  final val SocketOption = "socket_option_failed"
  final val AddressParse = "address_parse_failed"
  final val Bind = "bind_failed"
  final val Listen = "listen_failed"
  final val ConnectStatus = "connect_status_failed"
  final val LocalPort = "local_port_failed"
  final val Accept = "accept_failed"

  def outboundBufferOverflow(
      fd: Int,
      queuedBytes: Int,
      attemptedBytes: Int,
      maxQueuedWriteBytes: Int
  ): TransportFailure =
    new TransportFailure(
      code = OutboundBufferOverflow,
      fd = fd,
      detail =
        s"connection outbound buffer overflow (queued=$queuedBytes, attempted=$attemptedBytes, max=$maxQueuedWriteBytes)"
    )

  def syscall(
      code: String,
      operation: String,
      fd: Int,
      errnoCode: Int
  ): TransportFailure =
    new TransportFailure(
      code = code,
      fd = fd,
      detail = s"$operation(fd=$fd) failed, errno=$errnoCode"
    )

  def invalidAddress(host: String): TransportFailure =
    new TransportFailure(
      code = AddressParse,
      fd = -1,
      detail = s"inet_pton($host) failed"
    )

  def missingFailure(context: String): TransportFailure =
    new TransportFailure(
      code = MissingFailure,
      fd = -1,
      detail = s"$context expected a failure in scope but none was recorded"
    )
}

private final class FailureScope {
  private var failure: ConnectionFailure = _

  def fail(cause: ConnectionFailure): Unit =
    if ((failure eq null) && (cause ne null))
      failure = cause

  def isFailed: Boolean =
    failure ne null

  def failureOrNull: ConnectionFailure =
    failure

  def checkForError(): Done.type = {
    if (failure ne null)
      throw failure.toException
    Done
  }
}

private case object Done

private final class Res[+A](private val raw: A) extends AnyVal

private object Res {
  def value[A](raw: A): Res[A] =
    new Res(raw)

  def done: Res[Done.type] =
    new Res(Done)

  def fail[A](
      cause: ConnectionFailure
  )(implicit failures: FailureScope): Res[A] = {
    failures.fail(cause)
    failed[A]
  }

  def failed[A](implicit failures: FailureScope): Res[A] = {
    if (!failures.isFailed)
      failures.fail(
        TransportError.missingFailure("Res.failed")
      )
    new Res(null.asInstanceOf[A])
  }

  implicit final class ResOps[A](private val res: Res[A]) {
    def checkForError(implicit failures: FailureScope): A = {
      failures.checkForError()
      res.raw
    }

    def valueOr[A1 >: A](fallback: => A1)(implicit failures: FailureScope): A1 =
      if (failures.isFailed) fallback
      else res.raw
  }
}

trait ServerHandlerFactory {
  def create(connection: TcpConnection): ConnectionHandler
}

object ServerHandlerFactory {
  def apply(
      f: TcpConnection => ConnectionHandler
  ): ServerHandlerFactory = new ServerHandlerFactory {
    override def create(connection: TcpConnection): ConnectionHandler =
      f(connection)
  }
}

trait Reactor extends AutoCloseable {
  def submit(task: Runnable): Unit
  def wakeup(): Unit
  def stop(): Unit
  def listen(
      port: Int,
      factory: ServerHandlerFactory,
      host: String = "0.0.0.0",
      options: TcpServerOptions = TcpServerOptions()
  ): TcpServer
  def connect(
      host: String,
      port: Int,
      handler: ConnectionHandler,
      options: TcpConnectionOptions = TcpConnectionOptions()
  ): TcpConnection
  def run(): Unit
  def runOnce(timeoutMillis: Int): Int
  private[streamio] def unregisterServer(fd: Int): Unit
  private[streamio] def unregisterConnection(fd: Int): Unit
  private[streamio] def updateInterest(fd: Int, interest: Int): Unit
}

sealed trait AcceptOverloadStrategy

object AcceptOverloadStrategy {
  case object PauseAccepting extends AcceptOverloadStrategy
  case object RejectAccepted extends AcceptOverloadStrategy
}

final case class TcpConnectionOptions(
    maxQueuedWriteBytes: Int = 4 * 1024 * 1024,
    maxReadBytesPerCycle: Int = 64 * 1024
)

final case class TcpServerOptions(
    backlog: Int = socket.SOMAXCONN,
    maxOpenConnections: Int = Int.MaxValue,
    overloadStrategy: AcceptOverloadStrategy =
      AcceptOverloadStrategy.PauseAccepting,
    maxAcceptsPerCycle: Int = 256,
    childConnectionOptions: TcpConnectionOptions = TcpConnectionOptions()
)

object Reactor {
  def polling(
      maxEvents: Int = 1024,
      idleTimeoutMillis: Int = 100
  ): PollingReactor =
    new PollingReactor(maxEvents, idleTimeoutMillis)
}

final class TcpServer private[transport] (
    private[transport] val reactor: Reactor,
    val fd: Int,
    val port: Int,
    val options: TcpServerOptions
) extends AutoCloseable {
  private var closed = false

  override def close(): Unit =
    if (!closed) {
      closed = true
      reactor.unregisterServer(fd)
      unistd.close(fd)
    }
}

final class TcpConnection private[transport] (
    private[transport] val reactor: Reactor,
    val fd: Int,
    val options: TcpConnectionOptions
) extends AutoCloseable {
  private final class OutboundChunk(
      val bytes: Array[Byte],
      var offset: Int
  ) {
    def remaining: Int = bytes.length - offset
  }

  private val outbound = new ArrayDeque[OutboundChunk]()
  private val inboundBuffer = new ByteQueue(16 * 1024)
  private val maxQueuedWriteBytes = math.max(1, options.maxQueuedWriteBytes)
  private val maxReadBytesPerCycle =
    if (options.maxReadBytesPerCycle <= 0) Int.MaxValue
    else options.maxReadBytesPerCycle
  private var handler: ConnectionHandler = _
  private var connecting = false
  private var writeInterest = false
  private var closed = false
  private var closeAfterFlush = false
  private var queuedWriteBytes = 0

  def inbound: ByteQueue = inboundBuffer

  def queuedBytes: Int = {
    queuedWriteBytes
  }

  def isClosed: Boolean = closed

  private[transport] def isConnected: Boolean = !connecting && !closed

  private[transport] def isConnecting: Boolean = connecting

  def setHandler(next: ConnectionHandler): Unit =
    handler = next

  def write(bytes: Array[Byte]): Unit = {
    val owned = java.util.Arrays.copyOf(bytes, bytes.length)
    writeOwned(owned)
  }

  def writeUtf8(value: String): Unit =
    writeOwned(value.getBytes(StandardCharsets.UTF_8))

  def closeWhenFlushed(): Unit = {
    closeAfterFlush = true
    if (outbound.isEmpty) close()
    else enableWriteInterest()
  }

  override def close(): Unit =
    if (!closed) {
      StreamIoDebug.log(
        "transport",
        s"fd=$fd close queuedBytes=$queuedWriteBytes inbound=${inboundBuffer.readableBytes}"
      )
      closed = true
      reactor.unregisterConnection(fd)
      unistd.close(fd)
      if (handler != null) {
        try handler.onClosed(this)
        catch {
          case _: Throwable =>
        }
      }
    }

  private[transport] def start(): Unit = {
    StreamIoDebug.log("transport", s"fd=$fd start onConnected")
    safeInvoke(_.onConnected(this))
  }

  private[transport] def beginConnect(): Unit = {
    connecting = true
    writeInterest = true
    StreamIoDebug.log("transport", s"fd=$fd beginConnect")
  }

  private[streamio] def submit(task: Runnable): Unit =
    reactor.submit(task)

  private[streamio] def writeOwned(bytes: Array[Byte]): Unit =
    if (!closed) {
      StreamIoDebug.log(
        "transport",
        s"fd=$fd queueWrite bytes=${bytes.length} queuedBefore=$queuedWriteBytes connecting=$connecting"
      )
      if (queuedWriteBytes + bytes.length > maxQueuedWriteBytes) {
        failConnection(
          TransportError.outboundBufferOverflow(
            fd = fd,
            queuedBytes = queuedWriteBytes,
            attemptedBytes = bytes.length,
            maxQueuedWriteBytes = maxQueuedWriteBytes
          )
        )
        return
      }
      outbound.addLast(new OutboundChunk(bytes, 0))
      queuedWriteBytes += bytes.length
      enableWriteInterest()
    }

  private[transport] def failSocket(cause: SocketFailure): Unit = {
    StreamIoDebug.log(
      "transport",
      s"fd=$fd socketFailure operation=${cause.operation} errno=${cause.errnoCode} message=${cause.message}"
    )
    safeInvokeFailure(cause)
    close()
  }

  private[transport] def failConnection(cause: TransportFailure): Unit = {
    StreamIoDebug.log(
      "transport",
      s"fd=$fd transportFailure code=${cause.code} message=${cause.message}"
    )
    safeInvokeFailure(cause)
    close()
  }

  private[transport] def onReadableReady(): Unit = {
    if (closed) return

    var keepReading = true
    var closedByPeer = false
    var sawData = false
    var readBytesThisCycle = 0

    while (keepReading && !closed) {
      inboundBuffer.ensureWritable(16 * 1024)
      val target = inboundBuffer.backingArray
      val offset = inboundBuffer.writerIndex
      val writable = inboundBuffer.writableBytes
      val rc =
        socket
          .recv(fd, target.at(offset), writable.toUInt, 0)
          .toInt

      if (rc > 0) {
        sawData = true
        readBytesThisCycle += rc
        inboundBuffer.advanceWrite(rc)
        if (readBytesThisCycle >= maxReadBytesPerCycle)
          keepReading = false
      } else if (rc == 0) {
        keepReading = false
        closedByPeer = true
      } else {
        val err = errno
        if (err == EINTR) ()
        else if (err == EAGAIN || err == EWOULDBLOCK)
          keepReading = false
        else {
          keepReading = false
          failSocket(SocketError.recv(fd, err))
        }
      }
    }

    if (sawData)
      StreamIoDebug.log(
        "transport",
        s"fd=$fd readable bytesThisCycle=$readBytesThisCycle inbound=${inboundBuffer.readableBytes}"
      )

    if (sawData && !closed)
      safeInvoke(_.onReadable(this, inboundBuffer))

    if (closedByPeer && !closed)
      close()
  }

  private[transport] def onWritableReady(): Unit = {
    if (closed) return

    if (connecting) {
      val failures = new FailureScope
      implicit val scope: FailureScope = failures
      val _: Done.type = SocketSupport.finishConnect(fd).valueOr(Done)
      failures.failureOrNull match {
        case transport: TransportFailure =>
          failConnection(transport)
          return
        case socket: SocketFailure =>
          failSocket(socket)
          return
        case null =>
      }
      connecting = false
      StreamIoDebug.log("transport", s"fd=$fd connectComplete")
      safeInvoke(_.onConnected(this))
      if (closed) return
      if (outbound.isEmpty) disableWriteInterest()
    }

    var keepWriting = true
    while (keepWriting && !closed && !outbound.isEmpty) {
      val chunk = outbound.peekFirst()
      val rc =
        socket
          .send(
            fd,
            chunk.bytes.at(chunk.offset),
            chunk.remaining.toUInt,
            socket.MSG_NOSIGNAL
          )
          .toInt

      if (rc > 0) {
        queuedWriteBytes -= rc
        chunk.offset += rc
        StreamIoDebug.log(
          "transport",
          s"fd=$fd wrote=$rc remainingChunk=${chunk.remaining} queuedNow=$queuedWriteBytes"
        )
        if (chunk.remaining == 0) outbound.removeFirst()
      } else if (rc == 0) {
        keepWriting = false
      } else {
        val err = errno
        if (err == EINTR) ()
        else if (err == EAGAIN || err == EWOULDBLOCK)
          keepWriting = false
        else {
          keepWriting = false
          failSocket(SocketError.send(fd, err))
        }
      }
    }

    if (outbound.isEmpty) {
      disableWriteInterest()
      if (closeAfterFlush) close()
    }
  }

  private def enableWriteInterest(): Unit =
    if (!writeInterest && !closed) {
      writeInterest = true
      reactor.updateInterest(fd, SelectorInterest.Read | SelectorInterest.Write)
    }

  private def disableWriteInterest(): Unit =
    if (writeInterest && !closed) {
      writeInterest = false
      reactor.updateInterest(fd, SelectorInterest.Read)
    }

  private def fail(cause: Throwable): Unit = {
    safeInvoke(_.onError(this, cause))
    close()
  }

  private def safeInvokeFailure(cause: ConnectionFailure): Unit =
    if (handler != null) {
      try handler.onConnectionFailure(this, cause)
      catch {
        case NonFatal(t) =>
          try handler.onError(this, t)
          catch {
            case _: Throwable =>
          }
      }
    }

  private def safeInvoke(f: ConnectionHandler => Unit): Unit =
    if (handler != null) {
      try f(handler)
      catch {
        case NonFatal(t) =>
          try handler.onError(this, t)
          catch {
            case _: Throwable =>
          }
          close()
      }
    }
}

private object SelectorInterest {
  final val Read = 1
  final val Write = 2
  final val Error = 4
  final val Hangup = 8
}

private trait SelectorBackend extends AutoCloseable {
  def register(fd: Int, interest: Int)(implicit
      failures: FailureScope
  ): Res[Done.type]
  def update(fd: Int, interest: Int)(implicit
      failures: FailureScope
  ): Res[Done.type]
  def unregister(fd: Int)(implicit failures: FailureScope): Res[Done.type]
  def waitEvents(
      timeoutMillis: Int
  )(handler: (Int, Int) => Unit)(implicit failures: FailureScope): Res[Int]
}

private object SelectorBackend {
  def create(
      maxEvents: Int
  )(implicit failures: FailureScope): Res[SelectorBackend] =
    if (LinktimeInfo.isLinux) EpollBackend.create(maxEvents)
    else if (LinktimeInfo.isMac || LinktimeInfo.isFreeBSD ||
        LinktimeInfo.isOpenBSD || LinktimeInfo.isNetBSD)
      KqueueBackend.create(maxEvents)
    else Res.value(new PollBackend)
}

private trait WakeupSupport extends AutoCloseable {
  def readFd: Int
  def signal()(implicit failures: FailureScope): Res[Done.type]
  def drain()(implicit failures: FailureScope): Res[Done.type]
}

private object WakeupSupport {
  def create()(implicit failures: FailureScope): Res[WakeupSupport] =
    PipeWakeupSupport.create()

  private object PipeWakeupSupport {
    def create()(implicit failures: FailureScope): Res[WakeupSupport] = {
      val fds = stackalloc[CInt](2)
      val rc = unistd.pipe(fds)
      if (rc < 0)
        Res.fail(
          TransportError.syscall(
            TransportError.WakeupCreate,
            "pipe",
            -1,
            errno
          )
        )
      else {
        val read = !fds
        val write = !(fds + 1)
        val _: Done.type = SocketSupport.setNonBlocking(read).valueOr(Done)
        if (!failures.isFailed)
          SocketSupport.setNonBlocking(write).valueOr(Done)
        if (failures.isFailed) {
          if (read >= 0)
            unistd.close(read)
          if (write >= 0)
            unistd.close(write)
          Res.failed[WakeupSupport]
        } else
          Res.value(new PipeWakeupSupport(read, write))
      }
    }
  }

  private final class PipeWakeupSupport(
      val readFd: Int,
      writeFd: Int
  ) extends WakeupSupport {
    private val pending = new AtomicBoolean(false)

    override def signal()(implicit failures: FailureScope): Res[Done.type] =
      if (pending.compareAndSet(false, true)) {
        val buf = stackalloc[Byte]()
        !buf = 1.toByte
        var keepTrying = true
        while (keepTrying) {
          val rc = unistd.write(writeFd, buf, 1.toUSize).toInt
          if (rc >= 0) keepTrying = false
          else {
            val err = errno
            if (err == EINTR) ()
            else if (err == EAGAIN || err == EWOULDBLOCK)
              keepTrying = false
            else {
              pending.set(false)
              keepTrying = false
              return Res.fail(
                TransportError.syscall(
                  TransportError.WakeupSignal,
                  "write(wakeup)",
                  writeFd,
                  err
                )
              )
            }
          }
        }
        Res.done
      } else Res.done

    override def drain()(implicit failures: FailureScope): Res[Done.type] = {
      pending.set(false)
      val buf = stackalloc[Byte](64)
      var continue = true
      while (continue) {
        val rc = unistd.read(readFd, buf, 64.toUSize).toInt
        if (rc > 0) ()
        else if (rc == 0) continue = false
        else {
          val err = errno
          if (err == EINTR) ()
          else if (err == EAGAIN || err == EWOULDBLOCK)
            continue = false
          else {
            continue = false
            return Res.fail(
              TransportError.syscall(
                TransportError.WakeupDrain,
                "read(wakeup)",
                readFd,
                err
              )
            )
          }
        }
      }
      Res.done
    }

    override def close(): Unit = {
      if (readFd >= 0) unistd.close(readFd)
      if (writeFd >= 0) unistd.close(writeFd)
    }
  }
}

private object EpollBackend {
  def create(
      maxEvents: Int
  )(implicit failures: FailureScope): Res[SelectorBackend] = {
    val fd = epoll.epoll_create1(epoll.EPOLL_CLOEXEC)
    if (fd < 0)
      Res.fail(
        TransportError.syscall(
          TransportError.SelectorCreate,
          "epoll_create1",
          -1,
          errno
        )
      )
    else Res.value(new EpollBackend(fd, maxEvents))
  }
}

private final class EpollBackend private (
    epfd: Int,
    maxEvents: Int
) extends SelectorBackend {
  import epoll._

  import SelectorInterest._

  private val eventSize = scalanative_epoll_event_size()
  private val events =
    scala.scalanative.libc.stdlib.calloc(maxEvents.toCSize, eventSize)

  override def close(): Unit = {
    scala.scalanative.libc.stdlib.free(events)
    unistd.close(epfd)
  }

  override def register(
      fd: Int,
      interest: Int
  )(implicit failures: FailureScope): Res[Done.type] =
    ctl(EPOLL_CTL_ADD, fd, interest)

  override def update(
      fd: Int,
      interest: Int
  )(implicit failures: FailureScope): Res[Done.type] =
    ctl(EPOLL_CTL_MOD, fd, interest)

  override def unregister(
      fd: Int
  )(implicit failures: FailureScope): Res[Done.type] = {
    val rc = epoll_ctl(epfd, EPOLL_CTL_DEL, fd, null)
    if (rc < 0 && errno != EBADF && errno != ENOENT)
      Res.fail(
        TransportError.syscall(
          TransportError.SelectorControl,
          "epoll_ctl DEL",
          fd,
          errno
        )
      )
    else Res.done
  }

  override def waitEvents(
      timeoutMillis: Int
  )(handler: (Int, Int) => Unit)(implicit failures: FailureScope): Res[Int] = {
    val ready = epoll_wait(epfd, events, maxEvents, timeoutMillis)
    if (ready < 0) {
      if (errno != EINTR)
        Res.fail(
          TransportError.syscall(
            TransportError.SelectorWait,
            "epoll_wait",
            epfd,
            errno
          )
        )
      else Res.value(0)
    } else {
      val ptrFlags = stackalloc[posix.stdint.uint32_t]()
      val ptrData = stackalloc[posix.stdint.uint64_t]()
      var idx = 0
      while (idx < ready) {
        scalanative_epoll_event_get(events, idx, ptrFlags, ptrData)
        idx += 1
        val raw = (!ptrFlags).toInt
        var interest = 0
        if ((raw & EPOLLIN) != 0) interest |= Read
        if ((raw & EPOLLOUT) != 0) interest |= Write
        if ((raw & EPOLLERR) != 0) interest |= Error
        if ((raw & (EPOLLHUP | EPOLLRDHUP)) != 0) interest |= Hangup
        handler((!ptrData).toInt, interest)
      }
      Res.value(ready)
    }
  }

  private def ctl(
      op: Int,
      fd: Int,
      interest: Int
  )(implicit failures: FailureScope): Res[Done.type] = {
    val event = stackalloc[Byte](eventSize)
    val flags = {
      var value = EPOLLERR | EPOLLHUP | EPOLLRDHUP | EPOLLET
      if ((interest & Read) != 0) value |= EPOLLIN
      if ((interest & Write) != 0) value |= EPOLLOUT
      value
    }
    scalanative_epoll_event_set(event, 0, flags.toUInt, fd.toULong)
    val rc = epoll_ctl(epfd, op, fd, event)
    if (rc < 0)
      Res.fail(
        TransportError.syscall(
          TransportError.SelectorControl,
          s"epoll_ctl(op=$op)",
          fd,
          errno
        )
      )
    else Res.done
  }
}

private object KqueueBackend {
  def create(
      maxEvents: Int
  )(implicit failures: FailureScope): Res[SelectorBackend] = {
    val fd = bsdKevent.kqueue()
    if (fd < 0)
      Res.fail(
        TransportError.syscall(
          TransportError.SelectorCreate,
          "kqueue",
          -1,
          errno
        )
      )
    else Res.value(new KqueueBackend(fd, maxEvents))
  }
}

private final class KqueueBackend private (
    kq: Int,
    maxEvents: Int
) extends SelectorBackend {
  import SelectorInterest._
  import bsdKevent._

  private val eventSize = scalanative_kevent_size()
  private val changeBufferBytes = 128.toCSize
  private val events =
    scala.scalanative.libc.stdlib.calloc(maxEvents.toCSize, eventSize)
  private val writeRegistered = mutable.HashSet.empty[Int]

  override def close(): Unit = {
    scala.scalanative.libc.stdlib.free(events)
    unistd.close(kq)
  }

  override def register(
      fd: Int,
      interest: Int
  )(implicit failures: FailureScope): Res[Done.type] =
    change(fd, interest)

  override def update(
      fd: Int,
      interest: Int
  )(implicit failures: FailureScope): Res[Done.type] =
    change(fd, interest)

  override def unregister(
      fd: Int
  )(implicit failures: FailureScope): Res[Done.type] = {
    val changes = stackalloc[Byte](changeBufferBytes)
    setEvent(changes, 0, fd, EVFILT_READ, EV_DELETE)
    setEvent(changes, 1, fd, EVFILT_WRITE, EV_DELETE)
    val rc = bsdKevent.kevent(kq, changes, 2, null, 0, null)
    writeRegistered.remove(fd)
    if (rc < 0 && errno != EBADF && errno != ENOENT)
      Res.fail(
        TransportError.syscall(
          TransportError.SelectorControl,
          "kevent DELETE",
          fd,
          errno
        )
      )
    else Res.done
  }

  override def waitEvents(
      timeoutMillis: Int
  )(handler: (Int, Int) => Unit)(implicit failures: FailureScope): Res[Int] = {
    val ts = stackalloc[time.timespec]()
    val tsPtr =
      if (timeoutMillis < 0) null
      else {
        val nanos = math.max(0L, timeoutMillis.toLong) * 1000000L
        ts.tv_sec = (nanos / 1000000000L).toSize
        ts.tv_nsec = (nanos % 1000000000L).toSize
        ts
      }

    val ready = bsdKevent.kevent(kq, null, 0, events, maxEvents, tsPtr)
    if (ready < 0) {
      if (errno != EINTR)
        Res.fail(
          TransportError.syscall(
            TransportError.SelectorWait,
            "kevent wait",
            kq,
            errno
          )
        )
      else Res.value(0)
    } else {
      val ident = stackalloc[posix.stdint.uintptr_t]()
      val filter = stackalloc[posix.stdint.int16_t]()
      val flags = stackalloc[posix.stdint.uint16_t]()
      var idx = 0
      while (idx < ready) {
        scalanative_kevent_get(
          events,
          idx,
          ident,
          filter,
          flags,
          null,
          null,
          null
        )
        idx += 1
        var interest = 0
        if ((!filter).toInt == EVFILT_READ) interest |= Read
        if ((!filter).toInt == EVFILT_WRITE) interest |= Write
        if (((!flags).toInt & EV_ERROR) != 0) interest |= Error
        if (((!flags).toInt & EV_EOF) != 0) interest |= Hangup
        handler((!ident).toInt, interest)
      }
      Res.value(ready)
    }
  }

  private def change(
      fd: Int,
      interest: Int
  )(implicit failures: FailureScope): Res[Done.type] = {
    val needsWrite = (interest & Write) != 0
    val hasWrite = writeRegistered.contains(fd)
    val changeCount =
      if (needsWrite || hasWrite) 2
      else 1
    val changes = stackalloc[Byte](changeBufferBytes)
    setEvent(changes, 0, fd, EVFILT_READ, EV_ADD | EV_ENABLE | EV_CLEAR)
    if (changeCount == 2) {
      val writeFlags =
        if (needsWrite) EV_ADD | EV_ENABLE | EV_CLEAR
        else EV_DELETE
      setEvent(changes, 1, fd, EVFILT_WRITE, writeFlags)
    }
    val rc = bsdKevent.kevent(kq, changes, changeCount, null, 0, null)
    if (rc < 0)
      Res.fail(
        new TransportFailure(
          TransportError.SelectorControl,
          fd,
          s"kevent register(kq=$kq, fd=$fd, interest=$interest, needsWrite=$needsWrite, hasWrite=$hasWrite) failed, errno=$errno"
        )
      )
    else if (needsWrite)
      writeRegistered += fd
    else
      writeRegistered -= fd
    Res.done
  }

  private def setEvent(
      buf: Ptr[Byte],
      idx: Int,
      fd: Int,
      filter: Int,
      flags: Int
  ): Unit =
    scalanative_kevent_set(
      buf,
      idx,
      fd.toUSize,
      filter.toShort,
      flags.toUShort,
      0.toUInt,
      0,
      null
    )
}

private final class PollBackend extends SelectorBackend {
  import SelectorInterest._

  private val registrations = mutable.ArrayBuffer.empty[(Int, Int)]

  override def close(): Unit = ()

  override def register(
      fd: Int,
      interest: Int
  )(implicit failures: FailureScope): Res[Done.type] =
    update(fd, interest)

  override def update(
      fd: Int,
      interest: Int
  )(implicit failures: FailureScope): Res[Done.type] = {
    val idx = registrations.indexWhere(_._1 == fd)
    if (idx >= 0) registrations(idx) = ((fd, interest))
    else registrations += ((fd, interest))
    Res.done
  }

  override def unregister(
      fd: Int
  )(implicit failures: FailureScope): Res[Done.type] = {
    val idx = registrations.indexWhere(_._1 == fd)
    if (idx >= 0) registrations.remove(idx)
    Res.done
  }

  override def waitEvents(
      timeoutMillis: Int
  )(handler: (Int, Int) => Unit)(implicit failures: FailureScope): Res[Int] = {
    if (registrations.isEmpty) {
      if (timeoutMillis > 0) Thread.sleep(math.min(timeoutMillis, 50))
      Res.value(0)
    } else {
      val fds = stackalloc[poll.struct_pollfd](registrations.length.toUInt)
      var idx = 0
      while (idx < registrations.length) {
        val (fd, interest) = registrations(idx)
        (fds + idx).fd = fd
        (fds + idx).revents = 0
        var events = (poll.POLLERR | poll.POLLHUP).toShort
        if ((interest & Read) != 0) events = (events | poll.POLLIN).toShort
        if ((interest & Write) != 0) events = (events | poll.POLLOUT).toShort
        (fds + idx).events = events
        idx += 1
      }

      val ready = poll.poll(fds, registrations.length.toUInt, timeoutMillis)
      if (ready < 0) {
        if (errno != EINTR)
          Res.fail(
            TransportError.syscall(
              TransportError.PollWait,
              "poll",
              -1,
              errno
            )
          )
        else Res.value(0)
      } else {
        idx = 0
        while (idx < registrations.length) {
          val fd = (fds + idx).fd
          val revents = (fds + idx).revents.toInt
          var interest = 0
          if ((revents & poll.POLLIN) != 0) interest |= Read
          if ((revents & poll.POLLOUT) != 0) interest |= Write
          if ((revents & poll.POLLERR) != 0) interest |= Error
          if ((revents & poll.POLLHUP) != 0) interest |= Hangup
          if (interest != 0) handler(fd, interest)
          idx += 1
        }
        Res.value(ready)
      }
    }
  }
}

private[transport] object SocketSupport {
  import in._

  import inOps._

  final class BindResult(val fd: Int, val boundPort: Int)
  final class ConnectResult(val fd: Int, val connectedNow: Boolean)

  def setNonBlocking(
      fd: Int
  )(implicit failures: FailureScope): Res[Done.type] = {
    val current = fcntl.fcntl(fd, fcntl.F_GETFL, 0)
    if (current < 0)
      Res.fail(
        TransportError.syscall(
          TransportError.FcntlGetFlags,
          "fcntl(F_GETFL)",
          fd,
          errno
        )
      )
    else {
      val rc =
        fcntl.fcntl(fd, fcntl.F_SETFL, current | fcntl.O_NONBLOCK)
      if (rc < 0)
        Res.fail(
          TransportError.syscall(
            TransportError.FcntlSetFlags,
            "fcntl(F_SETFL, O_NONBLOCK)",
            fd,
            errno
          )
        )
      else Res.done
    }
  }

  def bindTcpListener(
      port: Int,
      host: String = "0.0.0.0",
      backlog: Int = socket.SOMAXCONN
  )(implicit failures: FailureScope): Res[BindResult] = {
    val fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    if (fd < 0)
      Res.fail(
        TransportError.syscall(
          TransportError.SocketCreate,
          "socket",
          -1,
          errno
        )
      )
    else {
      setSockOptInt(
        fd,
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1,
        required = true
      ).valueOr(Done)
      if (!failures.isFailed)
        setSockOptInt(
          fd,
          socket.SOL_SOCKET,
          socket.SO_REUSEPORT,
          1,
          required = false
        ).valueOr(Done)
      if (!failures.isFailed)
        setNonBlocking(fd).valueOr(Done)
      if (!failures.isFailed) {
        val addr = stackalloc[sockaddr_in]()
        addr.sin_family = socket.AF_INET.toUShort
        addr.sin_port = inet.htons(port.toUShort)
        initSockAddr(addr, host, port).valueOr(Done)
        if (!failures.isFailed) {
          val bindRc =
            socket.bind(
              fd,
              addr.asInstanceOf[Ptr[socket.sockaddr]],
              sizeof[sockaddr_in].toUInt
            )
          if (bindRc < 0)
            failures.fail(
              TransportError.syscall(
                TransportError.Bind,
                "bind",
                fd,
                errno
              )
            )
          else {
            val listenRc = socket.listen(fd, math.max(1, backlog))
            if (listenRc < 0)
              failures.fail(
                TransportError.syscall(
                  TransportError.Listen,
                  "listen",
                  fd,
                  errno
                )
              )
          }
        }
      }

      val boundPort =
        if (failures.isFailed) 0
        else localPort(fd).valueOr(0)

      if (failures.isFailed) {
        unistd.close(fd)
        Res.failed[BindResult]
      } else Res.value(new BindResult(fd, boundPort))
    }
  }

  def connectTcp(
      host: String,
      port: Int
  )(implicit failures: FailureScope): Res[ConnectResult] = {
    val fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    if (fd < 0)
      Res.fail(
        TransportError.syscall(
          TransportError.SocketCreate,
          "socket",
          -1,
          errno
        )
      )
    else {
      val _: Done.type = setNonBlocking(fd).valueOr(Done)
      if (!failures.isFailed)
        setSockOptInt(
          fd,
          in.IPPROTO_TCP,
          tcp.TCP_NODELAY,
          1,
          required = false
        ).valueOr(Done)
      var connectedNow = false
      if (!failures.isFailed) {
        val addr = stackalloc[sockaddr_in]()
        val _: Done.type = initSockAddr(addr, host, port).valueOr(Done)
        if (!failures.isFailed) {
          val connectRc = socket.connect(
            fd,
            addr.asInstanceOf[Ptr[socket.sockaddr]],
            sizeof[sockaddr_in].toUInt
          )
          if (connectRc == 0) connectedNow = true
          else {
            val err = errno
            if (err == EINPROGRESS || err == EWOULDBLOCK) connectedNow = false
            else failures.fail(SocketError.connect(fd, err))
          }
        }
      }
      if (failures.isFailed) {
        unistd.close(fd)
        Res.failed[ConnectResult]
      } else Res.value(new ConnectResult(fd, connectedNow))
    }
  }

  def accept(fd: Int): Int = {
    val storage = stackalloc[socket.sockaddr_storage]()
    val len = stackalloc[socket.socklen_t]()
    !len = sizeof[socket.sockaddr_storage].toUInt
    socket.accept(
      fd,
      storage.asInstanceOf[Ptr[socket.sockaddr]],
      len
    )
  }

  def configureAccepted(
      fd: Int
  )(implicit failures: FailureScope): Res[Done.type] = {
    val _: Done.type = setNonBlocking(fd).valueOr(Done)
    if (!failures.isFailed)
      setSockOptInt(
        fd,
        in.IPPROTO_TCP,
        tcp.TCP_NODELAY,
        1,
        required = false
      ).valueOr(Done)
    Res.done
  }

  def finishConnect(
      fd: Int
  )(implicit failures: FailureScope): Res[Done.type] = {
    val opt = stackalloc[CInt]()
    val len = stackalloc[socket.socklen_t]()
    !len = sizeof[CInt].toUInt
    val rc = socket.getsockopt(
      fd,
      socket.SOL_SOCKET,
      socket.SO_ERROR,
      opt.asInstanceOf[CVoidPtr],
      len
    )
    if (rc < 0)
      failures.fail(
        TransportError.syscall(
          TransportError.ConnectStatus,
          "getsockopt(SO_ERROR)",
          fd,
          errno
        )
      )
    else {
      val connectErr = !opt
      if (connectErr != 0)
        failures.fail(SocketError.connect(fd, connectErr))
    }
    Res.done
  }

  def localPort(fd: Int)(implicit failures: FailureScope): Res[Int] = {
    val addr = stackalloc[sockaddr_in]()
    val len = stackalloc[socket.socklen_t]()
    !len = sizeof[sockaddr_in].toUInt
    val rc = socket.getsockname(
      fd,
      addr.asInstanceOf[Ptr[socket.sockaddr]],
      len
    )
    if (rc < 0)
      failures.fail(
        TransportError.syscall(
          TransportError.LocalPort,
          "getsockname",
          fd,
          errno
        )
      )
    if (failures.isFailed) Res.value(0)
    else Res.value(inet.ntohs(addr.sin_port).toInt)
  }

  private def setSockOptInt(
      fd: Int,
      level: Int,
      option: Int,
      value: Int,
      required: Boolean
  )(implicit failures: FailureScope): Res[Done.type] = {
    val opt = stackalloc[CInt]()
    !opt = value
    val rc = socket.setsockopt(
      fd,
      level,
      option,
      opt.asInstanceOf[CVoidPtr],
      sizeof[CInt].toUInt
    )
    if (required && rc < 0)
      Res.fail(
        TransportError.syscall(
          TransportError.SocketOption,
          s"setsockopt(level=$level, option=$option)",
          fd,
          errno
        )
      )
    else Res.done
  }

  private def initSockAddr(
      addr: Ptr[sockaddr_in],
      host: String,
      port: Int
  )(implicit failures: FailureScope): Res[Done.type] = {
    addr.sin_family = socket.AF_INET.toUShort
    addr.sin_port = inet.htons(port.toUShort)

    if (host == "0.0.0.0" || host == "" || host == null)
      addr.sin_addr.s_addr = inet.htonl(INADDR_ANY)
    else {
      var parseOk = true
      Zone.acquire { implicit z =>
        val rc = inet.inet_pton(
          socket.AF_INET,
          toCString(host),
          addr.at3.asInstanceOf[CVoidPtr]
        )
        if (rc != 1)
          parseOk = false
      }
      if (!parseOk)
        failures.fail(TransportError.invalidAddress(host))
    }
    Res.done
  }
}

private[streamio] final class PollingCore(
    owner: Reactor,
    maxEvents: Int = 1024
) extends AutoCloseable {
  private final class ServerRegistration(
      val server: TcpServer,
      val factory: ServerHandlerFactory,
      val options: TcpServerOptions
  ) {
    var acceptingPaused = false
  }

  private val backend = {
    val failures = new FailureScope
    implicit val scope: FailureScope = failures
    val created = SelectorBackend.create(maxEvents).checkForError
    created
  }
  private val servers = mutable.HashMap.empty[Int, ServerRegistration]
  private val connections = mutable.HashMap.empty[Int, TcpConnection]

  def listen(
      port: Int,
      factory: ServerHandlerFactory,
      host: String = "0.0.0.0",
      options: TcpServerOptions = TcpServerOptions()
  ): TcpServer = synchronized {
    val failures = new FailureScope
    implicit val scope: FailureScope = failures
    val bound =
      SocketSupport.bindTcpListener(port, host, options.backlog).checkForError
    val fd = bound.fd
    val boundPort = bound.boundPort
    try {
      val _: Done.type =
        backend.register(fd, SelectorInterest.Read).checkForError
      val server = new TcpServer(owner, fd, boundPort, options)
      servers(fd) = new ServerRegistration(server, factory, options)
      StreamIoDebug.log(
        "reactor",
        s"listen fd=$fd host=$host port=$boundPort backlog=${options.backlog}"
      )
      server
    } catch {
      case t: Throwable =>
        unistd.close(fd)
        throw t
    }
  }

  def connect(
      host: String,
      port: Int,
      handler: ConnectionHandler,
      options: TcpConnectionOptions = TcpConnectionOptions()
  ): TcpConnection = synchronized {
    val failures = new FailureScope
    implicit val scope: FailureScope = failures
    val connected = SocketSupport.connectTcp(host, port).checkForError
    val fd = connected.fd
    val connectedNow = connected.connectedNow
    val connection = new TcpConnection(owner, fd, options)
    try {
      connections(fd) = connection
      connection.setHandler(handler)
      StreamIoDebug.log(
        "reactor",
        s"connect fd=$fd host=$host port=$port connectedNow=$connectedNow"
      )
      if (connectedNow) {
        val _: Done.type =
          backend.register(fd, SelectorInterest.Read).checkForError
        connection.start()
      } else {
        connection.beginConnect()
        val _: Done.type = backend
          .register(
            fd,
            SelectorInterest.Read | SelectorInterest.Write
          )
          .checkForError
      }
      connection
    } catch {
      case t: Throwable =>
        connections.remove(fd)
        unistd.close(fd)
        throw t
    }
  }

  def registerEventSource(fd: Int, interest: Int): Unit = synchronized {
    val failures = new FailureScope
    implicit val scope: FailureScope = failures
    val _: Done.type = backend.register(fd, interest).checkForError
  }

  def unregisterEventSource(fd: Int): Unit = synchronized {
    val failures = new FailureScope
    implicit val scope: FailureScope = failures
    val _: Done.type = backend.unregister(fd).checkForError
  }

  def poll(
      timeoutMillis: Int
  )(handleEventSource: (Int, Int) => Boolean): Int = synchronized {
    val failures = new FailureScope
    implicit val scope: FailureScope = failures
    val ready =
      backend
        .waitEvents(timeoutMillis) { (fd, interest) =>
          StreamIoDebug.log("reactor", s"event fd=$fd interest=$interest")
          if (handleEventSource(fd, interest)) ()
          else if (servers.contains(fd)) onServerReady(fd)
          else
            connections.get(fd).foreach { connection =>
              if (connection.isConnecting && interest != 0)
                connection.onWritableReady()
              else if ((interest & SelectorInterest.Write) != 0)
                connection.onWritableReady()
              if (!connection.isClosed &&
                  connection.isConnected &&
                  ((interest & SelectorInterest.Read) != 0 ||
                  (interest & SelectorInterest.Hangup) != 0))
                connection.onReadableReady()
            }
        }
        .checkForError
    ready
  }

  override def close(): Unit = synchronized {
    val activeConnections = connections.values.toList
    val activeServers = servers.values.map(_.server).toList
    activeConnections.foreach(_.close())
    activeServers.foreach(_.close())
    backend.close()
  }

  private[streamio] def unregisterServer(fd: Int): Unit = synchronized {
    servers.remove(fd)
    val failures = new FailureScope
    implicit val scope: FailureScope = failures
    val _: Done.type = backend.unregister(fd).checkForError
  }

  private[streamio] def unregisterConnection(fd: Int): Unit = synchronized {
    connections.remove(fd)
    val failures = new FailureScope
    implicit val scope: FailureScope = failures
    val _: Done.type = backend.unregister(fd).checkForError
    maybeResumeServers()
  }

  private[streamio] def updateInterest(fd: Int, interest: Int): Unit =
    synchronized {
      if (connections.contains(fd)) {
        val failures = new FailureScope
        implicit val scope: FailureScope = failures
        val _: Done.type = backend.update(fd, interest).checkForError
      }
    }

  private def onServerReady(serverFd: Int): Unit = {
    val registration = servers(serverFd)
    var continue = true
    var handledThisCycle = 0
    while (continue && handledThisCycle < registration.options.maxAcceptsPerCycle) {
      if (connections.size >= registration.options.maxOpenConnections) {
        registration.options.overloadStrategy match {
          case AcceptOverloadStrategy.PauseAccepting =>
            registration.acceptingPaused = true
            val failures = new FailureScope
            implicit val scope: FailureScope = failures
            val _: Done.type = backend.unregister(serverFd).checkForError
            continue = false
          case AcceptOverloadStrategy.RejectAccepted =>
            val rejectedFd = SocketSupport.accept(serverFd)
            if (rejectedFd >= 0) {
              unistd.close(rejectedFd)
              handledThisCycle += 1
            } else {
              val err = errno
              if (err == EINTR) ()
              else if (err == EAGAIN || err == EWOULDBLOCK)
                continue = false
              else if (err == EMFILE || err == ENFILE) {
                pauseAccepting(serverFd, registration)
                continue = false
              } else {
                continue = false
                throw TransportError
                  .syscall(TransportError.Accept, "accept", serverFd, err)
                  .toException
              }
            }
        }
      } else {
        val clientFd = SocketSupport.accept(serverFd)
        if (clientFd >= 0) {
          val failures = new FailureScope
          implicit val scope: FailureScope = failures
          val _: Done.type =
            SocketSupport.configureAccepted(clientFd).valueOr(Done)
          if (!failures.isFailed)
            backend.register(clientFd, SelectorInterest.Read).valueOr(Done)
          if (failures.isFailed) {
            unistd.close(clientFd)
            failures.checkForError()
          } else {
            val connection =
              new TcpConnection(
                owner,
                clientFd,
                registration.options.childConnectionOptions
              )
            connections(clientFd) = connection
            connection.setHandler(registration.factory.create(connection))
            connection.start()
            handledThisCycle += 1
          }
        } else {
          val err = errno
          if (err == EINTR) ()
          else if (err == EAGAIN || err == EWOULDBLOCK)
            continue = false
          else if (err == EMFILE || err == ENFILE) {
            pauseAccepting(serverFd, registration)
            continue = false
          } else {
            continue = false
            throw TransportError
              .syscall(TransportError.Accept, "accept", serverFd, err)
              .toException
          }
        }
      }
    }
  }

  private def pauseAccepting(
      serverFd: Int,
      registration: ServerRegistration
  ): Unit = {
    registration.acceptingPaused = true
    val failures = new FailureScope
    implicit val scope: FailureScope = failures
    val _: Done.type = backend.unregister(serverFd).checkForError
  }

  private def maybeResumeServers(): Unit =
    servers.foreach {
      case (fd, registration) =>
        if (registration.acceptingPaused &&
            connections.size < registration.options.maxOpenConnections) {
          registration.acceptingPaused = false
          val failures = new FailureScope
          implicit val scope: FailureScope = failures
          val _: Done.type =
            backend.register(fd, SelectorInterest.Read).checkForError
        }
    }
}

class PollingReactor(
    maxEvents: Int = 1024,
    idleTimeoutMillis: Int = 100
) extends Reactor {
  private val core = new PollingCore(this, maxEvents)
  private val wakeupSupport = {
    val failures = new FailureScope
    implicit val scope: FailureScope = failures
    val created = WakeupSupport.create().checkForError
    created
  }
  private val pendingTasks = new ConcurrentLinkedQueue[Runnable]()
  private val closed = new AtomicBoolean(false)
  @volatile private var stopped = false
  @volatile private var loopThread: Thread = _

  core.registerEventSource(wakeupSupport.readFd, SelectorInterest.Read)

  override def submit(task: Runnable): Unit = {
    pendingTasks.add(task)
    StreamIoDebug.log("reactor", s"submit pending=${pendingTasks.size()}")
    if (!stopped && !closed.get && (loopThread ne null) && (Thread
          .currentThread() ne loopThread))
      wakeup()
  }

  override def wakeup(): Unit =
    if (!closed.get) {
      StreamIoDebug.log("reactor", "wakeup")
      val failures = new FailureScope
      implicit val scope: FailureScope = failures
      val _: Done.type = wakeupSupport.signal().valueOr(Done)
      failures.failureOrNull match {
        case transport: TransportFailure
            if stopped && transport.code == TransportError.WakeupSignal =>
        case null                        =>
        case transport: TransportFailure =>
          throw transport.toException
        case socket: SocketFailure =>
          throw socket.toException
      }
    }

  override def stop(): Unit = {
    stopped = true
    wakeup()
  }

  override def listen(
      port: Int,
      factory: ServerHandlerFactory,
      host: String = "0.0.0.0",
      options: TcpServerOptions = TcpServerOptions()
  ): TcpServer =
    callOnLoop(core.listen(port, factory, host, options))

  override def connect(
      host: String,
      port: Int,
      handler: ConnectionHandler,
      options: TcpConnectionOptions = TcpConnectionOptions()
  ): TcpConnection =
    callOnLoop(core.connect(host, port, handler, options))

  override def run(): Unit = {
    loopThread = Thread.currentThread()
    StreamIoDebug.log(
      "reactor",
      s"run start idleTimeoutMillis=$idleTimeoutMillis"
    )
    try
      while (!stopped)
        runOnce(idleTimeoutMillis)
    finally {
      loopThread = null
      closeResources()
    }
  }

  override def runOnce(timeoutMillis: Int): Int = {
    loopThread = Thread.currentThread()
    drainPendingTasks()
    val ready = core.poll(timeoutMillis) { (fd, _) =>
      if (fd == wakeupSupport.readFd) {
        val failures = new FailureScope
        implicit val scope: FailureScope = failures
        val _: Done.type = wakeupSupport.drain().checkForError
        true
      } else false
    }
    if (ready > 0)
      StreamIoDebug.log("reactor", s"runOnce ready=$ready")
    ready
  }

  override def close(): Unit = {
    stop()
    if ((loopThread eq null) || (Thread.currentThread() eq loopThread))
      closeResources()
  }

  override private[streamio] def unregisterServer(fd: Int): Unit = {
    runOnLoop(core.unregisterServer(fd))
  }

  override private[streamio] def unregisterConnection(fd: Int): Unit = {
    runOnLoop(core.unregisterConnection(fd))
  }

  override private[streamio] def updateInterest(
      fd: Int,
      interest: Int
  ): Unit =
    runOnLoop(core.updateInterest(fd, interest))

  private def drainPendingTasks(): Unit = {
    var task = pendingTasks.poll()
    while (task != null) {
      StreamIoDebug.log("reactor", "drain task")
      task.run()
      task = pendingTasks.poll()
    }
  }

  private def wakeIfOffLoop(): Unit =
    if (!stopped && !closed.get &&
        (loopThread ne null) &&
        (Thread.currentThread() ne loopThread))
      wakeup()

  private def runOnLoop(body: => Unit): Unit =
    if ((loopThread eq null) || (Thread.currentThread() eq loopThread)) body
    else {
      val failure = new AtomicReference[Throwable](null)
      val done = new CountDownLatch(1)
      submit(new Runnable {
        override def run(): Unit =
          try body
          catch {
            case t: Throwable =>
              failure.set(t)
          } finally done.countDown()
      })
      done.await()
      val error = failure.get()
      if (error != null) throw error
    }

  private def callOnLoop[T](body: => T): T =
    if ((loopThread eq null) || (Thread.currentThread() eq loopThread)) body
    else {
      val result = new AtomicReference[AnyRef](null)
      val failure = new AtomicReference[Throwable](null)
      val done = new CountDownLatch(1)
      submit(new Runnable {
        override def run(): Unit =
          try {
            val value = body
            result.set(value.asInstanceOf[AnyRef])
          } catch {
            case t: Throwable =>
              failure.set(t)
          } finally done.countDown()
      })
      done.await()
      val error = failure.get()
      if (error != null) throw error
      result.get().asInstanceOf[T]
    }

  private def closeResources(): Unit =
    if (closed.compareAndSet(false, true)) {
      core.unregisterEventSource(wakeupSupport.readFd)
      wakeupSupport.close()
      core.close()
    }
}
