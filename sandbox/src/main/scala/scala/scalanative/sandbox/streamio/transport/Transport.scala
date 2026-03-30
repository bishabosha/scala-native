package scala.scalanative.sandbox.streamio.transport

import java.io.IOException
import java.nio.charset.{Charset, StandardCharsets}
import java.util.ArrayDeque
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

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
  def onSocketError(connection: TcpConnection, cause: SocketFailure): Unit =
    onError(connection, cause.toException)
  def onError(connection: TcpConnection, cause: Throwable): Unit = ()
}

final class SocketFailure private[transport] (
    val operation: String,
    val fd: Int,
    val errnoCode: Int
) {
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

  private[transport] def start(): Unit =
    safeInvoke(_.onConnected(this))

  private[transport] def beginConnect(): Unit = {
    connecting = true
    writeInterest = true
  }

  private[streamio] def submit(task: Runnable): Unit =
    reactor.submit(task)

  private[streamio] def writeOwned(bytes: Array[Byte]): Unit =
    if (!closed) {
      if (queuedWriteBytes + bytes.length > maxQueuedWriteBytes) {
        fail(new IOException("connection outbound buffer overflow"))
        return
      }
      outbound.addLast(new OutboundChunk(bytes, 0))
      queuedWriteBytes += bytes.length
      enableWriteInterest()
    }

  private[transport] def failSocket(cause: SocketFailure): Unit = {
    safeInvokeSocket(cause)
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

    if (sawData && !closed)
      safeInvoke(_.onReadable(this, inboundBuffer))

    if (closedByPeer && !closed)
      close()
  }

  private[transport] def onWritableReady(): Unit = {
    if (closed) return

    if (connecting) {
      val connectErr = SocketSupport.finishConnect(fd)
      if (connectErr != 0) {
        failSocket(SocketError.connect(fd, connectErr))
        return
      }
      connecting = false
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

  private def safeInvokeSocket(cause: SocketFailure): Unit =
    if (handler != null) {
      try handler.onSocketError(this, cause)
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
  def register(fd: Int, interest: Int): Unit
  def update(fd: Int, interest: Int): Unit
  def unregister(fd: Int): Unit
  def waitEvents(timeoutMillis: Int)(handler: (Int, Int) => Unit): Int
}

private object SelectorBackend {
  def create(maxEvents: Int): SelectorBackend =
    if (LinktimeInfo.isLinux) new EpollBackend(maxEvents)
    else if (LinktimeInfo.isMac || LinktimeInfo.isFreeBSD ||
        LinktimeInfo.isOpenBSD || LinktimeInfo.isNetBSD)
      new KqueueBackend(maxEvents)
    else new PollBackend
}

private trait WakeupSupport extends AutoCloseable {
  def readFd: Int
  def signal(): Unit
  def drain(): Unit
}

private object WakeupSupport {
  def create(): WakeupSupport =
    new PipeWakeupSupport

  private final class PipeWakeupSupport extends WakeupSupport {
    private val pending = new AtomicBoolean(false)
    private var writeFd: Int = -1
    val readFd: Int = {
      val fds = stackalloc[CInt](2)
      val rc = unistd.pipe(fds)
      if (rc < 0)
        throw new IOException(s"pipe() failed, errno=$errno")
      val read = !fds
      val write = !(fds + 1)
      SocketSupport.setNonBlocking(read)
      SocketSupport.setNonBlocking(write)
      writeFd = write
      read
    }

    override def signal(): Unit =
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
              throw new IOException(s"write(wakeup) failed, errno=$err")
            }
          }
        }
      }

    override def drain(): Unit = {
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
          else
            throw new IOException(s"read(wakeup) failed, errno=$err")
        }
      }
    }

    override def close(): Unit = {
      if (readFd >= 0) unistd.close(readFd)
      if (writeFd >= 0) unistd.close(writeFd)
    }
  }
}

private final class EpollBackend(maxEvents: Int) extends SelectorBackend {
  import epoll._

  import SelectorInterest._

  private val epfd = {
    val fd = epoll_create1(EPOLL_CLOEXEC)
    if (fd < 0)
      throw new IOException(s"epoll_create1 failed, errno=$errno")
    fd
  }

  private val eventSize = scalanative_epoll_event_size()
  private val events =
    scala.scalanative.libc.stdlib.calloc(maxEvents.toCSize, eventSize)

  override def close(): Unit = {
    scala.scalanative.libc.stdlib.free(events)
    unistd.close(epfd)
  }

  override def register(fd: Int, interest: Int): Unit =
    ctl(EPOLL_CTL_ADD, fd, interest)

  override def update(fd: Int, interest: Int): Unit =
    ctl(EPOLL_CTL_MOD, fd, interest)

  override def unregister(fd: Int): Unit = {
    val rc = epoll_ctl(epfd, EPOLL_CTL_DEL, fd, null)
    if (rc < 0 && errno != EBADF && errno != ENOENT)
      throw new IOException(s"epoll_ctl DEL failed, errno=$errno")
  }

  override def waitEvents(
      timeoutMillis: Int
  )(handler: (Int, Int) => Unit): Int = {
    val ready = epoll_wait(epfd, events, maxEvents, timeoutMillis)
    if (ready < 0) {
      if (errno == EINTR) 0
      else throw new IOException(s"epoll_wait failed, errno=$errno")
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
      ready
    }
  }

  private def ctl(op: Int, fd: Int, interest: Int): Unit = {
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
      throw new IOException(s"epoll_ctl(op=$op, fd=$fd) failed, errno=$errno")
  }
}

private final class KqueueBackend(maxEvents: Int) extends SelectorBackend {
  import SelectorInterest._
  import bsdKevent._

  private val kq = {
    val fd = bsdKevent.kqueue()
    if (fd < 0)
      throw new IOException(s"kqueue() failed, errno=$errno")
    fd
  }

  private val eventSize = scalanative_kevent_size()
  private val events =
    scala.scalanative.libc.stdlib.calloc(maxEvents.toCSize, eventSize)

  override def close(): Unit = {
    scala.scalanative.libc.stdlib.free(events)
    unistd.close(kq)
  }

  override def register(fd: Int, interest: Int): Unit =
    change(fd, interest)

  override def update(fd: Int, interest: Int): Unit =
    change(fd, interest)

  override def unregister(fd: Int): Unit = {
    val changes = stackalloc[Byte](eventSize * 2.toCSize)
    setEvent(changes, 0, fd, EVFILT_READ, EV_DELETE)
    setEvent(changes, 1, fd, EVFILT_WRITE, EV_DELETE)
    val rc = bsdKevent.kevent(kq, changes, 2, null, 0, null)
    if (rc < 0 && errno != EBADF && errno != ENOENT)
      throw new IOException(s"kevent DELETE failed, errno=$errno")
  }

  override def waitEvents(
      timeoutMillis: Int
  )(handler: (Int, Int) => Unit): Int = {
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
      if (errno == EINTR) 0
      else throw new IOException(s"kevent wait failed, errno=$errno")
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
      ready
    }
  }

  private def change(fd: Int, interest: Int): Unit = {
    val changes = stackalloc[Byte](eventSize * 2.toCSize)
    setEvent(changes, 0, fd, EVFILT_READ, EV_ADD | EV_ENABLE | EV_CLEAR)
    val writeFlags =
      if ((interest & Write) != 0) EV_ADD | EV_ENABLE | EV_CLEAR
      else EV_ADD | EV_DISABLE | EV_CLEAR
    setEvent(changes, 1, fd, EVFILT_WRITE, writeFlags)
    val rc = bsdKevent.kevent(kq, changes, 2, null, 0, null)
    if (rc < 0)
      throw new IOException(s"kevent register failed, errno=$errno")
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

  override def register(fd: Int, interest: Int): Unit =
    update(fd, interest)

  override def update(fd: Int, interest: Int): Unit = {
    val idx = registrations.indexWhere(_._1 == fd)
    if (idx >= 0) registrations(idx) = ((fd, interest))
    else registrations += ((fd, interest))
  }

  override def unregister(fd: Int): Unit = {
    val idx = registrations.indexWhere(_._1 == fd)
    if (idx >= 0) registrations.remove(idx)
  }

  override def waitEvents(
      timeoutMillis: Int
  )(handler: (Int, Int) => Unit): Int = {
    if (registrations.isEmpty) {
      if (timeoutMillis > 0) Thread.sleep(math.min(timeoutMillis, 50))
      0
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
        if (errno == EINTR) 0
        else throw new IOException(s"poll() failed, errno=$errno")
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
        ready
      }
    }
  }
}

object SocketSupport {
  import in._

  import inOps._

  def setNonBlocking(fd: Int): Unit = {
    val current = fcntl.fcntl(fd, fcntl.F_GETFL, 0)
    if (current < 0)
      throw new IOException(s"fcntl(F_GETFL) failed, errno=$errno")

    val rc =
      fcntl.fcntl(fd, fcntl.F_SETFL, current | fcntl.O_NONBLOCK)
    if (rc < 0)
      throw new IOException(s"fcntl(F_SETFL, O_NONBLOCK) failed, errno=$errno")
  }

  def bindTcpListener(
      port: Int,
      host: String = "0.0.0.0",
      backlog: Int = socket.SOMAXCONN
  ): (Int, Int) = {
    val fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    if (fd < 0)
      throw new IOException(s"socket() failed, errno=$errno")

    try {
      setSockOptInt(
        fd,
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1,
        required = true
      )
      setSockOptInt(
        fd,
        socket.SOL_SOCKET,
        socket.SO_REUSEPORT,
        1,
        required = false
      )
      setNonBlocking(fd)

      val addr = stackalloc[sockaddr_in]()
      addr.sin_family = socket.AF_INET.toUShort
      addr.sin_port = inet.htons(port.toUShort)

      if (host == "0.0.0.0" || host == "" || host == null)
        addr.sin_addr.s_addr = inet.htonl(INADDR_ANY)
      else {
        Zone.acquire { implicit z =>
          val rc = inet.inet_pton(
            socket.AF_INET,
            toCString(host),
            addr.at3.asInstanceOf[CVoidPtr]
          )
          if (rc != 1)
            throw new IOException(s"inet_pton($host) failed")
        }
      }

      val bindRc =
        socket.bind(
          fd,
          addr.asInstanceOf[Ptr[socket.sockaddr]],
          sizeof[sockaddr_in].toUInt
        )
      if (bindRc < 0)
        throw new IOException(s"bind() failed, errno=$errno")

      val listenRc = socket.listen(fd, math.max(1, backlog))
      if (listenRc < 0)
        throw new IOException(s"listen() failed, errno=$errno")

      (fd, localPort(fd))
    } catch {
      case t: Throwable =>
        unistd.close(fd)
        throw t
    }
  }

  def connectTcp(host: String, port: Int): (Int, Boolean) = {
    val fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    if (fd < 0)
      throw new IOException(s"socket() failed, errno=$errno")

    try {
      setNonBlocking(fd)
      setSockOptInt(fd, in.IPPROTO_TCP, tcp.TCP_NODELAY, 1, required = false)

      val addr = stackalloc[sockaddr_in]()
      initSockAddr(addr, host, port)

      val connectRc = socket.connect(
        fd,
        addr.asInstanceOf[Ptr[socket.sockaddr]],
        sizeof[sockaddr_in].toUInt
      )
      if (connectRc == 0) (fd, true)
      else {
        val err = errno
        if (err == EINPROGRESS || err == EWOULDBLOCK) (fd, false)
        else throw SocketError.connect(fd, err).toException
      }
    } catch {
      case t: Throwable =>
        unistd.close(fd)
        throw t
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

  def configureAccepted(fd: Int): Unit = {
    setNonBlocking(fd)
    setSockOptInt(fd, in.IPPROTO_TCP, tcp.TCP_NODELAY, 1, required = false)
  }

  def finishConnect(fd: Int): Int = {
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
      throw new IOException(s"getsockopt(SO_ERROR) failed, errno=$errno")

    !opt
  }

  def localPort(fd: Int): Int = {
    val addr = stackalloc[sockaddr_in]()
    val len = stackalloc[socket.socklen_t]()
    !len = sizeof[sockaddr_in].toUInt
    val rc = socket.getsockname(
      fd,
      addr.asInstanceOf[Ptr[socket.sockaddr]],
      len
    )
    if (rc < 0)
      throw new IOException(s"getsockname() failed, errno=$errno")
    inet.ntohs(addr.sin_port).toInt
  }

  private def setSockOptInt(
      fd: Int,
      level: Int,
      option: Int,
      value: Int,
      required: Boolean
  ): Unit = {
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
      throw new IOException(
        s"setsockopt(level=$level, option=$option) failed, errno=$errno"
      )
  }

  private def initSockAddr(
      addr: Ptr[sockaddr_in],
      host: String,
      port: Int
  ): Unit = {
    addr.sin_family = socket.AF_INET.toUShort
    addr.sin_port = inet.htons(port.toUShort)

    if (host == "0.0.0.0" || host == "" || host == null)
      addr.sin_addr.s_addr = inet.htonl(INADDR_ANY)
    else {
      Zone.acquire { implicit z =>
        val rc = inet.inet_pton(
          socket.AF_INET,
          toCString(host),
          addr.at3.asInstanceOf[CVoidPtr]
        )
        if (rc != 1)
          throw new IOException(s"inet_pton($host) failed")
      }
    }
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

  private val backend = SelectorBackend.create(maxEvents)
  private val servers = mutable.HashMap.empty[Int, ServerRegistration]
  private val connections = mutable.HashMap.empty[Int, TcpConnection]

  def listen(
      port: Int,
      factory: ServerHandlerFactory,
      host: String = "0.0.0.0",
      options: TcpServerOptions = TcpServerOptions()
  ): TcpServer = synchronized {
    val (fd, boundPort) =
      SocketSupport.bindTcpListener(port, host, options.backlog)
    val server = new TcpServer(owner, fd, boundPort, options)
    servers(fd) = new ServerRegistration(server, factory, options)
    backend.register(fd, SelectorInterest.Read)
    server
  }

  def connect(
      host: String,
      port: Int,
      handler: ConnectionHandler,
      options: TcpConnectionOptions = TcpConnectionOptions()
  ): TcpConnection = synchronized {
    val (fd, connectedNow) = SocketSupport.connectTcp(host, port)
    val connection = new TcpConnection(owner, fd, options)
    try {
      connections(fd) = connection
      connection.setHandler(handler)
      if (connectedNow) {
        backend.register(fd, SelectorInterest.Read)
        connection.start()
      } else {
        connection.beginConnect()
        backend.register(fd, SelectorInterest.Read | SelectorInterest.Write)
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
    backend.register(fd, interest)
  }

  def unregisterEventSource(fd: Int): Unit = synchronized {
    backend.unregister(fd)
  }

  def poll(
      timeoutMillis: Int
  )(handleEventSource: (Int, Int) => Boolean): Int = synchronized {
    backend.waitEvents(timeoutMillis) { (fd, interest) =>
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
    backend.unregister(fd)
  }

  private[streamio] def unregisterConnection(fd: Int): Unit = synchronized {
    connections.remove(fd)
    backend.unregister(fd)
    maybeResumeServers()
  }

  private[streamio] def updateInterest(fd: Int, interest: Int): Unit =
    synchronized {
      if (connections.contains(fd))
        backend.update(fd, interest)
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
            backend.unregister(serverFd)
            continue = false
          case AcceptOverloadStrategy.RejectAccepted =>
            val rejectedFd = SocketSupport.accept(serverFd)
            if (rejectedFd >= 0) {
              unistd.close(rejectedFd)
              handledThisCycle += 1
            }
            else {
              val err = errno
              if (err == EINTR) ()
              else if (err == EAGAIN || err == EWOULDBLOCK)
                continue = false
              else {
                continue = false
                throw new IOException(s"accept() failed, errno=$err")
              }
            }
        }
      } else {
        val clientFd = SocketSupport.accept(serverFd)
        if (clientFd >= 0) {
          SocketSupport.configureAccepted(clientFd)
          val connection =
            new TcpConnection(
              owner,
              clientFd,
              registration.options.childConnectionOptions
            )
          connections(clientFd) = connection
          backend.register(clientFd, SelectorInterest.Read)
          connection.setHandler(registration.factory.create(connection))
          connection.start()
          handledThisCycle += 1
        } else {
          val err = errno
          if (err == EINTR) ()
          else if (err == EAGAIN || err == EWOULDBLOCK)
            continue = false
          else {
            continue = false
            throw new IOException(s"accept() failed, errno=$err")
          }
        }
      }
    }
  }

  private def maybeResumeServers(): Unit =
    servers.foreach { case (fd, registration) =>
      if (registration.acceptingPaused &&
          connections.size < registration.options.maxOpenConnections) {
        registration.acceptingPaused = false
        backend.register(fd, SelectorInterest.Read)
      }
    }
}

class PollingReactor(
    maxEvents: Int = 1024,
    idleTimeoutMillis: Int = 100
) extends Reactor {
  private val core = new PollingCore(this, maxEvents)
  private val wakeupSupport = WakeupSupport.create()
  private val pendingTasks = new ConcurrentLinkedQueue[Runnable]()
  @volatile private var stopped = false
  @volatile private var loopThread: Thread = _

  core.registerEventSource(wakeupSupport.readFd, SelectorInterest.Read)

  override def submit(task: Runnable): Unit = {
    pendingTasks.add(task)
    if ((loopThread ne null) && (Thread.currentThread() ne loopThread))
      wakeup()
  }

  override def wakeup(): Unit =
    wakeupSupport.signal()

  override def stop(): Unit = {
    stopped = true
    wakeup()
  }

  override def listen(
      port: Int,
      factory: ServerHandlerFactory,
      host: String = "0.0.0.0",
      options: TcpServerOptions = TcpServerOptions()
  ): TcpServer = {
    wakeIfOffLoop()
    core.listen(port, factory, host, options)
  }

  override def connect(
      host: String,
      port: Int,
      handler: ConnectionHandler,
      options: TcpConnectionOptions = TcpConnectionOptions()
  ): TcpConnection = {
    wakeIfOffLoop()
    core.connect(host, port, handler, options)
  }

  override def run(): Unit = {
    loopThread = Thread.currentThread()
    try
      while (!stopped)
        runOnce(idleTimeoutMillis)
    finally loopThread = null
  }

  override def runOnce(timeoutMillis: Int): Int = {
    loopThread = Thread.currentThread()
    drainPendingTasks()
    core.poll(timeoutMillis) { (fd, _) =>
      if (fd == wakeupSupport.readFd) {
        wakeupSupport.drain()
        true
      } else false
    }
  }

  override def close(): Unit = {
    stop()
    core.unregisterEventSource(wakeupSupport.readFd)
    wakeupSupport.close()
    core.close()
  }

  override private[streamio] def unregisterServer(fd: Int): Unit = {
    wakeIfOffLoop()
    core.unregisterServer(fd)
  }

  override private[streamio] def unregisterConnection(fd: Int): Unit = {
    wakeIfOffLoop()
    core.unregisterConnection(fd)
  }

  override private[streamio] def updateInterest(
      fd: Int,
      interest: Int
  ): Unit = {
    wakeIfOffLoop()
    core.updateInterest(fd, interest)
  }

  private def drainPendingTasks(): Unit = {
    var task = pendingTasks.poll()
    while (task != null) {
      task.run()
      task = pendingTasks.poll()
    }
  }

  private def wakeIfOffLoop(): Unit =
    if ((loopThread ne null) && (Thread.currentThread() ne loopThread))
      wakeup()
}
