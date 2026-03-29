package scala.scalanative.sandbox.streamio.gears

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import scala.scalanative.sandbox.streamio.transport.{
  ConnectionHandler, PollingCore, Reactor, ServerHandlerFactory, TcpConnection,
  TcpServer
}

import gears.async.{AsyncSupport, Cancellable, ChannelClosedException, UnboundedChannel}

object GearsReactor {
  def polling[S <: AsyncSupport](
      maxEvents: Int = 1024,
      idlePollInterval: FiniteDuration = 100.millis
  )(using support: S, scheduler: support.Scheduler): Reactor = {
    val reactor =
      new EmbeddedReactor[S](
        maxEvents = maxEvents,
        idlePollInterval = idlePollInterval
      )
    reactor.start()
    reactor
  }

  private final class EmbeddedReactor[S <: AsyncSupport](
      maxEvents: Int,
      idlePollInterval: FiniteDuration
  )(using support: S, scheduler: support.Scheduler)
      extends Reactor {
    private val core = new PollingCore(this, maxEvents)
    private val pendingTasks = UnboundedChannel[Runnable]()
    private val pendingTaskCount = new AtomicInteger(0)
    private var closed = false
    private var stopped = false
    private var stepScheduled = false
    private var stepRunning = false
    private var immediateRequested = false
    private var delayedPoll: Cancellable = _

    private val step = new Runnable {
      override def run(): Unit = {
        val shouldRun =
          synchronized {
            delayedPoll = null
            if (closed || stopped || stepRunning) {
              stepScheduled = false
              false
            } else {
              stepScheduled = false
              stepRunning = true
              true
            }
          }
        if (shouldRun) {
          val ready = runOnce(0)
          finishStep(ready)
        }
      }
    }

    def start(): this.type = synchronized {
      if (!closed && !stopped && !stepScheduled && !stepRunning) {
        stepScheduled = true
        scheduler.execute(step)
      }
      this
    }

    override def submit(task: Runnable): Unit = {
      try {
        pendingTaskCount.incrementAndGet()
        pendingTasks.sendImmediately(task)
        requestImmediateStep()
      } catch {
        case _: ChannelClosedException =>
          pendingTaskCount.decrementAndGet()
      }
    }

    override def wakeup(): Unit =
      requestImmediateStep()

    override def stop(): Unit = synchronized {
      if (!closed && !stopped) {
        stopped = true
        cancelDelayedPoll()
      }
    }

    override def listen(
        port: Int,
        factory: ServerHandlerFactory,
        host: String = "0.0.0.0"
    ): TcpServer = {
      val server = core.listen(port, factory, host)
      requestImmediateStep()
      server
    }

    override def connect(
        host: String,
        port: Int,
        handler: ConnectionHandler
    ): TcpConnection = {
      val connection = core.connect(host, port, handler)
      requestImmediateStep()
      connection
    }

    override def run(): Unit =
      start()

    override def runOnce(timeoutMillis: Int): Int = {
      drainPendingTasks()
      core.poll(timeoutMillis)((_, _) => false)
    }

    override def close(): Unit = {
      val shouldClose = synchronized {
        if (closed) false
        else {
          closed = true
          stopped = true
          cancelDelayedPoll()
          pendingTasks.close()
          true
        }
      }
      if (shouldClose) core.close()
    }

    override def unregisterServer(fd: Int): Unit = {
      core.unregisterServer(fd)
      requestImmediateStep()
    }

    override def unregisterConnection(fd: Int): Unit = {
      core.unregisterConnection(fd)
      requestImmediateStep()
    }

    override def updateInterest(
        fd: Int,
        interest: Int
    ): Unit = {
      core.updateInterest(fd, interest)
      requestImmediateStep()
    }

    private def requestImmediateStep(): Unit = synchronized {
      if (!closed && !stopped) {
        immediateRequested = true
        cancelDelayedPoll()
        if (!stepScheduled && !stepRunning) {
          stepScheduled = true
          scheduler.execute(step)
        }
      }
    }

    private def finishStep(ready: Int): Unit = {
      val scheduleImmediateNow = synchronized {
        stepRunning = false
        if (closed || stopped) false
        else {
          val shouldRunImmediately =
            immediateRequested || hasPendingTasks || ready > 0
          immediateRequested = false
          if (shouldRunImmediately) {
            stepScheduled = true
            true
          } else {
            stepScheduled = true
            delayedPoll = scheduler.schedule(idlePollInterval, step)
            false
          }
        }
      }

      if (scheduleImmediateNow) scheduler.execute(step)
    }

    private def hasPendingTasks: Boolean =
      pendingTaskCount.get() > 0

    private def drainPendingTasks(): Unit = {
      var continue = true
      while (continue) {
        pendingTasks.readSource.poll() match {
          case Some(Right(task)) =>
            pendingTaskCount.decrementAndGet()
            task.run()
          case Some(Left(_)) | None =>
            continue = false
        }
      }
    }

    private def cancelDelayedPoll(): Unit = {
      val current = delayedPoll
      if (current != null) {
        delayedPoll = null
        current.cancel()
        stepScheduled = false
      }
    }
  }
}
