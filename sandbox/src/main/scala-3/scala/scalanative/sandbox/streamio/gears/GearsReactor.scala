package scala.scalanative.sandbox.streamio.gears

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import gears.async.{AsyncSupport, Cancellable}

import scala.scalanative.sandbox.streamio.transport.{
  PollingReactor,
  Reactor,
  ReactorTaskScheduler
}

object GearsReactor {
  def polling[S <: AsyncSupport](
      maxEvents: Int = 256,
      idlePollInterval: FiniteDuration = 100.millis
  )(using support: S, scheduler: support.Scheduler): Reactor = {
    val taskScheduler = new EmbeddedTaskScheduler
    val reactor =
      new EmbeddedPollingReactor[S](
        maxEvents = maxEvents,
        idlePollInterval = idlePollInterval,
        taskScheduler = taskScheduler
      )
    taskScheduler.attach(reactor)
    reactor.start()
    reactor
  }

  private final class EmbeddedPollingReactor[S <: AsyncSupport](
      maxEvents: Int,
      idlePollInterval: FiniteDuration,
      taskScheduler: EmbeddedTaskScheduler
  )(using support: S, scheduler: support.Scheduler)
      extends PollingReactor(
        maxEvents = maxEvents,
        idleTimeoutMillis = 0,
        taskScheduler = taskScheduler
      ) {
    private var closed = false
    private var stepScheduled = false
    private var stepRunning = false
    private var immediateRequested = false
    private var delayedPoll: Cancellable = _

    private val step = new Runnable {
      override def run(): Unit = {
        val shouldRun =
          synchronized {
            delayedPoll = null
            if (closed) {
              stepScheduled = false
              false
            } else {
              stepRunning = true
              true
            }
          }
        if (shouldRun) {
          val ready = EmbeddedPollingReactor.this.runPollStep()
          finishStep(ready)
        }
      }
    }

    def start(): this.type = synchronized {
      if (!closed && !stepScheduled && !stepRunning) {
        stepScheduled = true
        scheduler.execute(step)
      }
      this
    }

    override def submit(task: Runnable): Unit = {
      taskScheduler.submit(task)
    }

    override def wakeup(): Unit =
      requestImmediateStep()

    override def run(): Unit =
      start()

    override def stop(): Unit = synchronized {
      if (!closed) {
        closed = true
        cancelDelayedPoll()
      }
      super.stop()
    }

    override def close(): Unit = synchronized {
      if (!closed) {
        closed = true
        cancelDelayedPoll()
      }
      super.close()
    }

    private[gears] def requestImmediateStep(): Unit = synchronized {
      if (!closed) {
        immediateRequested = true
        cancelDelayedPoll()
        if (!stepScheduled && !stepRunning) {
          stepScheduled = true
          scheduler.execute(step)
        }
      }
    }

    private def runPollStep(): Int =
      EmbeddedPollingReactor.this.runOnce(0)

    private def finishStep(ready: Int): Unit = {
      val scheduleImmediateNow = synchronized {
        stepRunning = false
        stepScheduled = false
        if (closed) false
        else {
          val shouldRunImmediately =
            immediateRequested || taskScheduler.hasPendingTasks || ready > 0
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

    private def cancelDelayedPoll(): Unit = {
      val current = delayedPoll
      if (current != null) {
        delayedPoll = null
        current.cancel()
        stepScheduled = false
      }
    }
  }

  private final class EmbeddedTaskScheduler extends ReactorTaskScheduler {
    private val tasks = new ConcurrentLinkedQueue[Runnable]()
    private val pendingCount = new AtomicInteger(0)
    @volatile private var owner: EmbeddedPollingReactor[?] = _

    def attach(reactor: EmbeddedPollingReactor[?]): Unit =
      owner = reactor

    def hasPendingTasks: Boolean =
      pendingCount.get() > 0

    override def submit(task: Runnable): Unit = {
      tasks.add(task)
      pendingCount.incrementAndGet()
      val currentOwner = owner
      if (currentOwner != null)
        currentOwner.requestImmediateStep()
    }

    override def drain(run: Runnable => Unit): Unit = {
      var task = tasks.poll()
      while (task != null) {
        pendingCount.decrementAndGet()
        run(task)
        task = tasks.poll()
      }
    }
  }
}
