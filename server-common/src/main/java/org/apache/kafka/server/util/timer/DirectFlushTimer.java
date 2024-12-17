package org.apache.kafka.server.util.timer;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class DirectFlushTimer implements Timer {
    private final Thread timerThread;
    private final DelayQueue<DelayedTimerTask> delayQueue;
    private final AtomicInteger taskCounter;
    private volatile boolean running = true;

    public DirectFlushTimer(long intervalMs, Runnable flushTask) {
        this.delayQueue = new DelayQueue<>();
        this.taskCounter = new AtomicInteger(0);

        this.timerThread = new Thread(() -> {
            try {
                while (running) {
                    DelayedTimerTask delayedTask = delayQueue.poll(intervalMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                    if (delayedTask != null && !delayedTask.getTimerTask().isCancelled()) {
                        delayedTask.getTimerTask().run();
                        taskCounter.decrementAndGet();
                    }
                    flushTask.run(); // Trigger flush task periodically
                }
            } catch (InterruptedException e) {
                // Thread interrupted, allow it to exit
            }
        });
    }

    public void start() {
        timerThread.start();
    }

    public void stop() {
        running = false;
        timerThread.interrupt();
    }

    @Override
    public void add(TimerTask timerTask) {
        if (!running) {
            throw new IllegalStateException("Timer is not running.");
        }
        delayQueue.offer(new DelayedTimerTask(timerTask, timerTask.delayMs()));
        taskCounter.incrementAndGet();
    }

    @Override
    public boolean advanceClock(long timeoutMs) throws InterruptedException {
        DelayedTimerTask delayedTask = delayQueue.poll(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        if (delayedTask != null) {
            if (!delayedTask.getTimerTask().isCancelled()) {
                delayedTask.getTimerTask().run();
                taskCounter.decrementAndGet();
            }
            return true;
        }
        return false;
    }

    @Override
    public int size() {
        return taskCounter.get();
    }

    @Override
    public void close() {
        stop();
    }
}
