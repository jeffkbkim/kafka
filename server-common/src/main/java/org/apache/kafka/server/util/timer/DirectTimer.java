package org.apache.kafka.server.util.timer;

import org.apache.kafka.common.utils.KafkaThread;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DirectTimer executes tasks with minimal delay handling for testing.
 */
public class DirectTimer implements Timer {
    private static final String THREAD_NAME_PREFIX = "direct-timer-";

    private final ScheduledExecutorService scheduler;
    private final AtomicInteger taskCounter;

    public DirectTimer(String name) {
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
            runnable -> KafkaThread.nonDaemon(THREAD_NAME_PREFIX + name, runnable)
        );
        this.taskCounter = new AtomicInteger(0);
    }

    @Override
    public void add(TimerTask timerTask) {
        long delayMs = timerTask.delayMs();
        new TimerTaskEntry(timerTask, delayMs);
        taskCounter.incrementAndGet();
        scheduler.schedule(() -> {
            try {
                timerTask.run();
            } finally {
                taskCounter.decrementAndGet();
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean advanceClock(long timeoutMs) {
        // No-op for DirectTimer; tasks execute immediately after delay
        return false;
    }

    @Override
    public int size() {
        return taskCounter.get();
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
