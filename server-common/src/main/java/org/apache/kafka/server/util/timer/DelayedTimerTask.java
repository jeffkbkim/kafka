package org.apache.kafka.server.util.timer;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class DelayedTimerTask implements Delayed {
    private final TimerTask timerTask;
    private final long expirationTimeMs;

    public DelayedTimerTask(TimerTask timerTask, long delayMs) {
        this.timerTask = timerTask;
        this.expirationTimeMs = System.currentTimeMillis() + delayMs;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        long delay = expirationTimeMs - System.currentTimeMillis();
        return unit.convert(delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
        if (other instanceof DelayedTimerTask) {
            return Long.compare(this.expirationTimeMs, ((DelayedTimerTask) other).expirationTimeMs);
        }
        return 0;
    }

    public TimerTask getTimerTask() {
        return timerTask;
    }
}
