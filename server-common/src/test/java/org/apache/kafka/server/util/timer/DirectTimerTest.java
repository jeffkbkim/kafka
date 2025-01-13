package org.apache.kafka.server.util.timer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class DirectTimerTest {
    @Test
    void testTaskExecution() throws Exception {
        DirectTimer timer = new DirectTimer("test");

        AtomicBoolean taskExecuted = new AtomicBoolean(false);
        TimerTask task = new TimerTask(50) {
            @Override
            public void run() {
                taskExecuted.set(true);
            }
        };

        timer.add(task);

        Thread.sleep(100); // Allow time for task to execute
        assertTrue(taskExecuted.get(), "Task should have executed after delay");
        timer.close();
    }

    @Test
    void testImmediateTaskExecution() throws Exception {
        DirectTimer timer = new DirectTimer("test");

        AtomicBoolean taskExecuted = new AtomicBoolean(false);
        TimerTask task = new TimerTask(0) {
            @Override
            public void run() {
                taskExecuted.set(true);
            }
        };

        timer.add(task);

        Thread.sleep(50); // Allow time for task to execute
        assertTrue(taskExecuted.get(), "Task with 0 delay should execute immediately");
        timer.close();
    }

    @Test
    void testMultipleTaskExecution() throws Exception {
        DirectTimer timer = new DirectTimer("test");

        AtomicInteger executionCount = new AtomicInteger(0);
        TimerTask task1 = new TimerTask(50) {
            @Override
            public void run() {
                executionCount.incrementAndGet();
            }
        };
        TimerTask task2 = new TimerTask(100) {
            @Override
            public void run() {
                executionCount.incrementAndGet();
            }
        };

        timer.add(task1);
        timer.add(task2);

        Thread.sleep(150); // Allow time for both tasks to execute
        assertEquals(2, executionCount.get(), "Both tasks should have executed");
        timer.close();
    }
}
