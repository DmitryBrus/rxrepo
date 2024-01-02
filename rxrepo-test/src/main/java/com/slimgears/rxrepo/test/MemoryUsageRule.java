package com.slimgears.rxrepo.test;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MemoryUsageRule implements TestRule {
    private final String title;

    public MemoryUsageRule(String title) {
        this.title = title;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                long usedMemoryBefore = usedMemory();
                int activeThreadsBefore = activeThreads();
                try {
                    base.evaluate();
                } finally {
                    long consumption = usedMemory() - usedMemoryBefore;
                    int threads = activeThreads() - activeThreadsBefore;
                    System.out.println("[" + title + "] Consumed memory: " + consumption / (1024*1024) + "MB");
                    System.out.println("[" + title + "] Threads: " + threads);
                }
            }
        };
    }

    private int activeThreads() {
        Map<Thread, StackTraceElement[]> stackTraces = Thread.getAllStackTraces();
        Map<Thread.State, List<Thread>> threadsByState = stackTraces.keySet()
                .stream()
                .collect(Collectors.groupingBy(Thread::getState));
        return stackTraces.size();
    }

    private long usedMemory() {
        System.gc();
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}
