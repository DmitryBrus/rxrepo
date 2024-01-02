package com.slimgears.rxrepo.util;

import java.util.concurrent.Callable;

public interface LockProvider {
    AutoCloseable lock();

    default void withLock(Runnable runnable) {
        this.<Void>withLock(() -> {
            runnable.run();
            return null;
        });
    }

    default <T> T withLock(Callable<T> callable) {
        try (AutoCloseable ignored = lock()) {
            return callable.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
