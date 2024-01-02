package com.slimgears.rxrepo.test;

import io.reactivex.functions.Consumer;
import io.reactivex.observers.BaseTestConsumer;
import io.reactivex.observers.TestObserver;
import org.junit.Assert;

import java.time.Duration;

@SuppressWarnings("WeakerAccess")
public class TestUtils {
    private static Duration defaultTimeout = Duration.ofSeconds(10);

    public static <T> Consumer<TestObserver<T>> countLessThan(int count) {
        return countLessThan(count, Duration.ofMillis(500));
    }

    public static <T> Consumer<TestObserver<T>> countLessThan(int count, Duration timeout) {
        return observer -> observer
                .assertOf(o -> Assert.assertTrue(o.valueCount() < count))
                .awaitCount(count, BaseTestConsumer.TestWaitStrategy.SLEEP_10MS, timeout.toMillis())
                .assertOf(o -> Assert.assertTrue(o.valueCount() < count))
                .assertNoErrors();
    }

    public static <T> Consumer<TestObserver<T>> countAtLeast(int count) {
        return countAtLeast(count, defaultTimeout);
    }

    public static <T> Consumer<TestObserver<T>> awaitCount(int count) {
        return awaitCount(count, defaultTimeout);
    }

    public static <T> Consumer<TestObserver<T>> awaitCount(int count, Duration timeout) {
        return observer -> observer
                .awaitCount(count, BaseTestConsumer.TestWaitStrategy.SLEEP_100MS, timeout.toMillis())
                .assertNoErrors();
    }

    public static <T> Consumer<TestObserver<T>> countAtLeast(int count, Duration timeout) {
        return observer -> observer
                .assertOf(awaitCount(count, timeout))
                .assertNoTimeout()
                .assertOf(o -> Assert.assertTrue(o.valueCount() >= count));
    }

    public static <T> Consumer<TestObserver<T>> countExactly(int count) {
        return observer -> observer
                .assertOf(countAtLeast(count))
                .assertValueCount(count);
    }
}
