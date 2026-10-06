package com.apigw.proxy.resilience;

import com.apigw.domain.route.CircuitBreakerPolicy;
import com.apigw.domain.route.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CircuitBreakerTest {

    private final MutableClock clock = new MutableClock();

    private CircuitBreaker breaker(int failureRate, int minimumCalls, int window,
                                   Duration openDuration, int halfOpenPercent) {
        return new CircuitBreaker(CircuitBreakerPolicy.of(true, window, minimumCalls,
                failureRate, openDuration, halfOpenPercent, Set.of(500, 502, 503, 504)), clock);
    }

    private CircuitBreaker breaker(int failureRate, int minimumCalls, int window,
                                   Duration openDuration) {
        return breaker(failureRate, minimumCalls, window, openDuration, 100);
    }

    @Test
    void opensAfterEnoughSamplesReachFailureRate_thenRejectsAndProbesHalfOpen() {
        CircuitBreaker cb = breaker(50, 4, 10, Duration.ofSeconds(30), 25);

        assertThat(cb.acquire().accepted()).isTrue();
        cb.recordResult(false);
        assertThat(cb.acquire().accepted()).isTrue();
        cb.recordResult(true);
        assertThat(cb.acquire().accepted()).isTrue();
        cb.recordResult(false);
        // 3 笔：样本数未到 4，不能因一次偶发失败断开
        assertThat(cb.stateName()).isEqualTo("CLOSED");
        assertThat(cb.acquire().accepted()).isTrue();
        cb.recordResult(false);

        assertThat(cb.stateName()).isEqualTo("OPEN");
        assertThat(cb.acquire().accepted()).isFalse();

        clock.advanceSeconds(30);
        // 25% 半开放量：先到达的 4 个请求只放 1 个，且第一个就是探测请求；
        // 首批探测结果回来前不能为了凑样本再额外放量。
        assertThat(cb.acquire().accepted()).isTrue();
        assertThat(cb.acquire().accepted()).isFalse();
        assertThat(cb.acquire().accepted()).isFalse();
        assertThat(cb.acquire().accepted()).isFalse();
        cb.recordResult(true);

        CircuitBreaker recovery = breaker(50, 4, 10, Duration.ofSeconds(30));
        for (int i = 0; i < 4; i++) {
            recovery.acquire();
            recovery.recordResult(false);
        }
        clock.advanceSeconds(30);
        for (int i = 0; i < 4; i++) {
            assertThat(recovery.acquire().accepted()).isTrue();
            recovery.recordResult(true);
        }
        assertThat(recovery.stateName()).isEqualTo("CLOSED");
    }

    @Test
    void failedHalfOpenProbesReopenAndKeepRejecting() {
        CircuitBreaker cb = breaker(50, 2, 5, Duration.ofSeconds(10), 100);
        failAndOpen(cb);

        clock.advanceSeconds(10);
        assertThat(cb.acquire().accepted()).isTrue();
        cb.recordResult(false);
        assertThat(cb.acquire().accepted()).isTrue();
        cb.recordResult(true);
        assertThat(cb.stateName()).isEqualTo("OPEN");
        assertThat(cb.acquire().accepted()).isFalse();
    }

    @Test
    void failurePolicyRejectsFourXStatusesAndBadRanges() {
        assertThatThrownBy(() -> CircuitBreakerPolicy.of(true, 10, 5, 50,
                Duration.ofSeconds(10), 10, Set.of(400, 500)))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("4xx");
    }

    @Test
    void retryMethodConventionIsNotBasedOnUrl() {
        RetryPolicy defaults = RetryPolicy.of(true, 2, Duration.ofMillis(1),
                Duration.ofSeconds(1), null, false, null);
        assertThat(defaults.allowsMethod("GET")).isTrue();
        assertThat(defaults.allowsMethod("POST")).isFalse();
        assertThat(defaults.allowsMethod("PATCH")).isFalse();
        // PUT/DELETE 只有路由显式列入才重试
        assertThat(defaults.allowsMethod("PUT")).isFalse();

        RetryPolicy put = RetryPolicy.of(true, 2, Duration.ofMillis(1), Duration.ofSeconds(1),
                Set.of("GET", "PUT"), false, null);
        assertThat(put.allowsMethod("PUT")).isTrue();

        RetryPolicy idempotentPost = RetryPolicy.of(true, 2, Duration.ofMillis(1),
                Duration.ofSeconds(1), Set.of("GET", "POST"), true, null);
        assertThat(idempotentPost.allowsMethod("POST")).isTrue();

        assertThatThrownBy(() -> RetryPolicy.of(true, 2, Duration.ofMillis(1),
                Duration.ofSeconds(1), Set.of("POST"), false, null))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("idempotent=true");
    }

    private void failAndOpen(CircuitBreaker cb) {
        cb.acquire();
        cb.recordResult(false);
        cb.acquire();
        cb.recordResult(false);
    }

    private static final class MutableClock extends Clock {
        private long millis;

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        void advanceSeconds(long seconds) {
            millis += Duration.ofSeconds(seconds).toMillis();
        }
    }
}
