package com.apigw.proxy.resilience;

import java.time.Duration;

/**
 * 熔断状态机的入口凭据。rejected=true 表示这笔请求不能打上游，网关直接快速失败；
 * accepted=true 时调用方必须在这笔逻辑请求最终结束后回调一次 recordResult。
 *
 * 重试不重复拿凭据：同一逻辑请求的多次尝试共用一次准入、一次结果统计，避免重试把半开放量放大。
 */
public record CircuitBreakerPermit(boolean accepted, CircuitBreaker breaker, Duration retryAfter) {

    static CircuitBreakerPermit rejected(Duration retryAfter) {
        return new CircuitBreakerPermit(false, null, retryAfter);
    }

    static CircuitBreakerPermit accepted(CircuitBreaker breaker) {
        return new CircuitBreakerPermit(true, breaker, null);
    }

    public void recordSuccess() {
        if (accepted) {
            breaker.recordResult(true);
        }
    }

    public void recordFailure() {
        if (accepted) {
            breaker.recordResult(false);
        }
    }

    public void recordResult(boolean success) {
        if (accepted) {
            breaker.recordResult(success);
        }
    }
}
