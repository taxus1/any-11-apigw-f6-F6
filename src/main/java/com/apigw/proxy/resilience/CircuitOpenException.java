package com.apigw.proxy.resilience;

import java.time.Duration;

/**
 * 请求在熔断器打开/半开未抽中探测名额时被网关挡下；这个异常不代表一次上游调用失败。
 */
public class CircuitOpenException extends RuntimeException {

    private final Duration retryAfter;

    public CircuitOpenException(String message, Duration retryAfter) {
        super(message);
        this.retryAfter = retryAfter;
    }

    public Duration retryAfter() {
        return retryAfter;
    }
}
