package com.apigw.proxy.resilience;

/**
 * 上游返回了配置为「可重试」的 5xx 响应。非最后一次尝试抛它触发重试；
 * 最后一次不再抛，直接把真实 5xx 响应交回调用方。
 */
public class UpstreamStatusRetryException extends RuntimeException {

    private final int statusCode;

    public UpstreamStatusRetryException(int statusCode) {
        super("Upstream returned retryable status " + statusCode);
        this.statusCode = statusCode;
    }

    public int statusCode() {
        return statusCode;
    }
}
