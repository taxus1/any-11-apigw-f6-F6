package com.apigw.proxy.resilience;

/**
 * 错误发生在网关已经拿到上游响应、并开始交回调用方之后。此时可能已经提交响应头/写出部分响应体，
 * 重试无法安全进行。
 */
public class ResponseDeliveryException extends RuntimeException {

    public ResponseDeliveryException(Throwable cause) {
        super(cause);
    }
}
