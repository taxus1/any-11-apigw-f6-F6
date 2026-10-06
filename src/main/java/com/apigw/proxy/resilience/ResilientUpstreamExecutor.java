package com.apigw.proxy.resilience;

import com.apigw.domain.route.GatewayRoute;
import com.apigw.domain.route.RetryPolicy;
import com.apigw.proxy.config.GatewayProxyProperties;
import com.apigw.proxy.error.UpstreamFailureKind;
import com.apigw.proxy.forward.UpstreamForwarder;
import com.apigw.proxy.forward.UpstreamResponse;
import com.apigw.proxy.userauth.OutboundAuth;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * 熔断 + 重试的统一执行器。
 *
 * 固定口径：
 * 1. 熔断在前、重试在后。OPEN 拒绝的请求根本不准备请求体、也不访问上游，更不会触发重试；
 * 2. 一次逻辑请求只拿一次熔断许可，多次尝试共用一次结果统计，避免重试放大半开流量；
 * 3. 请求头清洗、身份头和请求动作只做一次，然后重放同一份已准备请求；
 * 4. 只有有界且长度明确的请求体才会缓存重放；大 body、chunked body 继续流式转发且不重试；
 * 5. 重试总预算从真正开始执行时起算，包含等待时间，超时后不再发起新尝试。
 */
@Component
public class ResilientUpstreamExecutor {

    private final UpstreamForwarder forwarder;
    private final CircuitBreakerRegistry circuitBreakers;
    private final GatewayProxyProperties properties;

    public ResilientUpstreamExecutor(UpstreamForwarder forwarder,
                                     CircuitBreakerRegistry circuitBreakers,
                                     GatewayProxyProperties properties) {
        this.forwarder = forwarder;
        this.circuitBreakers = circuitBreakers;
        this.properties = properties;
    }

    public Mono<Void> execute(GatewayRoute route,
                              ServerHttpRequest incoming,
                              String traceId,
                              URI targetUri,
                              String grayGroup,
                              OutboundAuth auth,
                              Function<UpstreamResponse, Mono<Void>> responseHandler) {
        CircuitBreakerPermit permit = circuitBreakers.acquire(route, grayGroup);
        if (!permit.accepted()) {
            return Mono.error(new CircuitOpenException(
                    "上游熔断打开，route=" + route.getRouteNo()
                            + ", group=" + (grayGroup == null ? "-" : grayGroup),
                    permit.retryAfter()));
        }

        return Mono.defer(() -> {
            RetryContext context = RetryContext.create(route, incoming, properties);
            AtomicInteger finalStatus = new AtomicInteger();
            Function<UpstreamResponse, Mono<Void>> wrappedHandler = upstream -> {
                finalStatus.set(upstream.statusCode());
                // 标记发生在「把上游响应写给调用方」阶段的错误；这种错误可能已提交，
                // 绝不能再重试，避免重复写响应。
                return responseHandler.apply(upstream)
                        .onErrorMap(ResponseDeliveryException::new);
            };

            Function<UpstreamResponse, Mono<Void>> effectiveHandler = wrappedHandler;
            RetryPolicy retry = route.getRetry();
            Sinks.One<Void> firstResponse = null;
            if (retry != null && retry.isEnabled() && retry.maxAttempts() > 1) {
                // 总时限只约束「拿到最终上游响应头之前」的连接、等待与重试；响应头一旦拿到，
                // 上游响应体继续流式交回调用方，不因为 body 很大而被总预算错误截断。
                firstResponse = Sinks.one();
                Sinks.One<Void> first = firstResponse;
                effectiveHandler = upstream ->
                        Mono.fromRunnable(() -> first.tryEmitEmpty())
                                .then(wrappedHandler.apply(upstream));
            }

            final Function<UpstreamResponse, Mono<Void>> handler = effectiveHandler;
            Mono<Void> upstreamWork = prepareBody(incoming, context)
                    .flatMap(body -> {
                        var prepared = forwarder.prepareRequest(route, incoming, traceId, auth,
                                body.replay() ? body.bytes() : null);
                        return attempt(prepared, targetUri, handler, context, 1,
                                System.nanoTime());
                    });

            Mono<Void> execution = upstreamWork;
            if (firstResponse != null) {
                Sinks.One<Void> first = firstResponse;
                Duration totalTimeout = retry.totalTimeout();
                execution = Mono.<Void>create(sink -> {
                    var upstreamSub = upstreamWork.subscribe(
                            v -> { }, sink::error, sink::success);
                    // first 完成后 timeout 流也完成，但不结束 sink；只有它超时报错才结束上游。
                    var deadlineSub = first.asMono()
                            .timeout(totalTimeout)
                            .subscribe(v -> { }, sink::error, () -> { });
                    sink.onDispose(() -> {
                        deadlineSub.dispose();
                        upstreamSub.dispose();
                    });
                });
            }
            return execution
                    .doOnSuccess(v -> permit.recordResult(
                            !route.getCircuitBreaker().isFailureStatus(finalStatus.get())))
                    .doOnError(err -> {
                        if (!(err instanceof CircuitOpenException) && isUpstreamFailure(err)) {
                            permit.recordFailure();
                        }
                    });
        });
    }

    private Mono<Void> attempt(UpstreamForwarder.PreparedUpstreamRequest prepared,
                               URI targetUri,
                               Function<UpstreamResponse, Mono<Void>> responseHandler,
                               RetryContext context,
                               int attemptNumber,
                               long startNanos) {
        boolean mayHaveNextAttempt = context.retryEnabled()
                && attemptNumber < context.maxAttempts()
                && hasTimeForAnotherAttempt(startNanos, context);
        return forwarder.forwardAttempt(prepared, targetUri, responseHandler,
                        status -> mayHaveNextAttempt && context.retryPolicy().isRetryStatus(status))
                .onErrorResume(UpstreamStatusRetryException.class, err -> {
                    // predicate 只在「不是最后一次」时抛这个异常，这里只负责按固定间隔退避后重发。
                    return Mono.delay(context.retryPolicy().backoff())
                            .then(Mono.defer(() -> attempt(prepared, targetUri, responseHandler,
                                    context, attemptNumber + 1, startNanos)));
                })
                .onErrorResume(err -> {
                    if (err instanceof CircuitOpenException
                            || err instanceof ResponseDeliveryException
                            || attemptNumber >= context.maxAttempts()
                            || !context.retryEnabled()
                            || !isRetryableUpstreamError(err)
                            || !hasTimeForAnotherAttempt(startNanos, context)) {
                        return Mono.error(err);
                    }
                    return Mono.delay(context.retryPolicy().backoff())
                            .then(Mono.defer(() -> attempt(prepared, targetUri, responseHandler,
                                    context, attemptNumber + 1, startNanos)));
                });
    }

    private boolean isRetryableUpstreamError(Throwable error) {
        UpstreamFailureKind kind = UpstreamFailureKind.classify(error);
        // 只重试连接失败/超时等可恢复的上游传输问题；协议解析等不确定错误不盲目放大。
        return kind == UpstreamFailureKind.UPSTREAM_UNAVAILABLE
                || kind == UpstreamFailureKind.UPSTREAM_TIMEOUT;
    }

    /**
     * 响应已开始交回调用方后，可能是调用方断连或下游写出失败，不能一律归咎上游；
     * 只有其内部原因仍能归类为连接/读取故障时，熔断器才计一笔失败。
     */
    private boolean isUpstreamFailure(Throwable error) {
        if (error instanceof ResponseDeliveryException delivery && error.getCause() != null) {
            // 响应已开始写给调用方后的异常可能是调用方断连，只有明确的上游连接/读超时才计熔断。
            return isRetryableUpstreamError(delivery.getCause());
        }
        return isRetryableUpstreamError(error)
                || UpstreamFailureKind.classify(error) == UpstreamFailureKind.UPSTREAM_PROTOCOL_ERROR;
    }

    private boolean hasTimeForAnotherAttempt(long startNanos, RetryContext context) {
        Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);
        Duration required = context.retryPolicy().backoff().plus(properties.responseTimeout());
        return context.retryPolicy().totalTimeout().minus(elapsed).compareTo(required) >= 0;
    }

    /**
     * 可重放时把请求体收成一个小 byte[]；否则返回 null，转发器继续从原始请求流读取。
     * 这里不使用全局无限 body 缓存：长度未知（chunked）或超过上限直接放弃重试，保护网关堆内存。
     */
    private Mono<BodyInput> prepareBody(ServerHttpRequest request, RetryContext context) {
        if (!context.retryEnabled()) {
            return Mono.just(BodyInput.stream());
        }
        long length = request.getHeaders().getContentLength();
        long maxBytes = properties.maxRetryBodySize().toBytes();
        boolean bodylessSafeMethod = request.getMethod() != null
                && (request.getMethod() == org.springframework.http.HttpMethod.GET
                || request.getMethod() == org.springframework.http.HttpMethod.HEAD
                || request.getMethod() == org.springframework.http.HttpMethod.OPTIONS)
                && length <= 0
                && !request.getHeaders().containsKey("Transfer-Encoding");
        if (length < 0 && !bodylessSafeMethod) {
            context.disableRetry();
            return Mono.just(BodyInput.stream());
        }
        if (length > maxBytes) {
            context.disableRetry();
            return Mono.just(BodyInput.stream());
        }
        return DataBufferUtils.join(request.getBody(), (int) maxBytes)
                .map(buffer -> {
                    try {
                        byte[] bytes = new byte[buffer.readableByteCount()];
                        buffer.read(bytes);
                        return BodyInput.replay(bytes);
                    } finally {
                        DataBufferUtils.release(buffer);
                    }
                })
                .defaultIfEmpty(BodyInput.replay(new byte[0]));
    }

    private record BodyInput(boolean replay, byte[] bytes) {
        static BodyInput stream() {
            return new BodyInput(false, null);
        }

        static BodyInput replay(byte[] bytes) {
            return new BodyInput(true, bytes);
        }
    }

    private static final class RetryContext {
        private final RetryPolicy retryPolicy;
        private final int maxAttempts;
        private boolean retryEnabled;

        private RetryContext(RetryPolicy retryPolicy, int maxAttempts, boolean retryEnabled) {
            this.retryPolicy = retryPolicy;
            this.maxAttempts = maxAttempts;
            this.retryEnabled = retryEnabled;
        }

        static RetryContext create(GatewayRoute route, ServerHttpRequest request,
                                   GatewayProxyProperties properties) {
            RetryPolicy policy = route.getRetry() == null
                    ? com.apigw.domain.route.RetryPolicy.disabled() : route.getRetry();
            boolean enabled = policy.isEnabled()
                    && policy.maxAttempts() > 1
                    && request.getMethod() != null
                    && policy.allowsMethod(request.getMethod().name());
            return new RetryContext(policy, policy.maxAttempts(), enabled);
        }

        RetryPolicy retryPolicy() {
            return retryPolicy;
        }

        int maxAttempts() {
            return maxAttempts;
        }

        boolean retryEnabled() {
            return retryEnabled;
        }

        void disableRetry() {
            retryEnabled = false;
        }
    }
}
