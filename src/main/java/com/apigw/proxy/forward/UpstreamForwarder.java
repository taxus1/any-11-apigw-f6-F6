package com.apigw.proxy.forward;

import com.apigw.common.web.GatewayHeaders;
import com.apigw.domain.route.GatewayRoute;
import com.apigw.proxy.action.HeaderActionApplier;
import com.apigw.proxy.resilience.UpstreamStatusRetryException;
import com.apigw.proxy.userauth.OutboundAuth;
import io.netty.handler.codec.http.HttpHeaderValues;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpMethod;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserter;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * 把「已经匹配好路由」的请求发往上游，拿到上游响应头后交给上层落回给调用方。
 *
 * 请求方向做的事（顺序固定）：
 * 1. 复制调用方请求头，剔除「逐跳头」和与具体报文绑定的头——
 *    Connection 及其列出的 keep-alive/te/trailer/upgrade/proxy-authorization 等
 *    只对「调用方↔网关」这一段有意义，不能转发给上游；
 *    Content-Length / Transfer-Encoding 与即将发出的报文绑定，交给 HTTP 客户端按实际请求体重算；
 *    Host 也不能沿用，客户端按目标上游地址重新生成；
 * 2. 清掉网关独占的身份/通行头（X-User-Id / X-Tenant-Id / X-Gateway-Pass）：
 *    这几个头只由网关按验签结果写入，调用方在入站塞的同名头一律视为伪造、先清干净；
 *    用户鉴权启用时连 Authorization 一起剥掉——原始令牌不原样递上游；
 * 3. 补 X-Forwarded-For / X-Forwarded-Proto / X-Forwarded-Host，让上游看得到原始链路信息；
 * 4. 写入网关认定的身份头与通行标记（来自 {@link OutboundAuth}，没验出身份就一个都不写）；
 * 5. 按顺序号执行本路由的请求类动作（补头覆盖同名旧值、删头彻底删除）；
 * 6. 请求体默认以数据流形式透传，不在网关里全量缓冲；只有上层明确要求「可安全重放」且
 *    Content-Length 明确、未超过上限时才缓存成字节数组供重试复用。
 */
@Component
public class UpstreamForwarder {

    /** RFC 7230 逐跳头 + 与报文强绑定的头：这些绝不原样转发。 */
    private static final Set<String> HOP_BY_HOP = new LinkedHashSet<>(List.of(
            "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
            "trailer", "transfer-encoding", "upgrade", "http2-settings",
            // 内容长度/传输编码与具体报文绑死，交给客户端/框架按实际报文重算
            "content-length", "host"));

    private final WebClient webClient;

    public UpstreamForwarder(WebClient webClient) {
        this.webClient = webClient;
    }

    /**
     * 旧转发入口：只尝试一次，且不识别「可重试 5xx」。保留给不经过韧性执行器的测试/调用方。
     */
    public Mono<Void> forward(GatewayRoute route, ServerHttpRequest incoming,
                              String traceId, URI targetUri, OutboundAuth auth,
                              Function<UpstreamResponse, Mono<Void>> responseHandler) {
        PreparedUpstreamRequest prepared = prepareRequest(route, incoming, traceId, auth, null);
        return forwardAttempt(prepared, targetUri, responseHandler, status -> false);
    }

    /**
     * 只做一次请求头清洗/改写，并决定请求体如何发送。重试时反复调用 {@link #forwardAttempt}，
     * 不再重复执行 mutate/action，避免 X-Forwarded-For 追加两次或某个补/删头动作被做两遍。
     */
    public PreparedUpstreamRequest prepareRequest(GatewayRoute route, ServerHttpRequest incoming,
                                                  String traceId, OutboundAuth auth,
                                                  byte[] bufferedBody) {
        ServerHttpRequest mutated = incoming.mutate().headers(headers -> {
            // 1. 剔除逐跳头与报文绑定头（头名大小写不敏感，统一小写比对）
            HOP_BY_HOP.forEach(headers::remove);
            // TE 是逐跳头但 RFC 7231 允许 "trailers"：调用方带了就只放行这一个值，其余一律去掉
            String te = headers.getFirst("te");
            headers.remove("te");
            if (te != null && te.toLowerCase(Locale.ROOT).contains("trailers")) {
                headers.set("te", HttpHeaderValues.TRAILERS.toString());
            }

            // 2. 网关独占头清零：身份头/通行标记只由网关写。调用方塞的同名头先全部清掉，
            //    再按网关验签结果写——伪造身份一个字都到不了上游（在动作之前清，运营显式配置的
            //    补头动作仍可覆盖，那是配置侧的明确选择，不是调用方能影响的）
            headers.remove(GatewayHeaders.USER_ID_HEADER);
            headers.remove(GatewayHeaders.TENANT_ID_HEADER);
            headers.remove(GatewayHeaders.GATEWAY_PASS_HEADER);
            if (auth.stripAuthorization()) {
                // 用户鉴权启用：原始令牌只在「调用方↔网关」这段有效，绝不原样递上游
                headers.remove(GatewayHeaders.AUTHORIZATION_HEADER);
            }

            // 3. 透传原始链路信息（先加，动作如果想覆盖可以再覆盖）
            String remote = incoming.getRemoteAddress() == null
                    ? null : incoming.getRemoteAddress().getAddress().getHostAddress();
            if (remote != null) {
                String prev = headers.getFirst("X-Forwarded-For");
                headers.set("X-Forwarded-For",
                        prev == null || prev.isBlank() ? remote : prev + ", " + remote);
            }
            headers.set("X-Forwarded-Proto", incoming.getURI().getScheme() == null
                    ? "http" : incoming.getURI().getScheme());
            String originalHost = incoming.getHeaders().getFirst("Host");
            if (originalHost != null) {
                headers.set("X-Forwarded-Host", originalHost);
            }
            headers.set("X-Gateway-Trace-Id", traceId);

            // 4. 网关认定的身份与通行标记：只有验签后有值才写，匿名就没有这些头
            if (auth.identity() != null) {
                headers.set(GatewayHeaders.USER_ID_HEADER, auth.identity().userId());
                headers.set(GatewayHeaders.TENANT_ID_HEADER, auth.identity().tenantId());
            }
            if (auth.gatewayPass() != null) {
                headers.set(GatewayHeaders.GATEWAY_PASS_HEADER, auth.gatewayPass());
            }

            // 5. 请求类动作按顺序号执行；补头覆盖同名值（包括调用方自己塞的），删头彻底删除
            HeaderActionApplier.applyRequestActions(route, headers);
        }).build();
        return new PreparedUpstreamRequest(mutated, bufferedBody);
    }

    /**
     * 用已经准备好的请求发起一次上游调用。retryStatus 命中时：
     * - 最后一次（deliverRetryStatus=true）把该上游响应原样交给 responseHandler，调用方看到真实状态；
     * - 非最后一次先释放响应体并抛 {@link UpstreamStatusRetryException}，由韧性执行器决定是否重试。
     */
    public Mono<Void> forwardAttempt(PreparedUpstreamRequest prepared,
                                     URI targetUri,
                                     Function<UpstreamResponse, Mono<Void>> responseHandler,
                                     Predicate<Integer> retryStatus) {
        ServerHttpRequest request = prepared.request();
        WebClient.RequestBodySpec spec = webClient
                .method(HttpMethod.valueOf(request.getMethod().name()))
                .uri(targetUri)
                .headers(h -> h.addAll(request.getHeaders()));

        BodyInserter<?, ? super org.springframework.http.client.reactive.ClientHttpRequest> body =
                prepared.bodyBytes() != null
                        ? BodyInserters.fromValue(prepared.bodyBytes())
                        : BodyInserters.fromDataBuffers(request.getBody());

        return spec.body(body).exchangeToMono(response -> {
            int status = response.statusCode().value();
            if (retryStatus.test(status)) {
                // 读完/释放响应体后再重试，不能让这条上游连接悬着；最终尝试不会走这里。
                return response.releaseBody()
                        .then(Mono.error(new UpstreamStatusRetryException(status)));
            }
            UpstreamResponse upstreamResponse = new UpstreamResponse(
                    status,
                    response.headers().asHttpHeaders(),
                    response.bodyToFlux(DataBuffer.class));
            return responseHandler.apply(upstreamResponse);
        });
    }

    /**
     * 拼上游目标地址：上游 base（scheme://host:port）+ 原始请求路径（含原始 query）。
     * 用原始（raw）形式拼接，避免对已经编码过的路径做二次编码。
     */
    public static URI resolveTargetUri(String upstreamBase, ServerHttpRequest request) {
        URI base = URI.create(upstreamBase);
        StringBuilder sb = new StringBuilder();
        sb.append(base.getScheme()).append("://").append(base.getRawAuthority());
        // base 里带的路径前缀（如 http://host/svc）拼在请求路径前；一般配置里不带
        String basePath = base.getRawPath();
        if (basePath != null && !basePath.isBlank() && !"/".equals(basePath)) {
            sb.append(basePath);
        }
        sb.append(request.getPath().pathWithinApplication().value());
        if (request.getURI().getRawQuery() != null) {
            sb.append('?').append(request.getURI().getRawQuery());
        }
        return URI.create(sb.toString());
    }

    /**
     * 已完成请求头改写的一次转发输入。bodyBytes 非空表示可重复发送的小请求体；
     * 为空表示使用 {@link ServerHttpRequest#getBody()} 流式发送，调用方不得重试。
     */
    public record PreparedUpstreamRequest(ServerHttpRequest request, byte[] bodyBytes) {
    }
}
