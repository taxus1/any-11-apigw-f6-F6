package com.apigw.proxy.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.util.concurrent.TimeUnit;

/**
 * 转发用的上游 HTTP 客户端。
 *
 * 超时刻意分两段，对应两类不同的故障答复：
 * - connectTimeout：TCP/TLS 握手都完不成 → 上游不可达（502）；
 * - responseTimeout（Netty readTimeout）：连上了但上游迟迟不回 → 网关等不起（504）。
 *
 * 连接走复用池，避免每个转发请求都重新握手；不自动跟随重定向——重定向是上游给调用方的
 * 业务响应，网关不替它做决定，原样转回去由调用方处理。
 */
@Configuration
public class UpstreamClientConfig {

    @Bean
    public HttpClient upstreamHttpClient(GatewayProxyProperties properties) {
        ConnectionProvider provider = ConnectionProvider.builder("apigw-upstream")
                .maxConnections(properties.maxConnections())
                // 池满后不能无限排队：故障上游超时越久，排队请求会越多，必须有硬上限让网关快速失败
                .pendingAcquireMaxCount(properties.maxPendingConnections())
                .pendingAcquireTimeout(properties.connectTimeout())
                .build();

        return HttpClient.create(provider)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                        (int) properties.connectTimeout().toMillis())
                .responseTimeout(properties.responseTimeout())
                .doOnConnected(conn -> conn
                        .addHandlerLast(new ReadTimeoutHandler(
                                properties.responseTimeout().toMillis(), TimeUnit.MILLISECONDS))
                        .addHandlerLast(new WriteTimeoutHandler(
                                properties.connectTimeout().toMillis(), TimeUnit.MILLISECONDS)));
    }

    @Bean
    public WebClient upstreamWebClient(HttpClient upstreamHttpClient) {
        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(upstreamHttpClient))
                .build();
    }
}
