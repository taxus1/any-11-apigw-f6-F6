package com.apigw.proxy.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;

import java.time.Duration;

/**
 * 转发链路可调参数（前缀 apigw.proxy）。
 *
 * 超时拆成两段：连不上上游（connect）和上游半天不吭声（response），
 * 两类故障给调用方的答复要区分得开（502 vs 504）。
 *
 * @param connectTimeout 与上游建连超时（时间内握不上手 → 502 UPSTREAM_UNAVAILABLE）
 * @param responseTimeout 单次上游尝试的响应超时（连上了但迟迟不回首字节 → 504 UPSTREAM_TIMEOUT）
 * @param routeRefreshInterval 路由快照兜底轮询周期：本实例配置变更走事件即时刷新，
 *                             轮询是给「别的实例改了配置、没收到事件」留的最终一致兜底
 * @param maxConnections 全局上游连接池上限，防止单个故障上游拖垮整个网关
 * @param maxPendingConnections 连接池占满后最多排队的新请求数，超出立即失败而非无限堆积
 * @param maxRetryBodySize 允许为了重试缓存的最大请求体；超过或长度未知一律不重试、继续流式转发
 */
@ConfigurationProperties(prefix = "apigw.proxy")
public record GatewayProxyProperties(Duration connectTimeout,
                                     Duration responseTimeout,
                                     Duration routeRefreshInterval,
                                     Integer maxConnections,
                                     Integer maxPendingConnections,
                                     DataSize maxRetryBodySize) {

    public GatewayProxyProperties {
        if (connectTimeout == null) {
            connectTimeout = Duration.ofSeconds(3);
        }
        if (responseTimeout == null) {
            responseTimeout = Duration.ofSeconds(10);
        }
        if (routeRefreshInterval == null) {
            routeRefreshInterval = Duration.ofSeconds(10);
        }
        if (maxConnections == null) {
            maxConnections = 200;
        }
        if (maxPendingConnections == null) {
            maxPendingConnections = 200;
        }
        if (maxRetryBodySize == null) {
            maxRetryBodySize = DataSize.ofMegabytes(1);
        }
        if (maxConnections < 1 || maxPendingConnections < 0) {
            throw new IllegalArgumentException("上游连接池大小必须为正数，排队上限不能为负");
        }
        if (maxRetryBodySize.toBytes() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("重试请求体缓存上限不能超过 2GB");
        }
    }
}
