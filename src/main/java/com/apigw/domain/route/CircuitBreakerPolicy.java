package com.apigw.domain.route;

import com.apigw.common.exception.BizException;

import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 一条路由自己的熔断配置。默认 disabled=false/null，配置缺省时转发行为与旧版本完全一致。
 *
 * 判定口径选「滚动窗口内失败率 + 最小样本数」，而不是只看失败次数：
 * 低流量路由不会因为一次偶发故障立刻断开，最小样本数到了才让失败率生效；
 * 高流量时也不会被大量成功请求稀释到永远断不开（窗口固定只看最近 N 次）。
 */
public record CircuitBreakerPolicy(Boolean enabled,
                                   Integer windowSize,
                                   Integer minimumCalls,
                                   Integer failureRate,
                                   Duration openDuration,
                                   Integer halfOpenPermitPercent,
                                   Set<Integer> failureStatuses) {

    public static final Set<Integer> DEFAULT_FAILURE_STATUSES = Set.of(500, 502, 503, 504);

    public static CircuitBreakerPolicy disabled() {
        return new CircuitBreakerPolicy(false, 20, 5, 50, Duration.ofSeconds(30), 10,
                DEFAULT_FAILURE_STATUSES);
    }

    /**
     * 外部配置使用字符串时长（如 30s / 1m），由接口层解析后再进入领域对象。
     */
    public static CircuitBreakerPolicy of(Boolean enabled,
                                          Integer windowSize,
                                          Integer minimumCalls,
                                          Integer failureRate,
                                          Duration openDuration,
                                          Integer halfOpenPermitPercent,
                                          Set<Integer> failureStatuses) {
        CircuitBreakerPolicy p = new CircuitBreakerPolicy(
                Boolean.TRUE.equals(enabled),
                windowSize == null ? 20 : windowSize,
                minimumCalls == null ? 5 : minimumCalls,
                failureRate == null ? 50 : failureRate,
                openDuration == null ? Duration.ofSeconds(30) : openDuration,
                halfOpenPermitPercent == null ? 10 : halfOpenPermitPercent,
                failureStatuses == null || failureStatuses.isEmpty()
                        ? DEFAULT_FAILURE_STATUSES : Set.copyOf(failureStatuses));
        p.validate();
        return p;
    }

    public boolean isEnabled() {
        return Boolean.TRUE.equals(enabled);
    }

    public boolean isFailureStatus(int status) {
        return failureStatuses.contains(status);
    }

    private void validate() {
        if (windowSize < 2 || windowSize > 1000) {
            throw new BizException("熔断滚动窗口大小必须在 2~1000 次之间");
        }
        if (minimumCalls < 1 || minimumCalls > windowSize) {
            throw new BizException("熔断最小样本数必须在 1~滚动窗口大小之间");
        }
        if (failureRate < 1 || failureRate > 100) {
            throw new BizException("熔断失败比例必须是 1~100 的整数百分比");
        }
        if (openDuration == null || openDuration.isZero() || openDuration.isNegative()
                || openDuration.compareTo(Duration.ofHours(1)) > 0) {
            throw new BizException("熔断等待时长必须大于 0，且最长不超过 1 小时");
        }
        if (halfOpenPermitPercent < 1 || halfOpenPermitPercent > 100) {
            throw new BizException("半开试放比例必须是 1~100 的整数百分比");
        }
        if (failureStatuses.stream().anyMatch(s -> s < 500 || s > 599)) {
            throw new BizException("熔断只允许把 5xx 状态码算作上游失败；4xx 是调用方请求问题，不能触发熔断");
        }
    }

    /** 日志/错误头使用稳定、可预测的状态码集合字符串。 */
    public String failureStatusesText() {
        return failureStatuses.stream().sorted().map(String::valueOf).collect(Collectors.joining(","));
    }
}
