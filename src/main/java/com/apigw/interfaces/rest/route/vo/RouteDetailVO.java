package com.apigw.interfaces.rest.route.vo;

import com.apigw.domain.route.CircuitBreakerPolicy;
import com.apigw.domain.route.DurationSpec;
import com.apigw.domain.route.GatewayRoute;
import com.apigw.domain.route.GrayGroup;
import com.apigw.domain.route.GatewayRule;
import com.apigw.domain.route.RetryPolicy;
import com.apigw.infrastructure.store.RouteStore;

import java.io.Serializable;
import java.util.List;

/**
 * 路由详情视图：带回全部匹配条件与转发动作，子项按顺序号排好；
 * 配了灰度时带回灰度分组（配置顺序即权重轮询的稳定次序）。
 */
public record RouteDetailVO(String id,
                            String routeNo,
                            String name,
                            String upstream,
                            Integer enabled,
                            Integer authRequired,
                            String remark,
                            Integer version,
                            List<RuleVO> conditions,
                            List<RuleVO> actions,
                            List<GrayGroupVO> grayGroups,
                            CircuitBreakerVO circuitBreaker,
                            RetryVO retry) implements Serializable {

    public record RuleVO(String stage,
                         String type,
                         String name,
                         String value,
                         Integer sortNo) implements Serializable {
    }

    public record GrayGroupVO(String groupName,
                              String upstream,
                              Integer weight,
                              List<String> tags) implements Serializable {
    }

    public record CircuitBreakerVO(Boolean enabled,
                                   Integer windowSize,
                                   Integer minimumCalls,
                                   Integer failureRate,
                                   String openDuration,
                                   Integer halfOpenPermitPercent,
                                   List<Integer> failureStatuses) implements Serializable {
    }

    public record RetryVO(Boolean enabled,
                          Integer maxAttempts,
                          String backoff,
                          String totalTimeout,
                          List<String> retryMethods,
                          Boolean idempotent,
                          List<Integer> retryStatuses) implements Serializable {
    }

    public static RouteDetailVO of(GatewayRoute r) {
        return new RouteDetailVO(
                r.getId(),
                r.getRouteNo(),
                r.getName(),
                r.getUpstream(),
                r.getEnabled(),
                r.getAuthRequired(),
                r.getRemark(),
                r.getVersion(),
                RouteStore.sorted(r.getConditions()).stream().map(RouteDetailVO::toRuleVo).toList(),
                RouteStore.sorted(r.getActions()).stream().map(RouteDetailVO::toRuleVo).toList(),
                r.getGrayGroups() == null ? List.of()
                        : r.getGrayGroups().stream().map(RouteDetailVO::toGroupVo).toList(),
                toCircuitBreakerVo(r.getCircuitBreaker()),
                toRetryVo(r.getRetry()));
    }

    private static RuleVO toRuleVo(GatewayRule g) {
        return new RuleVO(g.getStage(), g.getType(), g.getName(), g.getValue(), g.getSortNo());
    }

    private static GrayGroupVO toGroupVo(GrayGroup g) {
        return new GrayGroupVO(g.getGroupName(), g.getUpstream(), g.getWeight(),
                g.getTags() == null ? List.of() : List.copyOf(g.getTags()));
    }

    private static CircuitBreakerVO toCircuitBreakerVo(CircuitBreakerPolicy p) {
        return new CircuitBreakerVO(p.isEnabled(), p.windowSize(), p.minimumCalls(), p.failureRate(),
                DurationSpec.format(p.openDuration()), p.halfOpenPermitPercent(),
                p.failureStatuses().stream().sorted().toList());
    }

    private static RetryVO toRetryVo(RetryPolicy p) {
        return new RetryVO(p.isEnabled(), p.maxAttempts(), DurationSpec.format(p.backoff()),
                DurationSpec.format(p.totalTimeout()),
                p.retryMethods().stream().sorted().toList(), p.idempotent(),
                p.retryStatuses().stream().sorted().toList());
    }
}
