package com.apigw.interfaces.rest.route.vo;

import java.io.Serializable;
import java.util.List;

/**
 * 保存路由的请求体。
 *
 * 用 record 承载外部输入：字段与题目描述一一对应，不做多余包装。
 * conditions / actions 里每条对应一个 {@link RuleVO}；
 * grayGroups 可空：不传或传空数组 = 这条路由不做灰度，所有请求打主上游 upstream。
 * circuitBreaker / retry 可空：不传 = 该路由对应能力关闭，其他路由不受影响。
 */
public record RouteSaveVO(String routeNo,
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

    /**
     * 一个灰度分组：
     * weight 0~100 整数，各组之和必须恰好为 100；
     * tags 为该组认领的灰度标记值（精确、大小写敏感），可空表示该组只按权重接量。
     */
    public record GrayGroupVO(String groupName,
                              String upstream,
                              Integer weight,
                              List<String> tags) implements Serializable {
    }

    /**
     * 熔断策略。failureRate 是最近 windowSize 次、且至少 minimumCalls 次后的失败百分比。
     */
    public record CircuitBreakerVO(Boolean enabled,
                                   Integer windowSize,
                                   Integer minimumCalls,
                                   Integer failureRate,
                                   String openDuration,
                                   Integer halfOpenPermitPercent,
                                   List<Integer> failureStatuses) implements Serializable {
    }

    /**
     * 重试策略。maxAttempts 包含首次；totalTimeout 是所有尝试和退避的总预算。
     */
    public record RetryVO(Boolean enabled,
                          Integer maxAttempts,
                          String backoff,
                          String totalTimeout,
                          List<String> retryMethods,
                          Boolean idempotent,
                          List<Integer> retryStatuses) implements Serializable {
    }
}
