package com.apigw.domain.route;

import com.apigw.common.exception.BizException;

import java.time.Duration;
import java.util.Locale;
import java.util.Set;

/**
 * 一条路由自己的重试配置。默认关闭；只有同时满足「方法可靠 + 请求体可重放」才真的再打一次。
 *
 * 幂等判断不能看 URL：/order/query 可能因为 POST 而创建临时资源，/payment/refund 也可能
 * 只是命名像查询。这里以 HTTP 方法语义和路由方的显式约定为准。
 */
public record RetryPolicy(Boolean enabled,
                          Integer maxAttempts,
                          Duration backoff,
                          Duration totalTimeout,
                          Set<String> retryMethods,
                          Boolean idempotent,
                          Set<Integer> retryStatuses) {

    /** RFC 9110 安全方法：除服务端主动埋点等非常规实现外，重复调用不会改变业务状态。 */
    public static final Set<String> SAFE_METHODS = Set.of("GET", "HEAD", "OPTIONS");

    /** 语义上可能幂等，但必须由路由配置显式声明后才重试的方法。 */
    public static final Set<String> OPT_IN_METHODS = Set.of("PUT", "DELETE");

    /** 非幂等写方法：下单/支付默认落在这组，网关绝不靠 URL 猜。 */
    private static final Set<String> NEVER_UNLESS_IDEMPOTENT = Set.of("POST", "PATCH");

    public static final Set<Integer> DEFAULT_RETRY_STATUSES = Set.of(500, 502, 503, 504);

    public static RetryPolicy disabled() {
        return new RetryPolicy(false, 2, Duration.ofMillis(100), Duration.ofSeconds(15),
                SAFE_METHODS, false, DEFAULT_RETRY_STATUSES);
    }

    public static RetryPolicy of(Boolean enabled,
                                 Integer maxAttempts,
                                 Duration backoff,
                                 Duration totalTimeout,
                                 Set<String> retryMethods,
                                 Boolean idempotent,
                                 Set<Integer> retryStatuses) {
        Set<String> methods = (retryMethods == null || retryMethods.isEmpty()
                ? SAFE_METHODS : normalizeMethods(retryMethods));
        RetryPolicy p = new RetryPolicy(
                Boolean.TRUE.equals(enabled),
                maxAttempts == null ? 2 : maxAttempts,
                backoff == null ? Duration.ofMillis(100) : backoff,
                totalTimeout == null ? Duration.ofSeconds(15) : totalTimeout,
                methods,
                Boolean.TRUE.equals(idempotent),
                retryStatuses == null || retryStatuses.isEmpty()
                        ? DEFAULT_RETRY_STATUSES : Set.copyOf(retryStatuses));
        p.validate();
        return p;
    }

    public boolean isEnabled() {
        return Boolean.TRUE.equals(enabled);
    }

    /**
     * 是否允许这笔请求重试。
     *
     * GET/HEAD/OPTIONS：HTTP 安全方法，默认允许；
     * PUT/DELETE：只有本路由显式列入 retryMethods 才允许（上游必须用幂等键/资源状态保证重复安全）；
     * POST/PATCH：默认禁止；即使列入 retryMethods，也必须再把路由 idempotent=true，
     * 用来表达「这个 POST 是内部约定的幂等接口」。下单、支付没做这个约定就永远不会重试。
     */
    public boolean allowsMethod(String method) {
        if (method == null) {
            return false;
        }
        String m = method.toUpperCase(Locale.ROOT);
        if (!retryMethods.contains(m)) {
            return false;
        }
        if (SAFE_METHODS.contains(m)) {
            return true;
        }
        if (OPT_IN_METHODS.contains(m)) {
            return true;
        }
        return idempotent && NEVER_UNLESS_IDEMPOTENT.contains(m);
    }

    public boolean isRetryStatus(int status) {
        return retryStatuses.contains(status);
    }

    private static Set<String> normalizeMethods(Set<String> input) {
        Set<String> methods = input.stream()
                .filter(m -> m != null && !m.isBlank())
                .map(m -> m.trim().toUpperCase(Locale.ROOT))
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        methods.forEach(RetryPolicy::requireKnownMethod);
        return methods;
    }

    private static void requireKnownMethod(String method) {
        if (!SAFE_METHODS.contains(method) && !OPT_IN_METHODS.contains(method)
                && !NEVER_UNLESS_IDEMPOTENT.contains(method)) {
            throw new BizException("不支持的重试方法：" + method
                    + "；只允许 GET/HEAD/OPTIONS/PUT/DELETE/POST/PATCH");
        }
    }

    private void validate() {
        if (maxAttempts < 1 || maxAttempts > 5) {
            throw new BizException("最大尝试次数必须是 1~5（含首次请求）");
        }
        if (backoff.isNegative() || backoff.compareTo(Duration.ofSeconds(10)) > 0) {
            throw new BizException("重试间隔不能为负，且最长不超过 10 秒");
        }
        if (totalTimeout.isZero() || totalTimeout.isNegative()
                || totalTimeout.compareTo(Duration.ofMinutes(5)) > 0) {
            throw new BizException("重试总时限必须大于 0，且最长不超过 5 分钟");
        }
        if (enabled && maxAttempts > 1 && totalTimeout.compareTo(backoff.multipliedBy(maxAttempts - 1)) < 0) {
            throw new BizException("重试总时限必须容得下配置的重试间隔，否则后续重试一定会被总时限截断");
        }
        if (retryStatuses.stream().anyMatch(s -> s < 500 || s > 599)) {
            throw new BizException("只允许对 5xx 临时响应配置重试；4xx 是请求自身问题，重试没有意义");
        }
        retryMethods.forEach(m -> {
            if (NEVER_UNLESS_IDEMPOTENT.contains(m) && !idempotent) {
                throw new BizException("方法 " + m + " 默认非幂等；只有该路由显式 idempotent=true，"
                        + "确认上游按幂等键去重后，才能列入重试方法");
            }
        });
    }
}
