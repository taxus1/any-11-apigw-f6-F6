package com.apigw.proxy.resilience;

import com.apigw.domain.route.CircuitBreakerPolicy;
import com.apigw.domain.route.GatewayRoute;
import com.apigw.proxy.route.RoutesChangedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 按「路由 + 实际上游分组」维护本机熔断器。
 *
 * 灰度时同一路由的不同组可能是不同部署单元，因此键里必须带 grayGroup；
 * 无灰度用固定 "-"。熔断状态不跨网关实例共享，一台打开不影响其他两台的本机判断。
 */
@Component
public class CircuitBreakerRegistry {

    private static final String NO_GROUP = "-";

    private final ConcurrentHashMap<String, CircuitBreaker> breakers = new ConcurrentHashMap<>();
    private final Clock clock;

    public CircuitBreakerRegistry() {
        this(Clock.systemUTC());
    }

    CircuitBreakerRegistry(Clock clock) {
        this.clock = clock;
    }

    public CircuitBreakerPermit acquire(GatewayRoute route, String groupName) {
        CircuitBreakerPolicy policy = route.getCircuitBreaker();
        if (policy == null || !policy.isEnabled()) {
            return CircuitBreakerPermit.accepted(NoOpCircuitBreaker.INSTANCE);
        }
        return breakers.computeIfAbsent(key(route.getRouteNo(), groupName),
                        k -> new CircuitBreaker(policy, clock))
                .acquire();
    }

    /**
     * 路由配置变更后清掉这一路由下的运行时状态。配置里的阈值已经改变，旧状态窗口继续沿用会造成
     * 口径混乱；删路由也顺手回收内存。三台各自监听本机刷新事件即可，不需要共享状态清理。
     */
    @EventListener
    public void onRouteChanged(RoutesChangedEvent event) {
        String prefix = event.routeNo() + "#";
        breakers.keySet().removeIf(k -> k.startsWith(prefix));
    }

    static String key(String routeNo, String groupName) {
        return routeNo + "#" + (groupName == null || groupName.isBlank() ? NO_GROUP : groupName);
    }

    private static final class NoOpCircuitBreaker extends CircuitBreaker {
        private static final NoOpCircuitBreaker INSTANCE = new NoOpCircuitBreaker();

        private NoOpCircuitBreaker() {
            super(CircuitBreakerPolicy.disabled(), Clock.systemUTC());
        }

        @Override
        void recordResult(boolean success) {
            // 熔断未开启：不统计、不改变转发行为。
        }
    }
}
