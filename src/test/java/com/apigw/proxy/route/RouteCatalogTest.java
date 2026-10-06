package com.apigw.proxy.route;

import com.apigw.domain.route.GatewayRoute;
import com.apigw.domain.route.GatewayRule;
import com.apigw.infrastructure.store.RouteStore;
import com.apigw.proxy.config.GatewayProxyProperties;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 路由目录快照测试（不依赖 Redis，store mock）：
 * - 快照在新鲜期内不重复打 Redis；
 * - 配置变更事件触发立即重载，新路由马上可见（对应「新配路由立刻走通、不用重启」）；
 * - Redis 故障：已有旧快照时沿用旧的继续转发；从没加载成功时才报错（上层回 503）；
 * - 停用的、没有条件的路由不进快照。
 */
class RouteCatalogTest {

    private final GatewayProxyProperties props =
            new GatewayProxyProperties(Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofHours(1),
                    200, 200, org.springframework.util.unit.DataSize.ofMegabytes(1));

    private GatewayRule pathPrefix(String v) {
        return GatewayRule.create(null, "PATH_PREFIX", null, v, 1);
    }

    private GatewayRoute route(String no, Integer enabled, int conditionCount) {
        GatewayRoute r = GatewayRoute.create(no, no, "http://h:1", enabled, null);
        List<GatewayRule> conds = conditionCount == 0 ? List.of()
                : List.of(pathPrefix("/" + no + "/"));
        r.replaceRules(conds, List.of());
        return r;
    }

    @Test
    void freshSnapshot_isNotReLoadedFromRedis() {
        RouteStore store = mock(RouteStore.class);
        when(store.findAll()).thenReturn(Flux.just(route("a", 1, 1)));
        RouteCatalog catalog = new RouteCatalog(store, props);

        catalog.refresh().block();
        catalog.routes().block();
        catalog.routes().block();

        // TTL 很长（1 小时），后两次都命中内存快照，只打一次 Redis
        verify(store, times(1)).findAll();
        assertEquals(1, catalog.routes().block().size());
    }

    @Test
    void changedEvent_makesNewRouteVisibleImmediately() {
        AtomicInteger version = new AtomicInteger();
        RouteStore store = mock(RouteStore.class);
        when(store.findAll()).thenAnswer(inv -> switch (version.get()) {
            case 0 -> Flux.just(route("old", 1, 1));
            default -> Flux.just(route("old", 1, 1), route("new", 1, 1));
        });
        RouteCatalog catalog = new RouteCatalog(store, props);
        catalog.refresh().block();
        assertEquals(1, catalog.routes().block().size());

        // 模拟管理接口在本实例新建了一条路由
        version.set(1);
        catalog.onRoutesChanged(RoutesChangedEvent.created("new"));

        List<GatewayRoute> routes = catalog.routes().block();
        assertEquals(2, routes.size(), "事件即时刷新后，新路由立刻可见，不用重启、不等轮询");
    }

    @Test
    void redisFailureAfterGoodSnapshot_keepsServingStaleRoutes() {
        AtomicInteger fail = new AtomicInteger();
        RouteStore store = mock(RouteStore.class);
        when(store.findAll()).thenAnswer(inv -> {
            if (fail.get() == 0) {
                return Flux.just(route("a", 1, 1));
            }
            return Flux.error(new RuntimeException("redis down"));
        });
        RouteCatalog catalog = new RouteCatalog(store, props);
        catalog.refresh().block();

        fail.set(1);
        // 强制过期后再取：刷新失败，但有旧快照，继续给旧配置而不是把转发打挂
        catalog.refresh().block(); // 这次失败回落旧快照，不抛
        List<GatewayRoute> routes = catalog.routes().block();
        assertEquals(1, routes.size());
        assertEquals("a", routes.get(0).getRouteNo());
    }

    @Test
    void redisFailureBeforeAnySnapshot_propagatesError() {
        RouteStore store = mock(RouteStore.class);
        when(store.findAll()).thenReturn(Flux.error(new RuntimeException("redis down")));
        RouteCatalog catalog = new RouteCatalog(store, props);

        // 启动期就拿不到任何配置：错误冒上去，由过滤器回 503 CONFIG_UNAVAILABLE
        assertThrows(RuntimeException.class, () -> catalog.routes().block());
    }

    @Test
    void disabledAndConditionlessRoutes_areExcluded() {
        RouteStore store = mock(RouteStore.class);
        when(store.findAll()).thenReturn(Flux.just(
                route("enabled-with-conds", 1, 1),
                route("disabled", 0, 1),
                route("enabled-no-conds", 1, 0)));
        RouteCatalog catalog = new RouteCatalog(store, props);

        List<GatewayRoute> routes = catalog.refresh().block();
        assertEquals(1, routes.size());
        assertEquals("enabled-with-conds", routes.get(0).getRouteNo());
    }

    @Test
    void concurrentLoads_shareOneRedisCall() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        RouteStore store = mock(RouteStore.class);
        when(store.findAll()).thenAnswer(inv -> {
            calls.incrementAndGet();
            return Flux.just(route("a", 1, 1)).delayElements(Duration.ofMillis(50));
        });
        RouteCatalog catalog = new RouteCatalog(store, props);

        // 冷缓存时并发触发，不应打成雷群
        List<Thread> threads = new java.util.ArrayList<>();
        for (int i = 0; i < 8; i++) {
            Thread t = new Thread(() -> catalog.routes().block());
            threads.add(t);
        }
        threads.forEach(Thread::start);
        for (Thread t : threads) {
            t.join();
        }
        assertEquals(1, calls.get(), "并发冷加载应共用同一个进行中的 Mono");
    }
}
