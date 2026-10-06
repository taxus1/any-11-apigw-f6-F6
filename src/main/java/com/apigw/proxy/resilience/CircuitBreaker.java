package com.apigw.proxy.resilience;

import com.apigw.domain.route.CircuitBreakerPolicy;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 单机、内存态的熔断器；每台网关各管各的，不使用 Redis 共享状态。
 *
 * 三个状态：
 * - CLOSED：正常放量，只保留最近 windowSize 次上游结果；失败率达到阈值且样本数达标后断开；
 * - OPEN：一个请求都不打上游，等待 openDuration，让故障上游喘息；
 * - HALF_OPEN：等待结束后按 halfOpenPermitPercent 均匀放探测请求，其余仍快速失败；
 *   探满 minimumCalls 笔后，失败率低于阈值则 CLOSED，否则重新 OPEN。
 *
 * 4xx 不进入这里；只由上层把「连接失败/超时/明确配置为上游失败的 5xx」记为失败。
 */
class CircuitBreaker {

    private enum State { CLOSED, OPEN, HALF_OPEN }

    private final CircuitBreakerPolicy policy;
    private final Clock clock;

    private State state = State.CLOSED;
    private final Deque<Boolean> closedWindow = new ArrayDeque<>();
    private final Deque<Boolean> halfOpenWindow = new ArrayDeque<>();
    private long openedAtMillis;

    /** HALF_OPEN 中已经到达本网关的请求序号，用于把试放比例均匀摊开。 */
    private final AtomicLong halfOpenArrivals = new AtomicLong();
    /** HALF_OPEN 中已经接受、尚未回结果的探测请求数。 */
    private int halfOpenInFlight;

    CircuitBreaker(CircuitBreakerPolicy policy, Clock clock) {
        this.policy = policy;
        this.clock = clock;
    }

    CircuitBreakerPolicy policy() {
        return policy;
    }

    synchronized CircuitBreakerPermit acquire() {
        long now = clock.millis();
        if (state == State.OPEN) {
            if (now - openedAtMillis >= policy.openDuration().toMillis()) {
                enterHalfOpen(now);
            } else {
                long remaining = policy.openDuration().toMillis() - (now - openedAtMillis);
                return CircuitBreakerPermit.rejected(Duration.ofMillis(Math.max(1, remaining)));
            }
        }

        if (state == State.HALF_OPEN) {
            if (halfOpenInFlight >= policy.minimumCalls() - halfOpenWindow.size()) {
                // 首批探测名额放满后等待结果，避免低流量下为了凑数把「试放比例」悄悄放大成 100%。
                return CircuitBreakerPermit.rejected(Duration.ZERO);
            }
            long arrival = halfOpenArrivals.getAndIncrement();
            if (!admitByPercent(arrival, policy.halfOpenPermitPercent())) {
                return CircuitBreakerPermit.rejected(Duration.ZERO);
            }
            halfOpenInFlight++;
        }
        return CircuitBreakerPermit.accepted(this);
    }

    synchronized void recordResult(boolean success) {
        long now = clock.millis();
        if (state == State.OPEN) {
            // 等待期间理论上没有被接受的请求；兜底忽略迟到结果，不污染下一轮探测。
            return;
        }

        if (state == State.HALF_OPEN) {
            halfOpenInFlight = Math.max(0, halfOpenInFlight - 1);
            halfOpenWindow.addLast(success);
            if (halfOpenWindow.size() >= policy.minimumCalls()) {
                if (failureRate(halfOpenWindow) < policy.failureRate()) {
                    closeAndReset();
                } else {
                    enterOpen(now);
                }
            }
            return;
        }

        closedWindow.addLast(success);
        while (closedWindow.size() > policy.windowSize()) {
            closedWindow.removeFirst();
        }
        if (closedWindow.size() >= policy.minimumCalls()
                && failureRate(closedWindow) >= policy.failureRate()) {
            enterOpen(now);
        }
    }

    synchronized String stateName() {
        return state.name();
    }

    private void enterOpen(long now) {
        state = State.OPEN;
        openedAtMillis = now;
        halfOpenInFlight = 0;
        closedWindow.clear();
        halfOpenWindow.clear();
    }

    private void enterHalfOpen(long now) {
        state = State.HALF_OPEN;
        halfOpenWindow.clear();
        halfOpenInFlight = 0;
        // 每个半开周期从 0 开始做均匀分流；首个请求必然可作为探测。
        halfOpenArrivals.set(0);
        openedAtMillis = now;
    }

    private void closeAndReset() {
        state = State.CLOSED;
        halfOpenInFlight = 0;
        closedWindow.clear();
        halfOpenWindow.clear();
    }

    private int failureRate(Deque<Boolean> results) {
        int failures = 0;
        for (Boolean success : results) {
            if (!success) {
                failures++;
            }
        }
        return (int) Math.ceil(failures * 100.0 / results.size());
    }

    /**
     * Bresenham 式均匀取点：10% 时每 10 个放 1 个且第一个放行，50% 隔一个放一个，
     * 比随机抽样更稳定，也不会高并发下一下子放进远超比例的请求。
     */
    private boolean admitByPercent(long arrival, int percent) {
        int divisor = gcd(percent, 100);
        long cycleLength = 100L / divisor;
        long acceptsPerCycle = (long) percent / divisor;
        return arrival % cycleLength < acceptsPerCycle;
    }

    private int gcd(int a, int b) {
        return b == 0 ? a : gcd(b, a % b);
    }
}
