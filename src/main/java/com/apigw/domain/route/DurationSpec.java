package com.apigw.domain.route;

import com.apigw.common.exception.BizException;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 路由配置里的时长文本解析。支持 {@code 100ms}、{@code 10s}、{@code 5m}、{@code 1h}
 * 以及 ISO-8601 的 {@code PT10S}，避免管理接口要求运营手写不易读的 PT 格式。
 */
public final class DurationSpec {

    private static final Pattern SIMPLE = Pattern.compile("^(?<value>\\d+)(?<unit>ms|s|m|h)$");

    private DurationSpec() {
    }

    public static Duration parse(String text) {
        if (text == null || text.isBlank()) {
            return null;
        }
        String v = text.trim();
        try {
            if (v.startsWith("PT") || v.startsWith("P")) {
                return Duration.parse(v);
            }
            Matcher matcher = SIMPLE.matcher(v);
            if (!matcher.matches()) {
                throw new IllegalArgumentException();
            }
            long value = Long.parseLong(matcher.group("value"));
            return switch (matcher.group("unit")) {
                case "ms" -> Duration.ofMillis(value);
                case "s" -> Duration.ofSeconds(value);
                case "m" -> Duration.ofMinutes(value);
                case "h" -> Duration.ofHours(value);
                default -> throw new IllegalStateException();
            };
        } catch (Exception e) {
            throw new BizException("时长格式不正确：" + text + "；请使用 100ms、10s、5m 或 1h");
        }
    }

    public static String format(Duration duration) {
        if (duration == null) {
            return null;
        }
        long millis = duration.toMillis();
        if (millis % 3_600_000 == 0) {
            return millis / 3_600_000 + "h";
        }
        if (millis % 60_000 == 0) {
            return millis / 60_000 + "m";
        }
        if (millis % 1000 == 0) {
            return millis / 1000 + "s";
        }
        return millis + "ms";
    }
}
