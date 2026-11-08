package offheap;

/**
 * 堆外缓存淘汰与门禁准入策略枚举：
 *
 * 1. K_SAMPLE_LRU:
 *    Redis 风格 K-Sample 采样近似 LRU 淘汰算法。
 *    实现轻量，微秒级执行，适用于常规均匀访问场景。
 *
 * 2. WINDOW_TINY_LFU:
 *    Caffeine 风格基于 4-bit Count-Min 频率草图的 Window TinyLFU 门禁准入与频次偏向淘汰算法。
 *    在堆外仅需 32KB~64KB 极小内存，具备极致的【抗全表扫描污染 (Scan Resistance)】能力，
 *    在帕累托倾斜与突发突增负载下，缓存命中率相比传统 LRU 提升 15% ~ 30%！
 */
public enum EvictionPolicy {
    /**
     * Redis 风格 K-Sample 采样近似 LRU 淘汰
     */
    K_SAMPLE_LRU,

    /**
     * Caffeine 风格 Window TinyLFU 门禁准入与频率淘汰 (推荐生产默认)
     */
    WINDOW_TINY_LFU
}
