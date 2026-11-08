package offheap;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Window TinyLFU 深度逻辑专项验证与命中率对比基准测试 (Window TinyLFU Deep Verification Test)
 *
 * 覆盖范围：
 * 1. 4-bit 饱和计数器上限隔离 (0 ~ 15 饱和不溢出)
 * 2. 硬件级单周期位运算半衰衰减数学精度 (15 -> 7 -> 3 -> 1 -> 0)
 * 3. 准入门禁防御性拦截 (Cold 一击即走数据 0% 侵入率)
 * 4. 准入门禁晋升机制 (真热点 Candidate 成功淘汰冷门 Victim)
 * 5. 过期键优先级最高准则 (即使是曾经的高频热点，过期后立即淘汰无需经过门禁拦截)
 * 6. 真实 Zipfian + 扫描场景下的命中率对比测试 (TinyLFU 远胜标准 LRU)
 * 7. 多线程极端并发风暴下零死锁与零损坏
 */
public class WindowTinyLfuDeepVerificationTest {

    public static void main(String[] args) throws Exception {
        System.out.println("================================================================================");
        System.out.println("      🧪 Window TinyLFU 深度逻辑专项验证与缓存命中率基准测试 🧪");
        System.out.println("================================================================================");

        test4BitCounterSaturation();
        testBitwiseHalvingDecay();
        testAdmissionGateScanDefense();
        testHotCandidateAdmission();
        testExpiredKeyImmediateEviction();
        testHitRateComparisonZipfianWithScans();
        testConcurrentMultiThreadStress();

        System.out.println("\n================================================================================");
        System.out.println("  🎉 ALL 7 WINDOW TINY-LFU DEEP VERIFICATION TESTS PASSED WITH 100% SUCCESS! 🎉");
        System.out.println("================================================================================\n");
    }

    /**
     * 测试 1: 4-bit 饱和计数器上限隔离
     */
    private static void test4BitCounterSaturation() {
        System.out.print("[Test 1/7] 4-bit 饱和计数器上限验证 (0..15 绝不溢出) ... ");
        try (OffHeapFrequencySketch sketch = new OffHeapFrequencySketch(1024, null)) {
            long testHash = 0x123456789ABCDEFL;

            // 初始估值应为 0
            if (sketch.estimate(testHash) != 0) {
                throw new AssertionError("Initial estimate must be 0");
            }

            // 递增 20 次 (超过 4-bit 最大值 15)
            for (int i = 0; i < 20; i++) {
                sketch.record(testHash);
            }

            int count = sketch.estimate(testHash);
            if (count != 15) {
                throw new AssertionError("Counter did not saturate at 15! Value: " + count);
            }

            // 验证未访问的 hash 估值不受干扰（误判率控制在极低水平）
            long otherHash = 0xFEDCBA987654321L;
            if (sketch.estimate(otherHash) > 1) {
                throw new AssertionError("Unrelated hash has unexpectedly high frequency");
            }
        }
        System.out.println("PASSED!");
    }

    /**
     * 测试 2: 硬件单周期并行半衰衰减 (Aging / Decay) 精度
     */
    private static void testBitwiseHalvingDecay() {
        System.out.print("[Test 2/7] 硬件级位运算半衰衰减 (Aging: 15->7->3->1->0) ... ");
        try (OffHeapFrequencySketch sketch = new OffHeapFrequencySketch(1024, null)) {
            long testHash = 0xCAFEBABEDEADBEEFL;

            // 记录 15 次至饱和
            for (int i = 0; i < 15; i++) {
                sketch.record(testHash);
            }
            if (sketch.estimate(testHash) != 15) {
                throw new AssertionError("Expected 15 before decay");
            }

            // 1. 验证单周期 SIMD 并行除以 2 精度
            sketch.reset();
            int count1 = sketch.estimate(testHash);
            if (count1 != 7) {
                throw new AssertionError("After 1st decay (15>>>1), expected 7 but got: " + count1);
            }

            sketch.reset();
            int count2 = sketch.estimate(testHash);
            if (count2 != 3) {
                throw new AssertionError("After 2nd decay (7>>>1), expected 3 but got: " + count2);
            }

            sketch.reset();
            int count3 = sketch.estimate(testHash);
            if (count3 != 1) {
                throw new AssertionError("After 3rd decay (3>>>1), expected 1 but got: " + count3);
            }

            sketch.reset();
            int count4 = sketch.estimate(testHash);
            if (count4 != 0) {
                throw new AssertionError("After 4th decay (1>>>1), expected 0 but got: " + count4);
            }

            // 2. 验证达到 sampleSize 时自动触发 tryReset
            for (int i = 0; i < 10; i++) {
                sketch.record(testHash);
            }
            if (sketch.estimate(testHash) != 10) {
                throw new AssertionError("Expected 10 before auto reset");
            }

            int sampleSize = sketch.getSampleSize();
            for (int i = 0; i < sampleSize; i++) {
                sketch.record(0x100000000L + i);
            }
            // 自动衰减后，原键的频次应当已减半
            int countAfterAuto = sketch.estimate(testHash);
            if (countAfterAuto > 7) {
                throw new AssertionError("Auto reset did not decay count! Value: " + countAfterAuto);
            }
        }
        System.out.println("PASSED!");
    }

    /**
     * 测试 3: 准入门禁防御性拦截 (Cold 一击即走数据 0% 侵入率)
     */
    private static void testAdmissionGateScanDefense() throws Exception {
        System.out.print("[Test 3/7] 准入门禁拦截全表冷扫描 (500/500 热键 100% 留存) ... ");
        int[] slotCounts = new int[11];
        Arrays.fill(slotCounts, 500);

        try (PersistentOffHeapStore store = PersistentOffHeapStore.builder()
                .mode(StorageMode.IN_MEMORY)
                .slotCounts(slotCounts)
                .maxBuckets(1024)
                .evictionPolicy(EvictionPolicy.WINDOW_TINY_LFU)
                .build()) {

            byte[] val = new byte[32];

            // 1. 填满 500 个槽位并多次读取建立热度 (频次 >= 4)
            for (int i = 0; i < 500; i++) {
                store.put(("hot:item:" + i).getBytes(StandardCharsets.UTF_8), val);
            }
            for (int r = 0; r < 4; r++) {
                for (int i = 0; i < 500; i++) {
                    store.get(("hot:item:" + i).getBytes(StandardCharsets.UTF_8));
                }
            }

            // 2. 模拟 30,000 次突发冷数据写入冲刷
            for (int i = 0; i < 30_000; i++) {
                store.put(("scan:cold:" + i).getBytes(StandardCharsets.UTF_8), val);
            }

            // 3. 统计热数据存活率
            int hotSurvived = 0;
            for (int i = 0; i < 500; i++) {
                if (store.containsKey(("hot:item:" + i).getBytes(StandardCharsets.UTF_8))) {
                    hotSurvived++;
                }
            }

            if (hotSurvived != 500) {
                throw new AssertionError("Expected 500 hot items survived, but got: " + hotSurvived);
            }
        }
        System.out.println("PASSED! (500/500 survived)");
    }

    /**
     * 测试 4: 准入门禁晋升机制 (真热点 Candidate 成功淘汰冷门 Victim)
     */
    private static void testHotCandidateAdmission() throws Exception {
        System.out.print("[Test 4/7] 准入门禁晋升机制 (高频 Candidate 成功置换冷门槽位) ... ");
        int[] slotCounts = new int[11];
        Arrays.fill(slotCounts, 100);

        try (PersistentOffHeapStore store = PersistentOffHeapStore.builder()
                .mode(StorageMode.IN_MEMORY)
                .slotCounts(slotCounts)
                .maxBuckets(256)
                .evictionPolicy(EvictionPolicy.WINDOW_TINY_LFU)
                .build()) {

            byte[] val = new byte[32];

            // 填入 100 个仅写入 1 次的冷门项 (频次 = 1)
            for (int i = 0; i < 100; i++) {
                store.put(("cold:victim:" + i).getBytes(StandardCharsets.UTF_8), val);
            }

            // 预先给候选键建立超高频次 (在 sketch 中记录 10 次)
            byte[] superstarKey = "superstar:candidate".getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i < 10; i++) {
                store.getFrequencySketch().record(PersistentOffHeapStore.hash64(superstarKey));
            }

            // 此时放入 superstarKey，门禁裁决 Candidate(10) > Victim(1)，必须顺利晋升准入！
            boolean admitted = store.put(superstarKey, val);
            if (!admitted) {
                throw new AssertionError("Superstar key was rejected by admission gate!");
            }
            if (!store.containsKey(superstarKey)) {
                throw new AssertionError("Superstar key is not present in store!");
            }
        }
        System.out.println("PASSED!");
    }

    /**
     * 测试 5: 过期键优先级最高准则 (过期项立即淘汰无需门禁阻拦)
     */
    private static void testExpiredKeyImmediateEviction() throws Exception {
        System.out.print("[Test 5/7] 过期槽位最高优先级淘汰验证 (无视历史频次立即回收) ... ");
        int[] slotCounts = new int[11];
        Arrays.fill(slotCounts, 50);

        try (PersistentOffHeapStore store = PersistentOffHeapStore.builder()
                .mode(StorageMode.IN_MEMORY)
                .slotCounts(slotCounts)
                .maxBuckets(128)
                .evictionPolicy(EvictionPolicy.WINDOW_TINY_LFU)
                .build()) {

            byte[] val = new byte[32];

            // 填满 50 个槽位，设置 20ms TTL 并频繁读取使其具有高频次
            for (int i = 0; i < 50; i++) {
                store.put(("expiring:key:" + i).getBytes(StandardCharsets.UTF_8), val, 20L);
            }
            for (int r = 0; r < 5; r++) {
                for (int i = 0; i < 50; i++) {
                    store.get(("expiring:key:" + i).getBytes(StandardCharsets.UTF_8));
                }
            }

            // 等待 TTL 过期
            Thread.sleep(40);

            // 即使新进的是一个频次为 0 的新冷数据，也应当顺畅淘汰已过期槽位并成功写入
            for (int i = 0; i < 50; i++) {
                boolean ok = store.put(("new:incoming:" + i).getBytes(StandardCharsets.UTF_8), val);
                if (!ok) {
                    throw new AssertionError("Failed to insert into expired slot: " + i);
                }
            }

            // 验证 50 个新键全数成功写入
            for (int i = 0; i < 50; i++) {
                if (!store.containsKey(("new:incoming:" + i).getBytes(StandardCharsets.UTF_8))) {
                    throw new AssertionError("Missing newly admitted key: " + i);
                }
            }
        }
        System.out.println("PASSED!");
    }

    /**
     * 测试 6: 真实 Zipfian 倾斜工作负载 + 全表扫描冲击下的命中率对比测试
     */
    private static void testHitRateComparisonZipfianWithScans() throws Exception {
        System.out.print("[Test 6/7] 真实 Zipfian + 扫描场景下的命中率对决 (TinyLFU vs LRU) ... \n");

        int capacity = 500;
        int totalDistinctKeys = 5_000;
        int operations = 100_000;
        byte[] payload = new byte[32];

        // 1. 运行 LRU 策略
        double lruHitRate = runWorkload(EvictionPolicy.K_SAMPLE_LRU, capacity, totalDistinctKeys, operations, payload);

        // 2. 运行 Window TinyLFU 策略
        double tinyLfuHitRate = runWorkload(EvictionPolicy.WINDOW_TINY_LFU, capacity, totalDistinctKeys, operations, payload);

        System.out.printf("       * Standard LRU  缓存命中率 : %6.2f%%\n", lruHitRate);
        System.out.printf("       * Window TinyLFU 缓存命中率 : %6.2f%%\n", tinyLfuHitRate);

        if (tinyLfuHitRate <= lruHitRate) {
            throw new AssertionError(String.format("Window TinyLFU hit rate (%.2f%%) must outperform LRU (%.2f%%)",
                    tinyLfuHitRate, lruHitRate));
        }
        System.out.printf("       ==> Window TinyLFU 相比传统 LRU 提升命中率 +%.2f%%!\n", (tinyLfuHitRate - lruHitRate));
        System.out.println("      ... PASSED!");
    }

    private static double runWorkload(EvictionPolicy policy, int capacity, int distinctKeys, int ops, byte[] payload) throws Exception {
        int[] slotCounts = new int[11];
        Arrays.fill(slotCounts, capacity);

        try (PersistentOffHeapStore store = PersistentOffHeapStore.builder()
                .mode(StorageMode.IN_MEMORY)
                .slotCounts(slotCounts)
                .maxBuckets(1024)
                .evictionPolicy(policy)
                .build()) {

            // 生成不同键
            byte[][] keyPool = new byte[distinctKeys][];
            for (int i = 0; i < distinctKeys; i++) {
                keyPool[i] = ("key:" + i).getBytes(StandardCharsets.UTF_8);
            }

            Random rng = new Random(12345);
            long hits = 0;
            long reads = 0;

            for (int step = 0; step < ops; step++) {
                // 15% 概率为全表扫频批次 (Scan Bursts)，85% 为常规 Zipfian 业务查询
                int keyIdx;
                if (rng.nextInt(100) < 15) {
                    // 全表扫描冷数据 (位于 key 2000 ~ 4999 之间)
                    keyIdx = 2000 + rng.nextInt(distinctKeys - 2000);
                } else {
                    // Zipfian 倾斜热点 (80% 流量落在前 200 个热点键上)
                    if (rng.nextInt(100) < 80) {
                        keyIdx = rng.nextInt(200);
                    } else {
                        keyIdx = rng.nextInt(1000);
                    }
                }

                byte[] key = keyPool[keyIdx];
                byte[] val = store.get(key);
                reads++;
                if (val != null) {
                    hits++;
                } else {
                    // 缓存未命中，回源填充
                    store.put(key, payload);
                }
            }

            return (double) hits / reads * 100.0;
        }
    }

    /**
     * 测试 7: 多线程高并发风暴安全性测试
     */
    private static void testConcurrentMultiThreadStress() throws Exception {
        System.out.print("[Test 7/7] 多线程极限并发风暴验证 (8 线程 × 50,000 Ops 零死锁零损坏) ... ");

        int workers = 8;
        int opsPerWorker = 50_000;
        int capacity = 200;
        int[] slotCounts = new int[11];
        Arrays.fill(slotCounts, capacity);

        try (PersistentOffHeapStore store = PersistentOffHeapStore.builder()
                .mode(StorageMode.IN_MEMORY)
                .slotCounts(slotCounts)
                .maxBuckets(512)
                .evictionPolicy(EvictionPolicy.WINDOW_TINY_LFU)
                .build()) {

            ExecutorService pool = Executors.newFixedThreadPool(workers);
            CountDownLatch latch = new CountDownLatch(workers);
            AtomicInteger errorCount = new AtomicInteger(0);

            byte[] payload = new byte[32];
            for (int t = 0; t < workers; t++) {
                final int workerId = t;
                pool.submit(() -> {
                    try {
                        Random rng = new Random( workerId * 7919L );
                        for (int i = 0; i < opsPerWorker; i++) {
                            int k = rng.nextInt(1000);
                            byte[] key = ("worker:key:" + k).getBytes(StandardCharsets.UTF_8);

                            int op = rng.nextInt(100);
                            if (op < 60) {
                                store.get(key);
                            } else if (op < 90) {
                                store.put(key, payload);
                            } else {
                                store.remove(key);
                            }
                        }
                    } catch (Throwable e) {
                        errorCount.incrementAndGet();
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            boolean finished = latch.await(30, TimeUnit.SECONDS);
            pool.shutdown();

            if (!finished) {
                throw new AssertionError("Concurrent stress test timed out, possible deadlock!");
            }
            if (errorCount.get() > 0) {
                throw new AssertionError("Errors detected during concurrent stress: " + errorCount.get());
            }

            PersistentOffHeapStore.Stats stats = store.getStats();
            if (stats.hits() + stats.misses() == 0) {
                throw new AssertionError("No reads tracked in stats!");
            }
        }
        System.out.println("PASSED!");
    }
}
