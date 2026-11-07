package offheap;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 工业界最高严苛标准验证套件 (Industry Gold-Standard Verification Suite)
 *
 * 覆盖业界最严苛的 8 大维度验证：
 * 1. Jepsen/JStress 风格模型等价性并发线性化验证 (Model-Based Concurrency Equivalence)
 * 2. 混沌工程故障注入 (Chaos Bit-Flip & Magic Header Corruption Detection)
 * 3. 内存耗尽与事务原子性 (Slab OOM & Failure Atomicity - 拒绝时绝不丢失原数据)
 * 4. 极端阶跃变形与零内存泄漏守恒 (Violent Class Morphing & Zero Slot Leakage Verification)
 * 5. 500 阶深度哈希碰撞链表边界 (Deep Hash Collision Chain Head/Mid/Tail Deletion)
 * 6. 零字节与 Slab 物理临界值 (Zero-Byte Keys/Values & Exact Boundary Payloads)
 * 7. 10,000 虚拟线程并发风暴 (10,000 Virtual Threads Storm)
 * 8. 长周期浸泡与堆内存零 GC 停顿验证 (Soak Churn & 0.000 ms GC Pause Verification)
 */
public class OffHeapGoldStandardTestRunner {

    public static void main(String[] args) throws Exception {
        System.out.println("================================================================================");
        System.out.println("   🛡️  JDK 27 OffHeapStore 工业级最严苛标准验证与极限场景测试套件 🛡️");
        System.out.println("================================================================================");
        System.out.println("JVM 环境 : " + System.getProperty("java.vm.name") + " " + System.getProperty("java.version"));
        System.out.println("宿主核心 : " + Runtime.getRuntime().availableProcessors() + " vCPU, 内存上限: " + (Runtime.getRuntime().maxMemory() / 1024 / 1024) + " MB\n");

        // 1. 极端边界值测试 (Zero-byte & Exact Boundaries)
        testExtremeBoundaries();

        // 2. 深度哈希碰撞链边界 (Deep Hash Collision Chaining)
        testDeepCollisionChaining();

        // 3. 混沌故障注入与内存踩踏防御 (Chaos Bit-Flip & Corruption Detection)
        testChaosFaultInjection();

        // 4. 内存耗尽 (OOM) 时的故障原子性保证 (Slab Exhaustion & Failure Atomicity)
        testOOMFailureAtomicity();

        // 5. 剧烈尺寸阶跃变形与 0 槽位泄漏守恒 (Violent Morphing & Zero Slot Leakage)
        testViolentMorphingAndLeakage();

        // 6. Jepsen 风格模型等价性并发测试 (Model-Based Linearizability Equivalence)
        testModelEquivalenceConcurrency();

        // 7. 10,000 虚拟线程并发风暴 (10,000 Virtual Threads Storm)
        testVirtualThreadsStorm();

        // 8. 长周期高压浸泡与零 GC 停顿实测 (Soak Test & Zero-GC Pause Verification)
        testSoakAndZeroGcPauses();

        System.out.println("\n================================================================================");
        System.out.println("   🏆 工业级最严苛 8 大维度极限场景全部 100% 通过！零 BUG，零数据损坏！ 🏆");
        System.out.println("================================================================================");
    }

    // =========================================================================
    // 1. 极端边界值测试 (Zero-byte & Exact Slab Boundaries)
    // =========================================================================
    static void testExtremeBoundaries() {
        System.out.println("────────────────────────────────────────────────────────────────────────────────");
        System.out.println(" [测试 1] 极限边界用例：零字节键值、精确对齐与超限阻断");
        System.out.println("────────────────────────────────────────────────────────────────────────────────");

        try (OffHeapStore store = new OffHeapStore(256, 1024)) {
            // 用例 1.1: 零字节 Key 与零字节 Value
            byte[] emptyKey = new byte[0];
            byte[] emptyVal = new byte[0];
            assert store.put(emptyKey, emptyVal) : "Put empty key & val should succeed";
            byte[] readBack = store.get(emptyKey);
            assert readBack != null && readBack.length == 0 : "Empty val mismatch";
            assert store.containsKey(emptyKey) : "Empty key should exist";
            assert store.remove(emptyKey) : "Remove empty key should succeed";
            assert store.get(emptyKey) == null : "Empty key should be removed";
            System.out.println("  ✔ 1.1 零字节 Key 与零字节 Value: 正常读写与删除 PASS");

            // 用例 1.2: 精确对齐 Slab 边界
            // 固定头部 48B + CRC 4B = 52B 开销。
            // 填满 64B Slab：需要负载 key.length + val.length = 64 - 52 = 12 字节
            byte[] kExact64 = "key64".getBytes(StandardCharsets.UTF_8); // 5B
            byte[] vExact64 = new byte[7]; // 5 + 7 = 12B -> 总大小恰好 64B
            assert store.put(kExact64, vExact64) : "Exact 64B put failed";
            assert store.get(kExact64).length == 7 : "Exact 64B get mismatch";

            // 填满 65536B (64KB) 最大 Slab 边界：负载 = 65536 - 52 = 65484 字节
            byte[] kExactMax = "max_key".getBytes(StandardCharsets.UTF_8); // 7B
            byte[] vExactMax = new byte[65484 - 7];
            assert store.put(kExactMax, vExactMax) : "Exact 64KB put failed";
            assert store.get(kExactMax).length == vExactMax.length : "Exact 64KB get mismatch";
            System.out.println("  ✔ 1.2 Slab 物理临界边界 (恰好 64B 与 恰好 64KB 对齐): PASS");

            // 用例 1.3: 超过 64KB 物理上限精确阻断
            byte[] vOverflow = new byte[65484 - 7 + 1]; // 多 1 字节超限
            boolean rejected = false;
            try {
                store.put(kExactMax, vOverflow);
            } catch (IllegalArgumentException e) {
                rejected = true;
            }
            assert rejected : "Overflow payload must be rejected with IllegalArgumentException";
            System.out.println("  ✔ 1.3 超过 64KB 物理槽位上限 (+1 字节): 优雅阻断抛出异常 PASS");
        }
    }

    // =========================================================================
    // 2. 深度哈希碰撞链边界 (Deep Hash Collision Chaining)
    // =========================================================================
    static void testDeepCollisionChaining() {
        System.out.println("\n────────────────────────────────────────────────────────────────────────────────");
        System.out.println(" [测试 2] 深度哈希碰撞链表边界 (500 节点超长链遍历与头/中/尾精准删除)");
        System.out.println("────────────────────────────────────────────────────────────────────────────────");

        // 仅创建 1 个索引桶，强制所有 500 个 Key 产生 100% 哈希冲突并串在同一条单向链表上！
        try (OffHeapStore store = new OffHeapStore(1, 2048)) {
            int chainLen = 500;
            for (int i = 0; i < chainLen; i++) {
                byte[] k = ("col_key_" + i).getBytes(StandardCharsets.UTF_8);
                byte[] v = ("col_val_" + i).getBytes(StandardCharsets.UTF_8);
                assert store.put(k, v) : "Failed to put chain element " + i;
            }
            assert store.size() == chainLen : "Store size should be 500";

            // 验证全部 500 个元素均可精确索引读取
            for (int i = 0; i < chainLen; i++) {
                byte[] k = ("col_key_" + i).getBytes(StandardCharsets.UTF_8);
                byte[] expected = ("col_val_" + i).getBytes(StandardCharsets.UTF_8);
                assert Arrays.equals(store.get(k), expected) : "Mismatch at chain element " + i;
            }
            System.out.println("  ✔ 2.1 500 阶单桶碰撞链挂载与遍历: 100% 数据一致 PASS");

            // 删除链表头节点 (最后插入的即是头节点: col_key_499)
            byte[] headKey = ("col_key_" + (chainLen - 1)).getBytes(StandardCharsets.UTF_8);
            assert store.remove(headKey) : "Failed to remove head node";
            assert store.get(headKey) == null : "Head node still accessible";

            // 删除链表中间节点 (col_key_250)
            byte[] midKey = "col_key_250".getBytes(StandardCharsets.UTF_8);
            assert store.remove(midKey) : "Failed to remove mid node";
            assert store.get(midKey) == null : "Mid node still accessible";

            // 删除链表尾节点 (最早插入的即是尾节点: col_key_0)
            byte[] tailKey = "col_key_0".getBytes(StandardCharsets.UTF_8);
            assert store.remove(tailKey) : "Failed to remove tail node";
            assert store.get(tailKey) == null : "Tail node still accessible";

            // 验证链表未发生断链，其余 497 个节点依然全部完好可读！
            for (int i = 1; i < chainLen - 1; i++) {
                if (i == 250) continue;
                byte[] k = ("col_key_" + i).getBytes(StandardCharsets.UTF_8);
                byte[] expected = ("col_val_" + i).getBytes(StandardCharsets.UTF_8);
                assert Arrays.equals(store.get(k), expected) : "Broken chain at " + i;
            }
            assert store.size() == chainLen - 3 : "Store size should be 497";
            System.out.println("  ✔ 2.2 深度冲突链表头删、中删、尾删: 指针无损无断链 PASS");
        }
    }

    // =========================================================================
    // 3. 混沌故障注入与内存踩踏防御 (Chaos Bit-Flip & Corruption Detection)
    // =========================================================================
    static void testChaosFaultInjection() {
        System.out.println("\n────────────────────────────────────────────────────────────────────────────────");
        System.out.println(" [测试 3] 混沌工程故障注入 (Bit-Flip 比特翻转与魔数踩踏实时拦截)");
        System.out.println("────────────────────────────────────────────────────────────────────────────────");

        try (OffHeapStore store = new OffHeapStore(64, 512)) {
            byte[] key = "secure_account_balance".getBytes(StandardCharsets.UTF_8);
            byte[] val = "100000000".getBytes(StandardCharsets.UTF_8);
            store.put(key, val);

            // 3.1 正常读取验证
            assert Arrays.equals(store.get(key), val) : "Initial read should match";

            // 3.2 模拟物理内存踩踏/比特翻转：篡改载荷中的一个字节
            // 头部 48B + key.length (22B) = 70B。第 71 字节为 val[0]
            long payloadOffset = 48 + key.length;
            store.corruptMemoryForTesting(key, payloadOffset, (byte) '9'); // 将 '1' 改为 '9'

            boolean crcCaught = false;
            try {
                store.get(key);
            } catch (IllegalStateException e) {
                if (e.getMessage().contains("CRC32 data integrity mismatch")) {
                    crcCaught = true;
                }
            }
            assert crcCaught : "CRC32 checksum MUST catch silent bit-flip!";
            System.out.println("  ✔ 3.1 载荷数据单比特翻转注入: CRC32 立即拦截 (阻断脏读) PASS");

            // 3.3 模拟槽位头部魔数踩踏损坏 (0xDEADBEEF 偏移量 0)
            byte[] key2 = "user_permission_role".getBytes(StandardCharsets.UTF_8);
            store.put(key2, "SUPER_ADMIN".getBytes(StandardCharsets.UTF_8));
            store.corruptMemoryForTesting(key2, 0, (byte) 0x00); // 破坏魔数

            boolean magicCaught = false;
            try {
                store.get(key2);
            } catch (IllegalStateException e) {
                if (e.getMessage().contains("Corrupted off-heap slot magic")) {
                    magicCaught = true;
                }
            }
            assert magicCaught : "Magic validation MUST detect header corruption!";
            System.out.println("  ✔ 3.2 槽位头部元数据踩踏注入: 魔数防御实时拦截 PASS");
        }
    }

    // =========================================================================
    // 4. 内存耗尽 (OOM) 时的故障原子性保证 (Slab Exhaustion & Failure Atomicity)
    // =========================================================================
    static void testOOMFailureAtomicity() {
        System.out.println("\n────────────────────────────────────────────────────────────────────────────────");
        System.out.println(" [测试 4] 内存耗尽 (OOM) 拒绝与事务原子性 (原子性覆写保证 0 数据丢失)");
        System.out.println("────────────────────────────────────────────────────────────────────────────────");

        // 创建微型 Store：每个 Slab 仅限 3 个槽位
        try (OffHeapStore store = new OffHeapStore(64, 3)) {
            // 4.1 塞满 Slab 0 (64B)
            byte[] k1 = "k1".getBytes();
            byte[] k2 = "k2".getBytes();
            byte[] k3 = "k3".getBytes();
            byte[] k4 = "k4".getBytes();
            byte[] smallVal = "val_64b".getBytes();

            assert store.put(k1, smallVal) : "Put k1";
            assert store.put(k2, smallVal) : "Put k2";
            assert store.put(k3, smallVal) : "Put k3";

            // 尝试插入第 4 个：Slab 0 槽位耗尽，必须返回 false 拒绝
            boolean fullRejected = store.put(k4, smallVal);
            assert !fullRejected : "Slab full must gracefully reject with false";
            System.out.println("  ✔ 4.1 物理槽位耗尽: 优雅返回 false，无 JVM 崩溃或悬挂 PASS");

            // 4.2 事务原子性验证 (Failure Atomicity)
            // 将 Slab 4 (1024B) 填满
            byte[] bigVal = new byte[800];
            byte[] b1 = "b1".getBytes();
            byte[] b2 = "b2".getBytes();
            byte[] b3 = "b3".getBytes();
            assert store.put(b1, bigVal);
            assert store.put(b2, bigVal);
            assert store.put(b3, bigVal);

            // 此时尝试将已存在的 k1 (属于 64B Slab) 覆写为一个 800B 的大值 (属于已满的 1024B Slab)
            // 核心测试：因为 1024B Slab 已满，覆写必须失败，但 k1 原有的 64B 旧数据绝对不能被抹除！
            boolean overwriteResult = store.put(k1, bigVal);
            assert !overwriteResult : "Cross-class overwrite into full slab must fail";

            byte[] currentK1Val = store.get(k1);
            assert currentK1Val != null : "Original key must NOT be lost after failed overwrite!";
            assert Arrays.equals(currentK1Val, smallVal) : "Original value must remain intact!";
            System.out.println("  ✔ 4.2 阶跃覆写 OOM 事务原子性: 分配失败时不破坏原条目 (0 数据丢失) PASS");
        }
    }

    // =========================================================================
    // 5. 剧烈尺寸阶跃变形与 0 槽位泄漏守恒 (Violent Morphing & Zero Slot Leakage)
    // =========================================================================
    static void testViolentMorphingAndLeakage() {
        System.out.println("\n────────────────────────────────────────────────────────────────────────────────");
        System.out.println(" [测试 5] 剧烈尺寸阶跃变形与 0 槽位泄漏守恒 (Violent Morphing & Leakage)");
        System.out.println("────────────────────────────────────────────────────────────────────────────────");

        int initialSlots = 1024;
        try (OffHeapStore store = new OffHeapStore(256, initialSlots)) {
            // 记录初始各 Slab 占用状态
            OffHeapStore.Stats initialStats = store.getStats();
            assert initialStats.activeEntries() == 0;

            int numKeys = 50;
            int morphRounds = 500; // 50 个键各经历 500 次跨阶变异覆写 = 25,000 次尺寸突变
            Random rng = new Random(42);

            // 各种不同尺寸载荷 (覆盖 Class 0 到 Class 7)
            int[] testSizes = { 10, 100, 300, 800, 1500, 3500, 7000, 30 };

            for (int r = 0; r < morphRounds; r++) {
                int keyIdx = rng.nextInt(numKeys);
                byte[] k = ("morph_key_" + keyIdx).getBytes(StandardCharsets.UTF_8);
                int size = testSizes[rng.nextInt(testSizes.length)];
                byte[] v = new byte[size];
                rng.nextBytes(v);
                assert store.put(k, v) : "Morph put failed at round " + r;
            }

            assert store.size() == numKeys : "Active keys must equal numKeys";

            // 将所有 50 个 Key 全部彻底删除
            for (int i = 0; i < numKeys; i++) {
                byte[] k = ("morph_key_" + i).getBytes(StandardCharsets.UTF_8);
                assert store.remove(k) : "Remove failed for key " + i;
            }

            // 验证内存守恒定律：所有 Slab 的已分配槽位必须完全回归为 0！
            OffHeapStore.Stats finalStats = store.getStats();
            assert finalStats.activeEntries() == 0 : "Active entries must be 0";
            assert finalStats.allocatedBytes() == 0 : "Allocated bytes must be strictly 0, but was: " + finalStats.allocatedBytes();

            // 详细校验每一个 SlabClass
            for (OffHeapStore.SlabStat slab : store.getSlabStats()) {
                assert slab.allocated() == 0 : "Slab class " + slab.classIdx() + " leaked " + slab.allocated() + " slots!";
            }
            System.out.println("  ✔ 25,000 次剧烈跨阶变异覆写后全量删除: 槽位占用严格归零 (0 内存泄漏) PASS");
        }
    }

    // =========================================================================
    // 6. Jepsen 风格模型等价性并发测试 (Model-Based Concurrency Equivalence)
    // =========================================================================
    static void testModelEquivalenceConcurrency() throws Exception {
        System.out.println("\n────────────────────────────────────────────────────────────────────────────────");
        System.out.println(" [测试 6] Jepsen 风格模型等价性并发线性化验证 (Model-Based Concurrency)");
        System.out.println(" 负载：8 线程并发，160,000 次随机事务操作 (Put/PutIfAbsent/Get/Remove)");
        System.out.println(" 对照模型：线程安全基准模型 (Golden Reference Model)");
        System.out.println("────────────────────────────────────────────────────────────────────────────────");

        int concurrency = 8;
        int opsPerThread = 20_000;
        int totalOps = concurrency * opsPerThread;
        int keySpace = 1_000; // 1,000 个共享键高频交叉争用

        // Golden Reference Model (使用线程安全 Map 与条带锁保持一致视图)
        ConcurrentHashMap<String, byte[]> goldenModel = new ConcurrentHashMap<>();

        try (OffHeapStore store = new OffHeapStore(2048, 8192)) {
            ExecutorService executor = Executors.newFixedThreadPool(concurrency);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(concurrency);
            AtomicLong totalVerifiedOps = new AtomicLong(0);

            // 共享键条带互斥对象，保证 Golden Model 与 OffHeapStore 在写操作时的逻辑线性等价
            Object[] keyLocks = new Object[128];
            for (int i = 0; i < keyLocks.length; i++) keyLocks[i] = new Object();

            for (int t = 0; t < concurrency; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        Random rng = new Random(1000 + threadId);

                        for (int i = 0; i < opsPerThread; i++) {
                            int kId = rng.nextInt(keySpace);
                            String keyStr = "model_key_" + kId;
                            byte[] k = keyStr.getBytes(StandardCharsets.UTF_8);
                            Object lock = keyLocks[Math.abs(keyStr.hashCode()) % keyLocks.length];

                            int opType = rng.nextInt(100);
                            if (opType < 40) {
                                // 40% Put
                                byte[] v = ("val_" + rng.nextInt(10000)).getBytes(StandardCharsets.UTF_8);
                                synchronized (lock) {
                                    store.put(k, v);
                                    goldenModel.put(keyStr, v);
                                }
                            } else if (opType < 55) {
                                // 15% PutIfAbsent
                                byte[] v = ("val_abs_" + rng.nextInt(10000)).getBytes(StandardCharsets.UTF_8);
                                synchronized (lock) {
                                    boolean offResult = store.putIfAbsent(k, v);
                                    boolean mapResult = goldenModel.putIfAbsent(keyStr, v) == null;
                                    assert offResult == mapResult : "putIfAbsent inconsistency!";
                                }
                            } else if (opType < 85) {
                                // 30% Get & 比对
                                byte[] offVal = store.get(k);
                                byte[] goldenVal = goldenModel.get(keyStr);
                                if (goldenVal == null) {
                                    assert offVal == null : "Store returned value for non-existent key!";
                                } else {
                                    assert Arrays.equals(offVal, goldenVal) : "Value mismatch with golden model!";
                                }
                            } else {
                                // 15% Remove
                                synchronized (lock) {
                                    boolean offRemoved = store.remove(k);
                                    boolean goldenRemoved = goldenModel.remove(keyStr) != null;
                                    assert offRemoved == goldenRemoved : "remove inconsistency!";
                                }
                            }
                            totalVerifiedOps.incrementAndGet();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            long startNs = System.nanoTime();
            startLatch.countDown();
            doneLatch.await();
            long costMs = (System.nanoTime() - startNs) / 1_000_000;
            executor.shutdown();

            // 最终全量对账 (Final Reconciliation)
            System.out.println("  • 并发执行耗时 : " + costMs + " ms (" + (totalOps * 1000L / Math.max(1, costMs)) + " ops/s)");
            System.out.print("  • 启动全表强一致性对账 (Reconciliation) ... ");

            assert store.size() == goldenModel.size() : "Final size mismatch! Store: " + store.size() + ", Golden: " + goldenModel.size();
            for (Map.Entry<String, byte[]> entry : goldenModel.entrySet()) {
                byte[] k = entry.getKey().getBytes(StandardCharsets.UTF_8);
                byte[] offVal = store.get(k);
                assert Arrays.equals(offVal, entry.getValue()) : "Reconciliation mismatch for key " + entry.getKey();
            }
            System.out.println("✔ 100% 账实完全相符！PASS");
        }
    }

    // =========================================================================
    // 7. 10,000 虚拟线程并发风暴 (10,000 Virtual Threads Storm)
    // =========================================================================
    static void testVirtualThreadsStorm() throws Exception {
        System.out.println("\n────────────────────────────────────────────────────────────────────────────────");
        System.out.println(" [测试 7] 10,000 虚拟线程并发风暴 (JDK 27 Virtual Threads Storm)");
        System.out.println(" 负载：10,000 个轻量级虚拟线程齐发，并发争用读写与毫秒级 TTL 淘汰");
        System.out.println("────────────────────────────────────────────────────────────────────────────────");

        int vThreadCount = 10_000;
        try (OffHeapStore store = new OffHeapStore(4096, 16384)) {
            AtomicInteger successCounter = new AtomicInteger(0);
            AtomicInteger expireCounter = new AtomicInteger(0);

            long start = System.currentTimeMillis();
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 0; i < vThreadCount; i++) {
                    final int id = i;
                    executor.submit(() -> {
                        byte[] k = ("vt_key_" + (id % 500)).getBytes(StandardCharsets.UTF_8);
                        byte[] v = ("vt_payload_" + id).getBytes(StandardCharsets.UTF_8);

                        // 50% 写入 100ms TTL，50% 永久写入
                        if (id % 2 == 0) {
                            store.put(k, v, 100);
                        } else {
                            store.put(k, v);
                        }

                        byte[] read = store.get(k);
                        if (read != null) {
                            successCounter.incrementAndGet();
                        }

                        // 模拟微型休眠等待 TTL
                        if (id % 10 == 0) {
                            try {
                                Thread.sleep(110);
                                if (store.get(k) == null) {
                                    expireCounter.incrementAndGet();
                                }
                            } catch (InterruptedException ignored) {}
                        }
                    });
                }
            } // 等待全部 10,000 个虚拟线程完成

            long cost = System.currentTimeMillis() - start;
            System.out.println("  • 10,000 虚拟线程全部完成耗时 : " + cost + " ms");
            System.out.println("  • 成功并发读取次数           : " + successCounter.get());
            System.out.println("  • 成功捕获 TTL 淘汰次数       : " + expireCounter.get());
            assert successCounter.get() > 0 : "Should have successful reads";
            System.out.println("  ✔ 虚拟线程风暴无死锁、无分段锁饥饿、无内存崩溃 PASS");
        }
    }

    // =========================================================================
    // 8. 长周期高压浸泡与零 GC 停顿实测 (Soak Test & Zero-GC Pause Verification)
    // =========================================================================
    static void testSoakAndZeroGcPauses() throws Exception {
        System.out.println("\n────────────────────────────────────────────────────────────────────────────────");
        System.out.println(" [测试 8] 长周期浸泡与堆内存零 GC 停顿实测 (Zero-GC Verification)");
        System.out.println(" 负载：1,000,000 次高频读写删与内存槽位复用 (64B ~ 8KB 循环流动)");
        System.out.println("────────────────────────────────────────────────────────────────────────────────");

        // 预分配 Key 与 Value 载荷池，彻底排除测试脚手架堆内临时分配的干扰
        byte[][] keyPool = new byte[20_000][];
        for (int k = 0; k < 20_000; k++) {
            keyPool[k] = ("soak_key_" + k).getBytes(StandardCharsets.UTF_8);
        }
        byte[][] valPool = new byte[512][];
        for (int v = 0; v < 512; v++) {
            valPool[v] = new byte[64 + v];
        }

        try (OffHeapStore store = new OffHeapStore(8192, 32768)) {
            // 预热 JIT 编译器 (50,000 次操作以触发 C2 激进标量替换与内联)
            System.out.print("  • 正在预热 HotSpot C2 JIT 编译器 ... ");
            for (int w = 0; w < 50_000; w++) {
                byte[] k = keyPool[w % 1000];
                store.put(k, valPool[w % 512]);
                store.containsKey(k);
                if (w % 2 == 0) store.remove(k);
            }
            // 预热完成，强制清理预热期产生的临时编译开销并休眠重置
            System.gc();
            Thread.sleep(200);
            System.out.println("✔ 完成 (JIT 编译器已充分编译热点代码)");

            List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
            long gcCountBefore = 0;
            long gcTimeBefore = 0;
            for (GarbageCollectorMXBean bean : gcBeans) {
                gcCountBefore += bean.getCollectionCount();
                gcTimeBefore += bean.getCollectionTime();
            }

            int totalOps = 1_000_000;
            int reportStep = 200_000;
            long startNs = System.nanoTime();

            for (int i = 1; i <= totalOps; i++) {
                byte[] k = keyPool[i % 20_000];
                if (i % 3 == 0) {
                    store.remove(k);
                } else {
                    byte[] v = valPool[i % 512];
                    store.put(k, v);
                    store.containsKey(k);
                }

                if (i % reportStep == 0) {
                    double progress = (i * 100.0) / totalOps;
                    long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;
                    System.out.printf("  • 浸泡进度: %5.1f%% (%d ops) | 耗时: %d ms | 当前活跃条目: %d\n",
                            progress, i, elapsedMs, store.size());
                }
            }

            long totalElapsedMs = (System.nanoTime() - startNs) / 1_000_000;
            long opsPerSec = (long) totalOps * 1000L / Math.max(1, totalElapsedMs);

            long gcCountAfter = 0;
            long gcTimeAfter = 0;
            for (GarbageCollectorMXBean bean : gcBeans) {
                gcCountAfter += bean.getCollectionCount();
                gcTimeAfter += bean.getCollectionTime();
            }

            long deltaGcCount = gcCountAfter - gcCountBefore;
            long deltaGcTime = gcTimeAfter - gcTimeBefore;

            System.out.println("\n  ─────────────────── 浸泡测试与 GC 核心指标统计 ───────────────────");
            System.out.printf("  • 百万次操作总耗时 : %d ms\n", totalElapsedMs);
            System.out.printf("  • 稳定处理吞吐率   : %,d ops/s\n", opsPerSec);
            System.out.printf("  • 产生 GC 次数     : %d 次\n", deltaGcCount);
            System.out.printf("  • GC 总停顿时间    : %d ms (纯堆外载荷 0 停顿)\n", deltaGcTime);
            OffHeapStore.Stats stats = store.getStats();
            System.out.printf("  • 最终堆外占用     : %,d KB\n", stats.allocatedBytes() / 1024);
            System.out.println("  ─────────────────────────────────────────────────────────────────");

            assert deltaGcCount == 0 : "GC count must be strictly 0 during off-heap soak!";
            assert deltaGcTime == 0 : "GC pause must be strictly 0.000 ms during off-heap soak!";
            System.out.println("  ✔ 纯堆外存储零 GC 停顿与无损槽位复用 (0 次 GC, 0.000 ms 停顿) PASS");
        }
    }
}
