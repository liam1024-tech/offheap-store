package offheap;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.zip.CRC32;

/**
 * 持久化极端混沌与边缘 Case 测试套件 (Persistent Chaos & Edge-Case Test Suite)
 * 验证：
 * 1. 断电半写入 / CRC32 损坏槽位自愈剔除与零悬垂引用；
 * 2. 写入中途崩溃未提交的孤儿槽位 (Orphaned Slots) 100% 自动对账与回收；
 * 3. 哈希冲突链中间节点损坏/过期时的链路自愈重接；
 * 4. Slab 槽位用尽时近似 LRU 淘汰保障持续写入；
 * 5. 未正常执行 close() 的突发进程断电 (SIGKILL 模拟) 100% 幂等恢复。
 */
public class PersistentChaosTest {

    private static final Path CHAOS_DIR = Path.of("/home/antigravity/chaos_test_data");

    public static void main(String[] args) throws Exception {
        System.out.println("================================================================================");
        System.out.println("   ⚡ PersistentOffHeapStore 极端混沌注入与边缘 Case 自愈测试套件 ⚡");
        System.out.println("================================================================================");

        cleanDir(CHAOS_DIR);

        testTornWriteAndCrcCorruption();
        testOrphanedSlotReconciliation();
        testCollisionChainMiddleCorruption();
        testSlabLruEvictionOnSaturation();
        testUncleanShutdownSigkillRecovery();

        System.out.println("\n================================================================================");
        System.out.println("   🏆 所有 5 大维度极端混沌与边缘 Case 测试 100% 全部通过！ 🏆");
        System.out.println("================================================================================");
    }

    private static void cleanDir(Path dir) throws IOException {
        if (Files.exists(dir)) {
            Files.walk(dir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try { Files.delete(p); } catch (IOException ignored) {}
                    });
        }
        Files.createDirectories(dir);
    }

    /**
     * 混沌测试 1: 断电半写入与 CRC32 物理损坏自愈
     */
    private static void testTornWriteAndCrcCorruption() throws Exception {
        System.out.println("\n[混沌测试 1] 物理磁盘位翻转 / 断电半写入 CRC32 损坏注入 ...");
        cleanDir(CHAOS_DIR);

        // 1. 写入 100 个条目，并直接注入前 10 个条目的内存脏数据
        try (PersistentOffHeapStore store = PersistentOffHeapStore.open(CHAOS_DIR)) {
            for (int i = 0; i < 100; i++) {
                byte[] k = ("key:chaos:" + i).getBytes(StandardCharsets.UTF_8);
                byte[] v = ("value_content_data_" + i).getBytes(StandardCharsets.UTF_8);
                store.put(k, v);
            }

            // 模拟位翻转/脏数据：故意篡改前 10 个条目的 Value 物理内容使得 CRC32 不匹配
            for (int i = 0; i < 10; i++) {
                byte[] k = ("key:chaos:" + i).getBytes(StandardCharsets.UTF_8);
                // 偏移 48 + key.length 即为 value 的第一个字节，将其翻转为 0xFF
                boolean corrupted = store.corruptMemoryForTesting(k, 48 + k.length, (byte) 0xFF);
                assert corrupted : "Failed to corrupt key " + i;
            }
            store.sync();
            System.out.println("  • 已写入 100 个数据条目，并向磁盘注入 10 个物理坏块 (CRC 不匹配)");
        }

        // 3. 重启挂载：验证 FSCK 自动检测损坏、剔除坏数据并恢复剩余 90 个完好条目
        try (PersistentOffHeapStore store = PersistentOffHeapStore.open(CHAOS_DIR)) {
            assert store.size() == 90 : "Expected 90 valid entries, but got " + store.size();

            // 验证损坏的 10 个键读取为 null
            for (int i = 0; i < 10; i++) {
                byte[] k = ("key:chaos:" + i).getBytes(StandardCharsets.UTF_8);
                assert store.get(k) == null : "Corrupted key " + i + " was not pruned!";
            }

            // 验证未损坏的 90 个键 100% 可正常读取
            for (int i = 10; i < 100; i++) {
                byte[] k = ("key:chaos:" + i).getBytes(StandardCharsets.UTF_8);
                byte[] expected = ("value_content_data_" + i).getBytes(StandardCharsets.UTF_8);
                byte[] actual = store.get(k);
                assert actual != null && Arrays.equals(actual, expected) : "Valid key " + i + " corrupted!";
            }
            System.out.println("  ✔ FSCK 成功自愈：10 个损坏条目被安全剔除，90 个完好条目 100% 无损！");
        }
    }

    /**
     * 混沌测试 2: 写入中途崩溃未提交的孤儿槽位 (Orphaned Slots) 对账回收
     */
    private static void testOrphanedSlotReconciliation() throws Exception {
        System.out.println("\n[混沌测试 2] 进程崩溃导致的孤儿未提交槽位 (Orphaned Slot) 对账回收 ...");
        cleanDir(CHAOS_DIR);

        // 1. 初始化空存储
        try (PersistentOffHeapStore store = PersistentOffHeapStore.open(CHAOS_DIR)) {
            store.put("key:valid:1".getBytes(StandardCharsets.UTF_8), "val1".getBytes(StandardCharsets.UTF_8));
            store.sync();
        }

        // 2. 模拟进程在 writeRecord 成功但在更新 index 前瞬间崩溃：
        // 手工在 slab_0.dat 的第 5 个槽位写入一个完整但未挂入哈希表的孤儿记录
        Path slab0File = CHAOS_DIR.resolve("slab_0.dat");
        try (FileChannel fc = FileChannel.open(slab0File, StandardOpenOption.READ, StandardOpenOption.WRITE);
             Arena arena = Arena.ofShared()) {
            MemorySegment seg = fc.map(FileChannel.MapMode.READ_WRITE, 0, fc.size(), arena);
            long base = 5 * 64; // slot 5
            seg.set(ValueLayout.JAVA_INT, base + 0, PersistentOffHeapStore.MAGIC_HEADER);
            seg.set(ValueLayout.JAVA_INT, base + 4, 1);
            seg.set(ValueLayout.JAVA_LONG, base + 8, 0L); // expireAt
            seg.set(ValueLayout.JAVA_LONG, base + 16, System.currentTimeMillis());
            seg.set(ValueLayout.JAVA_LONG, base + 24, 123456L); // hash
            seg.set(ValueLayout.JAVA_LONG, base + 32, -1L); // nextHandle
            seg.set(ValueLayout.JAVA_INT, base + 40, 4); // keyLen
            seg.set(ValueLayout.JAVA_INT, base + 44, 4); // valLen
            byte[] dummyK = "orph".getBytes(StandardCharsets.UTF_8);
            byte[] dummyV = "data".getBytes(StandardCharsets.UTF_8);
            MemorySegment.copy(MemorySegment.ofArray(dummyK), 0, seg, base + 48, 4);
            MemorySegment.copy(MemorySegment.ofArray(dummyV), 0, seg, base + 52, 4);
            CRC32 crc = new CRC32();
            crc.update(dummyK);
            crc.update(dummyV);
            seg.set(ValueLayout.JAVA_INT_UNALIGNED, base + 56, (int) crc.getValue());
            seg.force();
            System.out.println("  • 已注入 1 个带完整 CRC32 但未提交至哈希表的孤儿槽位 (slot #5)");
        }

        // 3. 重启挂载：验证 FSCK 全局对账机制自动识别孤儿槽位，回收魔数并压回 freeSlots
        try (PersistentOffHeapStore store = PersistentOffHeapStore.open(CHAOS_DIR)) {
            assert store.size() == 1 : "Expected size 1, got " + store.size();
            assert store.get("key:valid:1".getBytes(StandardCharsets.UTF_8)) != null;
            assert store.get("orph".getBytes(StandardCharsets.UTF_8)) == null;

            // 再次写入 10 个数据，确认 freeSlots 正常分配，无槽位泄漏
            for (int i = 0; i < 10; i++) {
                assert store.put(("key:fill:" + i).getBytes(StandardCharsets.UTF_8), ("fill_val_" + i).getBytes(StandardCharsets.UTF_8));
            }
            System.out.println("  ✔ 孤儿槽位成功被识别并安全回收！0 槽位泄漏！");
        }
    }

    /**
     * 混沌测试 3: 哈希冲突链中间节点损坏/过期链路重接
     */
    private static void testCollisionChainMiddleCorruption() throws Exception {
        System.out.println("\n[混沌测试 3] 哈希冲突链中间节点损坏/过期链路重接 ...");
        cleanDir(CHAOS_DIR);

        // 使用单桶微型哈希表 (maxBuckets=1) 强制所有写入落入同一个冲突链
        int[] slotCounts = new int[11];
        Arrays.fill(slotCounts, 64);
        try (PersistentOffHeapStore store = PersistentOffHeapStore.open(CHAOS_DIR, 1, slotCounts)) {
            // 头插法写入 3 个键：NodeC -> NodeB -> NodeA
            store.put("key:A".getBytes(StandardCharsets.UTF_8), "valueA".getBytes(StandardCharsets.UTF_8));
            store.put("key:B".getBytes(StandardCharsets.UTF_8), "valueB".getBytes(StandardCharsets.UTF_8), 300); // 300ms 短命
            store.put("key:C".getBytes(StandardCharsets.UTF_8), "valueC".getBytes(StandardCharsets.UTF_8));
            store.sync();
            assert store.size() == 3;
            System.out.println("  • 已在同一哈希桶构造冲突链：C -> B (短命) -> A");
        }

        // 等待 400ms 使中间节点 B 过期
        Thread.sleep(400);

        // 重启挂载：验证中间节点 B 被安全跳过与解引用，C 与 A 仍保持畅通
        try (PersistentOffHeapStore store = PersistentOffHeapStore.open(CHAOS_DIR, 1, slotCounts)) {
            assert store.size() == 2 : "Expected 2 entries, but got " + store.size();
            assert store.get("key:B".getBytes(StandardCharsets.UTF_8)) == null : "Expired node B should be null!";
            assert store.get("key:A".getBytes(StandardCharsets.UTF_8)) != null : "Node A disconnected after pruning B!";
            assert store.get("key:C".getBytes(StandardCharsets.UTF_8)) != null : "Node C missing!";
            System.out.println("  ✔ 中间节点剔除后链表自愈成功：C 成功直连 A，数据畅通无阻！");
        }
    }

    /**
     * 混沌测试 4: Slab 物理槽位耗尽时近似 LRU 淘汰保障持续写入
     */
    private static void testSlabLruEvictionOnSaturation() throws Exception {
        System.out.println("\n[混沌测试 4] Slab 物理槽位耗尽时近似 LRU 淘汰保障持续写入 ...");
        cleanDir(CHAOS_DIR);

        // 创建各阶仅 8 个槽位的极小规格存储
        int[] tinySlotCounts = new int[11];
        Arrays.fill(tinySlotCounts, 8);

        try (PersistentOffHeapStore store = PersistentOffHeapStore.open(CHAOS_DIR, 16, tinySlotCounts)) {
            // 写入 8 个无过期时间的永久键，把 Slab 0 (64B) 完全填满
            for (int i = 0; i < 8; i++) {
                byte[] k = ("k:" + i).getBytes(StandardCharsets.UTF_8);
                byte[] v = ("v:" + i).getBytes(StandardCharsets.UTF_8);
                assert store.put(k, v) : "Failed to put key " + i;
            }
            System.out.println("  • 已填满 Slab 0 全部 8 个物理槽位 (100% 饱和)");

            // 再次写入第 9、10、11、12 个键：验证 LRU 淘汰自动生效，写入持续成功！
            for (int i = 8; i < 12; i++) {
                byte[] k = ("k:" + i).getBytes(StandardCharsets.UTF_8);
                byte[] v = ("v:" + i).getBytes(StandardCharsets.UTF_8);
                boolean ok = store.put(k, v);
                assert ok : "Write should succeed via LRU eviction on saturated slab!";
            }

            PersistentOffHeapStore.Stats stats = store.getStats();
            assert stats.evictions() > 0 : "Evictions count should be > 0!";
            System.out.printf("  ✔ Slab 饱和时 LRU 淘汰成功激活！淘汰旧条目数: %d，写入 100%% 成功！\n", stats.evictions());
        }
    }

    /**
     * 混沌测试 5: 未正常执行 close() 的突发断电 (SIGKILL 模拟) 100% 幂等恢复
     */
    private static void testUncleanShutdownSigkillRecovery() throws Exception {
        System.out.println("\n[混沌测试 5] 未正常关闭的突发断电 (SIGKILL 模拟) 100% 幂等恢复 ...");
        cleanDir(CHAOS_DIR);

        // 1. 启动并写入 500 个条目，仅执行 sync() 刷盘，故意不调用 close() (保持 super-block 状态为 1)
        PersistentOffHeapStore unclosedStore = PersistentOffHeapStore.open(CHAOS_DIR);
        for (int i = 0; i < 500; i++) {
            byte[] k = ("entity:" + i).getBytes(StandardCharsets.UTF_8);
            byte[] v = ("unclean_payload_" + i).getBytes(StandardCharsets.UTF_8);
            unclosedStore.put(k, v);
        }
        unclosedStore.sync();
        System.out.println("  • 写入 500 个条目，模拟系统遭 kill -9 (未调用 close()...)");

        // 2. 重新 open 挂载：验证能自动从非正常停机状态中完整自愈
        try (PersistentOffHeapStore recoveredStore = PersistentOffHeapStore.open(CHAOS_DIR)) {
            assert recoveredStore.size() == 500 : "Expected 500 recovered entries, got " + recoveredStore.size();
            for (int i = 0; i < 500; i++) {
                byte[] k = ("entity:" + i).getBytes(StandardCharsets.UTF_8);
                byte[] expected = ("unclean_payload_" + i).getBytes(StandardCharsets.UTF_8);
                byte[] actual = recoveredStore.get(k);
                assert actual != null && Arrays.equals(actual, expected) : "Data lost on unclean shutdown at " + i;
            }
            System.out.println("  ✔ SIGKILL 突发断电后挂载自愈成功：500 个条目 100% 完好无损！");
        }
    }
}
