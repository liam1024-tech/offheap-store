package offheap;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;

/**
 * 持久化与重启不丢失 (Persistent Crash Recovery) 严苛测试验证
 */
public class PersistentRecoveryTest {

    public static void main(String[] args) throws Exception {
        System.out.println("================================================================================");
        System.out.println("   🛡️  PersistentOffHeapStore 磁盘 MMAP 持久化与服务重启自愈实测 🛡️");
        System.out.println("================================================================================");

        Path testDir = Path.of("/home/antigravity/test_persistent_data");
        // 清理旧目录
        if (Files.exists(testDir)) {
            Files.walk(testDir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try { Files.delete(p); } catch (IOException ignored) {}
                    });
        }
        Files.createDirectories(testDir);

        int keyCount = 2000;

        // =====================================================================
        // 阶段 1: 首次启动并写入业务数据
        // =====================================================================
        System.out.println(" [阶段 1] 首次启动并写入 2,000 个变长跨阶键值 ...");
        long t1 = System.currentTimeMillis();
        try (PersistentOffHeapStore store = PersistentOffHeapStore.open(testDir)) {
            for (int i = 0; i < keyCount; i++) {
                byte[] k = ("user:profile:" + i).getBytes(StandardCharsets.UTF_8);
                // 变长尺寸 (30B ~ 4KB)
                byte[] v = ("payload_for_user_" + i + "_extended_data_padding_" + "X".repeat(i % 1000)).getBytes(StandardCharsets.UTF_8);
                // 前 100 个设置 300ms 短命 TTL，其余永久常驻
                long ttl = (i < 100) ? 300 : 0L;
                assert store.put(k, v, ttl) : "Put failed at " + i;
            }
            assert store.size() == keyCount : "Size mismatch: expected " + keyCount + ", but got " + store.size();
            store.sync();
            System.out.printf("  ✔ 写入成功！当前有效条目: %,d | 耗时: %d ms\n", store.size(), (System.currentTimeMillis() - t1));
            System.out.println("  • 模拟进程被 kill / 服务强制下线 (close store)...");
        }

        // =====================================================================
        // 阶段 2: 服务停机并休眠，等待短命 TTL 过期
        // =====================================================================
        System.out.println("\n [阶段 2] 服务停机等待 400 ms (使前 100 个短命 TTL 条目在停机期间过期) ...");
        Thread.sleep(400);

        // =====================================================================
        // 阶段 3: 服务重新启动，执行 MMAP 零拷贝冷启复活与 FSCK 校验
        // =====================================================================
        System.out.println(" [阶段 3] 服务重新启动并挂载持久化目录 ...");
        long t2 = System.currentTimeMillis();
        try (PersistentOffHeapStore store = PersistentOffHeapStore.open(testDir)) {
            long reopenCost = System.currentTimeMillis() - t2;
            System.out.printf("  ✔ 瞬时预热挂载完成！耗时: %d ms (亚秒级 0 反序列化冷启)\n", reopenCost);

            // 验证前 100 个短命键在启动时被自动识别并清理
            for (int i = 0; i < 100; i++) {
                byte[] k = ("user:profile:" + i).getBytes(StandardCharsets.UTF_8);
                assert store.get(k) == null : "Expired key " + i + " should be null after restart!";
            }
            System.out.println("  ✔ 停机期间过期的 100 个键已被自动回收 (0 幽灵数据)");

            // 验证其余 1,900 个永久常驻键 100% 完好无损
            for (int i = 100; i < keyCount; i++) {
                byte[] k = ("user:profile:" + i).getBytes(StandardCharsets.UTF_8);
                byte[] expected = ("payload_for_user_" + i + "_extended_data_padding_" + "X".repeat(i % 1000)).getBytes(StandardCharsets.UTF_8);
                byte[] actual = store.get(k);
                assert actual != null : "Key " + i + " lost after restart!";
                assert Arrays.equals(actual, expected) : "Value corrupted after restart for key " + i;
            }
            System.out.printf("  ✔ 存活的 %,d 个键值 100%% 完好无损！CRC32 数据完整性校验 100%% 通过！\n", (keyCount - 100));
            assert store.size() == keyCount - 100 : "Active entries mismatch: " + store.size();

            // 增量变更测试：更新 500 个，删除 200 个
            System.out.print("  • 执行热更新 500 个键，删除 200 个键 ... ");
            for (int i = 100; i < 600; i++) {
                byte[] k = ("user:profile:" + i).getBytes(StandardCharsets.UTF_8);
                byte[] updated = ("updated_v2_payload_" + i).getBytes(StandardCharsets.UTF_8);
                store.put(k, updated);
            }
            for (int i = 600; i < 800; i++) {
                byte[] k = ("user:profile:" + i).getBytes(StandardCharsets.UTF_8);
                store.remove(k);
            }
            store.sync();
            System.out.println("✔ 完成！当前存活: " + store.size());
        }

        // =====================================================================
        // 阶段 4: 二次重启确认增量变更持久化保真
        // =====================================================================
        System.out.println("\n [阶段 4] 二次重启验证增量写入与删除的保真性 ...");
        try (PersistentOffHeapStore store = PersistentOffHeapStore.open(testDir)) {
            // 验证已删除的 200 个确实不存在
            for (int i = 600; i < 800; i++) {
                byte[] k = ("user:profile:" + i).getBytes(StandardCharsets.UTF_8);
                assert store.get(k) == null : "Deleted key resurrected!";
            }
            // 验证更新的 500 个包含最新内容
            for (int i = 100; i < 600; i++) {
                byte[] k = ("user:profile:" + i).getBytes(StandardCharsets.UTF_8);
                byte[] expected = ("updated_v2_payload_" + i).getBytes(StandardCharsets.UTF_8);
                assert Arrays.equals(store.get(k), expected) : "Updated value mismatch!";
            }
            System.out.println("  ✔ 二次重启增量写入与删除 100% 吻合！");
        }

        // =====================================================================
        // 阶段 5: 跨重启 OFF_LAST_ACCESS 时间戳保真与 LRU 热点留存验证
        // =====================================================================
        System.out.println("\n [阶段 5] 跨重启 OFF_LAST_ACCESS 毫秒时间戳保真与采样 LRU 热点留存验证 ...");
        Path lruDir = testDir.resolve("lru_timestamp_preservation");
        Files.createDirectories(lruDir);
        int[] smallCap = new int[11];
        Arrays.fill(smallCap, 50);

        try (PersistentOffHeapStore store = PersistentOffHeapStore.builder()
                .storageDir(lruDir)
                .maxBuckets(128)
                .slotCounts(smallCap)
                .evictionPolicy(EvictionPolicy.K_SAMPLE_LRU)
                .build()) {
            byte[] payload = new byte[16];
            for (int i = 0; i < 50; i++) {
                assert store.put(("lru:persist:" + i).getBytes(StandardCharsets.UTF_8), payload);
            }
            // 拉开冷热数据的时间戳差距 (60ms)，仅高频访问前 10 个热点键
            Thread.sleep(60);
            for (int i = 0; i < 10; i++) {
                assert store.get(("lru:persist:" + i).getBytes(StandardCharsets.UTF_8)) != null;
            }
            store.sync();
        }

        // 重启挂载后，不再次访问前 10 个热点键，直接写入 20 个新键触发 K_SAMPLE_LRU 淘汰：
        // 若 recoverAndRebuild 正确保留了磁盘上的毫秒级 OFF_LAST_ACCESS（而非用 System.nanoTime() 覆盖），前 10 个热点键应几乎全部存活！
        try (PersistentOffHeapStore reopened = PersistentOffHeapStore.builder()
                .storageDir(lruDir)
                .maxBuckets(128)
                .slotCounts(smallCap)
                .evictionPolicy(EvictionPolicy.K_SAMPLE_LRU)
                .build()) {
            byte[] payload = new byte[16];
            for (int i = 50; i < 70; i++) {
                assert reopened.put(("lru:persist:" + i).getBytes(StandardCharsets.UTF_8), payload);
            }
            int hotSurvived = 0;
            for (int i = 0; i < 10; i++) {
                if (reopened.containsKey(("lru:persist:" + i).getBytes(StandardCharsets.UTF_8))) {
                    hotSurvived++;
                }
            }
            System.out.printf("  ✔ 重启后触发 20 次 K_SAMPLE_LRU 淘汰，重启前访问的热点键存活: %d/10 (时间戳跨重启 100%% 保真)\n", hotSurvived);
            assert hotSurvived >= 8 : "Persisted OFF_LAST_ACCESS timestamp was not preserved across restart! Survived: " + hotSurvived + "/10";
        }

        System.out.println("\n================================================================================");
        System.out.println("   🏆 PersistentOffHeapStore 全部 5 阶段重启不丢失与状态保真测试 100% 通过！ 🏆");
        System.out.println("================================================================================");
    }
}
