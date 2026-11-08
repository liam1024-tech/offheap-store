package offheap;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Off-Heap Store Core Suite")
class OffHeapStoreJUnitTest {

    @Test
    @DisplayName("In-Memory Mode: Basic CRUD and TTL Expiration")
    void testInMemoryBasicOperations() {
        try (PersistentOffHeapStore store = PersistentOffHeapStore.inMemory()) {
            byte[] key = "test:key:1".getBytes();
            byte[] val = "hello_offheap".getBytes();

            // Put and Get
            store.put(key, val, 60_000L);
            assertArrayEquals(val, store.get(key));

            // Overwrite
            byte[] val2 = "hello_world_v2".getBytes();
            store.put(key, val2, 60_000L);
            assertArrayEquals(val2, store.get(key));

            // Remove
            assertTrue(store.remove(key));
            assertNull(store.get(key));
            assertFalse(store.remove(key));

            // Expired key
            store.put(key, val, 1L); // 1ms TTL
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {}
            assertNull(store.get(key), "Expired key should return null");
        }
    }

    @Test
    @DisplayName("Atomic Primitives: CAS and GetAndSet")
    void testAtomicPrimitives() {
        try (PersistentOffHeapStore store = PersistentOffHeapStore.inMemory()) {
            byte[] key = "atomic:counter".getBytes();
            byte[] initialVal = "100".getBytes();
            byte[] updatedVal = "200".getBytes();
            byte[] wrongVal = "999".getBytes();

            store.put(key, initialVal, 60_000L);

            // CAS failure with wrong expected value
            assertFalse(store.compareAndSwap(key, wrongVal, updatedVal, 60_000L));
            assertArrayEquals(initialVal, store.get(key));

            // CAS success
            assertTrue(store.compareAndSwap(key, initialVal, updatedVal, 60_000L));
            assertArrayEquals(updatedVal, store.get(key));

            // GetAndSet
            byte[] oldVal = store.getAndSet(key, "300".getBytes(), 60_000L);
            assertArrayEquals(updatedVal, oldVal);
            assertArrayEquals("300".getBytes(), store.get(key));
        }
    }

    @Test
    @DisplayName("Deadlock-Free BatchPut")
    void testBatchPut() {
        try (PersistentOffHeapStore store = PersistentOffHeapStore.inMemory()) {
            Map<byte[], byte[]> batch = Map.of(
                "batch:k1".getBytes(), "v1".getBytes(),
                "batch:k2".getBytes(), "v2".getBytes(),
                "batch:k3".getBytes(), "v3".getBytes()
            );

            store.batchPut(batch, 60_000L);

            for (Map.Entry<byte[], byte[]> entry : batch.entrySet()) {
                assertArrayEquals(entry.getValue(), store.get(entry.getKey()));
            }
        }
    }

    @Test
    @DisplayName("Atomic Consistent BatchGet (List & Map modes)")
    void testBatchGet() {
        try (PersistentOffHeapStore store = PersistentOffHeapStore.inMemory()) {
            byte[] k1 = "batch:get:1".getBytes();
            byte[] v1 = "val1".getBytes();
            byte[] k2 = "batch:get:2".getBytes();
            byte[] v2 = "val2".getBytes();
            byte[] k3 = "batch:get:3".getBytes();
            byte[] v3 = "val3".getBytes();
            byte[] kMissing = "batch:missing".getBytes();

            store.put(k1, v1);
            store.put(k2, v2);
            store.put(k3, v3);

            // 1. Test List mode (Redis MGET order-preserving)
            List<byte[]> keys = List.of(k1, kMissing, k3, k2);
            List<byte[]> vals = store.batchGet(keys);
            assertEquals(4, vals.size());
            assertArrayEquals(v1, vals.get(0));
            assertNull(vals.get(1));
            assertArrayEquals(v3, vals.get(2));
            assertArrayEquals(v2, vals.get(3));

            // 2. Test Map mode (content-safe Map via getAll & batchGetMap)
            Map<byte[], byte[]> map = store.getAll(List.of(k1, kMissing, k2));
            assertEquals(2, map.size());
            assertArrayEquals(v1, map.get(k1));
            assertArrayEquals(v2, map.get(k2));
            assertNull(map.get(kMissing));

            Map<byte[], byte[]> map2 = store.batchGetMap(List.of(k1, k2));
            assertEquals(2, map2.size());

            // 3. Test empty collection
            assertTrue(store.batchGet(List.of()).isEmpty());
            assertTrue(store.getAll(List.of()).isEmpty());
        }
    }

    @Test
    @DisplayName("Persistent Mode: MMAP Write, Flush and Re-Open Recovery")
    void testPersistentRecovery(@TempDir Path tempDir) throws Exception {
        byte[] key = "persistent:state".getBytes();
        byte[] val = "saved_state_data_payload_123456789".getBytes();

        // 1. Write in persistent store and close
        try (PersistentOffHeapStore store = PersistentOffHeapStore.builder()
                .storageDir(tempDir)
                .syncPolicy(SyncPolicy.MANUAL)
                .build()) {
            store.put(key, val, 300_000L);
            store.flush();
        }

        // 2. Reopen from same directory and verify recovery
        try (PersistentOffHeapStore store = PersistentOffHeapStore.builder()
                .storageDir(tempDir)
                .syncPolicy(SyncPolicy.MANUAL)
                .build()) {
            assertArrayEquals(val, store.get(key), "Value must be fully recovered across reopen");
        }
    }

    @Test
    @DisplayName("Window TinyLFU: Eviction Policy and Frequency Sketch Admission")
    void testWindowTinyLfuEviction() throws Exception {
        int[] cap = new int[11];
        java.util.Arrays.fill(cap, 50);

        try (PersistentOffHeapStore store = PersistentOffHeapStore.builder()
                .mode(StorageMode.IN_MEMORY)
                .maxBuckets(128)
                .slotCounts(cap)
                .evictionPolicy(EvictionPolicy.WINDOW_TINY_LFU)
                .build()) {

            assertEquals(EvictionPolicy.WINDOW_TINY_LFU, store.getEvictionPolicy());
            assertNotNull(store.getFrequencySketch());

            byte[] payload = new byte[16];

            // 1. Fill 50 slots and access hot keys
            for (int i = 0; i < 50; i++) {
                assertTrue(store.put(("hot:" + i).getBytes(), payload));
            }
            for (int r = 0; r < 5; r++) {
                for (int i = 0; i < 50; i++) {
                    assertNotNull(store.get(("hot:" + i).getBytes()));
                }
            }

            // 2. Perform 2,000 cold scans
            for (int i = 0; i < 2_000; i++) {
                store.put(("cold:scan:" + i).getBytes(), payload);
            }

            // 3. Verify hot keys survived against cold scans
            int survived = 0;
            for (int i = 0; i < 50; i++) {
                if (store.containsKey(("hot:" + i).getBytes())) {
                    survived++;
                }
            }
            assertTrue(survived >= 45, "Window TinyLFU must protect hot keys against cold scans! Survived: " + survived + "/50");
        }
    }

    @Test
    @DisplayName("FrequencySketch: 4-Bit Saturated Counter & Single-Cycle Parallel Halving Decay")
    void testFrequencySketchBitwiseSaturationAndDecay() {
        try (OffHeapFrequencySketch sketch = new OffHeapFrequencySketch(1024, null)) {
            long testHash = 0x123456789ABCDEFL;

            assertEquals(0, sketch.estimate(testHash));

            // Saturated at 15
            for (int i = 0; i < 25; i++) {
                sketch.record(testHash);
            }
            assertEquals(15, sketch.estimate(testHash), "4-bit counter must saturate at 15 without overflow");

            // Verify SIMD halving sequence: 15 -> 7 -> 3 -> 1 -> 0
            sketch.reset();
            assertEquals(7, sketch.estimate(testHash));
            sketch.reset();
            assertEquals(3, sketch.estimate(testHash));
            sketch.reset();
            assertEquals(1, sketch.estimate(testHash));
            sketch.reset();
            assertEquals(0, sketch.estimate(testHash));
        }
    }

    @Test
    @DisplayName("Window TinyLFU: Hit Rate Superiority over Standard LRU under Mixed Scan Bursts")
    void testWindowTinyLfuHitRateOutperformsLru() throws Exception {
        int capacity = 200;
        int distinctKeys = 2000;
        int ops = 20000;
        byte[] payload = new byte[32];

        double lruHitRate = runBenchmarkWorkload(EvictionPolicy.K_SAMPLE_LRU, capacity, distinctKeys, ops, payload);
        double tinyLfuHitRate = runBenchmarkWorkload(EvictionPolicy.WINDOW_TINY_LFU, capacity, distinctKeys, ops, payload);

        assertTrue(tinyLfuHitRate > lruHitRate,
                String.format("Window TinyLFU hit rate (%.2f%%) must outperform LRU (%.2f%%)", tinyLfuHitRate, lruHitRate));
    }

    @Test
    @DisplayName("OffHeapStore: TinyLFU Eviction Policy & Admission Gate in Pure In-Memory Wrapper")
    void testOffHeapStoreWindowTinyLfuEviction() {
        try (OffHeapStore store = new OffHeapStore(128, 50)) {
            store.setEvictionPolicy(EvictionPolicy.WINDOW_TINY_LFU);
            assertEquals(EvictionPolicy.WINDOW_TINY_LFU, store.getEvictionPolicy());
            assertNotNull(store.getFrequencySketch());

            byte[] payload = new byte[16];

            // 1. Fill 50 slots and access hot keys
            for (int i = 0; i < 50; i++) {
                assertTrue(store.put(("mem:hot:" + i).getBytes(), payload));
            }
            for (int r = 0; r < 5; r++) {
                for (int i = 0; i < 50; i++) {
                    assertNotNull(store.get(("mem:hot:" + i).getBytes()));
                }
            }

            // 2. Perform 2,000 cold scans
            for (int i = 0; i < 2_000; i++) {
                store.put(("mem:cold:scan:" + i).getBytes(), payload);
            }

            // 3. Verify hot keys survived against cold scans
            int survived = 0;
            for (int i = 0; i < 50; i++) {
                if (store.containsKey(("mem:hot:" + i).getBytes())) {
                    survived++;
                }
            }
            assertTrue(survived >= 45, "OffHeapStore TinyLFU must protect hot keys! Survived: " + survived + "/50");
            assertTrue(store.getAdmissionRejections() > 0, "Admission gate must record rejected cold candidates");
            assertEquals(store.getAdmissionRejections(), store.getStats().admissionRejections());
        }
    }

    @Test
    @DisplayName("Stats: Accurate Real-time Cache Hit Rate and Metric Calculations")
    void testStatsHitRateAndMetricCalculations() {
        try (PersistentOffHeapStore store = PersistentOffHeapStore.inMemory()) {
            byte[] k1 = "stat:k1".getBytes();
            byte[] v1 = "val1".getBytes();
            store.put(k1, v1);

            // 3 hits, 1 miss -> 75% hit rate
            store.get(k1);
            store.get(k1);
            store.get(k1);
            store.get("stat:non_existent".getBytes());

            PersistentOffHeapStore.Stats stats = store.getStats();
            assertEquals(3, stats.hits());
            assertEquals(1, stats.misses());
            assertEquals(75.0, stats.hitRate(), 0.001, "Hit rate must be precisely 75.0%");
        }
    }

    private double runBenchmarkWorkload(EvictionPolicy policy, int capacity, int distinctKeys, int ops, byte[] payload) throws Exception {
        int[] slotCounts = new int[11];
        java.util.Arrays.fill(slotCounts, capacity);

        try (PersistentOffHeapStore store = PersistentOffHeapStore.builder()
                .mode(StorageMode.IN_MEMORY)
                .slotCounts(slotCounts)
                .maxBuckets(512)
                .evictionPolicy(policy)
                .build()) {

            byte[][] keyPool = new byte[distinctKeys][];
            for (int i = 0; i < distinctKeys; i++) {
                keyPool[i] = ("key:" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }

            java.util.Random rng = new java.util.Random(9999);
            long hits = 0;
            long reads = 0;

            for (int i = 0; i < ops; i++) {
                int keyIdx;
                if (rng.nextInt(100) < 15) {
                    keyIdx = 1000 + rng.nextInt(distinctKeys - 1000); // 15% cold scan burst
                } else {
                    if (rng.nextInt(100) < 80) {
                        keyIdx = rng.nextInt(100); // 80% hits top 100 keys
                    } else {
                        keyIdx = rng.nextInt(500);
                    }
                }

                byte[] key = keyPool[keyIdx];
                byte[] v = store.get(key);
                reads++;
                if (v != null) {
                    hits++;
                } else {
                    store.put(key, payload);
                }
            }
            return (double) hits / reads * 100.0;
        }
    }

    @Test
    @DisplayName("Anti-Cycle Safety: Extreme Eviction in Constrained Buckets and Slabs")
    void testAntiCycleEvictionSafety(@TempDir Path tempDir) throws Exception {
        int[] tinySlots = new int[11];
        java.util.Arrays.fill(tinySlots, 16); // Only 16 slots per slab class

        // 1. In-memory mode with TinyLFU eviction under extreme bucket collision (4 buckets)
        try (PersistentOffHeapStore store = PersistentOffHeapStore.builder()
                .mode(StorageMode.IN_MEMORY)
                .slotCounts(tinySlots)
                .maxBuckets(4)
                .evictionPolicy(EvictionPolicy.WINDOW_TINY_LFU)
                .build()) {

            byte[] valSmall = new byte[32];
            byte[] valLarge = new byte[200];
            java.util.Arrays.fill(valSmall, (byte) 1);
            java.util.Arrays.fill(valLarge, (byte) 2);

            // Write 500 keys, resizing payloads from small to large to trigger slot reallocation and eviction
            for (int i = 0; i < 500; i++) {
                byte[] key = ("anti-cycle:" + (i % 50)).getBytes();
                boolean ok = store.put(key, (i % 2 == 0) ? valSmall : valLarge);
                if (ok) {
                    assertNotNull(store.get(key));
                }
            }
            assertTrue(store.size() <= 16 * 11);
        }

        // 2. Persistent mode with crash recovery under extreme eviction and collision
        Path pDir = tempDir.resolve("cycle_test_db");
        try (PersistentOffHeapStore store = PersistentOffHeapStore.builder()
                .storageDir(pDir)
                .slotCounts(tinySlots)
                .maxBuckets(4)
                .evictionPolicy(EvictionPolicy.WINDOW_TINY_LFU)
                .build()) {

            for (int i = 0; i < 300; i++) {
                byte[] k = ("p-cycle:" + i).getBytes();
                byte[] v = new byte[50 + (i % 100)];
                store.put(k, v);
            }
            store.sync();
        }

        // Reopen and ensure recoverAndRebuild detects 0 cycles and cleanly mounts
        try (PersistentOffHeapStore store = PersistentOffHeapStore.builder()
                .storageDir(pDir)
                .slotCounts(tinySlots)
                .maxBuckets(4)
                .evictionPolicy(EvictionPolicy.WINDOW_TINY_LFU)
                .build()) {
            assertTrue(store.size() > 0);
            assertTrue(store.size() <= 16 * 11);
        }
    }
}
