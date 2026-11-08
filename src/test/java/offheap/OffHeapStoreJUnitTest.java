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
}
