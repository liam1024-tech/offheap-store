package offheap;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
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
}
