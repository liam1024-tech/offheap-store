# Contributing to Off-Heap Store

Thank you for your interest in contributing to **Off-Heap Store**! We welcome bug reports, design ideas, benchmarks, and pull requests from developers around the world.

---

## Code of Conduct

This project adheres to the [Contributor Covenant](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

---

## Development Setup

### Prerequisites
* **JDK**: Modern Java 22+ (JDK 25, 26, or 27 recommended).
* **Build Tool**: Maven 3.8+ or direct execution via `./build.sh`.

### Building & Running Tests Locally
```bash
# 1. Clone repository
git clone https://github.com/liam1024-tech/offheap-store.git
cd offheap-store

# 2. Build jar and test classes
chmod +x build.sh
./build.sh

# 3. Run the 9 Extreme Battle Tests
java --enable-native-access=ALL-UNNAMED -ea -cp target/classes:target/test-classes offheap.AdvancedExtremeBattleTest
```

---

## Architectural Guidelines

When submitting code changes, please adhere to these core architectural commitments:
1. **Zero Heap Allocation on Hot Paths**: Hot read/write code paths (`get`, `put`, `compareAndSwap`, `batchPut`) must produce zero GC overhead. Use `MemorySegment` slices, native FFM intrinsics, and reusable thread-local scratch holders.
2. **Deadlock Freedom**: Any multi-key operation must sort resource acquisitions (e.g., bitmask ascending order across stripe locks).
3. **No Unsafe Dependencies**: Use standard Java FFM (JEP 454: `java.lang.foreign.*`) and `VarHandle`. Avoid deprecated `sun.misc.Unsafe`.
4. **Compatibility**: Code must compile and pass cleanly across JDK 25, JDK 26, and JDK 27.

---

## Submitting Pull Requests

1. Fork the repository and create your branch from `main`.
2. Ensure `./build.sh` runs and all 9 battle tests pass with 100% assertions satisfied.
3. Open a Pull Request with a clear summary of your changes, motivation, and benchmark comparison (if applicable).
