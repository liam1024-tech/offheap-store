#!/usr/bin/env bash
# ==============================================================================
# Off-Heap Store: Standalone Build & Battle Test Script (Zero-Dependency)
# Supports JDK 22, 25, 26, 27+ with standard Foreign Function & Memory (FFM) API
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

JAVAC="${JAVAC:-$(which javac)}"
JAVA="${JAVA:-$(which java)}"

echo "=== Using Compiler: $($JAVAC -version) ==="
echo "=== Using Runtime:  $($JAVA -version 2>&1 | head -n 1) ==="

TARGET_CLASSES="target/classes"
TARGET_TEST_CLASSES="target/test-classes"
VERSION=$(grep -m1 '<version>' pom.xml | sed 's/[^0-9.]//g')
TARGET_JAR="target/offheap-store-${VERSION}.jar"

mkdir -p "$TARGET_CLASSES" "$TARGET_TEST_CLASSES"

echo -e "\n[1/3] Compiling core off-heap sources..."
"$JAVAC" --release 25 \
  -d "$TARGET_CLASSES" \
  src/main/java/offheap/*.java

echo "[2/3] Packaging jar: $TARGET_JAR..."
jar --create --file "$TARGET_JAR" -C "$TARGET_CLASSES" .

echo "[3/3] Compiling battle test suite (excluding JUnit wrapper if not present)..."
TEST_SOURCES=$(ls src/test/java/offheap/*.java | grep -v "JUnit" || true)
"$JAVAC" --release 25 \
  -cp "$TARGET_CLASSES" \
  -d "$TARGET_TEST_CLASSES" \
  $TEST_SOURCES

echo -e "\n========================================================"
echo "  Build SUCCESS! All components compiled without errors."
echo "========================================================"
echo -e "\nTo run the 10 Extreme Battle Tests directly:"
echo "  $JAVA --enable-native-access=ALL-UNNAMED -ea -cp target/classes:target/test-classes offheap.AdvancedExtremeBattleTest"
echo -e "\nTo run Dual Mode Verification:"
echo "  $JAVA --enable-native-access=ALL-UNNAMED -ea -cp target/classes:target/test-classes offheap.DualModeStorageTest"
echo -e "\nTo start 7-Day Continuous Soak Engine:"
echo "  $JAVA --enable-native-access=ALL-UNNAMED -ea -Xms2g -Xmx2g -XX:+UseZGC -cp target/classes:target/test-classes offheap.ContinuousSoakStressEngine --duration-minutes 10080 --workers 8"
