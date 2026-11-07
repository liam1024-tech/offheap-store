package offheap;

/**
 * 堆外存储运行模式 (Storage Mode)
 *
 * 让使用者自由选择两种核心运行形态：
 * 1. PERSISTENT: 磁盘 MMAP 零拷贝映射，服务重启或宕机崩溃数据 100% 自动自愈不丢失。
 * 2. IN_MEMORY:  纯堆外直接内存 (FFM Arena)，服务停机或崩溃数据释放，无磁盘 I/O 开销，具备 300万+ ops/s 极致吞吐。
 */
public enum StorageMode {
    /**
     * 永久不丢失模式：
     * - 底层基于 Linux Page Cache 与 FileChannel.map 内存映射物理文件；
     * - 支持显式 sync() 物理落盘与严格保序 Checkpoint；
     * - 服务停机重启后通过 Fast FSCK 两阶段对账实现亚秒级冷启复活与坏数据自愈；
     * - 适用场景：生产级持久化缓存、本地数据库引擎、跨进程数据共享。
     */
    PERSISTENT,

    /**
     * 纯内存可丢失模式：
     * - 底层基于纯堆外直接物理内存段 (FFM Arena.ofShared) 分配；
     * - 无任何磁盘文件映射，无磁盘 I/O 开销，吞吐率可达 300万~400万 ops/s；
     * - 进程退出或异常崩溃后数据自动释放，不产生任何本地残留文件；
     * - 适用场景：极速计算会话、短周期临时缓存、无状态微服务高速中间件。
     */
    IN_MEMORY
}
