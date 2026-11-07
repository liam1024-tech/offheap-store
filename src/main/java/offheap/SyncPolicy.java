package offheap;

/**
 * 持久化刷盘策略 (Fsync Flush Policy)
 *
 * 供使用者根据业务对延迟与极端掉电安全性的不同需求灵活选配：
 * 1. {@link #MANUAL} (默认)：依赖操作系统 Page Cache 高速异步回写，并在显式调用 sync() 或 close() 时强制落盘。吞吐最高。
 * 2. {@link #PERIODIC_BACKGROUND}：后台独立守护线程按固定周期 (如每 100ms / 1000ms) 自动检测脏页并异步执行 fsync 与超级块 Checkpoint，
 *    业务读写线程 0 阻塞、0 I/O 停顿。
 * 3. {@link #SYNC_ON_WRITE}：每次写入/更新/删除操作完成时立即执行落盘，提供金融级单笔事务持久性保证。
 */
public enum SyncPolicy {

    /**
     * 手动/关闭时刷盘 (默认，最高吞吐)
     */
    MANUAL,

    /**
     * 后台定时异步刷盘 (兼顾微秒级低延迟与周期性 Checkpoint 持久化)
     */
    PERIODIC_BACKGROUND,

    /**
     * 每次写操作同步刷盘 (强持久化保证)
     */
    SYNC_ON_WRITE
}
