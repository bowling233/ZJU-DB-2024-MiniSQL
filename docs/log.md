# 开发过程和文档阅读记录

## 6.2 Disk and buffer pool

### 概念

- 数据页：
    - 4kb
    - 物理标识符 `physical_page_id`
- disk manager:
    - bitmap：分配和回收数据页
        - bit 对应数据页分配情况
    - 逻辑标识符 `logical_page_id`
- buffer pool:
    - 数据页在磁盘和内存间的移动
    - 唯一标识符 `page_id`

可以涉及并发控制

### 位图页

`page/bitmap_page`

- metadata
    - 已分配页
    - 下一个空闲页
- 具体数据

### 数据页

`page/disk_file_meta_page` 给出了数据结构

`storage/disk_manager`

位图页+数据页一起放在一个文件中：

- 整个文件组织为分区 extents
    - metapage 维护分区信息4KB，可维护1K个分区。一个 u32int 字段维护分区信息，每个分区用 4 位？
    - 分区内：4K * 8 * 4KB = 128MB
        - 位图页：能够管理的32768个数据页
        - 一堆数据页：每个数据页 4kb

### LRU Buffer

`buffer/lru_replacer`

### Buffer Pool

`buffer/buffer_pool_manager`

