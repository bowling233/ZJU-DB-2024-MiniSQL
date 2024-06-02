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

- [x] `page/bitmap_page`

- metadata
    - 已分配页
    - 下一个空闲页
- 具体数据

### 数据页

- [x] `page/disk_file_meta_page`
- [ ] `storage/disk_manager`

位图页+数据页一起放在一个文件中：

- 整个文件组织为分区 extents
    - metapage 维护分区信息4KB，可维护1K个分区。（真的需要这个 u32int 字段维护分区信息吗？`page_allocated` 不就好了？）
    - 分区内：4Kx8x4K = 128MB
        - 位图页：能够管理的32768个数据页
        - 一堆数据页：每个数据页 4kb
- `DiskFileMetaPage` 这个类感觉封装得很不完善，干脆别封装了（
- `DiskManager` 已给出的代码：
    - 构造：打开并读 `meta_data_`
    - 关闭：写 `meta_data_`
    - 读/写逻辑页：依赖于读写物理页。**只负责元数据**。
    - 读物理页：`std::istream::read` 直读内存块
    - 写物理页：`std::ostream::write` 直写内存块
    - 只需要实现对 `BitmapPage` 操作的简单包装即可
    - 根据 `physical_page_id = logical_page_id + (ext_no + 1) + 1` 进行转换

Debug 时发现前面的 `bitmap` 爆了，没有初始化，数据大之后会遇到脏数据。**记得检查初始化**。

此外就是 offset 计算的问题。

### LRU Buffer

`buffer/lru_replacer`

- Replacer 用链表完成。几个功能：Victim 表尾，Pin 出表，Unpin 入表，Size。

### Buffer Pool

`buffer/buffer_pool_manager`

- 已有函数：
    - 构造：页的数组，replacer，把所有页都放入 free list
    - 析构：flush 写回所有页
    - Allocate、Deallocate、IsFree 全都直接向 DiskManager 申请（？！
- 读一下内存页 `Page`，和计组的缓存结构差不多。

`UnpinPage` 的 `is_dirty` 不知道干什么用的。

