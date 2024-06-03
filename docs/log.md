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
- [x] `storage/disk_manager`

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

- [x] `buffer/lru_replacer`

- Replacer 用链表完成。几个功能：Victim 表尾，Pin 出表，Unpin 入表，Size。

### Buffer Pool

- [x] `buffer/buffer_pool_manager`

- 已有函数：
    - 构造：页的数组，replacer，把所有页都放入 free list
    - 析构：flush 写回所有页
    - Allocate、Deallocate、IsFree 全都直接向 DiskManager 申请（？！
- 读一下内存页 `Page`，和计组的缓存结构差不多。
- 感觉写的过程中 frame 和 pages 的状态还没有完全搞清楚，可能有数据没有重置清楚之类的，不知道后面会不会爆

## 6.3 Record Manager

- Record
    - Column: name_, type_, len_, table_ind_, nullable_, unique_
    - Schema: colums, is_manage
    - Field: union, type_id, len, is_null, manage_data（？）
    - Row：rid + fields

### 序列化

- [x] `record`

参考 `Field` 完成其它对象的：

- SerializeTo: buf 指针向前推进了多少个字节。
- DeserializeFrom
- GetSerializedSize

### 堆表

- [x] `storage/table_iterator`
- [x] `page/table_page`
- [x] `storage/table_heap`

初次看 `table_heap` 这堆东西发现只有 id 有点懵，注意到 table_page 继承 `Page` 就能大概理解了。不过到了堆表这一层，使用的就直接是 `page_id`，不关心从 `buffer` 开始的下层了。

在着手实现的时候，有想法给堆表加上类似 B+ 树的快速查找功能，但是这个功能应该是在索引层实现的，所以先不管。一个是这个堆表似乎是无序的（如果是有序会有什么好处吗？数据库引擎好像也没有保证结果一定按某种顺序，给底层实现留了很大自由空间）而且 RowID 本身已经有两级地址可以直接索引了，不是很懂要怎么加元信息来加速Row的插入、查找和删除操作。

有一个涉及并发控制还是恢复的构造函数，不懂，先不管。

- 已有的函数：
    - 标记删除：FetchPage，WLatch，MarkDelete，WUnlatch，UnpinPage
    - 回滚删除：FetchPage，WLatch，RollbackDelete，WUnlatch，UnpinPage
    - 删除表：如果指定为 INVALID_PAGE_ID，直接删除首个table_page。【感觉有递归风险，如果 first_page_id 也是 INVALID_PAGE_ID】
- 自己实现的几个函数基本就是模仿这些步骤：
    - GetTuple：简单
    - InsertTuple：
        - 获取row的长度
        - 循环找到能放下的page，否则新建
        - 存放row
    - UpdateTuple: RowId 允许被更改
        - 先尝试直接更新
        - 先markdelete
        - 再调用插入
- TablePage 已提供：
    - 插入：长度，找空slot【疑问：为什么不直接从尾部呢？需要根据删除的方法确认】，写入，调整长度、指针、tuple记录数。
    - 标记删除：操作 tuplesize(根据slot找)，这个运算好像可逆，待会看看
    - 更新：长度不够直接返回false无操作。否则，操作。
        - 读取旧长度
        - 移动其余数据
        - 写入新数据
        - 更新所有slot的偏移
    - 删除：标记删除，调整指针、长度、记录数
    - IsDeleted 有两种情况：tuple_size 为 0（暂时没想到为什么会有这种情况）和 DELETEMASK。在 tablePage 各个函数中用于检测是否被删除，可以看出只要标记就认为是删除了。
    - 回滚删除：

暂时没考虑恢复的事情，markdelete 之类的时机应该要有调整，目前直接mark后apply。暂时按能通过测试用例就行。

写 TableIterator 时，给的构造函数是 explicit 的，但拷贝构造通常不应该是。删去。迭代器的锁没懂怎么用。

