#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  return sizeof(CATALOG_METADATA_MAGIC_NUM) + sizeof(index_meta_pages_.size()) + sizeof(table_meta_pages_.size()) +
         index_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t)) +
         table_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t));
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  // 在后续重新打开数据库实例时，从数据库文件中加载所有的表和索引信息，构建TableInfo和IndexInfo信息置于内存中。
  if (!init) {
    // 从catalog meta中获取表和索引的对应位置
    // CatalogMeta的信息将会被序列化到数据库文件的第CATALOG_META_PAGE_ID号数据页中（逻辑意义上），CATALOG_META_PAGE_ID默认值为0。
    auto catalog_page = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    // 反序列化得到catalog meta
    catalog_meta_ = CatalogMeta::DeserializeFrom(reinterpret_cast<char *>(catalog_page->GetData()));
    next_index_id_ = catalog_meta_->GetNextIndexId();
    next_table_id_ = catalog_meta_->GetNextTableId();
    // 加载所有表
    for (auto it : catalog_meta_->table_meta_pages_) {
      // 对每一个表找到table_meta
      auto table_meta_page = buffer_pool_manager_->FetchPage(it.second);
      buffer_pool_manager_->UnpinPage(it.second, true);
      TableMetadata *table_meta = nullptr;
      TableMetadata::DeserializeFrom(table_meta_page->GetData(), table_meta);
      // 加载table_names <std::string, table_id_t>
      table_names_[table_meta->GetTableName()] = table_meta->GetTableId();
      // 加载tables <table_id_t, TableInfo *>
      auto table_heap = TableHeap::Create(buffer_pool_manager, table_meta->GetFirstPageId(), table_meta->GetSchema(),
                                          log_manager_, lock_manager_);
      TableInfo *table_info = TableInfo::Create();
      table_info->Init(table_meta, table_heap);
      tables_[table_meta->GetTableId()] = table_info;
      // 更新next_table_id
      if (table_meta->GetTableId() >= next_table_id_) {
        next_table_id_ = table_meta->GetTableId() + 1;
      }
    }
    // 加载所有索引
    for (auto it : catalog_meta_->index_meta_pages_) {
      // 对每一个索引找到index_meta
      auto index_meta_page = buffer_pool_manager_->FetchPage(it.second);
      buffer_pool_manager_->UnpinPage(it.second, true);
      IndexMetadata *index_meta = nullptr;
      IndexMetadata::DeserializeFrom(index_meta_page->GetData(), index_meta);
      // 加载index_names <std::string, std::unordered_map<std::string, index_id_t>>
      index_names_[tables_[index_meta->GetTableId()]->GetTableName()][index_meta->GetIndexName()] =
          index_meta->GetIndexId();
      // 加载indexes <index_id_t, IndexInfo *>
      IndexInfo *index_info = IndexInfo::Create();
      index_info->Init(index_meta, tables_[index_meta->GetTableId()], buffer_pool_manager_);
      indexes_[index_meta->GetIndexId()] = index_info;
      // 更新next_index_id
      if (index_meta->GetIndexId() >= next_index_id_) {
        next_index_id_ = index_meta->GetIndexId() + 1;
      }
    }
  } else {  // 在数据库实例（DBStorageEngine）初次创建时（init = true）初始化元数据
    catalog_meta_ = CatalogMeta::NewInstance();
  }
  buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID, true);
  // buffer_pool_manager->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
  // FlushCatalogMetaPage();
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  // table has existed
  if (table_names_.count(table_name) != 0) {
    return DB_TABLE_ALREADY_EXIST;
  }

  IndexSchema *dschema = Schema::DeepCopySchema(schema);
  // create table
  // add to table_names <std::string, table_id_t>
  table_id_t table_id = next_table_id_;
  table_names_.emplace(table_name, table_id);

  // get a new page for table_meta
  page_id_t page_id;
  auto table_meta_page = buffer_pool_manager_->NewPage(page_id);
  auto table_heap = TableHeap::Create(buffer_pool_manager_, dschema, txn, log_manager_, lock_manager_);
  auto table_meta = TableMetadata::Create(table_id, table_name, table_heap->GetFirstPageId(), dschema);
  table_meta->SerializeTo(table_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(page_id, true);

  // update catalog_meta
  catalog_meta_->table_meta_pages_.emplace(table_id, page_id);

  // add to table
  table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);
  tables_[table_id] = table_info;

  next_table_id_++;
  // DLOG(INFO)<<"CreateTable pageid : "<<page_id<<endl;
  // buffer_pool_manager_->FlushPage(page_id);
  // FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  auto table = table_names_.find(table_name);
  if (table == table_names_.end())  // no such table
  {
    return DB_TABLE_NOT_EXIST;
  }
  auto table_id = table->second;
  table_info = tables_.find(table_id)->second;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for (auto &it : tables_) {
    tables.push_back(it.second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  // no such table
  auto table = table_names_.find(table_name);
  if (table == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }

  // there has been a same index for the table
  auto index = index_names_.find(table_name);
  if (index != index_names_.end() && index->second.find(index_name) != index->second.end()) {
    return DB_INDEX_ALREADY_EXIST;
  }

  // give a index for every column
  std::vector<uint32_t> column_index_;
  auto table_info = tables_.find(table->second)->second;
  auto schema = table_info->GetSchema();
  for (auto &it : index_keys) {
    uint32_t column_index;
    if (schema->GetColumnIndex(it, column_index) == DB_COLUMN_NAME_NOT_EXIST)  // no such column
    {
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    column_index_.push_back(column_index);
  }

  // create index
  // add to index_names <std::string, std::unordered_map<std::string, table_id_t>>
  index_names_[table_name][index_name] = next_index_id_;

  // get a new page for index_meta
  page_id_t page_id;
  auto index_meta_page = buffer_pool_manager_->NewPage(page_id);
  auto index_meta = IndexMetadata::Create(next_index_id_, index_name, table_names_[table_name], column_index_);
  index_meta->SerializeTo(index_meta_page->GetData());

  // update catalog_meta
  catalog_meta_->index_meta_pages_[next_index_id_] = page_id;

  // add to index
  index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info, buffer_pool_manager_);
  // may be meta page should reside in buffer pool? After Init the
  // key_map is then passed to KeyManager, can be unpinned
  // buffer_pool_manager_->UnpinPage(page_id, true);
  // 呜呜，这里应当将表中的所有数据插入到索引中
  Row key;
  for (auto it = table_info->GetTableHeap()->Begin(nullptr); it != table_info->GetTableHeap()->End(); it++) {
    auto row = *it;
    // std::vector<Field> fields;
    // for(auto &col : column_index_){
    //   fields.push_back(*row.GetField(col));
    // }
    // Row key(fields);
    row.GetKeyFromRow(table_info->GetSchema(), index_info->GetIndexKeySchema(), key);
    index_info->GetIndex()->InsertEntry(key, row.GetRowId(), txn);
  }
  indexes_[next_index_id_] = index_info;
  next_index_id_++;
  // DLOG(INFO)<<"CreateIndex pageid : "<<page_id<<endl;
  // buffer_pool_manager_->FlushPage(page_id);
  // FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  auto index_table = index_names_.find(table_name);
  if (index_table == index_names_.end())  // no such table
  {
    return DB_TABLE_NOT_EXIST;
  }
  auto index = index_table->second.find(index_name);
  if (index == index_table->second.end())  // no such index
  {
    return DB_INDEX_NOT_FOUND;
  }
  auto index_id = index->second;
  index_info = indexes_.find(index_id)->second;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  auto index_table = index_names_.find(table_name);
  if (index_table == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  indexes.clear();
  for (auto &it : index_table->second) {
    auto index_id = it.second;
    indexes.push_back(indexes_.find(index_id)->second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  auto table = table_names_.find(table_name);
  if (table == table_names_.end())  // no such table
  {
    return DB_TABLE_NOT_EXIST;
  }
  auto table_id = table->second;
  table_names_.erase(table_name);
  tables_.erase(table_id);

  page_id_t page_id = catalog_meta_->table_meta_pages_[table_id];
  catalog_meta_->table_meta_pages_.erase(table_id);
  buffer_pool_manager_->UnpinPage(page_id, true);
  buffer_pool_manager_->DeletePage(page_id);
  // FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  auto table = index_names_.find(table_name);
  if (table == index_names_.end())  // no such table
  {
    return DB_TABLE_NOT_EXIST;
  }
  if ((table->second.find(index_name) == table->second.end()))  // no such index
  {
    return DB_INDEX_NOT_FOUND;
  }
  auto index_id = index_names_[table_name][index_name];
  index_names_.erase(table_name);
  indexes_.erase(index_id);

  page_id_t page_id = catalog_meta_->index_meta_pages_[index_id];
  catalog_meta_->index_meta_pages_.erase(index_id);
  buffer_pool_manager_->UnpinPage(page_id, true);
  buffer_pool_manager_->DeletePage(page_id);
  // FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // flush page
  auto CatalogMetaPage = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(CatalogMetaPage->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  if (tables_.count(table_id) != 0) {  // there has been the table
    return DB_TABLE_ALREADY_EXIST;
  }
  catalog_meta_->table_meta_pages_[table_id] = page_id;

  // add to table_names
  auto table_meta_page = buffer_pool_manager_->FetchPage(page_id);
  TableMetadata *table_meta;
  TableMetadata::DeserializeFrom(table_meta_page->GetData(), table_meta);
  auto table_name = table_meta->GetTableName();
  table_names_[table_name] = table_id;

  // add to tables
  auto table_heap =
      TableHeap::Create(buffer_pool_manager_, page_id, table_meta->GetSchema(), log_manager_, lock_manager_);
  auto table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);
  tables_.emplace(table_id, table_info);
  // DLOG(INFO)<<"LoadTable pageid : "<<page_id<<endl;
  buffer_pool_manager_->UnpinPage(page_id, true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  if (indexes_.count(index_id) != 0)  // there has been the index
  {
    return DB_INDEX_ALREADY_EXIST;
  }
  catalog_meta_->index_meta_pages_[index_id] = page_id;

  // add to index_names
  auto index_meta_page = buffer_pool_manager_->FetchPage(page_id);
  IndexMetadata *index_meta;
  IndexMetadata::DeserializeFrom(index_meta_page->GetData(), index_meta);
  auto table_info = tables_[index_meta->GetTableId()];
  auto table_name = table_info->GetTableName();
  index_names_[table_name][index_meta->GetIndexName()] = index_id;

  // add to index
  auto index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info, buffer_pool_manager_);
  indexes_.emplace(index_id, index_info);
  // DLOG(INFO)<<"LoadIndex pageid : "<<page_id<<endl;
  buffer_pool_manager_->UnpinPage(page_id, true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto table = tables_.find(table_id);
  if (table == tables_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_info = table->second;
  return DB_SUCCESS;
}