#include "storage/table_heap.h"

#include <algorithm>

bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  auto row_size = row.GetSerializedSize(schema_);
  //DLOG(INFO) << "Row size: " << row_size;
  if (row_size >= PAGE_SIZE) {
    //DLOG(ERROR) << "The tuple is too large to insert.";
    return false;
  }
  // Find the page which can save the tuple.
  auto page = std::find_if(page_free_space_.begin(), page_free_space_.end(),
                           [row_size](const auto &pair) { return pair.second >= row_size + TablePage::SIZE_TUPLE; });
  TablePage *page_to_insert = nullptr;
  if (page == page_free_space_.end()) {
    //DLOG(INFO) << "[InsertTuple] Create a new page.";
    // Create a new page.
    page_id_t new_page_id;
    auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
    if (new_page == nullptr) return false;
    // setup the new page
    new_page->Init(new_page_id, page_free_space_.rbegin()->first, log_manager_, txn);
    // link to the previous page
    auto pre_page_id = page_free_space_.rbegin()->first;
    auto pre_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(pre_page_id));
    pre_page->WLatch();
    pre_page->SetNextPageId(new_page_id);
    pre_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(pre_page_id, true);
    page_to_insert = new_page;
  } else {
    //DLOG(INFO) << "[InsertTuple] Insert into existing page.";
    page_to_insert = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->first));
  }
  // Insert the tuple
  //DLOG(INFO) << "Insert tuple into page " << page_to_insert->GetTablePageId();
  page_to_insert->WLatch();
  if (!page_to_insert->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
    //DLOG(ERROR) << "InsertTuple Failed";
    return false;
  }
  page_to_insert->WUnlatch();
  // Update the free space of the page.
  page_free_space_[page_to_insert->GetTablePageId()] = page_to_insert->GetFreeSpaceRemaining();
  //DLOG(INFO) << "Free space remaining: " << page_to_insert->GetFreeSpaceRemaining();
  buffer_pool_manager_->UnpinPage(page_to_insert->GetPageId(), true);
  return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
  auto row_size = row.GetSerializedSize(schema_);
  if (row_size >= PAGE_SIZE) {
    //DLOG(ERROR) << "The tuple is too large to insert.";
    return false;
  }
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) return false;
  page->WLatch();
  Row old = Row(rid);
  auto updated = page->UpdateTuple(row, &old, schema_, txn, lock_manager_, log_manager_);
  page_free_space_[page->GetTablePageId()] = page->GetFreeSpaceRemaining();
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), updated);
  if (updated) {
    return true;
  }
  // Delete the old tuple.
  if (!MarkDelete(rid, txn)) {
    return false;
  }
  ApplyDelete(rid, txn);  // [TODO] rollback delete
  // Insert the new tuple.
  return InsertTuple(row, txn);
}

void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Apply the delete.
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page_free_space_[page->GetTablePageId()] = page->GetFreeSpaceRemaining();
  page->WUnlatch();
  // if page is empty, delete the page
  if (page->GetTupleCount() == 0) {
    auto next_page_id = page->GetNextPageId();
    auto pre_page_id = page->GetPrevPageId();
    if (next_page_id != INVALID_PAGE_ID) {
      auto next_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
      next_page->WLatch();
      next_page->SetPrevPageId(pre_page_id);
      next_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(next_page_id, true);
    }
    if (pre_page_id != INVALID_PAGE_ID) {
      auto pre_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(pre_page_id));
      pre_page->WLatch();
      pre_page->SetNextPageId(next_page_id);
      pre_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(pre_page_id, true);
    }
    buffer_pool_manager_->DeletePage(page->GetTablePageId());
    page_free_space_.erase(page->GetTablePageId());
  } else
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

bool TableHeap::GetTuple(Row *row, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, get the tuple from the page.
  page->RLatch();
  page->GetTuple(row, schema_, txn, lock_manager_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return true;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID) DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

TableIterator TableHeap::Begin(Txn *txn) { return TableIterator(this, RowId{0}, txn); }

TableIterator TableHeap::End() { return TableIterator(this, RowId{-1}, nullptr); }
