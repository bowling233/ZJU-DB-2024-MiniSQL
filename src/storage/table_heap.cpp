#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  auto row_size = row.GetSerializedSize(schema_);
  if (row_size >= PAGE_SIZE) {
    DLOG(ERROR) << "The tuple is too large to insert.";
    return false;
  }
  // Find the page which can save the tuple.
  page_id_t pre_page_id, page_id;
  page_id = pre_page_id = first_page_id_;
  while (page_id != INVALID_PAGE_ID) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    if (page == nullptr) {
      return false;
    }
    // try to insert the tuple into the page.
    page->WLatch();
    auto inserted_ = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, inserted_);
    if (inserted_) {
      // DLOG(INFO) << "Insert tuple into page " << page_id;
      return true;
    }
    // try next page
    pre_page_id = page_id;
    page_id = page->GetNextPageId();
  }
  // Create a new page
  auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(page_id));
  if (new_page == nullptr) {
    return false;
  }
  //DLOG(INFO) << "Create new page " << new_page->GetPageId();
  // Link the new page to the previous page.
  if (pre_page_id != INVALID_PAGE_ID) {
    auto pre_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(pre_page_id));
    pre_page->WLatch();
    pre_page->SetNextPageId(new_page->GetPageId());
    pre_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(pre_page_id, true);
  }
  // Insert the tuple
  new_page->WLatch();
  new_page->Init(new_page->GetPageId(), pre_page_id, log_manager_, txn);  // prev linked here
  if (!new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
    DLOG(ERROR) << "InsertTuple Failed";
    return false;
  }
  //DLOG(INFO) << "Insert tuple into page " << page_id;
  new_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
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
    DLOG(ERROR) << "The tuple is too large to insert.";
    return false;
  }
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) return false;
  page->WLatch();
  Row old = Row(rid);
  auto updated = page->UpdateTuple(row, &old, schema_, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), updated);
  if (updated) return true;
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
  page->WUnlatch();
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
