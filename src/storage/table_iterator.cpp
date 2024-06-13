#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn)
    : table_heap_(table_heap), rid_(rid), txn_(txn) {
  // get rid
  if (rid_ == RowId{0}) {
    auto page_id = table_heap_->GetFirstPageId();
    if (page_id == INVALID_PAGE_ID) {
      rid_ = RowId{-1};
      return;
    }
    // get first page
    auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page_id));
    if (page == nullptr) {
      DLOG(ERROR) << "Failed to fetch page";
    }
    // get first rid
    auto got = page->GetFirstTupleRid(&rid_);
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    if (!got) {
      //DLOG(ERROR) << "No tuple found in first table";
      rid_ = RowId{-1};
    }
  } 
  // RowId{-1} is the end of the table
  // RowId{n} no need to do anything
}

TableIterator::TableIterator(const TableIterator &other) {
  table_heap_ = other.table_heap_;
  rid_ = other.rid_;
  txn_ = other.txn_;
}

TableIterator::~TableIterator() {
  // use default destructor
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return table_heap_ == itr.table_heap_ && rid_ == itr.rid_;
}

bool TableIterator::operator!=(const TableIterator &itr) const { return !(*this == itr); }

const Row TableIterator::operator*() {
  Row row(rid_);
  table_heap_->GetTuple(&row, txn_);
  return row;
}

Row *TableIterator::operator->() {
  Row *row = new Row(rid_);
  table_heap_->GetTuple(row, txn_);
  return row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  table_heap_ = itr.table_heap_;
  rid_ = itr.rid_;
  txn_ = itr.txn_;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  // get this page
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(rid_.GetPageId()));
  if (page == nullptr) {
    DLOG(ERROR) << "Failed to fetch page";
  }
  // get next rid
  auto got = page->GetNextTupleRid(rid_, &rid_);
  table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  if (got) {
    return *this;
  }
  // get next page
  if (page->GetNextPageId() == INVALID_PAGE_ID) {
    rid_ = RowId{-1};
    return *this;
  }
  page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page->GetNextPageId()));
  if (page == nullptr) {
    DLOG(ERROR) << "Failed to fetch page";
  }
  // get first rid
  got = page->GetFirstTupleRid(&rid_);
  table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  if (!got) {
    DLOG(ERROR) << "No tuple found in next table";
    rid_ = RowId{-1};
  }
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator tmp = *this;
  ++(*this);
  return tmp;
}
