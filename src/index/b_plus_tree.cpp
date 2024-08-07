#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  auto header_page =
      reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  page_id_t root_page_id;
  // check if the index already exists
  if (header_page->GetRootId(index_id, &root_page_id)) {
    root_page_id_ = root_page_id;
  } else {
    root_page_id_ = INVALID_PAGE_ID;
    header_page->Insert(index_id, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
  //DLOG(INFO) << "BPlusTree Init, root page id: " << root_page_id_;
  // calculate node size
  if (leaf_max_size_ == UNDEFINED_SIZE || internal_max_size_ == UNDEFINED_SIZE) {
    leaf_max_size_ = (PAGE_SIZE - BPlusTreeLeafPage::LEAF_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(RowId));
    internal_max_size_ =
        (PAGE_SIZE - BPlusTreeInternalPage::INTERNAL_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(page_id_t));
    int min_size = std::min(leaf_max_size_, internal_max_size_);
    leaf_max_size_ = internal_max_size_ = min_size;
  }
  //DLOG(INFO) << "leaf_max_size: " << leaf_max_size_ << ", internal_max_size: " << internal_max_size_;
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  if (current_page_id == INVALID_PAGE_ID) {
    current_page_id = root_page_id_;
  }
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(current_page_id)->GetData());
  if (node->IsLeafPage()) {
    buffer_pool_manager_->DeletePage(current_page_id);
  } else {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    for (int i = 0; i < internal_node->GetSize(); i++) {
      Destroy(internal_node->ValueAt(i));
    }
    buffer_pool_manager_->DeletePage(current_page_id);
  }
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
  if(root_page_id_==INVALID_PAGE_ID)return false;
  BPlusTreeLeafPage *leaf = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key, root_page_id_, false));
  if (leaf == nullptr) {
    return false;
  }
  RowId value;
  auto res = leaf->Lookup(key, value, processor_);
  if (res) {
    result.push_back(value);
  }
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  page_id_t page_id;
  BPlusTreeLeafPage *leaf = reinterpret_cast<BPlusTreeLeafPage *>(buffer_pool_manager_->NewPage(page_id)->GetData());
  if (leaf == nullptr) {
    DLOG(ERROR) << "out of memory";
    throw "out of memory";
  }
  leaf->Init(page_id, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
  leaf->Insert(key, value, processor_);
  root_page_id_ = page_id;
  UpdateRootPageId();
  buffer_pool_manager_->UnpinPage(page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
  auto leaf = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key, root_page_id_, false));
  RowId fakeValue;
  if (leaf->Lookup(key, fakeValue, processor_)) {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return false;
  }
  leaf->Insert(key, value, processor_);
  if (leaf->GetSize() < leaf->GetMaxSize()) {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    return true;
  }
  auto new_leaf = Split(leaf, transaction);
  InsertIntoParent(leaf, new_leaf->KeyAt(0), new_leaf, transaction);
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
  page_id_t page_id;
  BPlusTreeInternalPage *new_internal =
      reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->NewPage(page_id)->GetData());
  if (new_internal == nullptr) {
    DLOG(ERROR) << "out of memory";
    throw "out of memory";
  }
  new_internal->Init(page_id, node->GetParentPageId(), processor_.GetKeySize(), internal_max_size_);
  node->MoveHalfTo(new_internal, buffer_pool_manager_);
  return new_internal;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
  page_id_t page_id;
  BPlusTreeLeafPage *new_leaf =
      reinterpret_cast<BPlusTreeLeafPage *>(buffer_pool_manager_->NewPage(page_id)->GetData());
  if (new_leaf == nullptr) {
    DLOG(ERROR) << "out of memory";
    throw "out of memory";
  }
  new_leaf->Init(page_id, node->GetParentPageId(), processor_.GetKeySize(), leaf_max_size_);
  node->MoveHalfTo(new_leaf);
  new_leaf->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(new_leaf->GetPageId());
  return new_leaf;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  if (old_node->IsRootPage()) {
    BPlusTreeInternalPage *new_root =
        reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->NewPage(root_page_id_)->GetData());
    if (new_root == nullptr) {
      DLOG(ERROR) << "out of memory";
      throw "out of memory";
    }
    new_root->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    UpdateRootPageId();
    reinterpret_cast<BPlusTreePage *>(old_node)->SetParentPageId(root_page_id_);
    reinterpret_cast<BPlusTreePage *>(new_node)->SetParentPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return;
  }
  auto parent =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(old_node->GetParentPageId())->GetData());
  parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  parent->SetKeyAt(parent->ValueIndex(old_node->GetPageId()),
                   reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(nullptr, old_node->GetPageId(), true))->KeyAt(0));
  if (parent->GetSize() < parent->GetMaxSize()) {
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return;
  }
  auto new_parent = Split(parent, transaction);
  InsertIntoParent(
      parent, reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(nullptr, new_parent->GetPageId(), true))->KeyAt(0),
      new_parent, transaction);
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_parent->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if (IsEmpty()) {
    return;
  }
  auto leaf = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key, root_page_id_, false));
  RowId fakeValue;
  if (!leaf->Lookup(key, fakeValue, processor_)) {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return;
  }
  leaf->RemoveAndDeleteRecord(key, processor_);
  page_id_t children_id = leaf->GetPageId();
  page_id_t parent_id = leaf->GetParentPageId();
  // may need update parent key
  while (parent_id != INVALID_PAGE_ID) {
    // DLOG(INFO) << "adjusting parent " << parent_id << " " << children_id;
    // fetch parent
    auto parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_id)->GetData());
    // update parent key
    parent->SetKeyAt(parent->ValueIndex(children_id),
                     reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(nullptr, children_id, true))->KeyAt(0));
    if (parent->IsRootPage()) break;
    children_id = parent_id;
    parent_id = parent->GetParentPageId();
    buffer_pool_manager_->UnpinPage(parent_id, true);
  }
  if (leaf->GetSize() >= leaf->GetMinSize()) {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    return;
  }
  CoalesceOrRedistribute(leaf, transaction);
  // leaf will be adjusted in CoalesceOrRedistribute
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  auto parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  int index = parent->ValueIndex(node->GetPageId());
  // index = 0: node | neighbor
  // index = 1: neighbor | node
  N *neighbor_node;
  if (index == 0) {
    neighbor_node = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent->ValueAt(1))->GetData());
  } else {
    neighbor_node = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent->ValueAt(index - 1))->GetData());
  }
  if (neighbor_node->GetSize() + node->GetSize() >= node->GetMaxSize()) {
    Redistribute(neighbor_node, node, index);
    if (index) {  // need update node's key
      parent->SetKeyAt(index,
                       reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(nullptr, node->GetPageId(), true))->KeyAt(0));
    } else {  // need update neighbor's key
      parent->SetKeyAt(
          index + 1,
          reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(nullptr, neighbor_node->GetPageId(), true))->KeyAt(0));
    }
    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return false;
  } else {
    auto parent_need_adjust = Coalesce(neighbor_node, node, parent, index, transaction);
    if (parent_need_adjust) {
      if (parent->IsRootPage()) {
        auto delete_root = AdjustRoot(parent);
        if (delete_root) {
          Destroy(parent->GetPageId());
        }
      } else {
        CoalesceOrRedistribute(parent, transaction);
      }
      return true;  // deletion happened to node or neighbor
    }
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  if (index)  // nei | node
  {
    node->MoveAllTo(neighbor_node);
    Destroy(node->GetPageId());
    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), false);
    parent->Remove(index);
  } else {  // node | nei
    neighbor_node->MoveAllTo(node);
    Destroy(neighbor_node->GetPageId());
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    parent->Remove(index + 1);
  }
  return parent->GetSize() < parent->GetMinSize();
}

// notice: need to adjust parent
bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  if (index) {  // nei | node
    node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);
    Destroy(node->GetPageId());
    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), false);
    parent->Remove(index);
  } else {  // node | nei
    neighbor_node->MoveAllTo(node, parent->KeyAt(index + 1), buffer_pool_manager_);
    Destroy(neighbor_node->GetPageId());
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    parent->Remove(index + 1);
  }
  return parent->GetSize() < parent->GetMinSize();
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  if (index == 0) {
    neighbor_node->MoveFirstToEndOf(node);
  } else {
    neighbor_node->MoveLastToFrontOf(node);
  }
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  if (index == 0) {  // node | nei
    auto parent =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(neighbor_node->GetParentPageId())->GetData());
    auto middle_key = parent->KeyAt(parent->ValueIndex(neighbor_node->GetPageId()));
    neighbor_node->MoveFirstToEndOf(node, middle_key, buffer_pool_manager_);
  } else {  // nei | node
    auto parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
    auto middle_key = parent->KeyAt(parent->ValueIndex(node->GetPageId()));
    neighbor_node->MoveLastToFrontOf(node, middle_key, buffer_pool_manager_);
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->IsLeafPage()) {
    // case 2
    if (old_root_node->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId();
      return true;
    }
  } else {
    // case 1: the root should be deleted
    auto old_root_page_id = old_root_node->GetPageId();
    auto new_root_page_id = reinterpret_cast<InternalPage *>(old_root_node)->RemoveAndReturnOnlyChild();
    root_page_id_ = new_root_page_id;
    auto new_root_page =
        reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(new_root_page_id)->GetData());
    new_root_page->SetParentPageId(INVALID_PAGE_ID);
    UpdateRootPageId();
    return true;
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  return IndexIterator(FindLeafPage(nullptr, root_page_id_, true)->GetPageId(), buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) { 
  auto leaf = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key, root_page_id_, false));
  int index = leaf->KeyIndex(key, processor_);
  if(index == leaf->GetSize()) {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return IndexIterator();
  }
  return IndexIterator(leaf->GetPageId(), buffer_pool_manager_, index); 
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() { return IndexIterator(); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
  if (page->IsLeafPage()) {
    return reinterpret_cast<Page *>(page);
  }
  auto internal_page = reinterpret_cast<InternalPage *>(page);
  auto next_page_id = leftMost ? internal_page->ValueAt(0) : internal_page->Lookup(key, processor_);
  buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);
  return FindLeafPage(key, next_page_id, leftMost);
}

/*
 * Update/Insert root page id in header page(where page_id = INDEX_ROOTS_PAGE_ID,
 * header_page isdefined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  auto header_page =
      reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  bool res;
  if (insert_record) {
    res = header_page->Insert(index_id_, root_page_id_);
  } else {
    res = header_page->Update(index_id_, root_page_id_);
  }
  if (!res) {
    DLOG(INFO) << "Fatal error";
    throw "Fatal error";
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">" << "max_size=" << leaf->GetMaxSize()
        << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">" << "max_size=" << inner->GetMaxSize()
        << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}