#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (lru_list_.empty()) return false;
  *frame_id = lru_list_.back();
  lru_list_.pop_back();
  lru_set_.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  lru_list_.remove(frame_id);
  lru_set_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (lru_set_.find(frame_id) != lru_set_.end()) return;
  lru_list_.push_front(frame_id);
  lru_set_.insert(frame_id);
}

size_t LRUReplacer::Size() { return lru_list_.size(); }