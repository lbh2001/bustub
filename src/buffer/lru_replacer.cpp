//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages_(num_pages) {}

LRUReplacer::~LRUReplacer() { frame_list_.clear(); }

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard lock(mutex_);
  if (Size() == 0) {
    return false;
  }
  *frame_id = frame_list_.back();
  frame_hashmap_.erase(*frame_id);
  frame_list_.pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard lock(mutex_);
  if (frame_hashmap_.count(frame_id) == 0) {
    return;
  }
  auto it = frame_hashmap_[frame_id];
  frame_hashmap_.erase(frame_id);
  frame_list_.erase(it);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard lock(mutex_);
  if (frame_hashmap_.count(frame_id) != 0) {
    return;
  }
  if (Size() < num_pages_) {
    frame_list_.push_front(frame_id);
    frame_hashmap_.emplace(frame_id, frame_list_.begin());
  }
}

size_t LRUReplacer::Size() { return frame_list_.size(); }

}  // namespace bustub
