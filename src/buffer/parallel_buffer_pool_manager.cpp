//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include "buffer/buffer_pool_manager_instance.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances
  for (size_t instance_index = 0; instance_index < num_instances; ++instance_index) {
    BufferPoolManagerInstance *buffer_pool_manager_instance =
        new BufferPoolManagerInstance(pool_size, num_instances, instance_index, disk_manager, log_manager);
    instances_.emplace(instance_index, buffer_pool_manager_instance);
  }
  starting_index_ = 0;
  instance_pool_size_ = pool_size;
  num_instances_ = num_instances;
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (size_t instance_index = 0; instance_index < num_instances_; ++instance_index) {
    delete instances_[instance_index];
  }
  instances_.clear();
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return num_instances_ * instance_pool_size_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  size_t num_instances = instances_.size();
  return instances_[page_id % num_instances];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool_manager = GetBufferPoolManager(page_id);
  return buffer_pool_manager->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool_manager = GetBufferPoolManager(page_id);
  return buffer_pool_manager->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool_manager = GetBufferPoolManager(page_id);
  return buffer_pool_manager->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  size_t num_instances = instances_.size();
  size_t loop_counter = 0;
  while (true) {
    Page *page;
    loop_counter++;
    if (loop_counter > num_instances) {
      break;
    }
    BufferPoolManager *buffer_pool_manager = instances_[starting_index_ % num_instances];
    starting_index_++;
    page = buffer_pool_manager->NewPage(page_id);
    if (page != nullptr) {
      return page;
    }
    delete page;
  }
  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool_manager = GetBufferPoolManager(page_id);
  return buffer_pool_manager->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  size_t num_instances = instances_.size();
  for (size_t instance_index = 0; instance_index < num_instances; ++instance_index) {
    BufferPoolManager *buffer_pool_manager = instances_[instance_index];
    buffer_pool_manager->FlushAllPages();
  }
}

}  // namespace bustub
