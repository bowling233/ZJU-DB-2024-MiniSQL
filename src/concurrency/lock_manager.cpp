#include "concurrency/lock_manager.h"

#include <algorithm>
#include <iostream>

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "concurrency/txn_manager.h"

#include "glog/logging.h"

void LockManager::SetTxnMgr(TxnManager *txn_mgr) { txn_mgr_ = txn_mgr; }

// 多线程编程注意：每个函数都需要锁

/**
 * @brief 事务获取共享锁
 *
 * 1. RU 状态下，抛出异常
 * 2. 事务状态不是 kGrowing，抛出异常
 * 3. 获取 rid 对应的请求队列，申请
 * 4. 等待直到被授予
 *
 * @param txn
 * @param rid
 * @return true
 * @return false
 */
bool LockManager::LockShared(Txn *txn, const RowId &rid) {
  if (txn->GetIsolationLevel() == IsolationLevel::kReadUncommitted) {
    // 事务状态变更
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockSharedOnReadUncommitted);
  }
  if (txn->GetState() != TxnState::kGrowing) {
    // 事务状态变更
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockOnShrinking);
  }
  auto &lock_request_queue = lock_table_[rid];
  std::unique_lock<std::mutex> lock(latch_);
  // 请求锁
  lock_request_queue.EmplaceLockRequest(txn->GetTxnId(), LockMode::kShared);
  // 阻塞情况：
  // 1. 有排他锁
  // 2. 有排他锁在队列中
  lock_request_queue.cv_.wait(lock, [&] {
    if (lock_request_queue.is_writing_ || lock_request_queue.is_upgrading_) {
      return false;
    }
    return true;
  });
  // 获得锁
  lock_request_queue.GetLockRequestIter(txn->GetTxnId())->granted_ = LockMode::kShared;
  lock_request_queue.sharing_cnt_++;
  lock_request_queue.cv_.notify_all();
  // 事务状态变更
  txn->GetSharedLockSet().insert(rid);
  return true;
}

/**
 * @brief 事务获取排他锁
 *
 * 1. 事务状态不是 kGrowing，抛出异常
 * 2. 获取 rid 对应的请求队列，等待直到可以获取排他锁，或者被死锁检测到
 *
 * @param txn
 * @param rid
 * @return true
 * @return false
 */
bool LockManager::LockExclusive(Txn *txn, const RowId &rid) {
  if (txn->GetState() != TxnState::kGrowing) {
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockOnShrinking);
  }
  auto &lock_request_queue = lock_table_[rid];
  std::unique_lock<std::mutex> lock(latch_);
  // 请求锁
  lock_request_queue.EmplaceLockRequest(txn->GetTxnId(), LockMode::kExclusive);
  // 阻塞情况：
  // 1. 有排他锁
  // 2. 有排他锁在队列中
  // 3. 有共享锁
  lock_request_queue.cv_.wait(lock, [&] {
    if (lock_request_queue.is_writing_ || lock_request_queue.is_upgrading_ || lock_request_queue.sharing_cnt_ > 0) {
      return false;
    }
    return true;
  });
  // 获得锁
  lock_request_queue.GetLockRequestIter(txn->GetTxnId())->granted_ = LockMode::kExclusive;
  lock_request_queue.is_writing_ = true;
  lock_request_queue.cv_.notify_all();
  // 事务状态变更
  txn->GetExclusiveLockSet().insert(rid);
  // 通知等待
  lock_request_queue.cv_.notify_all();
  return true;
}

/**
 * @brief 事务升级锁
 *
 * 1. 事务状态不是 kGrowing，抛出异常
 * 2. 获取 rid 对应的请求队列，等待直到可以获取排他锁，或者被死锁检测到
 *
 * @param txn
 * @param rid
 * @return true
 * @return false
 */
bool LockManager::LockUpgrade(Txn *txn, const RowId &rid) {
  // 事务状态错误
  if (txn->GetState() != TxnState::kGrowing) {
    // 事务状态变更
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockOnShrinking);
  }
  auto &lock_request_queue = lock_table_[rid];
  // 已有升级等待
  if (lock_request_queue.is_upgrading_) {
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kUpgradeConflict);
  }
  // 尝试获取原有的锁
  auto old_lock = lock_request_queue.GetLockRequestIter(txn->GetTxnId());
  if (old_lock == lock_request_queue.req_list_.end()) {
    throw "No lock to upgrade";
  }
  // 告知事务升级
  lock_request_queue.is_upgrading_ = true;
  std::unique_lock<std::mutex> lock(latch_);
  // 阻塞情况：
  // 1. 有排他锁
  // 因为有 UpgradeConflict，所以必然不会有其他锁等待
  // 3. 有共享锁
  // 到 Upgrade 时，应当保证共享锁是自己的
  lock_request_queue.cv_.wait(lock, [&] {
    //DLOG(INFO) << "notified";
    if (lock_request_queue.is_writing_ || lock_request_queue.sharing_cnt_ > 1) {
      return false;
    }
    return true;
  });
  if (lock_request_queue.req_list_iter_map_.find(txn->GetTxnId()) == lock_request_queue.req_list_iter_map_.end()) {
    // 等待过程中，事务可能被回滚
    // DLOG(INFO) << "txn not found";
    lock_request_queue.is_upgrading_ = false;
    lock_request_queue.cv_.notify_all();
    // 卧槽，这边怎么不是抛异常，抛异常没人接收，为什么啊，命名测试程序中直接try的这个函数。
    return false;
  }
  // 锁的类型变更
  old_lock->granted_ = LockMode::kExclusive;
  lock_request_queue.sharing_cnt_--;
  // 请求队列状态变更
  lock_request_queue.is_writing_ = true;
  lock_request_queue.is_upgrading_ = false;
  // 事务状态变更
  txn->GetSharedLockSet().erase(rid);
  txn->GetExclusiveLockSet().insert(rid);
  // 通知等待
  lock_request_queue.cv_.notify_all();
  return true;
}

/**
 * @brief 事务释放锁
 *
 * 1. 获取 rid 对应的请求队列
 * 2. 从请求队列中删除事务的请求
 * 3. 通知等待的事务
 *
 * @param txn
 * @param rid
 * @return true
 * @return false
 */
bool LockManager::Unlock(Txn *txn, const RowId &rid) {
  auto &lock_request_queue = lock_table_[rid];
  std::unique_lock<std::mutex> lock(latch_);
  // 获取原有的锁请求
  auto lock_request = lock_request_queue.GetLockRequestIter(txn->GetTxnId());
  // 清除所有锁请求。每个事务在队列中最多可能有两个请求：共享锁和升级锁
  if (lock_request == lock_request_queue.req_list_.end()) {
    throw "No lock to unlock";
  }
  // 共享锁
  if (lock_request->granted_ == LockMode::kShared) {
    lock_request_queue.sharing_cnt_--;
  }  // 排他锁
  else if (lock_request->granted_ == LockMode::kExclusive) {
    lock_request_queue.is_writing_ = false;
  }
  // 清理请求
  auto res = lock_request_queue.EraseLockRequest(txn->GetTxnId());
  if (!res) {
    return false;
  }
  // 通知等待
  lock_request_queue.cv_.notify_all();
  // 事务状态变更
  if (txn->GetState() == TxnState::kGrowing) txn->SetState(TxnState::kShrinking);
  txn->GetExclusiveLockSet().erase(rid);
  txn->GetSharedLockSet().erase(rid);
  return res;
}

//弃用
// void LockManager::LockPrepare(Txn *txn, const RowId &rid) {
// }
// 
// void LockManager::CheckAbort(Txn *txn, LockManager::LockRequestQueue &req_queue) {
// }

/**
 * @brief 构建等待图
 *
 * @param t1
 * @param t2
 */
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].insert(t2); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].erase(t2); }

/**
 * @brief 使用 DFS 检测环
 *
 * 始终选择首先探索最低的事务ID
 *
 * @param newest_tid_in_cycle
 * @return true
 * @return false
 */
bool LockManager::HasCycle(txn_id_t &newest_tid_in_cycle) {
  std::unordered_set<txn_id_t> explored_set_{};
  revisited_node_ = INVALID_TXN_ID;
  for (const auto &kv : waits_for_) {
    // DLOG(INFO) << "Checking " << kv.first;
    //  节点已探索过
    if (explored_set_.find(kv.first) != explored_set_.end()) {
      continue;
    }
    // 节点无邻居，探索完成
    if (kv.second.empty()) {
      explored_set_.insert(kv.first);
      continue;
    }
    // 节点未探索且有邻居，开始探索
    std::vector<std::pair<std::set<txn_id_t>::iterator, std::set<txn_id_t>::iterator>> visited_path_;
    std::set<txn_id_t> root_set_{kv.first};
    visited_path_.push_back({root_set_.begin(), root_set_.end()});
    while (!visited_path_.empty()) {
      auto &cur = visited_path_.back();
      // DLOG(INFO) << "Exploring " << *visited_path_.back().first;
      // DLOG(INFO) << "current path";
      // for (const auto &pair : visited_path_) {
      //   DLOG(INFO) << *pair.first;
      // }
      //  没有同级分枝，上层节点也探索完毕
      if (cur.first == cur.second) {
        // DLOG(INFO) << "Backtracking";
        visited_path_.pop_back();
        // 如果有上层节点
        if (!visited_path_.empty()) {
          explored_set_.insert(*visited_path_.back().first);
          visited_path_.back().first++;
        }
        continue;
      }
      // 没有邻居，当前节点探索完毕
      if (waits_for_[*cur.first].empty()) {
        // DLOG(INFO) << "Explored " << *cur.first;
        explored_set_.insert(*cur.first);
        cur.first++;
        continue;
      }
      // 遍历邻居
      for (auto it = waits_for_[*cur.first].begin(); it != waits_for_[*cur.first].end(); it++) {
        // DLOG(INFO) << "Visiting " << *it;
        //  邻居已探索过，跳过
        if (explored_set_.find(*it) != explored_set_.end()) {
          // DLOG(INFO) << "Visited " << *it;
          continue;
        }
        // 邻居出现在路径中，说明有环
        if (std::find_if(visited_path_.begin(), visited_path_.end(),
                         [it](const auto &pair) { return *pair.first == *it; }) != visited_path_.end()) {
          // DLOG(INFO) << "Cycle detected";
          newest_tid_in_cycle = *cur.first;
          revisited_node_ = *it;
          return true;
        }
        // 邻居未探索，加入路径
        // DLOG(INFO) << "Adding " << *it;
        visited_path_.push_back({it, waits_for_[*cur.first].end()});
        break;
      }
    }
  }
  return false;
}

void LockManager::DeleteNode(txn_id_t txn_id) {
  waits_for_.erase(txn_id);

  auto *txn = txn_mgr_->GetTransaction(txn_id);

  for (const auto &row_id : txn->GetSharedLockSet()) {
    for (const auto &lock_req : lock_table_[row_id].req_list_) {
      if (lock_req.granted_ == LockMode::kNone) {
        RemoveEdge(lock_req.txn_id_, txn_id);
      }
    }
  }

  for (const auto &row_id : txn->GetExclusiveLockSet()) {
    for (const auto &lock_req : lock_table_[row_id].req_list_) {
      if (lock_req.granted_ == LockMode::kNone) {
        RemoveEdge(lock_req.txn_id_, txn_id);
      }
    }
  }
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    // 从lock_table_创建等待图
    for (const auto &kv : lock_table_) {
      auto &req_list = kv.second.req_list_;
      for (auto it = req_list.begin(); it != req_list.end(); it++) {
        for (auto it2 = it; it2 != req_list.end(); it2++) {
          if (it->txn_id_ != it2->txn_id_) {
            AddEdge(it->txn_id_, it2->txn_id_);
          }
        }
      }
    }
    txn_id_t newest_tid_in_cycle;
    if (HasCycle(newest_tid_in_cycle)) {
      txn_mgr_->Abort(txn_mgr_->GetTransaction(newest_tid_in_cycle));
      DeleteNode(revisited_node_);
    }
    std::this_thread::sleep_for(cycle_detection_interval_);
  }
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> result;
  for (const auto &kv : waits_for_) {
    for (const auto &neighbor : kv.second) {
      result.emplace_back(kv.first, neighbor);
    }
  }
  return result;
}
