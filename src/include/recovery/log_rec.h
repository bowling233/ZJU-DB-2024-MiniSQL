#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"

enum class LogRecType {
  kInvalid,
  kInsert,
  kDelete,
  kUpdate,
  kBegin,
  kCommit,
  kAbort,
};

// used for testing only
using KeyType = std::string;
using ValType = int32_t;

/**
 * TODO: Student Implement
 */
struct LogRec {
  LogRec() = default;

  LogRecType type_{LogRecType::kInvalid};
  lsn_t lsn_{INVALID_LSN};
  txn_id_t txn_id_{INVALID_TXN_ID};
  lsn_t prev_lsn_{INVALID_LSN};

  LogRec(LogRecType type, lsn_t lsn, txn_id_t txn_id, lsn_t prev_lsn)
      : type_(type), lsn_(lsn), txn_id_(txn_id), prev_lsn_(prev_lsn) {}

  KeyType key[4];  // 0 for old, 1 for insert, 2 for delete, 3 for new
  ValType val[4];

  /* used for testing only */
  static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
  static lsn_t next_lsn_;

  static lsn_t GetPrevLSN(txn_id_t txn_id, lsn_t lsn) {
    lsn_t prev_lsn = INVALID_LSN;
    auto it = prev_lsn_map_.find(txn_id);
    if (it != prev_lsn_map_.end()) {
      prev_lsn = it->second;
      it->second = lsn;
    } else {
      prev_lsn_map_[txn_id] = lsn;
    }
    return prev_lsn;
  }
};

typedef std::shared_ptr<LogRec> LogRecPtr;

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
  LogRecPtr log = std::make_shared<LogRec>(LogRecType::kInsert, LogRec::next_lsn_ + 1, txn_id,
                                           LogRec::GetPrevLSN(txn_id, LogRec::next_lsn_ + 1));
  LogRec::next_lsn_++;
  log->key[1] = ins_key;
  log->val[1] = ins_val;
  return log;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
  LogRecPtr log = std::make_shared<LogRec>(LogRecType::kDelete, LogRec::next_lsn_ + 1, txn_id,
                                           LogRec::GetPrevLSN(txn_id, LogRec::next_lsn_ + 1));
  LogRec::next_lsn_++;
  log->key[2] = del_key;
  log->val[2] = del_val;
  return log;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
  LogRecPtr log = std::make_shared<LogRec>(LogRecType::kUpdate, LogRec::next_lsn_ + 1, txn_id,
                                           LogRec::GetPrevLSN(txn_id, LogRec::next_lsn_ + 1));
  LogRec::next_lsn_++;
  log->key[0] = old_key;
  log->val[0] = old_val;
  log->key[3] = new_key;
  log->val[3] = new_val;
  return log;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
  LogRecPtr log = std::make_shared<LogRec>(LogRecType::kBegin, LogRec::next_lsn_ + 1, txn_id,
                                           LogRec::GetPrevLSN(txn_id, LogRec::next_lsn_ + 1));
  LogRec::next_lsn_++;
  return log;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
  LogRecPtr log = std::make_shared<LogRec>(LogRecType::kCommit, LogRec::next_lsn_ + 1, txn_id,
                                           LogRec::GetPrevLSN(txn_id, LogRec::next_lsn_ + 1));
  LogRec::next_lsn_++;
  return log;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
  LogRecPtr log = std::make_shared<LogRec>(LogRecType::kAbort, LogRec::next_lsn_ + 1, txn_id,
                                           LogRec::GetPrevLSN(txn_id, LogRec::next_lsn_ + 1));
  LogRec::next_lsn_++;
  return log;
}

#endif  // MINISQL_LOG_REC_H
