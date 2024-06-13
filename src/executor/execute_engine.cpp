#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

static bool supress_output = false;

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
   **/
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());

  if (!supress_output) {
    // Return the result set as string.
    std::stringstream ss;
    ResultWriter writer(ss);
    if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
      auto schema = planner.plan_->OutputSchema();
      auto num_of_columns = schema->GetColumnCount();
      if (!result_set.empty()) {
        // find the max width for each column
        vector<int> data_width(num_of_columns, 0);
        for (const auto &row : result_set) {
          for (uint32_t i = 0; i < num_of_columns; i++) {
            data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
          }
        }
        int k = 0;
        for (const auto &column : schema->GetColumns()) {
          data_width[k] = max(data_width[k], int(column->GetName().length()));
          k++;
        }
        // Generate header for the result set.
        writer.Divider(data_width);
        k = 0;
        writer.BeginRow();
        for (const auto &column : schema->GetColumns()) {
          writer.WriteHeaderCell(column->GetName(), data_width[k++]);
        }
        writer.EndRow();
        writer.Divider(data_width);

        // Transforming result set into strings.
        for (const auto &row : result_set) {
          writer.BeginRow();
          for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
            writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
          }
          writer.EndRow();
        }
        writer.Divider(data_width);
      }
      writer.EndInformation(result_set.size(), duration_time, true);
    } else {
      writer.EndInformation(result_set.size(), duration_time, false);
    }
    std::cout << writer.stream_.rdbuf();
  }
  // todo:: use shared_ptr for schema
  if (ast->type_ == kNodeSelect) delete planner.plan_->OutputSchema();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(("./databases/" + db_name).c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  if (db_name == current_db_) current_db_ = "";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database" << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if (current_db_.empty())  // no database
    return DB_FAILED;

  // get table name
  if (ast->child_->type_ != kNodeIdentifier) {
    return DB_FAILED;
  }
  string table_name(ast->child_->val_);

  // get all columns
  vector<Column *> columns;
  uint32_t index = 0;
  auto list = ast->child_->next_;
  if (list->type_ != kNodeColumnDefinitionList) {
    return DB_FAILED;
  }
  auto definition = list->child_;

  // isunique primary
  vector<string> uniques;
  vector<string> primarys;
  auto columnlist = definition;
  while (columnlist != nullptr && columnlist->type_ != kNodeColumnList) {
    columnlist = columnlist->next_;
  }
  if (columnlist != nullptr && columnlist->type_ == kNodeColumnList) {
    auto pri = columnlist->child_;
    while (pri != nullptr && pri->type_ == kNodeIdentifier) {
      primarys.push_back(pri->val_);
      pri = pri->next_;
    }
  }

  while (definition != nullptr && definition->type_ == kNodeColumnDefinition) {
    Column *column;
    bool nullable = true;
    bool isunique = false;
    string c_name = definition->child_->val_;
    string c_type = definition->child_->next_->val_;
    if (definition->val_ != nullptr && string(definition->val_) == "unique") {
      isunique = true;
      uniques.push_back(c_name);
    }
    auto it = find(primarys.begin(), primarys.end(), c_name);
    if (it != primarys.end()) {
      isunique = true;
      uniques.push_back(c_name);
    }
    if (c_type == "int")
      column = new Column(c_name, kTypeInt, index, true, isunique);
    else if (c_type == "float")
      column = new Column(c_name, kTypeFloat, index, true, isunique);
    else if (c_type == "char") {
      uint32_t length = stoi(definition->child_->next_->child_->val_);
      column = new Column(c_name, kTypeChar, length, index, true, isunique);
    }
    columns.push_back(column);
    index++;
    definition = definition->next_;
  }

  // create table
  auto catalog = context->GetCatalog();
  Schema *schema = new Schema(columns);
  TableInfo *table_info;
  auto result = catalog->CreateTable(table_name, schema, context->GetTransaction(), table_info);
  if (result != DB_SUCCESS) return result;

  // unique index
  for (auto it : uniques) {
    string index_name = "UNIQUE_";
    index_name += it + "_ON_" + table_name;
    IndexInfo *index_info;
    catalog->CreateIndex(table_name, index_name, uniques, context->GetTransaction(), index_info, "btree");
  }
  if (primarys.size() > 0) {
    string index_name = "AUTO_CREATED_INDEX_OF_";
    for (auto it : primarys) index_name += it + "_";
    index_name += "ON_" + table_name;
    IndexInfo *index_info;
    catalog->CreateIndex(table_name, index_name, primarys, context->GetTransaction(), index_info, "btree");
  }

  return result;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if (current_db_.empty())  // no database
    return DB_FAILED;
  auto catalog = context->GetCatalog();

  // get table name
  string table_name(ast->child_->val_);
  auto result = catalog->DropTable(table_name);
  if (result != DB_SUCCESS) return result;

  // get indexes and delete
  vector<IndexInfo *> indexes;
  catalog->GetTableIndexes(table_name, indexes);
  for (auto it : indexes) catalog->DropIndex(table_name, it->GetIndexName());
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (current_db_.empty()) return DB_FAILED;
  auto catalog = context->GetCatalog();

  // get tables
  vector<TableInfo *> tables;
  catalog->GetTables(tables);

  // get and print indexes
  int count = 0;
  cout << "Show Indexes" << endl;
  for (auto table : tables) {
    vector<IndexInfo *> indexes;
    catalog->GetTableIndexes(table->GetTableName(), indexes);
    cout << "\ttable: " << table->GetTableName() << endl;
    for (auto index : indexes) {
      string index_name = index->GetIndexName();
      cout << "\t\tindex: " << index_name << endl;
      count++;
    }
  }
  cout << count << " index(es) have listed." << std::endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if (current_db_.empty()) return DB_FAILED;

  string i_name = ast->child_->val_;
  string t_name = ast->child_->next_->val_;

  auto list = ast->child_->next_->next_;
  if (list->type_ != kNodeColumnList) return DB_FAILED;

  vector<string> keys;
  auto key = list->child_;
  while (key != nullptr) {
    keys.push_back(key->val_);
    key = key->next_;
  }
  IndexInfo *index_info;
  auto catalog = context->GetCatalog();
  auto result = catalog->CreateIndex(t_name, i_name, keys, context->GetTransaction(), index_info, "btree");
  return result;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if (current_db_.empty()) return DB_FAILED;
  auto catalog = context->GetCatalog();

  // get table and find index
  string index_name = ast->child_->val_;
  vector<TableInfo *> tables;
  catalog->GetTables(tables);
  auto res = DB_INDEX_NOT_FOUND;
  for (auto it : tables) {
    if (catalog->DropIndex(it->GetTableName(), index_name) == DB_SUCCESS) auto res = DB_SUCCESS;
  }
  return res;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
extern "C" {
int yyparse(void);
#include <parser/minisql_lex.h>
#include <parser/parser.h>
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  string file_name = ast->child_->val_;
  FILE *file = fopen(file_name.c_str(), "r");
  if (file == nullptr)  // can't open
    return DB_FAILED;

  // command buffer
  const int buf_size = 1024;
  char cmd[buf_size];

  std::cout << "Execfile started, output supressed." << std::endl;
  // record start time
  auto start_time = std::chrono::system_clock::now();
  supress_output = true;
  while (1) {
    if (feof(file)) {
      break;
    }
    memset(cmd, 0, 1024);
    int i = 0;
    char ch;
    while (!feof(file) && (ch = getc(file)) != ';') {
      cmd[i++] = ch;
    }
    if (feof(file)) continue;
    cmd[i] = ch;
    // create buffer for sql input
    YY_BUFFER_STATE bp = yy_scan_string(cmd);
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);

    // init parser module
    MinisqlParserInit();

    // parse
    yyparse();

    // parse result handle
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
    }

    auto result = Execute(MinisqlGetParserRootNode());

    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();

    ExecuteInformation(result);
  }
  supress_output = false;
  // record stop time
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  std::cout << "Execfile finished in " << duration_time << " ms" << std::endl;
  fclose(file);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  current_db_ = "";
  return DB_QUIT;
}
