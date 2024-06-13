#include "catalog/catalog.h"

#include "common/instance.h"
#include "gtest/gtest.h"
#include "utils/utils.h"

static string db_file_name = "catalog_test.db";

TEST(CatalogTest, CatalogAllTest) {
  //test for init catalog
  auto db_01 = new DBStorageEngine(db_file_name, true);
  auto &catalog_01 = db_01->catalog_mgr_;

  //create table test
  TableInfo *table_info = nullptr;
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  Txn txn;
  catalog_01->CreateTable("table-1", schema.get(), &txn, table_info);
  ASSERT_TRUE(table_info != nullptr);
  ASSERT_EQ(DB_TABLE_ALREADY_EXIST,catalog_01->CreateTable("table-1", schema.get(), &txn, table_info));

  //get table test
  TableInfo *table_info_02 = nullptr;
  ASSERT_EQ(DB_SUCCESS, catalog_01->GetTable("table-1", table_info_02));
  ASSERT_EQ(table_info, table_info_02);
  TableInfo *table_info_03 = nullptr;
  ASSERT_EQ(DB_TABLE_NOT_EXIST,catalog_01->GetTable("table-2", table_info_03));
  TableInfo *table_info_04 = nullptr;
  std::vector<Column *> columns1 = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 50, 1, true, false)};
  schema = std::make_shared<Schema>(columns1);
  catalog_01->CreateTable("table-2", schema.get(), &txn, table_info_04);
  vector<TableInfo*> tableinfo_vector;
  catalog_01->GetTables(tableinfo_vector);
  vector<TableInfo*> tableinfo_temp;
  tableinfo_temp.push_back(table_info_04);
  tableinfo_temp.push_back(table_info);
  ASSERT_EQ(tableinfo_vector,tableinfo_temp);

  //create index test
  IndexInfo *index_info = nullptr;
  std::vector<std::string> bad_index_keys{"id", "age", "name"};
  std::vector<std::string> index_keys{"id", "name"};
  std::vector<std::string> index_keys_pri{"id"};
  auto r1 = catalog_01->CreateIndex("table-0", "index-0", index_keys, &txn, index_info, "bptree");
  ASSERT_EQ(DB_TABLE_NOT_EXIST, r1);
  auto r2 = catalog_01->CreateIndex("table-1", "index-1", bad_index_keys, &txn, index_info, "bptree");
  ASSERT_EQ(DB_COLUMN_NAME_NOT_EXIST, r2);
  auto r3 = catalog_01->CreateIndex("table-1", "index-1", index_keys, &txn, index_info, "bptree");
  ASSERT_EQ(DB_SUCCESS, r3);
  auto r4 = catalog_01->CreateIndex("table-1", "index-1", index_keys, &txn, index_info, "bptree");
  ASSERT_EQ(DB_INDEX_ALREADY_EXIST, r4);
  
  //get index test
  IndexInfo *index_info_02;
  catalog_01->GetIndex("table-1","index-1",index_info_02);
  ASSERT_EQ(index_info,index_info_02);
  IndexInfo *index_info_03;
  auto r5 = catalog_01->CreateIndex("table-1", "primary", index_keys_pri, &txn, index_info_03, "bptree");
  ASSERT_EQ(DB_SUCCESS, r5);
  vector<IndexInfo*> indexinfo_vector;
  vector<IndexInfo*> indexinfo_temp;
  indexinfo_temp.push_back(index_info_03);
  indexinfo_temp.push_back(index_info);
  catalog_01->GetTableIndexes("table-1",indexinfo_vector);
  ASSERT_EQ(indexinfo_vector,indexinfo_temp);

  delete db_01;

  //test loading
  auto db_02 = new DBStorageEngine(db_file_name, false);
  auto &catalog_02 = db_02->catalog_mgr_;
  TableInfo *table_info_13 = nullptr;
  ASSERT_EQ(DB_TABLE_NOT_EXIST, catalog_02->GetTable("table-3", table_info_13));
  ASSERT_EQ(DB_SUCCESS, catalog_02->GetTable("table-1", table_info_13));
  IndexInfo *index_info_13;
  ASSERT_EQ(DB_SUCCESS, catalog_02->GetIndex("table-1", "primary",index_info_13));

  //drop index test
  ASSERT_EQ(DB_INDEX_NOT_FOUND, catalog_02->DropIndex("table-1", "index-2"));
  ASSERT_EQ(DB_TABLE_NOT_EXIST, catalog_02->DropIndex("table-3", "index-1"));
  ASSERT_EQ(DB_SUCCESS, catalog_02->DropIndex("table-1", "primary"));

  //drop table test
  ASSERT_EQ(DB_TABLE_NOT_EXIST, catalog_02->DropTable("table-3"));
  ASSERT_EQ(DB_SUCCESS, catalog_02->DropTable("table-1"));
  TableInfo *table_info_23 = nullptr;
  ASSERT_EQ(DB_TABLE_NOT_EXIST, catalog_02->GetTable("table-1",table_info_23));
  delete db_02;

}