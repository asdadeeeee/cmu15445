//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// tuple_test.cpp
//
// Identification: test/table/tuple_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "execution/executors/topn_per_group_executor.h"
#include "gtest/gtest.h"
#include "logging/common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {
// NOLINTNEXTLINE
TEST(TupleTest, DISABLED_TableHeapTest) {
  // test1: parse create sql statement
  std::string create_stmt = "a varchar(20), b smallint, c bigint, d bool, e varchar(16)";
  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  Column col3{"c", TypeId::BIGINT};
  Column col4{"d", TypeId::BOOLEAN};
  Column col5{"e", TypeId::VARCHAR, 16};
  std::vector<Column> cols{col1, col2, col3, col4, col5};
  Schema schema{cols};
  Tuple tuple = ConstructTuple(&schema);

  // create transaction
  auto *disk_manager = new DiskManager("test.db");
  auto *buffer_pool_manager = new BufferPoolManager(50, disk_manager);
  auto *table = new TableHeap(buffer_pool_manager);

  std::vector<RID> rid_v;
  for (int i = 0; i < 5000; ++i) {
    auto rid = table->InsertTuple(TupleMeta{0, false}, tuple);
    rid_v.push_back(*rid);
  }

  TableIterator itr = table->MakeIterator();
  while (!itr.IsEnd()) {
    // std::cout << itr->ToString(schema) << std::endl;
    ++itr;
  }

  disk_manager->ShutDown();
  remove("test.db");  // remove db file
  remove("test.log");
  delete table;
  delete buffer_pool_manager;
  delete disk_manager;
}

TEST(RankTest, SampleTest) {
  std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys;
  AbstractExpressionRef ref = nullptr;
  order_bys.emplace_back(OrderByType::ASC, ref);
  RankTracker rank_tracker{order_bys};
  Value v1 = ValueFactory::GetIntegerValue(1);
  Value v2 = ValueFactory::GetIntegerValue(2);
  Value v3 = ValueFactory::GetIntegerValue(3);
  Value v4 = ValueFactory::GetIntegerValue(4);
  Value v5 = ValueFactory::GetIntegerValue(5);
  std::vector<Value> rankvalues1 = {v1};
  std::vector<Value> rankvalues2 = {v2};
  std::vector<Value> rankvalues3 = {v3};
  std::vector<Value> rankvalues4 = {v4};
  std::vector<Value> rankvalues5 = {v5};
  RankValue rankv1{rankvalues1};
  RankValue rankv2{rankvalues2};
  RankValue rankv3{rankvalues3};
  RankValue rankv4{rankvalues4};
  RankValue rankv5{rankvalues5};
  rank_tracker.Insert(rankv1);
  rank_tracker.Insert(rankv1);
  rank_tracker.Insert(rankv2);
  rank_tracker.Insert(rankv2);
  rank_tracker.Insert(rankv3);
  rank_tracker.Insert(rankv4);
  rank_tracker.Insert(rankv4);
  rank_tracker.Insert(rankv4);
  EXPECT_EQ(1, rank_tracker.GetRank(rankv1));
  EXPECT_EQ(3, rank_tracker.GetRank(rankv2));
  EXPECT_EQ(5, rank_tracker.GetRank(rankv3));
  EXPECT_EQ(6, rank_tracker.GetRank(rankv4));
  rank_tracker.Remove(rankv2);
  EXPECT_EQ(1, rank_tracker.GetRank(rankv1));
  EXPECT_EQ(3, rank_tracker.GetRank(rankv2));
  EXPECT_EQ(4, rank_tracker.GetRank(rankv3));
  EXPECT_EQ(5, rank_tracker.GetRank(rankv4));
  rank_tracker.Remove(rankv2);
  EXPECT_EQ(1, rank_tracker.GetRank(rankv1));
  EXPECT_EQ(-1, rank_tracker.GetRank(rankv2));
  EXPECT_EQ(3, rank_tracker.GetRank(rankv3));
  EXPECT_EQ(4, rank_tracker.GetRank(rankv4));
  rank_tracker.Insert(rankv1);
  rank_tracker.Insert(rankv1);
  rank_tracker.Insert(rankv5);
  rank_tracker.Insert(rankv5);
  rank_tracker.Insert(rankv2);
  rank_tracker.Insert(rankv2);
  rank_tracker.Insert(rankv3);
  rank_tracker.Insert(rankv4);
  rank_tracker.Insert(rankv4);
  EXPECT_EQ(1, rank_tracker.GetRank(rankv1));
  EXPECT_EQ(5, rank_tracker.GetRank(rankv2));
  EXPECT_EQ(7, rank_tracker.GetRank(rankv3));
  EXPECT_EQ(9, rank_tracker.GetRank(rankv4));
  EXPECT_EQ(14, rank_tracker.GetRank(rankv5));
  EXPECT_EQ(14, rank_tracker.GetMostRank());
  rank_tracker.Remove(rankv5);
  EXPECT_EQ(14, rank_tracker.GetMostRank());
  rank_tracker.Remove(rankv5);
  EXPECT_EQ(9, rank_tracker.GetMostRank());
}
}  // namespace bustub
