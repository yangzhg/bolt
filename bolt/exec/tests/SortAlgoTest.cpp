/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "bolt/exec/HybridSorter.h"

#include <algorithm>
#include <iterator>
#include <random>
#include <vector>
using namespace bytedance::bolt;
namespace bytedance::bolt::exec::test {

TEST(SortAlgo, basic) {
  constexpr size_t BATCH_SIZE = 4096;
  std::vector<int64_t> data(BATCH_SIZE, 0);
  std::random_device rd;
  std::mt19937 g(rd());

  // Asc
  {
    exec::HybridSorter sorter;
    for (auto i = 0; i < 10; ++i) {
      for (auto i = 0; i < BATCH_SIZE; ++i) {
        data[i] = i;
      }
      sorter.sort(std::begin(data), std::end(data), [](int64_t l, int64_t r) {
        return l < r;
      });
      ASSERT_TRUE(std::is_sorted(std::begin(data), std::end(data)));
    }
    ASSERT_TRUE(sorter.getSortAlgo() == SortAlgo::kTimSort);
  }

  {
    // Desc
    exec::HybridSorter sorter;
    for (auto i = 0; i < 10; ++i) {
      for (auto i = 0; i < BATCH_SIZE; ++i) {
        data[i] = BATCH_SIZE - i;
      }
      sorter.sort(std::begin(data), std::end(data), [](int64_t l, int64_t r) {
        return l < r;
      });
      ASSERT_TRUE(std::is_sorted(std::begin(data), std::end(data)));
    }
    ASSERT_TRUE(sorter.getSortAlgo() == SortAlgo::kTimSort);
  }

  {
    // Saw shape
    exec::HybridSorter sorter;
    for (auto i = 0; i < 10; ++i) {
      constexpr size_t SAW_SIZE = 64;
      bool isAsc = false;
      for (auto i = 0; i < BATCH_SIZE; ++i) {
        if (i % SAW_SIZE == 0) {
          isAsc = (!isAsc);
        }
        data[i] = isAsc ? i : (i + SAW_SIZE - 1 - 2 * (i % SAW_SIZE));
      }
      sorter.sort(std::begin(data), std::end(data), [](int64_t l, int64_t r) {
        return l < r;
      });
      ASSERT_TRUE(std::is_sorted(std::begin(data), std::end(data)));
    }
    ASSERT_TRUE(sorter.getSortAlgo() == SortAlgo::kTimSort);
  }

  {
    exec::HybridSorter sorter;
    data.clear();
    for (auto i = 0; i < BATCH_SIZE * 4; ++i) {
      data.push_back(i);
    }
    for (auto i = 0; i < 50; ++i) {
      std::shuffle(data.begin(), data.end(), g);

      sorter.sort(std::begin(data), std::end(data), [](int64_t l, int64_t r) {
        return l < r;
      });
      ASSERT_TRUE(std::is_sorted(std::begin(data), std::end(data)));
    }
    ASSERT_TRUE(sorter.getSortAlgo() != SortAlgo::kAuto);
  }

  {
    // Specify a sort algorithm
    exec::HybridSorter sorter{SortAlgo::kTimSort};
    for (auto i = 0; i < 10; ++i) {
      std::shuffle(data.begin(), data.end(), g);
      sorter.sort(std::begin(data), std::end(data), [](int64_t l, int64_t r) {
        return l < r;
      });
      ASSERT_TRUE(std::is_sorted(std::begin(data), std::end(data)));
    }
    ASSERT_TRUE(sorter.getSortAlgo() == SortAlgo::kTimSort);
  }
}

TEST(SortAlgo, probeAgain) {
  constexpr size_t BATCH_SIZE = 256;
  std::vector<int64_t> data(BATCH_SIZE, 0);
  std::random_device rd;
  std::mt19937 g(rd());

  {
    exec::HybridSorter sorter;
    for (auto i = 0; i < 1000; ++i) {
      for (auto i = 0; i < BATCH_SIZE; ++i) {
        data[i] = i;
      }
      sorter.sort(std::begin(data), std::end(data), [](int64_t l, int64_t r) {
        return l < r;
      });
      ASSERT_TRUE(std::is_sorted(std::begin(data), std::end(data)));
    }
    ASSERT_TRUE(sorter.getSortAlgo() == SortAlgo::kTimSort);

    // after many batches, we try to probe again
    for (auto i = 0; i < 200; ++i) {
      std::shuffle(data.begin(), data.end(), g);
      sorter.sort(std::begin(data), std::end(data), [](int64_t l, int64_t r) {
        return l < r;
      });
      ASSERT_TRUE(std::is_sorted(std::begin(data), std::end(data)));
    }
    ASSERT_TRUE(sorter.getSortAlgo() != SortAlgo::kAuto);
  }
}

} // namespace bytedance::bolt::exec::test
