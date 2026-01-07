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

#pragma once

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <cstring>
namespace bytedance::bolt::cpu {
typedef struct {
  int init;
  long unsigned int utime_ticks;
  long int cutime_ticks;
  long unsigned int stime_ticks;
  long int cstime_ticks;
  long unsigned int cpu_total_time;
} CpuUsage;

class CpuUsageTracker {
 public:
  int getCurrent(float* ucpu_usage, float* scpu_usage);

  // update lastUsage_ only
  int updateLastUsage();

  CpuUsageTracker();

 private:
  int readCpuUsage(CpuUsage* result);
  void calculateCpuUsage(
      CpuUsage* cur_usage,
      CpuUsage* last_usage,
      float* ucpu_usage,
      float* scpu_usage);
  CpuUsage lastUsage_;
  CpuUsage currentUsage_;
};
} // namespace bytedance::bolt::cpu
