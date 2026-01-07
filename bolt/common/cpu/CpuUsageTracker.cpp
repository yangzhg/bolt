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

#include "bolt/common/cpu/CpuUsageTracker.h"
#include <memory>
namespace bytedance::bolt::cpu {
CpuUsageTracker::CpuUsageTracker() {
  memset(&lastUsage_, 0, sizeof(CpuUsage));
  memset(&currentUsage_, 0, sizeof(CpuUsage));
}

int CpuUsageTracker::getCurrent(float* ucpu_usage, float* scpu_usage) {
  int err = 0;
  if (!(err = readCpuUsage(&currentUsage_))) {
    *ucpu_usage = 0.0f;
    *scpu_usage = 0.0f;

    if (lastUsage_.init) {
      calculateCpuUsage(&currentUsage_, &lastUsage_, ucpu_usage, scpu_usage);
    }

    // Copy the results
    lastUsage_ = currentUsage_;
    lastUsage_.init = true;
  }
  return err;
}

int CpuUsageTracker::updateLastUsage() {
  int err = 0;
  if (!(err = readCpuUsage(&currentUsage_))) {
    // Copy the results
    lastUsage_ = currentUsage_;
    lastUsage_.init = true;
  }
  return err;
}

// return 0 on success, -1 on error
int CpuUsageTracker::readCpuUsage(CpuUsage* result) {
  // convert  pid to string
  char pid_s[20];
  pid_t pid = getpid();

  snprintf(pid_s, sizeof(pid_s), "%d", pid);
  char stat_filepath[30] = "/proc/";
  strncat(
      stat_filepath, pid_s, sizeof(stat_filepath) - strlen(stat_filepath) - 1);
  strncat(
      stat_filepath,
      "/stat",
      sizeof(stat_filepath) - strlen(stat_filepath) - 1);

  // open /proc/pid/stat
  FILE* fpstat = fopen(stat_filepath, "r");
  if (fpstat == NULL) {
    printf("FOPEN ERROR pid stat %s:\n", stat_filepath);
    return -1;
  }
  std::unique_ptr<FILE, decltype(&fclose)> fpstat_guard(fpstat, fclose);

  // open /proc/stat
  FILE* fstat = fopen("/proc/stat", "r");
  if (fstat == NULL) {
    printf("FOPEN ERROR");
    return -1;
  }
  std::unique_ptr<FILE, decltype(&fclose)> fstat_guard(fstat, fclose);
  memset(result, 0, sizeof(CpuUsage));

  // read values from /proc/pid/stat
  if (fscanf(
          fpstat,
          "%*d %*s %*c %*d %*d %*d %*d %*d %*u %*u %*u %*u %*u %lu %lu %ld %ld",
          &result->utime_ticks,
          &result->stime_ticks,
          &result->cutime_ticks,
          &result->cstime_ticks) == EOF) {
    return -1;
  }

  // read+calc cpu total time from /proc/stat, on linux 2.6.35-23 x86_64 the cpu
  // row has 10 values could differ on different architectures
  long unsigned int cpu_time[10] = {0};
  memset(cpu_time, 0, sizeof(cpu_time));
  if (fscanf(
          fstat,
          "%*s %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu",
          &cpu_time[0],
          &cpu_time[1],
          &cpu_time[2],
          &cpu_time[3],
          &cpu_time[4],
          &cpu_time[5],
          &cpu_time[6],
          &cpu_time[7],
          &cpu_time[8],
          &cpu_time[9]) == EOF) {
    return -1;
  }

  for (int i = 0; i < 10; i++) {
    result->cpu_total_time += cpu_time[i];
  }

  return 0;
}

void CpuUsageTracker::calculateCpuUsage(
    CpuUsage* cur_usage,
    CpuUsage* last_usage,
    float* ucpu_usage,
    float* scpu_usage) {
  long unsigned int curTotalDiff =
      cur_usage->cpu_total_time - last_usage->cpu_total_time;

  if (curTotalDiff > 0) {
    *ucpu_usage = 100 *
        ((((cur_usage->utime_ticks + cur_usage->cutime_ticks) -
           (last_usage->utime_ticks + last_usage->cutime_ticks))) /
         ((float)curTotalDiff));

    *scpu_usage = 100 *
        ((((cur_usage->stime_ticks + cur_usage->cstime_ticks) -
           (last_usage->stime_ticks + last_usage->cstime_ticks))) /
         ((float)curTotalDiff));
  } else {
    *ucpu_usage = 0.0f;
    *scpu_usage = 0.0f;
  }
}

} // namespace bytedance::bolt::cpu
