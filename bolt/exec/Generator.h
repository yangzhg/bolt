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
#include "bolt/exec/Operator.h"

#include <string_view>
namespace bytedance::bolt::exec {

class Generator : public Operator {
 public:
  Generator(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::GeneratorNode>& generatorNode,
      std::string operatorType);

  void initialize() override;

  void addInput(RowVectorPtr input) override {
    input_ = std::move(input);
    if (input_ && isLazyNotLoaded(*input_)) {
      input_->loadedVector();
    }
  }

  bool needsInput() const override {
    return true;
  }

  bool isFinished() override {
    return noMoreInput_ && input_ == nullptr;
  }

  BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return BlockingReason::kNotBlocked;
  }

  // process generator function input column
  virtual void preGen();

 protected:
  // col index for generator function direct column access
  std::vector<column_index_t> generateChannels_;

  // generator function exprs
  std::unique_ptr<ExprSet> genExprs_;

  SelectivityVector inputRows_;

  // generator function data vector
  // direct column access or result of genExprs_ evaluation
  std::vector<VectorPtr> genInput_;
  // decoded genInput_
  std::vector<DecodedVector> generateDecoded_;

  // temporary store input expression
  std::vector<core::TypedExprPtr> allExpr_;
};

struct GeneratorIterator {
  int32_t rowOffset_{0};
  int32_t rowStart_{0};
  void reset() {
    *this = {};
  }
};

class GeneratorExplode : public Generator {
 public:
  GeneratorExplode(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::GeneratorNode>& generatorNode);

  RowVectorPtr getOutput() override;

  template <bool isOuter>
  void calRepeatedCount(
      DecodedVector& decode,
      int32_t rowNum,
      const vector_size_t* currentSizes,
      const vector_size_t* currentIndices,
      int64_t* rawMaxSizes,
      int32_t& numElements) {
    auto row = iterator_.rowOffset_;
    for (; row < rowNum; ++row) {
      if (numElements >= outputBatchSize_) {
        break;
      }
      if (!decode.isNullAt(row)) {
        auto generateSize = currentSizes[currentIndices[row]];
        if constexpr (isOuter) {
          generateSize = generateSize == 0 ? 1 : generateSize;
        }
        rawMaxSizes[row] = generateSize;
        numElements += generateSize;
      } else if constexpr (isOuter) {
        rawMaxSizes[row] = 1;
        ++numElements;
      }
    }
    iterator_.rowStart_ = iterator_.rowOffset_;
    iterator_.rowOffset_ = row;
  }

  template <bool isOuter>
  void setElementIndicesAndNulls(
      DecodedVector& currentDecode,
      const vector_size_t* currentSizes,
      const vector_size_t* currentIndices,
      const vector_size_t* currentOffsets,
      vector_size_t* rawElementIndices,
      uint64_t* rawNulls,
      bool& isIdentityMapping,
      std::shared_ptr<FlatVector<int32_t>>& ordinalityVector) {
    vector_size_t index = 0;
    for (auto row = iterator_.rowStart_; row < iterator_.rowOffset_; ++row) {
      if constexpr (isOuter) {
        if (!currentDecode.isNullAt(row) && currentSizes[currentIndices[row]]) {
          auto offset = currentOffsets[currentIndices[row]];
          auto genSize = currentSizes[currentIndices[row]];
          if (isIdentityMapping && offset != index) {
            isIdentityMapping = false;
          }
          for (auto i = 0; i < genSize; ++i) {
            rawElementIndices[index++] = offset + i;
          }
        } else { // null or empty
          if (ordinalityVector) {
            ordinalityVector->setNull(index, true);
          }
          bits::setNull(rawNulls, index++, true);
        }
      } else {
        if (!currentDecode.isNullAt(row)) {
          auto offset = currentOffsets[currentIndices[row]];
          auto genSize = currentSizes[currentIndices[row]];
          if (isIdentityMapping && offset != index) {
            isIdentityMapping = false;
          }
          for (auto i = 0; i < genSize; ++i) {
            rawElementIndices[index++] = offset + i;
          }
        }
      }
    }
  }

  bool needsInput() const override {
    return (input_ == nullptr);
  }

 private:
  const bool withOrdinality_;
  const bool isOuter_;
  GeneratorIterator iterator_;
  const uint32_t outputBatchSize_;
};

class GeneratorJsonTuple : public Generator {
 public:
  GeneratorJsonTuple(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::GeneratorNode>& generatorNode);

  RowVectorPtr getOutput() override;

  void initializeResults(vector_size_t outputsIndex);
  void decodedKeyColumn();

 private:
  std::vector<std::string> jsonKeys_;
  std::vector<int32_t> constKeyMap_;
  // indicate whether json key is null or not
  std::vector<bool> jsonKeysIsNull_;
  std::vector<int32_t> keyChannels_;
  std::vector<int32_t> colKeyMap_;
  int32_t numKeys_;
  std::vector<VectorPtr> results_;
  // for faster access
  std::vector<FlatVector<StringView>*> flatResults_;
  std::vector<DecodedVector> keyDecoded_;
  const bool useSonicLibrary_ = true;
};

} // namespace bytedance::bolt::exec
