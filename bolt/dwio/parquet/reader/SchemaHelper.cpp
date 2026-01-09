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

#include "bolt/dwio/parquet/reader/SchemaHelper.h"
#include <stack>
#include <stdexcept>
namespace bytedance::bolt::parquet {

SchemaHelper::SchemaHelper(const std::vector<SchemaElement>& schema)
    : schema_(schema) {
  buildMappings();
}

const SchemaElement& SchemaHelper::getElementByLeafIndex(
    size_t leafIndex) const {
  auto it = leafToSchemaIndex_.find(leafIndex);
  if (it == leafToSchemaIndex_.end()) {
    throw std::runtime_error("Leaf index out of bounds");
  }
  return schema_[it->second];
}

size_t SchemaHelper::mapLeafToSchemaIndex(size_t leafIndex) const {
  auto it = leafToSchemaIndex_.find(leafIndex);
  if (it == leafToSchemaIndex_.end()) {
    throw std::runtime_error("Leaf index out of bounds");
  }
  return it->second;
}

size_t SchemaHelper::getLeafCount() const {
  return leafToSchemaIndex_.size();
}

std::vector<std::string> SchemaHelper::getPath(size_t leafIndex) const {
  auto it = leafToPaths_.find(leafIndex);
  if (it == leafToPaths_.end()) {
    throw std::runtime_error("Leaf index out of bounds");
  }
  return it->second;
}

void SchemaHelper::buildMappings() {
  // 'idx' is like the global currentIdx; it marches through schema_.
  size_t idx = 1;
  // currentPath holds the names along the current branch.
  std::vector<std::string> currentPath;

  // Each frame represents the “call frame” corresponding to a node that has
  // children. 'expected' is the total number of children for that node,
  // 'processed' counts how many children have already been processed.
  struct Frame {
    int32_t expected;
    int32_t processed;
  };
  std::stack<Frame> frames;

  // The algorithm runs until both the flat schema has been consumed and
  // all active frames (i.e. pending sibling groups) are done.
  while (true) {
    // If there is still a node in the flat schema, process it.
    if (idx < schema_.size()) {
      const auto& node = schema_[idx];
      // Add the current node’s name to the path.
      currentPath.push_back(node.name);
      idx++; // Move to the next node in the flat representation.

      if (node.num_children > 0) {
        // For a node with children, push a new frame.
        // Its 'expected' count is the number of children.
        // We have not processed any child yet.
        frames.push(Frame{node.num_children, 0});
        // Continue so that we “descend” immediately into processing children.
        continue;
      } else {
        // This is a leaf; record the mapping.
        leafToSchemaIndex_[leafCount_] = idx - 1; // current node’s index
        leafToPaths_[leafCount_] = currentPath;
        leafCount_++;
        // As in the recursive version, when done processing a node (a leaf
        // here) we remove its name from the current path.
        currentPath.pop_back();
        // Also update the parent's processed count (if there is an active
        // frame).
        if (!frames.empty()) {
          frames.top().processed++;
        }
      }
    } else {
      // If we ran out of nodes to process, then we check whether there is a
      // pending parent frame waiting to be finished.
      if (frames.empty())
        break; // we are done with the entire schema
    }

    // When a node (leaf or a subtree whose children have been processed)
    // finishes, we must “unwind” the recursion: pop frames whose work is done.
    // (This corresponds to the post‐call pop in the recursive version.)
    while (!frames.empty() && frames.top().processed >= frames.top().expected) {
      // When all children in the current frame are processed, pop the frame,
      // and remove the corresponding node’s name from the current path.
      frames.pop();
      if (!currentPath.empty())
        currentPath.pop_back();
      // Then, if we’re still inside a parent frame, signal that one child has
      // finished.
      if (!frames.empty())
        frames.top().processed++;
    }
  }
}

} // namespace bytedance::bolt::parquet
