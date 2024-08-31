#include "primer/trie.h"
#include <memory>
#include <string_view>
#include <utility>
#include "common/exception.h"
#include "execution/executors/values_executor.h"

namespace bustub {
auto SearchNode(const std::shared_ptr<const TrieNode> &node, std::string_view key) -> std::shared_ptr<const TrieNode> {
  // 如果键空，则该node为所寻node
  if (key.empty()) {
    return node;
  }
  // 如果键非空，则还需继续查找；
  auto pair = node->children_.find(key.at(0));
  if (pair != node->children_.end()) {
    return SearchNode(pair->second, key.substr(1));
  }
  return nullptr;
}

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  if (root_ == nullptr) {
    return nullptr;
  }
  auto node = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(SearchNode(root_, key));
  if (node) {
    return node->value_.get();
  }
  return nullptr;
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto PutNode(const std::shared_ptr<const TrieNode> &old_node, std::string_view key, T value)
    -> std::shared_ptr<const TrieNode> {
  std::shared_ptr<TrieNode> new_node = nullptr;

  // 如果键空，则该node为所寻node
  if (key.empty()) {
    std::shared_ptr<T> value_ptr = std::make_shared<T>(std::move(value));
    if (old_node->children_.empty()) {
      new_node = std::make_shared<TrieNodeWithValue<T>>(value_ptr);
    } else {
      new_node = std::make_shared<TrieNodeWithValue<T>>(old_node->children_, value_ptr);
    }
  } else {
    // 如果键非空，则还需继续查找；查找前首先克隆源节点，在新节点上查找
    new_node = std::shared_ptr<TrieNode>(old_node->Clone());
    auto ptr = std::make_shared<TrieNode>();
    // 直接尝试添加需要查找的键0位char
    new_node->children_.emplace(key.at(0), std::move(ptr));
    auto pair = new_node->children_.find(key.at(0));
    if (pair != new_node->children_.end()) {
      pair->second = PutNode(pair->second, key.substr(1), std::move(value));
    } else {
      printf("should not be here \n ");
    }
  }
  return new_node;
}
template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  if (root_ == nullptr) {
    auto root = std::make_shared<TrieNode>();
    return Trie(PutNode(root, key, std::move(value)));
  }

  return Trie(PutNode(root_, key, std::move(value)));

  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto RemoveNode(const std::shared_ptr<const TrieNode> &old_node, std::string_view key)
    -> std::shared_ptr<const TrieNode> {
  std::shared_ptr<TrieNode> new_node = nullptr;
  // 如果键空，则该node为所寻node
  if (key.empty()) {
    if (old_node->children_.empty()) {
      new_node = nullptr;
    } else {
      new_node = std::make_shared<TrieNode>(old_node->children_);
    }
  } else {
    // 如果键非空，则还需继续查找；查找前首先克隆源节点，在新节点上查找
    new_node = std::shared_ptr<TrieNode>(old_node->Clone());
    auto pair = new_node->children_.find(key.at(0));
    if (pair != new_node->children_.end()) {
      auto ptr = RemoveNode(pair->second, key.substr(1));
      if (ptr == nullptr) {
        // 找到叶子节点后删除
        new_node->children_.erase(pair->first);
        // 若删除后其成为叶子节点且不含值则也删除
        if (new_node->children_.empty() && !new_node->is_value_node_) {
          new_node = nullptr;
        }
      } else {
        // 非叶子节点则替换
        pair->second = ptr;
      }
    }
  }
  return new_node;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  if (root_ == nullptr) {
    auto root = std::make_shared<TrieNode>();
    return Trie(RemoveNode(root, key));
  }
  return Trie(RemoveNode(root_, key));
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
