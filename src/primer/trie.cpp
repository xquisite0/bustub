#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  // std::cout << "ran ";
  if (this->root_ == nullptr) {
    return nullptr;
  }
  std::shared_ptr<const TrieNode> cur = this->root_;

  for (const char &c : key) {
    // c exists as a child of present node
    // for (auto& pair : cur->children_) {
    //   std::cout << pair.first << " ";
    // }
    // std::cout << "\n";
    // std::cout << c << " ";
    if (cur->children_.find(c) != cur->children_.end() && cur->children_.at(c) != nullptr) {
      cur = cur->children_.at(c);
    } else {
      // std::cout << "RANhere\n";
      return nullptr;
    }
  }
  // std::cout << "\n";

  // std::cout << "Is it a value: " << cur->is_value_node_ << "\n";
  std::shared_ptr<const TrieNodeWithValue<T>> terminal_node =
      std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(cur);
  // std::shared_ptr<const TrieNodeWithValue<T>> terminal_node = std::dynamic_pointer_cast<const
  // TrieNodeWithValue<T>>(cur);

  if (terminal_node == nullptr) {
    // std::cout << "RAN\n";
    return nullptr;
  }
  std::shared_ptr<T> ans_ptr = terminal_node->value_;
  // repackage shared_ptr to a typical * pointer
  const T *ans = &(*ans_ptr);
  return ans;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  std::string string_key(key.rbegin(), key.rend());
  // return Trie(this->root_);

  std::function<std::shared_ptr<const TrieNode>(std::string_view, std::shared_ptr<const TrieNode>)> helper =
      [&](std::string_view cur_key, std::shared_ptr<const TrieNode> cur_node) -> std::shared_ptr<const TrieNode> {
    if (cur_key.empty()) {
      std::shared_ptr<T> value_pointer = std::make_shared<T>(std::move(value));

      return std::make_shared<const TrieNodeWithValue<T>>(TrieNodeWithValue(cur_node->children_, value_pointer));
    }

    char cur_char = cur_key.back();
    cur_key.remove_suffix(1);
    std::shared_ptr<const TrieNode> child;

    if (cur_node == nullptr) {
      cur_node = std::make_shared<const TrieNode>(TrieNode());
    }

    if (cur_node->children_.find(cur_char) != cur_node->children_.end()) {
      child = helper(cur_key, cur_node->children_.at(cur_char));
    } else {
      std::shared_ptr<const TrieNode> temp_child = std::make_shared<const TrieNode>(TrieNode());
      child = helper(cur_key, temp_child);
    }

    std::unique_ptr<TrieNode> cloned_cur_node = cur_node->Clone();
    cloned_cur_node->children_[cur_char] = child;
    return cloned_cur_node;
  };
  std::shared_ptr<const TrieNode> new_root = helper(string_key, this->root_);
  return Trie(new_root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // std::cout << key << "\n";
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  std::string string_key(key.rbegin(), key.rend());

  std::function<std::shared_ptr<const TrieNode>(std::string_view, std::shared_ptr<const TrieNode>)> helper =
      [&](std::string_view cur_key,
          const std::shared_ptr<const TrieNode> &cur_node) -> std::shared_ptr<const TrieNode> {
    if (cur_key.empty()) {
      bool no_children = cur_node->children_.empty();

      if (no_children) {
        return nullptr;
      }
      // std::shared_ptr<T> value_pointer = std::make_shared<T>(std::move(value));
      TrieNode new_node = TrieNode(cur_node->children_);
      std::shared_ptr<const TrieNode> new_node_ptr = std::make_shared<const TrieNode>(new_node);
      return new_node_ptr;
    }

    char cur_char = cur_key.back();
    cur_key.remove_suffix(1);

    std::shared_ptr<const TrieNode> child;

    // if (cur_node == nullptr) {
    //   cur_node = std::make_shared<const TrieNode>(TrieNode());
    // }
    child = helper(cur_key, cur_node->children_.at(cur_char));
    // if (cur_node->children_.find(cur_char) != cur_node->children_.end()) {
    // child = helper(cur_key, cur_node->children_.at(cur_char));
    // } else {
    //   std::shared_ptr<const TrieNode> temp_child = std::make_shared<const TrieNode>(TrieNode());
    //   child = helper(cur_key, temp_child);
    // }
    std::unique_ptr<TrieNode> cloned_cur_node = cur_node->Clone();

    if (child == nullptr) {
      cloned_cur_node->children_.erase(cur_char);
    } else {
      cloned_cur_node->children_[cur_char] = child;
    }
    if (cloned_cur_node->children_.empty() && !cloned_cur_node->is_value_node_) {
      return nullptr;
    }
    // std::shared_ptr<const TrieNode> shared_cloned_cur_node = cloned_cur_node;
    return cloned_cur_node;
  };
  std::shared_ptr<const TrieNode> new_root = helper(string_key, this->root_);
  return Trie(new_root);
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
