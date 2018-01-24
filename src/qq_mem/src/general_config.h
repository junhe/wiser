#ifndef GENERAL_CONFIG_H
#define GENERAL_CONFIG_H

#include <map>
#include <vector>

#include <glog/logging.h>


class GeneralConfig {
 private:
  std::map<std::string, bool> bool_map_;
  std::map<std::string, int> int_map_;
  std::map<std::string, std::string> string_map_;
  std::map<std::string, std::vector<std::string>> string_vec_map_;

  void AssertKeyExistence(const std::string key) const {
    if (!HasKey(key)) {
      LOG(FATAL) << "Key " << key << " does not exist" << std::endl;
    }
  }

 public:
  bool HasKey(const std::string key) const {
    if (
        bool_map_.count(key) > 0 ||
        int_map_.count(key) > 0 ||
        string_map_.count(key) > 0 ||
        string_vec_map_.count(key) > 0
       ) {
      return true;
    } else {
      return false;
    }
  }

  void SetInt(const std::string key, const int value) {
    int_map_[key] = value;
  }

  const int GetInt(const std::string key) const {
    AssertKeyExistence(key);
    return int_map_.at(key);
  }

  void SetString(const std::string key, const std::string value) {
    string_map_[key] = value;
  }

  const std::string GetString(const std::string key) const {
    AssertKeyExistence(key);
    return string_map_.at(key);
  }

  void SetBool(const std::string key, const bool value) {
    bool_map_[key] = value;
  }

  const bool GetBool(const std::string key) const {
    AssertKeyExistence(key);
    return bool_map_.at(key);
  }

  void SetStringVec(const std::string key, 
                    const std::vector<std::string> value) {
    string_vec_map_[key] = value;
  }

  const std::vector<std::string> GetStringVec(const std::string key) const {
    AssertKeyExistence(key);
    return string_vec_map_.at(key);
  }
};

#endif
