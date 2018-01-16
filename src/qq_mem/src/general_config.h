#ifndef GENERAL_CONFIG_H
#define GENERAL_CONFIG_H

#include <map>
#include <vector>

class GeneralConfig {
 private:
  std::map<std::string, bool> bool_map_;
  std::map<std::string, int> int_map_;
  std::map<std::string, std::string> string_map_;
  std::map<std::string, std::vector<std::string>> string_vec_map_;

 public:
  void SetInt(const std::string key, const int value) {
    int_map_[key] = value;
  }

  int GetInt(const std::string key) {
    return int_map_[key];
  }

  void SetString(const std::string key, const std::string value) {
    string_map_[key] = value;
  }

  const std::string GetString(const std::string key) {
    return string_map_[key];
  }

  void SetBool(const std::string key, const bool value) {
    bool_map_[key] = value;
  }

  bool GetBool(const std::string key) {
    return bool_map_[key];
  }

  void SetStringVec(const std::string key, 
                    const std::vector<std::string> value) {
    string_vec_map_[key] = value;
  }

  std::vector<std::string> GetStringVec(const std::string key) {
    return string_vec_map_[key];
  }
};

#endif
