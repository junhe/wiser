#ifndef GENERAL_CONFIG_H
#define GENERAL_CONFIG_H

#include <map>

class GeneralConfig {
 private:
  std::map<std::string, int> int_map_;
  std::map<std::string, std::string> string_map_;

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
};

#endif
