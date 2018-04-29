#ifndef ENGINE_FACTORY_H
#define ENGINE_FACTORY_H

#include "qq_mem_engine.h"
#include "vacuum_engine.h"


struct VacuumConfig {
  VacuumConfig(std::string s, std::string p) {
    if (s != "vacuum_dump") {
      LOG(FATAL) << "type not supported. " << s;
    }

    source_type = s;
    path = p;
  }
  std::string source_type;
  std::string path;
};

inline bool IsVacuumUrl(std::string url) {
  return url.compare(0, 7, "vacuum:") == 0;
}

inline VacuumConfig ParseUrl(std::string url) {
  std::vector<std::string> parts = utils::explode(url, ':');
  if (parts.size() != 3)
    LOG(FATAL) << "url is in wrong format. " << url;

  return VacuumConfig(parts[1], parts[2]);
}

inline std::unique_ptr<SearchEngineServiceNew> CreateSearchEngine(
    std::string engine_type) {
  if (engine_type == "qq_mem_compressed") {
    return std::unique_ptr<SearchEngineServiceNew>(new QqMemEngineDelta());
  } else if (IsVacuumUrl(engine_type) == true) {
    VacuumConfig config = ParseUrl(engine_type);
    if (config.source_type == "vacuum_dump") {
      // Engine is not loaded yet
      return std::unique_ptr<SearchEngineServiceNew>(new VacuumEngine<AlignedFlashDocStore>(config.path));
    }
  } else {
    throw std::runtime_error("Wrong engine type: " + engine_type);
  }
  return nullptr;
}


#endif
