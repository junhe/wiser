#include "qq_mem_uncompressed_engine.h"

std::unique_ptr<SearchEngineServiceNew> CreateSearchEngine(
    std::string engine_type) {
  if (engine_type == "qq_mem_uncompressed") {
    return std::unique_ptr<SearchEngineServiceNew>(new QqMemUncompressedEngine);
  } else if (engine_type == "qq_mem_compressed") {
    return std::unique_ptr<SearchEngineServiceNew>(new QqMemUncompressedEngineDelta);
  } else {
    throw std::runtime_error("Wrong engine type: " + engine_type);
  }
}


