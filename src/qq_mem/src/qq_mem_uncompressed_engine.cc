#include "qq_mem_uncompressed_engine.h"

std::unique_ptr<SearchEngineServiceNew> CreateSearchEngine(
    std::string engine_type) {

  GeneralConfig config;
  if (engine_type == "qq_mem_uncompressed") {
    config.SetString("inverted_index", "uncompressed");
  } else if (engine_type == "qq_mem_compressed") {
    config.SetString("inverted_index", "compressed");
  } else {
    throw std::runtime_error("Wrong engine type: " + engine_type);
  }
  return std::unique_ptr<SearchEngineServiceNew>(new QqMemUncompressedEngineDelta(config));
}


