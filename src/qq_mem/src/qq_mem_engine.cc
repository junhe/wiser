#include "qq_mem_engine.h"
#include "flash_engine_dumper.h"

std::unique_ptr<SearchEngineServiceNew> CreateSearchEngine(
    std::string engine_type) {
  if (engine_type == "qq_mem_compressed") {
    return std::unique_ptr<SearchEngineServiceNew>(new QqMemEngineDelta());
  } else if (engine_type == "flash_engine_dumper") {
    return std::unique_ptr<SearchEngineServiceNew>(new FlashEngineDumper());
  } else {
    throw std::runtime_error("Wrong engine type: " + engine_type);
  }
}


