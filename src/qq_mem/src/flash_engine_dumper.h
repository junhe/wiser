#include "qq_mem_engine.h"


// Use this engine to load data from line doc and to write data 
// for qq-flash
class FlashEngineDumper: public QqMemEngineDelta {
 public:
   FlashEngineDumper(): QqMemEngineDelta() {}
};




