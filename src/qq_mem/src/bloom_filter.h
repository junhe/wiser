#include "bloom.h"




// Return:
// -------
// 0 - element was not present and was added
// 1 - element (or a collision) had already been added previously
// -1 - bloom not initialized
inline int AddToBloom(struct bloom *blm, const std::string &s) {
  return bloom_add(blm, s.data(), s.size()); 
}


const int BLM_NOT_PRESENT = 0;
const int BLM_MAY_PRESENT = 1;
const int BLM_NOT_INITIALIZED = -1;

// Return:
// -------
// 0 - element is not present
// 1 - element is present (or false positive due to collision)
// -1 - bloom not initialized
inline int CheckBloom(struct bloom *blm, const std::string &s) {
  return bloom_check(blm, s.data(), s.size());
}


