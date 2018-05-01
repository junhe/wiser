#include "flash_iterators.h"


DEFINE_bool(enable_prefetch, false, "Enable prefetch (default=false)");
DEFINE_int32(prefetch_threshold, 1024, 
    "Do prefetch if the number of pages in prefetch zone is larger than the threshold. "
    "The default is 1024, which prefers not to prefetch.");

