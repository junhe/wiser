#ifndef INTERSECT_H
#define INTERSECT_H

#include <vector>

#include "engine_services.h"
#include "posting_list_vec.h"


std::vector<DocIdType> intersect(
    const std::vector<const PostingList_Vec<Posting> *> lists);




#endif
