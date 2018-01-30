#include "unifiedhighlighter.h"
#include "posting.h"
#include <iostream>
#include <algorithm>

std::locale create_locale() {
  boost::locale::generator gen_all;        //
  return gen_all("en_US.UTF-8");
}


