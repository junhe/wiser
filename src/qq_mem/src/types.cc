#include "types.h" 
#include "utils.h"


TermList DocInfo::GetTokens() const {
  return utils::explode(tokens_, ' ');
}

// return a table of offset pairs
// Each row is for a term
std::vector<OffsetPairs> DocInfo::GetOffsetPairsVec() const {
  return utils::parse_offsets(token_offsets_);
}

// return a table of positions
// Each row is for a term
std::vector<Positions> DocInfo::GetPositions() const {
  std::vector<std::string> groups = utils::explode(token_positions_, '.');

  std::vector<Positions> table(groups.size());

  for (std::size_t i = 0; i < groups.size(); i++) {
    // example group: 1;3;8
    std::vector<std::string> pos_strs = utils::explode(groups[i], ';');
    for (auto & pos_str : pos_strs) {
      try {
        table[i].push_back(std::stoi(pos_str));
      } catch (...) {
        std::cout << "'" << pos_str << "',";
        throw;
      }
    }
  }

  return table;
}

const int DocInfo::BodyLength() const {
  return utils::count_terms(body_);
}

std::vector<Term> DocInfo::GetPhraseEnds() const {
  std::vector<Term> ret = utils::explode_strict(phrase_ends_, '!');
  if (ret.size() > 0) {
    // the last item is an empty one
    ret.pop_back();
  }
  return ret;
}


