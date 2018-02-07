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

  for (int i = 0; i < groups.size(); i++) {
    // group: 1;3;8
    std::vector<std::string> pos_strs = utils::explode(groups[i], ';');
    for (auto & pos_str : pos_strs) {
      std::cout << "pos_str: " << pos_str << std::endl;
      table[i].push_back(std::stoi(pos_str));
    }
  }

  return table;
}

const int DocInfo::BodyLength() const {return utils::count_terms(body_);}

