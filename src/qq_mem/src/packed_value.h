#include "utils.h"

int AppendToByte(long val, const int n_bits, uint8_t *buf, const int next_empty_bit);
int AppendValue(long val, int n_bits, uint8_t *buf, int next_empty_bit);
int NumBitsInByte(int next_empty_bit);

class PackedIntsWriter {
 public:
  void Add(long value) {
    values_.push_back(value);
    int n_bits = utils::NumOfBits(value);
    if (n_bits > max_bits_per_value_)
      max_bits_per_value_ = n_bits;

    for (auto &v : values_) {
      std::cout << v << ",";
    }
    std::cout << "max_bits_per_value_: " << max_bits_per_value_ << std::endl;
    std::cout << std::endl;
  }

  std::string Serialize() {
    
  }

 private:
  std::vector<long> values_;
  int max_bits_per_value_ = 0;
};




