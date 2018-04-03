#include "packed_value.h"


// Append the rightmost n_bits of val to next_empty_bit in buf
// val must have only n_bits
// You must garantee that val will not overflow the char
// Return: the next empty bit's bit index
int AppendToByte(long val, int n_bits, uint8_t *buf, int next_empty_bit) {
  int byte_index = next_empty_bit / 8;
  int in_byte_off = next_empty_bit % 8;
  int n_shift = 8 - n_bits - in_byte_off;

  buf[byte_index] |= val << n_shift;

  return next_empty_bit + n_bits;
}

int NumBitsInByte(int next_empty_bit) {
  return 8 - (next_empty_bit % 8);
}





