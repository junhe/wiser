#include "packed_value.h"


// Append the rightmost n_bits of val to next_empty_bit in buf
// val must have only n_bits
// You must garantee that val will not overflow the char
// Return: the next empty bit's bit index
int AppendToByte(const long val, const int n_bits, uint8_t *buf, const int next_empty_bit) {
  int byte_index = next_empty_bit / 8;
  int in_byte_off = next_empty_bit % 8;
  int n_shift = 8 - n_bits - in_byte_off;

  buf[byte_index] |= val << n_shift;

  return next_empty_bit + n_bits;
}

// Append the right-most n_bits to next_empty_bit in buf
// val must have only n_bits
int AppendValue(long val, int n_bits, uint8_t *buf, int next_empty_bit) {
  int n_bits_remain = n_bits;

  while (n_bits_remain > 0) {
    const int n_bits_in_byte = NumBitsInByte(next_empty_bit);  
    const int n_bits_to_send = n_bits_remain < n_bits_in_byte? n_bits_remain : n_bits_in_byte;
    const int n_shift = n_bits_remain - n_bits_to_send;
    long v = val >> n_shift; 

    next_empty_bit = AppendToByte(v, n_bits_to_send, buf, next_empty_bit);

    n_bits_remain = n_bits_remain - n_bits_to_send;
    int n = sizeof(long) * 8 - n_bits_remain;
    val = (val << n) >> n; // set the left n bits to 0
  }

  return next_empty_bit;
}

int NumBitsInByte(int next_empty_bit) {
  return 8 - (next_empty_bit % 8);
}

long ExtractBits(uint8_t *buf, const int bit_start, const int n_bits) {
  long val = 0;  
  int n_bits_remain = n_bits;
  int next_bit = bit_start;

  while (n_bits_remain > 0) {
    const int byte_index = next_bit / 8;
    const int in_byte_off = next_bit % 8;
    const int n_unread_bits = 8 - in_byte_off;
    const int n_bits_to_read = 
      n_bits_remain < n_unread_bits?  n_bits_remain : n_unread_bits;

    // number of bits to the left of the current bits
    const int n_bits_on_left = 64 - n_bits + (next_bit - bit_start); 
    uint8_t tmp = (buf[byte_index] << in_byte_off) >> (8 - n_bits_to_read);

    val |= tmp << (64 - n_bits_to_read) - n_bits_on_left;

    n_bits_remain -= n_bits_to_read;
    next_bit += n_bits_to_read;
  }

  return val;
}



