# include "varint.h"

void encodeVarint(uint64_t value, uint8_t* output, uint8_t &outputSize)
{
    outputSize = 0;

    while (value > 127)
    {
        output[outputSize] = ((uint8_t)(value & 127)) | 128;
        value >>= 7;
        outputSize++;
    }

    output[outputSize++] = ((uint8_t)value) & 127;
}

uint64_t decodeVarint(uint8_t* input, uint8_t &inputSize)
{
    uint64_t ret = 0;
    inputSize = 0;

    for (uint8_t i = 0; ; i++)
    {
        ret |= (input[i] & 127) << (7 * i);
        inputSize++;

        if(!(input[i] & 128))
            break;
    }

    return ret;
}

