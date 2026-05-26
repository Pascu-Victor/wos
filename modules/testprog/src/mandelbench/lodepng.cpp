/*
LodePNG version 20150912

Copyright (c) 2005-2015 Lode Vandevenne

This software is provided 'as-is', without any express or implied
warranty. In no event will the authors be held liable for any damages
arising from the use of this software.

Permission is granted to anyone to use this software for any purpose,
including commercial applications, and to alter it and redistribute it
freely, subject to the following restrictions:

    1. The origin of this software must not be misrepresented; you must not
    claim that you wrote the original software. If you use this software
    in a product, an acknowledgment in the product documentation would be
    appreciated but is not required.

    2. Altered source versions must be plainly marked as such, and must not be
    misrepresented as being the original software.

    3. This notice may not be removed or altered from any source
    distribution.
*/

/*
The manual and changelog are in the header file "lodepng.h"
Rename this file to lodepng.cpp to use it for C++, or to lodepng.c to use it for C.
*/

#include "lodepng.hpp"

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ios>
#include <string>
#include <vector>

#ifdef LODEPNG_COMPILE_CPP
#include <fstream>
#include <numbers>
#include <utility>
#endif /*LODEPNG_COMPILE_CPP*/

/*
LodePNG is kept as a single-file third-party implementation with a public C-compatible ABI, custom allocation hooks, macro-based
error unwinding, and byte-oriented PNG/zlib buffer walking. Suppress only the lint families that are inherent to that design.
*/
// NOLINTBEGIN(bugprone-branch-clone, bugprone-implicit-widening-of-multiplication-result, bugprone-narrowing-conversions,
// NOLINTNEXTLINE(readability-comment-starts-with-space)
// bugprone-switch-missing-default-case, clang-analyzer-security.ArrayBound, clang-analyzer-unix.Errno,
// NOLINTNEXTLINE(readability-comment-starts-with-space)
// cppcoreguidelines-avoid-c-arrays, cppcoreguidelines-macro-usage, cppcoreguidelines-narrowing-conversions,
// NOLINTNEXTLINE(readability-comment-starts-with-space)
// cppcoreguidelines-no-malloc, cppcoreguidelines-pro-bounds-array-to-pointer-decay,
// NOLINTNEXTLINE(readability-comment-starts-with-space)
// cppcoreguidelines-pro-bounds-avoid-unchecked-container-access, misc-const-correctness, misc-use-anonymous-namespace,
// NOLINTNEXTLINE(readability-comment-starts-with-space)
// misc-use-internal-linkage, modernize-avoid-c-arrays)

#if defined(_MSC_VER) && (_MSC_VER >= 1310) /*Visual Studio: A few warning types are not desired here.*/
#pragma warning(disable : 4244)             /*implicit conversions: not warned by gcc -Wall -Wextra and requires too much casts*/
#pragma warning(disable : 4996)             /*VS does not like fopen, but fopen_s is not standard C so unusable here*/
#endif                                      /*_MSC_VER */

const char* lodepng_version_string = "20150912";

/*
This source file is built up in the following large parts. The code sections
with the "LODEPNG_COMPILE_" #defines divide this up further in an intermixed way.
-Tools for C and common code for PNG and Zlib
-C Code for Zlib (huffman, deflate, ...)
-C Code for PNG (file format chunks, adam7, PNG filters, color conversions, ...)
-The C++ wrapper around all of the above
*/

/*The malloc, realloc and free functions defined here with "lodepng_" in front
of the name, so that you can easily change them to others related to your
platform if needed. Everything else in the code calls these. Pass
-DLODEPNG_NO_COMPILE_ALLOCATORS to the compiler, or comment out
#define LODEPNG_COMPILE_ALLOCATORS in the header, to disable the ones here and
define them in your own project's source files without needing to change
lodepng source code. Don't forget to remove "static" if you copypaste them
from here.*/

#ifdef LODEPNG_COMPILE_ALLOCATORS
static void* lodepng_malloc(size_t size) { return malloc(size); }

static void* lodepng_realloc(void* ptr, size_t new_size) { return realloc(ptr, new_size); }

static void lodepng_free(void* ptr) { free(ptr); }
#else  /*LODEPNG_COMPILE_ALLOCATORS*/
void* lodepng_malloc(size_t size);
void* lodepng_realloc(void* ptr, size_t new_size);
void lodepng_free(void* ptr);
#endif /*LODEPNG_COMPILE_ALLOCATORS*/

/* ////////////////////////////////////////////////////////////////////////// */
/* ////////////////////////////////////////////////////////////////////////// */
/* // Tools for C, and common code for PNG and Zlib.                       // */
/* ////////////////////////////////////////////////////////////////////////// */
/* ////////////////////////////////////////////////////////////////////////// */

/*
Often in case of an error a value is assigned to a variable and then it breaks
out of a loop (to go to the cleanup phase of a function). This macro does that.
It makes the error handling code shorter and more readable.

Example: if(!uivector_resizev(&frequencies_ll, 286, 0)) ERROR_BREAK(83);
*/
#define CERROR_BREAK(errorvar, code) \
    {                                \
        (errorvar) = code;           \
        break;                       \
    }

/*version of CERROR_BREAK that assumes the common case where the error variable is named "error"*/
#define ERROR_BREAK(code) CERROR_BREAK(error, code)

/*Set error var to the error code, and return it.*/
#define CERROR_RETURN_ERROR(errorvar, code) \
    {                                       \
        (errorvar) = code;                  \
        return code;                        \
    }

/*Try the code, if it returns error, also return the error.*/
#define CERROR_TRY_RETURN(call)  \
    {                            \
        unsigned error = call;   \
        if (error) return error; \
    }

/*Set error var to the error code, and return from the void function.*/
#define CERROR_RETURN(errorvar, code) \
    {                                 \
        (errorvar) = code;            \
        return;                       \
    }

/*
About uivector, ucvector and string:
-All of them wrap dynamic arrays or text strings in a similar way.
-LodePNG was originally written in C++. The vectors replace the std::vectors that were used in the C++ version.
-The string tools are made to avoid problems with compilers that declare things like strncat as deprecated.
-They're not used in the interface, only internally in this file as static functions.
-As with many other structs in this file, the init and cleanup functions serve as ctor and dtor.
*/

#ifdef LODEPNG_COMPILE_ZLIB
/*dynamic vector of unsigned ints*/
using uivector = struct Uivector {
    unsigned* data;
    size_t size;      /*size in number of unsigned longs*/
    size_t allocsize; /*allocated size in bytes*/
};

static void uivector_cleanup(void* p) {
    (static_cast<uivector*>(p))->size = (static_cast<uivector*>(p))->allocsize = 0;
    lodepng_free((static_cast<uivector*>(p))->data);
    (static_cast<uivector*>(p))->data = nullptr;
}

/*returns 1 if success, 0 if failure ==> nothing done*/
static unsigned uivector_reserve(uivector* p, size_t allocsize) {
    if (allocsize > p->allocsize) {
        size_t const NEWSIZE = (allocsize > p->allocsize * 2) ? allocsize : (allocsize * 3 / 2);
        void* data = lodepng_realloc(p->data, NEWSIZE);
        if (data != nullptr) {
            p->allocsize = NEWSIZE;
            p->data = static_cast<unsigned*>(data);
        } else {
            {
                return 0; /*error: not enough memory*/
            }
        }
    }
    return 1;
}

/*returns 1 if success, 0 if failure ==> nothing done*/
static unsigned uivector_resize(uivector* p, size_t size) {
    if (uivector_reserve(p, size * sizeof(unsigned)) == 0U) {
        return 0;
    }
    p->size = size;
    return 1; /*success*/
}

/*resize and give all new elements the value*/
static unsigned uivector_resizev(uivector* p, size_t size, unsigned value) {
    size_t const OLDSIZE = p->size;
    size_t i = 0;
    if (uivector_resize(p, size) == 0U) {
        return 0;
    }
    for (i = OLDSIZE; i < size; ++i) {
        p->data[i] = value;
    }
    return 1;
}

static void uivector_init(uivector* p) {
    p->data = nullptr;
    p->size = p->allocsize = 0;
}

#ifdef LODEPNG_COMPILE_ENCODER
/*returns 1 if success, 0 if failure ==> nothing done*/
static unsigned uivector_push_back(uivector* p, unsigned c) {
    if (uivector_resize(p, p->size + 1) == 0U) {
        return 0;
    }
    p->data[p->size - 1] = c;
    return 1;
}
#endif /*LODEPNG_COMPILE_ENCODER*/
#endif /*LODEPNG_COMPILE_ZLIB*/

/* /////////////////////////////////////////////////////////////////////////// */

/*dynamic vector of unsigned chars*/
using ucvector = struct Ucvector {
    unsigned char* data;
    size_t size;      /*used size*/
    size_t allocsize; /*allocated size*/
};

/*returns 1 if success, 0 if failure ==> nothing done*/
static unsigned ucvector_reserve(ucvector* p, size_t allocsize) {
    if (allocsize > p->allocsize) {
        size_t const NEWSIZE = (allocsize > p->allocsize * 2) ? allocsize : (allocsize * 3 / 2);
        void* data = lodepng_realloc(p->data, NEWSIZE);
        if (data != nullptr) {
            p->allocsize = NEWSIZE;
            p->data = static_cast<unsigned char*>(data);
        } else {
            {
                return 0; /*error: not enough memory*/
            }
        }
    }
    return 1;
}

/*returns 1 if success, 0 if failure ==> nothing done*/
static unsigned ucvector_resize(ucvector* p, size_t size) {
    if (ucvector_reserve(p, size * sizeof(unsigned char)) == 0U) {
        return 0;
    }
    p->size = size;
    return 1; /*success*/
}

#ifdef LODEPNG_COMPILE_PNG

static void ucvector_cleanup(void* p) {
    (static_cast<ucvector*>(p))->size = (static_cast<ucvector*>(p))->allocsize = 0;
    lodepng_free((static_cast<ucvector*>(p))->data);
    (static_cast<ucvector*>(p))->data = nullptr;
}

static void ucvector_init(ucvector* p) {
    p->data = nullptr;
    p->size = p->allocsize = 0;
}

#ifdef LODEPNG_COMPILE_DECODER
/*resize and give all new elements the value*/
static unsigned ucvector_resizev(ucvector* p, size_t size, unsigned char value) {
    size_t const OLDSIZE = p->size;
    size_t i = 0;
    if (ucvector_resize(p, size) == 0U) {
        return 0;
    }
    for (i = OLDSIZE; i < size; ++i) {
        p->data[i] = value;
    }
    return 1;
}
#endif /*LODEPNG_COMPILE_DECODER*/
#endif /*LODEPNG_COMPILE_PNG*/

#ifdef LODEPNG_COMPILE_ZLIB
/*you can both convert from vector to buffer&size and vica versa. If you use
init_buffer to take over a buffer and size, it is not needed to use cleanup*/
static void ucvector_init_buffer(ucvector* p, unsigned char* buffer, size_t size) {
    p->data = buffer;
    p->allocsize = p->size = size;
}
#endif /*LODEPNG_COMPILE_ZLIB*/

#if (defined(LODEPNG_COMPILE_PNG) && defined(LODEPNG_COMPILE_ANCILLARY_CHUNKS)) || defined(LODEPNG_COMPILE_ENCODER)
/*returns 1 if success, 0 if failure ==> nothing done*/
static unsigned ucvector_push_back(ucvector* p, unsigned char c) {
    if (ucvector_resize(p, p->size + 1) == 0U) {
        return 0;
    }
    p->data[p->size - 1] = c;
    return 1;
}
#endif /*defined(LODEPNG_COMPILE_PNG) || defined(LODEPNG_COMPILE_ENCODER)*/

/* ////////////////////////////////////////////////////////////////////////// */

#ifdef LODEPNG_COMPILE_PNG
#ifdef LODEPNG_COMPILE_ANCILLARY_CHUNKS
/*returns 1 if success, 0 if failure ==> nothing done*/
static unsigned string_resize(char** out, size_t size) {
    char* data = static_cast<char*>(lodepng_realloc(*out, size + 1));
    if (data != nullptr) {
        data[size] = 0; /*null termination char*/
        *out = data;
    }
    return static_cast<unsigned int>(data != nullptr);
}

/*init a {char*, size_t} pair for use as string*/
static void string_init(char** out) {
    *out = nullptr;
    string_resize(out, 0);
}

/*free the above pair again*/
static void string_cleanup(char** out) {
    lodepng_free(*out);
    *out = nullptr;
}

static void string_set(char** out, const char* in) {
    size_t const INSIZE = strlen(in);
    size_t i = 0;
    if (string_resize(out, INSIZE) != 0U) {
        for (i = 0; i != INSIZE; ++i) {
            (*out)[i] = in[i];
        }
    }
}
#endif /*LODEPNG_COMPILE_ANCILLARY_CHUNKS*/
#endif /*LODEPNG_COMPILE_PNG*/

/* ////////////////////////////////////////////////////////////////////////// */

static unsigned lodepng_read32bit_int(const unsigned char* buffer) {
    return static_cast<unsigned>((buffer[0] << 24) | (buffer[1] << 16) | (buffer[2] << 8) | buffer[3]);
}

#if defined(LODEPNG_COMPILE_PNG) || defined(LODEPNG_COMPILE_ENCODER)
/*buffer must have at least 4 allocated bytes available*/
static void lodepng_set32bit_int(unsigned char* buffer, unsigned value) {
    buffer[0] = static_cast<unsigned char>((value >> 24) & 0xff);
    buffer[1] = static_cast<unsigned char>((value >> 16) & 0xff);
    buffer[2] = static_cast<unsigned char>((value >> 8) & 0xff);
    buffer[3] = static_cast<unsigned char>(value & 0xff);
}
#endif /*defined(LODEPNG_COMPILE_PNG) || defined(LODEPNG_COMPILE_ENCODER)*/

#ifdef LODEPNG_COMPILE_ENCODER
static void lodepng_add32bit_int(ucvector* buffer, unsigned value) {
    ucvector_resize(buffer, buffer->size + 4); /*todo: give error if resize failed*/
    lodepng_set32bit_int(&buffer->data[buffer->size - 4], value);
}
#endif /*LODEPNG_COMPILE_ENCODER*/

/* ////////////////////////////////////////////////////////////////////////// */
/* / File IO                                                                / */
/* ////////////////////////////////////////////////////////////////////////// */

#ifdef LODEPNG_COMPILE_DISK

unsigned lodepng_load_file(unsigned char** out, size_t* outsize, const char* filename) {
    FILE* file = nullptr;
    long size = 0;

    /*provide some proper output values if error will happen*/
    *out = nullptr;
    *outsize = 0;

    file = fopen(filename, "rb");
    if (file == nullptr) {
        return 78;
    }

    /*get filesize:*/
    if (fseek(file, 0, SEEK_END) != 0) {
        fclose(file);
        return 78;
    }
    size = ftell(file);
    if (size < 0 || fseek(file, 0, SEEK_SET) != 0) {
        fclose(file);
        return 78;
    }

    /*read contents of the file into the vector*/
    *outsize = 0;
    *out = static_cast<unsigned char*>(lodepng_malloc(static_cast<size_t>(size)));
    if ((size != 0) && ((*out) != nullptr)) {
        (*outsize) = fread(*out, 1, static_cast<size_t>(size), file);
    }

    fclose(file);
    if (((*out) == nullptr) && (size != 0)) {
        return 83; /*the above malloc failed*/
    }
    return 0;
}

/*write given buffer to the file, overwriting the file, it doesn't append to it.*/
unsigned lodepng_save_file(const unsigned char* buffer, size_t buffersize, const char* filename) {
    FILE* file = nullptr;
    file = fopen(filename, "wb");
    if (file == nullptr) {
        return 79;
    }
    fwrite(buffer, 1, buffersize, file);
    fclose(file);
    return 0;
}

#endif /*LODEPNG_COMPILE_DISK*/

/* ////////////////////////////////////////////////////////////////////////// */
/* ////////////////////////////////////////////////////////////////////////// */
/* // End of common code and tools. Begin of Zlib related code.            // */
/* ////////////////////////////////////////////////////////////////////////// */
/* ////////////////////////////////////////////////////////////////////////// */

#ifdef LODEPNG_COMPILE_ZLIB
#ifdef LODEPNG_COMPILE_ENCODER
/*TODO: this ignores potential out of memory errors*/
#define ADD_BIT_TO_STREAM(/*size_t**/ bitpointer, /*ucvector**/ bitstream, /*unsigned char*/ bit)     \
    {                                                                                                 \
        /*add a new byte at the end*/                                                                 \
        if (((*(bitpointer)) & 7) == 0) ucvector_push_back(bitstream, static_cast<unsigned char>(0)); \
        /*earlier bit of huffman code is in a lesser significant bit of an earlier byte*/             \
        ((bitstream)->data[(bitstream)->size - 1]) |= ((bit) << ((*(bitpointer)) & 0x7));             \
        ++(*(bitpointer));                                                                            \
    }

static void add_bits_to_stream(size_t* bitpointer, ucvector* bitstream, unsigned value, size_t nbits) {
    size_t i = 0;
    for (i = 0; i != nbits; ++i) ADD_BIT_TO_STREAM(bitpointer, bitstream, static_cast<unsigned char>((value >> i) & 1));
}

static void add_bits_to_stream_reversed(size_t* bitpointer, ucvector* bitstream, unsigned value, size_t nbits) {
    size_t i = 0;
    for (i = 0; i != nbits; ++i) ADD_BIT_TO_STREAM(bitpointer, bitstream, static_cast<unsigned char>((value >> (nbits - 1 - i)) & 1));
}
#endif /*LODEPNG_COMPILE_ENCODER*/

#ifdef LODEPNG_COMPILE_DECODER

#define READBIT(bitpointer, bitstream) (((bitstream)[(bitpointer) >> 3] >> ((bitpointer) & 0x7)) & static_cast<unsigned char>(1))

static unsigned char read_bit_from_stream(size_t* bitpointer, const unsigned char* bitstream) {
    auto const RESULT = static_cast<unsigned char>(READBIT(*bitpointer, bitstream));
    ++(*bitpointer);
    return RESULT;
}

static unsigned read_bits_from_stream(size_t* bitpointer, const unsigned char* bitstream, size_t nbits) {
    unsigned result = 0;
    unsigned i = 0;
    for (i = 0; i != nbits; ++i) {
        result += (static_cast<unsigned> READBIT(*bitpointer, bitstream)) << i;
        ++(*bitpointer);
    }
    return result;
}
#endif /*LODEPNG_COMPILE_DECODER*/

/* ////////////////////////////////////////////////////////////////////////// */
/* / Deflate - Huffman                                                      / */
/* ////////////////////////////////////////////////////////////////////////// */

static constexpr unsigned FIRST_LENGTH_CODE_INDEX = 257;
static constexpr unsigned LAST_LENGTH_CODE_INDEX = 285;
/*256 literals, the end code, some length codes, and 2 unused codes*/
static constexpr unsigned NUM_DEFLATE_CODE_SYMBOLS = 288;
/*the distance codes have their own symbols, 30 used, 2 unused*/
static constexpr unsigned NUM_DISTANCE_SYMBOLS = 32;
/*the code length codes. 0-15: code lengths, 16: copy previous 3-6 times, 17: 3-10 zeros, 18: 11-138 zeros*/
static constexpr unsigned NUM_CODE_LENGTH_CODES = 19;

/*the base lengths represented by codes 257-285*/
static constexpr std::array<unsigned, 29> LENGTHBASE = {3,  4,  5,  6,  7,  8,  9,  10, 11,  13,  15,  17,  19,  23, 27,
                                                        31, 35, 43, 51, 59, 67, 83, 99, 115, 131, 163, 195, 227, 258};

/*the extra bits used by codes 257-285 (added to base length)*/
static constexpr std::array<unsigned, 29> LENGTHEXTRA = {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2,
                                                         2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0};

/*the base backwards distances (the bits of distance codes appear after length codes and use their own huffman tree)*/
static constexpr std::array<unsigned, 30> DISTANCEBASE = {1,    2,    3,    4,    5,    7,    9,    13,    17,    25,
                                                          33,   49,   65,   97,   129,  193,  257,  385,   513,   769,
                                                          1025, 1537, 2049, 3073, 4097, 6145, 8193, 12289, 16385, 24577};

/*the extra bits of backwards distances (added to base)*/
static constexpr std::array<unsigned, 30> DISTANCEEXTRA = {0, 0, 0, 0, 1, 1, 2, 2,  3,  3,  4,  4,  5,  5,  6,
                                                           6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13, 13};

/*the order in which "code length alphabet code lengths" are stored, out of this
the huffman tree of the dynamic huffman tree lengths is generated*/
static constexpr std::array<unsigned, NUM_CODE_LENGTH_CODES> CLCL_ORDER = {16, 17, 18, 0, 8,  7, 9,  6, 10, 5,
                                                                           11, 4,  12, 3, 13, 2, 14, 1, 15};

/* ////////////////////////////////////////////////////////////////////////// */

/*
Huffman tree struct, containing multiple representations of the tree
*/
using HuffmanTree = struct HuffmanTree {
    unsigned* tree2d;
    unsigned* tree1d;
    unsigned* lengths;  /*the lengths of the codes of the 1d-tree*/
    unsigned maxbitlen; /*maximum number of bits a single code can get*/
    unsigned numcodes;  /*number of symbols in the alphabet = number of codes*/
};

/*function used for debug purposes to draw the tree in ascii art with C++*/
/*
static void HuffmanTree_draw(HuffmanTree* tree)
{
  std::cout << "tree. length: " << tree->numcodes << " maxbitlen: " << tree->maxbitlen << std::endl;
  for(size_t i = 0; i != tree->tree1d.size; ++i)
  {
    if(tree->lengths.data[i])
      std::cout << i << " " << tree->tree1d.data[i] << " " << tree->lengths.data[i] << std::endl;
  }
  std::cout << std::endl;
}*/

static void huffman_tree_init(HuffmanTree* tree) {
    tree->tree2d = nullptr;
    tree->tree1d = nullptr;
    tree->lengths = nullptr;
}

static void huffman_tree_cleanup(HuffmanTree* tree) {
    lodepng_free(tree->tree2d);
    lodepng_free(tree->tree1d);
    lodepng_free(tree->lengths);
}

/*the tree representation used by the decoder. return value is error*/
static unsigned huffman_tree_make2_d_tree(HuffmanTree* tree) {
    unsigned nodefilled = 0; /*up to which node it is filled*/
    unsigned treepos = 0;    /*position in the tree (1 of the numcodes columns)*/
    unsigned n = 0;
    unsigned i = 0;

    tree->tree2d = static_cast<unsigned*>(lodepng_malloc(tree->numcodes * 2 * sizeof(unsigned)));
    if (tree->tree2d == nullptr) {
        return 83; /*alloc fail*/
    }

    /*
    convert tree1d[] to tree2d[][]. In the 2D array, a value of 32767 means
    uninited, a value >= numcodes is an address to another bit, a value < numcodes
    is a code. The 2 rows are the 2 possible bit values (0 or 1), there are as
    many columns as codes - 1.
    A good huffmann tree has N * 2 - 1 nodes, of which N - 1 are internal nodes.
    Here, the internal nodes are stored (what their 0 and 1 option point to).
    There is only memory for such good tree currently, if there are more nodes
    (due to too long length codes), error 55 will happen
    */
    for (n = 0; n < tree->numcodes * 2; ++n) {
        tree->tree2d[n] = 32767; /*32767 here means the tree2d isn't filled there yet*/
    }

    for (n = 0; n < tree->numcodes; ++n) /*the codes*/
    {
        for (i = 0; i != tree->lengths[n]; ++i) /*the bits for this code*/
        {
            auto const BIT = static_cast<unsigned char>((tree->tree1d[n] >> (tree->lengths[n] - i - 1)) & 1);
            /*oversubscribed, see comment in lodepng_error_text*/
            if (treepos > 2147483647 || treepos + 2 > tree->numcodes) {
                return 55;
            }
            if (tree->tree2d[(2 * treepos) + BIT] == 32767) /*not yet filled in*/
            {
                if (i + 1 == tree->lengths[n]) /*last bit*/
                {
                    tree->tree2d[(2 * treepos) + BIT] = n; /*put the current code in it*/
                    treepos = 0;
                } else {
                    /*put address of the next step in here, first that address has to be found of course
                    (it's just nodefilled + 1)...*/
                    ++nodefilled;
                    /*addresses encoded with numcodes added to it*/
                    tree->tree2d[(2 * treepos) + BIT] = nodefilled + tree->numcodes;
                    treepos = nodefilled;
                }
            } else {
                {
                    treepos = tree->tree2d[(2 * treepos) + BIT] - tree->numcodes;
                }
            }
        }
    }

    for (n = 0; n < tree->numcodes * 2; ++n) {
        if (tree->tree2d[n] == 32767) {
            tree->tree2d[n] = 0; /*remove possible remaining 32767's*/
        }
    }

    return 0;
}

/*
Second step for the ...makeFromLengths and ...makeFromFrequencies functions.
numcodes, lengths and maxbitlen must already be filled in correctly. return
value is error.
*/
static unsigned huffman_tree_make_from_lengths2(HuffmanTree* tree) {
    uivector blcount;
    uivector nextcode;
    unsigned error = 0;
    unsigned bits = 0;
    unsigned n = 0;

    uivector_init(&blcount);
    uivector_init(&nextcode);

    tree->tree1d = static_cast<unsigned*>(lodepng_malloc(tree->numcodes * sizeof(unsigned)));
    if (tree->tree1d == nullptr) {
        error = 83; /*alloc fail*/
    }

    if ((uivector_resizev(&blcount, tree->maxbitlen + 1, 0) == 0U) || (uivector_resizev(&nextcode, tree->maxbitlen + 1, 0) == 0U)) {
        error = 83; /*alloc fail*/
    }

    if (error == 0U) {
        /*step 1: count number of instances of each code length*/
        for (bits = 0; bits != tree->numcodes; ++bits) {
            ++blcount.data[tree->lengths[bits]];
        }
        /*step 2: generate the nextcode values*/
        for (bits = 1; bits <= tree->maxbitlen; ++bits) {
            nextcode.data[bits] = (nextcode.data[bits - 1] + blcount.data[bits - 1]) << 1;
        }
        /*step 3: generate all the codes*/
        for (n = 0; n != tree->numcodes; ++n) {
            if (tree->lengths[n] != 0) {
                tree->tree1d[n] = nextcode.data[tree->lengths[n]]++;
            }
        }
    }

    uivector_cleanup(&blcount);
    uivector_cleanup(&nextcode);

    if (error == 0U) {
        return huffman_tree_make2_d_tree(tree);
    }
    return error;
}

/*
given the code lengths (as stored in the PNG file), generate the tree as defined
by Deflate. maxbitlen is the maximum bits that a code in the tree can have.
return value is error.
*/
static unsigned huffman_tree_make_from_lengths(HuffmanTree* tree, const unsigned* bitlen, size_t numcodes, unsigned maxbitlen) {
    unsigned i = 0;
    tree->lengths = static_cast<unsigned*>(lodepng_malloc(numcodes * sizeof(unsigned)));
    if (tree->lengths == nullptr) {
        return 83; /*alloc fail*/
    }
    for (i = 0; i != numcodes; ++i) {
        tree->lengths[i] = bitlen[i];
    }
    tree->numcodes = static_cast<unsigned>(numcodes); /*number of symbols*/
    tree->maxbitlen = maxbitlen;
    return huffman_tree_make_from_lengths2(tree);
}

#ifdef LODEPNG_COMPILE_ENCODER

/*BPM: Boundary Package Merge, see "A Fast and Space-Economical Algorithm for Length-Limited Coding",
Jyrki Katajainen, Alistair Moffat, Andrew Turpin, 1995.*/

/*chain node for boundary package merge*/
using BPMNode = struct BPMNode {
    int weight;           /*the sum of all weights in this chain*/
    unsigned index;       /*index of this leaf node (called "count" in the paper)*/
    struct BPMNode* tail; /*the next nodes in this chain (null if last)*/
    int in_use;
};

/*lists of chains*/
using BPMLists = struct BPMLists {
    /*memory pool*/
    unsigned memsize;
    BPMNode* memory;
    unsigned numfree;
    unsigned nextfree;
    BPMNode** freelist;
    /*two heads of lookahead chains per list*/
    unsigned listsize;
    BPMNode** chains0;
    BPMNode** chains1;
};

/*creates a new chain node with the given parameters, from the memory in the lists */
static BPMNode* bpmnode_create(BPMLists* lists, int weight, unsigned index, BPMNode* tail) {
    unsigned i = 0;
    BPMNode* result = nullptr;

    /*memory full, so garbage collect*/
    if (lists->nextfree >= lists->numfree) {
        /*mark only those that are in use*/
        for (i = 0; i != lists->memsize; ++i) {
            lists->memory[i].in_use = 0;
        }
        for (i = 0; i != lists->listsize; ++i) {
            BPMNode* node = nullptr;
            for (node = lists->chains0[i]; node != nullptr; node = node->tail) {
                node->in_use = 1;
            }
            for (node = lists->chains1[i]; node != nullptr; node = node->tail) {
                node->in_use = 1;
            }
        }
        /*collect those that are free*/
        lists->numfree = 0;
        for (i = 0; i != lists->memsize; ++i) {
            if (lists->memory[i].in_use == 0) {
                lists->freelist[lists->numfree++] = &lists->memory[i];
            }
        }
        lists->nextfree = 0;
    }

    result = lists->freelist[lists->nextfree++];
    result->weight = weight;
    result->index = index;
    result->tail = tail;
    return result;
}

static int bpmnode_compare(const void* a, const void* b) {
    int const WA = (static_cast<const BPMNode*>(a))->weight;
    int const WB = (static_cast<const BPMNode*>(b))->weight;
    if (WA < WB) {
        return -1;
    }
    if (WA > WB) {
        return 1;
    }
    /*make the qsort a stable sort*/
    return (static_cast<const BPMNode*>(a))->index < (static_cast<const BPMNode*>(b))->index ? 1 : -1;
}

/*Boundary Package Merge step, numpresent is the amount of leaves, and c is the current chain.*/
static void boundary_pm(BPMLists* lists, BPMNode* leaves, size_t numpresent, int c, int num) {
    unsigned const LASTINDEX = lists->chains1[c]->index;

    if (c == 0) {
        if (LASTINDEX >= numpresent) {
            return;
        }
        lists->chains0[c] = lists->chains1[c];
        lists->chains1[c] = bpmnode_create(lists, leaves[LASTINDEX].weight, LASTINDEX + 1, nullptr);
    } else {
        /*sum of the weights of the head nodes of the previous lookahead chains.*/
        int const SUM = lists->chains0[c - 1]->weight + lists->chains1[c - 1]->weight;
        lists->chains0[c] = lists->chains1[c];
        if (LASTINDEX < numpresent && SUM > leaves[LASTINDEX].weight) {
            lists->chains1[c] = bpmnode_create(lists, leaves[LASTINDEX].weight, LASTINDEX + 1, lists->chains1[c]->tail);
            return;
        }
        lists->chains1[c] = bpmnode_create(lists, SUM, LASTINDEX, lists->chains1[c - 1]);
        /*in the end we are only interested in the chain of the last list, so no
        need to recurse if we're at the last one (this gives measurable speedup)*/
        if (num + 1 < static_cast<int>((2 * numpresent) - 2)) {
            boundary_pm(lists, leaves, numpresent, c - 1, num);
            boundary_pm(lists, leaves, numpresent, c - 1, num);
        }
    }
}

unsigned lodepng_huffman_code_lengths(unsigned* lengths, const unsigned* frequencies, size_t numcodes, unsigned maxbitlen) {
    unsigned error = 0;
    unsigned i = 0;
    size_t numpresent = 0;     /*number of symbols with non-zero frequency*/
    BPMNode* leaves = nullptr; /*the symbols, only those with > 0 frequency*/

    if (numcodes == 0) {
        return 80; /*error: a tree of 0 symbols is not supposed to be made*/
    }
    if ((1U << maxbitlen) < numcodes) {
        return 80; /*error: represent all symbols*/
    }

    leaves = static_cast<BPMNode*>(lodepng_malloc(numcodes * sizeof(*leaves)));
    if (leaves == nullptr) {
        return 83; /*alloc fail*/
    }

    for (i = 0; i != numcodes; ++i) {
        if (frequencies[i] > 0) {
            leaves[numpresent].weight = frequencies[i];
            leaves[numpresent].index = i;
            ++numpresent;
        }
    }

    for (i = 0; i != numcodes; ++i) {
        lengths[i] = 0;
    }

    /*ensure at least two present symbols. There should be at least one symbol
    according to RFC 1951 section 3.2.7. Some decoders incorrectly require two. To
    make these work as well ensure there are at least two symbols. The
    Package-Merge code below also doesn't work correctly if there's only one
    symbol, it'd give it the theoritical 0 bits but in practice zlib wants 1 bit*/
    if (numpresent == 0) {
        lengths[0] = lengths[1] = 1; /*note that for RFC 1951 section 3.2.7, only lengths[0] = 1 is needed*/
    } else if (numpresent == 1) {
        lengths[leaves[0].index] = 1;
        lengths[leaves[0].index == 0 ? 1 : 0] = 1;
    } else {
        BPMLists lists;
        BPMNode const* node = nullptr;

        qsort(leaves, numpresent, sizeof(BPMNode), bpmnode_compare);

        lists.listsize = maxbitlen;
        lists.memsize = 2 * maxbitlen * (maxbitlen + 1);
        lists.nextfree = 0;
        lists.numfree = lists.memsize;
        lists.memory = static_cast<BPMNode*>(lodepng_malloc(lists.memsize * sizeof(*lists.memory)));
        lists.freelist = static_cast<BPMNode**>(lodepng_malloc(lists.memsize * sizeof(BPMNode*)));
        lists.chains0 = static_cast<BPMNode**>(lodepng_malloc(lists.listsize * sizeof(BPMNode*)));
        lists.chains1 = static_cast<BPMNode**>(lodepng_malloc(lists.listsize * sizeof(BPMNode*)));
        if ((lists.memory == nullptr) || (lists.freelist == nullptr) || (lists.chains0 == nullptr) || (lists.chains1 == nullptr)) {
            error = 83; /*alloc fail*/
        }

        if (error == 0U) {
            for (i = 0; i != lists.memsize; ++i) {
                lists.freelist[i] = &lists.memory[i];
            }

            bpmnode_create(&lists, leaves[0].weight, 1, nullptr);
            bpmnode_create(&lists, leaves[1].weight, 2, nullptr);

            for (i = 0; i != lists.listsize; ++i) {
                lists.chains0[i] = &lists.memory[0];
                lists.chains1[i] = &lists.memory[1];
            }

            /*each boundaryPM call adds one chain to the last list, and we need 2 * numpresent - 2 chains.*/
            for (i = 2; i != (2 * numpresent) - 2; ++i) {
                boundary_pm(&lists, leaves, numpresent, maxbitlen - 1, i);
            }

            for (node = lists.chains1[maxbitlen - 1]; node != nullptr; node = node->tail) {
                for (i = 0; i != node->index; ++i) {
                    ++lengths[leaves[i].index];
                }
            }
        }

        lodepng_free(lists.memory);
        lodepng_free(lists.freelist);
        lodepng_free(lists.chains0);
        lodepng_free(lists.chains1);
    }

    lodepng_free(leaves);
    return error;
}

/*Create the Huffman tree given the symbol frequencies*/
static unsigned huffman_tree_make_from_frequencies(HuffmanTree* tree, const unsigned* frequencies, size_t mincodes, size_t numcodes,
                                                   unsigned maxbitlen) {
    unsigned error = 0;
    while ((frequencies[numcodes - 1] == 0U) && numcodes > mincodes) {
        --numcodes; /*trim zeroes*/
    }
    tree->maxbitlen = maxbitlen;
    tree->numcodes = static_cast<unsigned>(numcodes); /*number of symbols*/
    tree->lengths = static_cast<unsigned*>(lodepng_realloc(tree->lengths, numcodes * sizeof(unsigned)));
    if (tree->lengths == nullptr) {
        return 83; /*alloc fail*/
    }
    /*initialize all lengths to 0*/
    memset(tree->lengths, 0, numcodes * sizeof(unsigned));

    error = lodepng_huffman_code_lengths(tree->lengths, frequencies, numcodes, maxbitlen);
    if (error == 0U) {
        error = huffman_tree_make_from_lengths2(tree);
    }
    return error;
}

static unsigned huffman_tree_get_code(const HuffmanTree* tree, unsigned index) { return tree->tree1d[index]; }

static unsigned huffman_tree_get_length(const HuffmanTree* tree, unsigned index) { return tree->lengths[index]; }
#endif /*LODEPNG_COMPILE_ENCODER*/

/*get the literal and length code tree of a deflated block with fixed tree, as per the deflate specification*/
static unsigned generate_fixed_lit_len_tree(HuffmanTree* tree) {
    unsigned i = 0;
    unsigned error = 0;
    auto* bitlen = static_cast<unsigned*>(lodepng_malloc(NUM_DEFLATE_CODE_SYMBOLS * sizeof(unsigned)));
    if (bitlen == nullptr) {
        return 83; /*alloc fail*/
    }

    /*288 possible codes: 0-255=literals, 256=endcode, 257-285=lengthcodes, 286-287=unused*/
    for (i = 0; i <= 143; ++i) {
        bitlen[i] = 8;
    }
    for (i = 144; i <= 255; ++i) {
        bitlen[i] = 9;
    }
    for (i = 256; i <= 279; ++i) {
        bitlen[i] = 7;
    }
    for (i = 280; i <= 287; ++i) {
        bitlen[i] = 8;
    }

    error = huffman_tree_make_from_lengths(tree, bitlen, NUM_DEFLATE_CODE_SYMBOLS, 15);

    lodepng_free(bitlen);
    return error;
}

/*get the distance code tree of a deflated block with fixed tree, as specified in the deflate specification*/
static unsigned generate_fixed_distance_tree(HuffmanTree* tree) {
    unsigned i = 0;
    unsigned error = 0;
    auto* bitlen = static_cast<unsigned*>(lodepng_malloc(NUM_DISTANCE_SYMBOLS * sizeof(unsigned)));
    if (bitlen == nullptr) {
        return 83; /*alloc fail*/
    }

    /*there are 32 distance codes, but 30-31 are unused*/
    for (i = 0; i != NUM_DISTANCE_SYMBOLS; ++i) {
        bitlen[i] = 5;
    }
    error = huffman_tree_make_from_lengths(tree, bitlen, NUM_DISTANCE_SYMBOLS, 15);

    lodepng_free(bitlen);
    return error;
}

#ifdef LODEPNG_COMPILE_DECODER

/*
returns the code, or (unsigned)(-1) if error happened
inbitlength is the length of the complete buffer, in bits (so its byte length times 8)
*/
static unsigned huffman_decode_symbol(const unsigned char* in, size_t* bp, const HuffmanTree* codetree, size_t inbitlength) {
    unsigned treepos = 0;
    unsigned ct = 0;
    for (;;) {
        if (*bp >= inbitlength) {
            return static_cast<unsigned>(-1); /*error: end of input memory reached without endcode*/
        }
        /*
        decode the symbol from the tree. The "readBitFromStream" code is inlined in
        the expression below because this is the biggest bottleneck while decoding
        */
        ct = codetree->tree2d[(treepos << 1) + READBIT(*bp, in)];
        ++(*bp);
        if (ct < codetree->numcodes) {
            return ct; /*the symbol is decoded, return it*/
        }
        treepos = ct - codetree->numcodes; /*symbol not yet decoded, instead move tree position*/

        if (treepos >= codetree->numcodes) {
            return static_cast<unsigned>(-1); /*error: it appeared outside the codetree*/
        }
    }
}
#endif /*LODEPNG_COMPILE_DECODER*/

#ifdef LODEPNG_COMPILE_DECODER

/* ////////////////////////////////////////////////////////////////////////// */
/* / Inflator (Decompressor)                                                / */
/* ////////////////////////////////////////////////////////////////////////// */

/*get the tree of a deflated block with fixed tree, as specified in the deflate specification*/
static void get_tree_inflate_fixed(HuffmanTree* tree_ll, HuffmanTree* tree_d) {
    /*TODO: check for out of memory errors*/
    generate_fixed_lit_len_tree(tree_ll);
    generate_fixed_distance_tree(tree_d);
}

/*get the tree of a deflated block with dynamic tree, the tree itself is also Huffman compressed with a known tree*/
static unsigned get_tree_inflate_dynamic(HuffmanTree* tree_ll, HuffmanTree* tree_d, const unsigned char* in, size_t* bp, size_t inlength) {
    /*make sure that length values that aren't filled in will be 0, or a wrong tree will be generated*/
    unsigned error = 0;
    unsigned n = 0;
    unsigned hlit = 0;
    unsigned hdist = 0;
    unsigned hclen = 0;
    unsigned i = 0;
    size_t const INBITLENGTH = inlength * 8;

    /*see comments in deflateDynamic for explanation of the context and these variables, it is analogous*/
    unsigned* bitlen_ll = nullptr; /*lit,len code lengths*/
    unsigned* bitlen_d = nullptr;  /*dist code lengths*/
    /*code length code lengths ("clcl"), the bit lengths of the huffman tree used to compress bitlen_ll and bitlen_d*/
    unsigned* bitlen_cl = nullptr;
    HuffmanTree tree_cl; /*the code tree for code length codes (the huffman tree for compressed huffman trees)*/

    if ((*bp) + 14 > (inlength << 3)) {
        return 49; /*error: the bit pointer is or will go past the memory*/
    }

    /*number of literal/length codes + 257. Unlike the spec, the value 257 is added to it here already*/
    hlit = read_bits_from_stream(bp, in, 5) + 257;
    /*number of distance codes. Unlike the spec, the value 1 is added to it here already*/
    hdist = read_bits_from_stream(bp, in, 5) + 1;
    /*number of code length codes. Unlike the spec, the value 4 is added to it here already*/
    hclen = read_bits_from_stream(bp, in, 4) + 4;

    if ((*bp) + (hclen * 3) > (inlength << 3)) {
        return 50; /*error: the bit pointer is or will go past the memory*/
    }

    huffman_tree_init(&tree_cl);

    while (error == 0U) {
        /*read the code length codes out of 3 * (amount of code length codes) bits*/

        bitlen_cl = static_cast<unsigned*>(lodepng_malloc(NUM_CODE_LENGTH_CODES * sizeof(unsigned)));
        if (bitlen_cl == nullptr) ERROR_BREAK(83 /*alloc fail*/);

        for (i = 0; i != NUM_CODE_LENGTH_CODES; ++i) {
            if (i < hclen) {
                bitlen_cl[CLCL_ORDER[i]] = read_bits_from_stream(bp, in, 3);
            } else {
                bitlen_cl[CLCL_ORDER[i]] = 0; /*if not, it must stay 0*/
            }
        }

        error = huffman_tree_make_from_lengths(&tree_cl, bitlen_cl, NUM_CODE_LENGTH_CODES, 7);
        if (error != 0U) {
            break;
        }

        /*now we can use this tree to read the lengths for the tree that this function will return*/
        bitlen_ll = static_cast<unsigned*>(lodepng_malloc(NUM_DEFLATE_CODE_SYMBOLS * sizeof(unsigned)));
        bitlen_d = static_cast<unsigned*>(lodepng_malloc(NUM_DISTANCE_SYMBOLS * sizeof(unsigned)));
        if ((bitlen_ll == nullptr) || (bitlen_d == nullptr)) ERROR_BREAK(83 /*alloc fail*/);
        for (i = 0; i != NUM_DEFLATE_CODE_SYMBOLS; ++i) {
            bitlen_ll[i] = 0;
        }
        for (i = 0; i != NUM_DISTANCE_SYMBOLS; ++i) {
            bitlen_d[i] = 0;
        }

        /*i is the current symbol we're reading in the part that contains the code lengths of lit/len and dist codes*/
        i = 0;
        while (i < hlit + hdist) {
            unsigned const CODE = huffman_decode_symbol(in, bp, &tree_cl, INBITLENGTH);
            if (CODE <= 15) /*a length code*/
            {
                if (i < hlit) {
                    bitlen_ll[i] = CODE;
                } else {
                    bitlen_d[i - hlit] = CODE;
                }
                ++i;
            } else if (CODE == 16) /*repeat previous*/
            {
                unsigned replength = 3; /*read in the 2 bits that indicate repeat length (3-6)*/
                unsigned value = 0;     /*set value to the previous code*/

                if (i == 0) ERROR_BREAK(54); /*can't repeat previous if i is 0*/

                if ((*bp + 2) > INBITLENGTH) ERROR_BREAK(50); /*error, bit pointer jumps past memory*/
                replength += read_bits_from_stream(bp, in, 2);

                if (i < hlit + 1) {
                    value = bitlen_ll[i - 1];
                } else {
                    value = bitlen_d[i - hlit - 1];
                }
                /*repeat this value in the next lengths*/
                for (n = 0; n < replength; ++n) {
                    if (i >= hlit + hdist) ERROR_BREAK(13); /*error: i is larger than the amount of codes*/
                    if (i < hlit) {
                        bitlen_ll[i] = value;
                    } else {
                        bitlen_d[i - hlit] = value;
                    }
                    ++i;
                }
            } else if (CODE == 17) /*repeat "0" 3-10 times*/
            {
                unsigned replength = 3;                       /*read in the bits that indicate repeat length*/
                if ((*bp + 3) > INBITLENGTH) ERROR_BREAK(50); /*error, bit pointer jumps past memory*/
                replength += read_bits_from_stream(bp, in, 3);

                /*repeat this value in the next lengths*/
                for (n = 0; n < replength; ++n) {
                    if (i >= hlit + hdist) ERROR_BREAK(14); /*error: i is larger than the amount of codes*/

                    if (i < hlit) {
                        bitlen_ll[i] = 0;
                    } else {
                        bitlen_d[i - hlit] = 0;
                    }
                    ++i;
                }
            } else if (CODE == 18) /*repeat "0" 11-138 times*/
            {
                unsigned replength = 11;                      /*read in the bits that indicate repeat length*/
                if ((*bp + 7) > INBITLENGTH) ERROR_BREAK(50); /*error, bit pointer jumps past memory*/
                replength += read_bits_from_stream(bp, in, 7);

                /*repeat this value in the next lengths*/
                for (n = 0; n < replength; ++n) {
                    if (i >= hlit + hdist) ERROR_BREAK(15); /*error: i is larger than the amount of codes*/

                    if (i < hlit) {
                        bitlen_ll[i] = 0;
                    } else {
                        bitlen_d[i - hlit] = 0;
                    }
                    ++i;
                }
            } else /*if(code == (unsigned)(-1))*/ /*huffmanDecodeSymbol returns (unsigned)(-1) in case of error*/
            {
                if (std::cmp_equal(CODE, (-1))) {
                    /*return error code 10 or 11 depending on the situation that happened in huffmanDecodeSymbol
                    (10=no endcode, 11=wrong jump outside of tree)*/
                    error = (*bp) > INBITLENGTH ? 10 : 11;
                } else {
                    {
                        error = 16; /*unexisting code, this can never happen*/
                    }
                }
                break;
            }
        }
        if (error != 0U) {
            break;
        }

        if (bitlen_ll[256] == 0) ERROR_BREAK(64); /*the length of the end code 256 must be larger than 0*/

        /*now we've finally got HLIT and HDIST, so generate the code trees, and the function is done*/
        error = huffman_tree_make_from_lengths(tree_ll, bitlen_ll, NUM_DEFLATE_CODE_SYMBOLS, 15);
        if (error != 0U) {
            break;
        }
        error = huffman_tree_make_from_lengths(tree_d, bitlen_d, NUM_DISTANCE_SYMBOLS, 15);

        break; /*end of error-while*/
    }

    lodepng_free(bitlen_cl);
    lodepng_free(bitlen_ll);
    lodepng_free(bitlen_d);
    huffman_tree_cleanup(&tree_cl);

    return error;
}

/*inflate a block with dynamic of fixed Huffman tree*/
static unsigned inflate_huffman_block(ucvector* out, const unsigned char* in, size_t* bp, size_t* pos, size_t inlength, unsigned btype) {
    unsigned error = 0;
    HuffmanTree tree_ll; /*the huffman tree for literal and length codes*/
    HuffmanTree tree_d;  /*the huffman tree for distance codes*/
    size_t const INBITLENGTH = inlength * 8;

    huffman_tree_init(&tree_ll);
    huffman_tree_init(&tree_d);

    if (btype == 1) {
        get_tree_inflate_fixed(&tree_ll, &tree_d);
    } else if (btype == 2) {
        error = get_tree_inflate_dynamic(&tree_ll, &tree_d, in, bp, inlength);
    }

    while (error == 0U) /*decode all symbols until end reached, breaks at end code*/
    {
        /*code_ll is literal, length or end code*/
        unsigned const CODE_LL = huffman_decode_symbol(in, bp, &tree_ll, INBITLENGTH);
        if (CODE_LL <= 255) /*literal symbol*/
        {
            /*ucvector_push_back would do the same, but for some reason the two lines below run 10% faster*/
            if (ucvector_resize(out, (*pos) + 1) == 0U) ERROR_BREAK(83 /*alloc fail*/);
            out->data[*pos] = static_cast<unsigned char>(CODE_LL);
            ++(*pos);
        } else if (CODE_LL >= FIRST_LENGTH_CODE_INDEX && CODE_LL <= LAST_LENGTH_CODE_INDEX) /*length code*/
        {
            unsigned code_d = 0;
            unsigned distance = 0;
            unsigned numextrabits_l = 0;
            unsigned numextrabits_d = 0; /*extra bits for length and distance*/
            size_t start = 0;
            size_t forward = 0;
            size_t backward = 0;
            size_t length = 0;

            /*part 1: get length base*/
            length = LENGTHBASE[CODE_LL - FIRST_LENGTH_CODE_INDEX];

            /*part 2: get extra bits and add the value of that to length*/
            numextrabits_l = LENGTHEXTRA[CODE_LL - FIRST_LENGTH_CODE_INDEX];
            if ((*bp + numextrabits_l) > INBITLENGTH) ERROR_BREAK(51); /*error, bit pointer will jump past memory*/
            length += read_bits_from_stream(bp, in, numextrabits_l);

            /*part 3: get distance code*/
            code_d = huffman_decode_symbol(in, bp, &tree_d, INBITLENGTH);
            if (code_d > 29) {
                if (std::cmp_equal(CODE_LL, (-1))) /*huffmanDecodeSymbol returns (unsigned)(-1) in case of error*/
                {
                    /*return error code 10 or 11 depending on the situation that happened in huffmanDecodeSymbol
                    (10=no endcode, 11=wrong jump outside of tree)*/
                    error = (*bp) > inlength * 8 ? 10 : 11;
                } else {
                    {
                        error = 18; /*error: invalid distance code (30-31 are never used)*/
                    }
                }
                break;
            }
            distance = DISTANCEBASE[code_d];

            /*part 4: get extra bits from distance*/
            numextrabits_d = DISTANCEEXTRA[code_d];
            if ((*bp + numextrabits_d) > INBITLENGTH) ERROR_BREAK(51); /*error, bit pointer will jump past memory*/
            distance += read_bits_from_stream(bp, in, numextrabits_d);

            /*part 5: fill in all the out[n] values based on the length and dist*/
            start = (*pos);
            if (distance > start) ERROR_BREAK(52); /*too long backward distance*/
            backward = start - distance;

            if (ucvector_resize(out, (*pos) + length) == 0U) ERROR_BREAK(83 /*alloc fail*/);
            if (distance < length) {
                for (forward = 0; forward < length; ++forward) {
                    out->data[(*pos)++] = out->data[backward++];
                }
            } else {
                memcpy(out->data + *pos, out->data + backward, length);
                *pos += length;
            }
        } else if (CODE_LL == 256) {
            break;                            /*end code, break the loop*/
        } else /*if(code == (unsigned)(-1))*/ /*huffmanDecodeSymbol returns (unsigned)(-1) in case of error*/
        {
            /*return error code 10 or 11 depending on the situation that happened in huffmanDecodeSymbol
            (10=no endcode, 11=wrong jump outside of tree)*/
            error = ((*bp) > inlength * 8) ? 10 : 11;
            break;
        }
    }

    huffman_tree_cleanup(&tree_ll);
    huffman_tree_cleanup(&tree_d);

    return error;
}

static unsigned inflate_no_compression(ucvector* out, const unsigned char* in, size_t* bp, size_t* pos, size_t inlength) {
    size_t p = 0;
    unsigned len = 0;
    unsigned nlen = 0;
    unsigned n = 0;
    unsigned const ERROR = 0;

    /*go to first boundary of byte*/
    while (((*bp) & 0x7) != 0) {
        ++(*bp);
    }
    p = (*bp) / 8; /*byte position*/

    /*read LEN (2 bytes) and NLEN (2 bytes)*/
    if (p + 4 >= inlength) {
        return 52; /*error, bit pointer will jump past memory*/
    }
    len = in[p] + (256U * in[p + 1]);
    p += 2;
    nlen = in[p] + (256U * in[p + 1]);
    p += 2;

    /*check if 16-bit NLEN is really the one's complement of LEN*/
    if (len + nlen != 65535) {
        return 21; /*error: NLEN is not one's complement of LEN*/
    }

    if (ucvector_resize(out, (*pos) + len) == 0U) {
        return 83; /*alloc fail*/
    }

    /*read the literal data: LEN bytes are now stored in the out buffer*/
    if (p + len > inlength) {
        return 23; /*error: reading outside of in buffer*/
    }
    for (n = 0; n < len; ++n) {
        out->data[(*pos)++] = in[p++];
    }

    (*bp) = p * 8;

    return ERROR;
}

static unsigned lodepng_inflatev(ucvector* out, const unsigned char* in, size_t insize, const LodePNGDecompressSettings* settings) {
    /*bit pointer in the "in" data, current byte is bp >> 3, current bit is bp & 0x7 (from lsb to msb of the byte)*/
    size_t bp = 0;
    unsigned bfinal = 0;
    size_t pos = 0; /*byte position in the out buffer*/
    unsigned error = 0;

    static_cast<void>(settings);

    while (bfinal == 0U) {
        unsigned btype = 0;
        if (bp + 2 >= insize * 8) {
            return 52; /*error, bit pointer will jump past memory*/
        }
        bfinal = read_bit_from_stream(&bp, in);
        btype = 1U * read_bit_from_stream(&bp, in);
        btype += 2U * read_bit_from_stream(&bp, in);

        if (btype == 3) {
            return 20; /*error: invalid BTYPE*/
        }
        if (btype == 0) {
            error = inflate_no_compression(out, in, &bp, &pos, insize); /*no compression*/
        } else {
            error = inflate_huffman_block(out, in, &bp, &pos, insize, btype); /*compression, BTYPE 01 or 10*/
        }

        if (error != 0U) {
            return error;
        }
    }

    return error;
}

unsigned lodepng_inflate(unsigned char** out, size_t* outsize, const unsigned char* in, size_t insize,
                         const LodePNGDecompressSettings* settings) {
    unsigned error = 0;
    ucvector v;
    ucvector_init_buffer(&v, *out, *outsize);
    error = lodepng_inflatev(&v, in, insize, settings);
    *out = v.data;
    *outsize = v.size;
    return error;
}

static unsigned inflate(unsigned char** out, size_t* outsize, const unsigned char* in, size_t insize,
                        const LodePNGDecompressSettings* settings) {
    if (settings->custom_inflate != nullptr) {
        return settings->custom_inflate(out, outsize, in, insize, settings);
    }
    return lodepng_inflate(out, outsize, in, insize, settings);
}

#endif /*LODEPNG_COMPILE_DECODER*/

#ifdef LODEPNG_COMPILE_ENCODER

/* ////////////////////////////////////////////////////////////////////////// */
/* / Deflator (Compressor)                                                  / */
/* ////////////////////////////////////////////////////////////////////////// */

static constexpr size_t MAX_SUPPORTED_DEFLATE_LENGTH = 258;

/*bitlen is the size in bits of the code*/
static void add_huffman_symbol(size_t* bp, ucvector* compressed, unsigned code, unsigned bitlen) {
    add_bits_to_stream_reversed(bp, compressed, code, bitlen);
}

/*search the index in the array, that has the largest value smaller than or equal to the given value,
given array must be sorted (if no value is smaller, it returns the size of the given array)*/
template <size_t Size>
static size_t search_code_index(const std::array<unsigned, Size>& array, size_t value) {
    /*linear search implementation*/
    /*for(size_t i = 1; i < array_size; ++i) if(array[i] > value) return i - 1;
    return array_size - 1;*/

    /*binary search implementation (not that much faster) (precondition: array_size > 0)*/
    size_t left = 1;
    size_t right = array.size() - 1;
    while (left <= right) {
        size_t const MID = (left + right) / 2;
        if (array[MID] <= value) {
            left = MID + 1; /*the value to find is more to the right*/
        } else if (array[MID - 1] > value) {
            right = MID - 1; /*the value to find is more to the left*/
        } else {
            return MID - 1;
        }
    }
    return array.size() - 1;
}

static void add_length_distance(uivector* values, size_t length, size_t distance) {
    /*values in encoded vector are those used by deflate:
    0-255: literal bytes
    256: end
    257-285: length/distance pair (length code, followed by extra length bits, distance code, extra distance bits)
    286-287: invalid*/

    auto const LENGTH_CODE = static_cast<unsigned>(search_code_index(LENGTHBASE, length));
    auto const EXTRA_LENGTH = static_cast<unsigned>(length - LENGTHBASE[LENGTH_CODE]);
    auto const DIST_CODE = static_cast<unsigned>(search_code_index(DISTANCEBASE, distance));
    auto const EXTRA_DISTANCE = static_cast<unsigned>(distance - DISTANCEBASE[DIST_CODE]);

    uivector_push_back(values, LENGTH_CODE + FIRST_LENGTH_CODE_INDEX);
    uivector_push_back(values, EXTRA_LENGTH);
    uivector_push_back(values, DIST_CODE);
    uivector_push_back(values, EXTRA_DISTANCE);
}

/*3 bytes of data get encoded into two bytes. The hash cannot use more than 3
bytes as input because 3 is the minimum match length for deflate*/
static constexpr unsigned HASH_NUM_VALUES = 65536;
static constexpr unsigned HASH_BIT_MASK = 65535; /*HASH_NUM_VALUES - 1, but C90 does not like that as initializer*/

using Hash = struct Hash {
    int* head; /*hash value to head circular pos - can be outdated if went around window*/
    /*circular pos to prev circular pos*/
    unsigned short* chain;
    int* val; /*circular pos to hash value*/

    /*TODO: do this not only for zeros but for any repeated byte. However for PNG
    it's always going to be the zeros that dominate, so not important for PNG*/
    int* headz;             /*similar to head, but for chainz*/
    unsigned short* chainz; /*those with same amount of zeros*/
    unsigned short* zeros;  /*length of zeros streak, used as a second hash chain*/
};

static unsigned hash_init(Hash* hash, unsigned windowsize) {
    unsigned i = 0;
    hash->head = static_cast<int*>(lodepng_malloc(sizeof(int) * HASH_NUM_VALUES));
    hash->val = static_cast<int*>(lodepng_malloc(sizeof(int) * windowsize));
    hash->chain = static_cast<unsigned short*>(lodepng_malloc(sizeof(unsigned short) * windowsize));

    hash->zeros = static_cast<unsigned short*>(lodepng_malloc(sizeof(unsigned short) * windowsize));
    hash->headz = static_cast<int*>(lodepng_malloc(sizeof(int) * (MAX_SUPPORTED_DEFLATE_LENGTH + 1)));
    hash->chainz = static_cast<unsigned short*>(lodepng_malloc(sizeof(unsigned short) * windowsize));

    if ((hash->head == nullptr) || (hash->chain == nullptr) || (hash->val == nullptr) || (hash->headz == nullptr) ||
        (hash->chainz == nullptr) || (hash->zeros == nullptr)) {
        return 83; /*alloc fail*/
    }

    /*initialize hash table*/
    for (i = 0; i != HASH_NUM_VALUES; ++i) {
        hash->head[i] = -1;
    }
    for (i = 0; i != windowsize; ++i) {
        hash->val[i] = -1;
    }
    for (i = 0; i != windowsize; ++i) {
        hash->chain[i] = i; /*same value as index indicates uninitialized*/
    }

    for (i = 0; i <= MAX_SUPPORTED_DEFLATE_LENGTH; ++i) {
        hash->headz[i] = -1;
    }
    for (i = 0; i != windowsize; ++i) {
        hash->chainz[i] = i; /*same value as index indicates uninitialized*/
    }

    return 0;
}

static void hash_cleanup(Hash* hash) {
    lodepng_free(hash->head);
    lodepng_free(hash->val);
    lodepng_free(hash->chain);

    lodepng_free(hash->zeros);
    lodepng_free(hash->headz);
    lodepng_free(hash->chainz);
}

static unsigned get_hash(const unsigned char* data, size_t size, size_t pos) {
    unsigned result = 0;
    if (pos + 2 < size) {
        /*A simple shift and xor hash is used. Since the data of PNGs is dominated
        by zeroes due to the filters, a better hash does not have a significant
        effect on speed in traversing the chain, and causes more time spend on
        calculating the hash.*/
        result ^= static_cast<unsigned>(data[pos + 0] << 0U);
        result ^= static_cast<unsigned>(data[pos + 1] << 4U);
        result ^= static_cast<unsigned>(data[pos + 2] << 8U);
    } else {
        size_t amount = 0;
        size_t i = 0;
        if (pos >= size) {
            return 0;
        }
        amount = size - pos;
        for (i = 0; i != amount; ++i) {
            result ^= static_cast<unsigned>(data[pos + i] << (i * 8U));
        }
    }
    return result & HASH_BIT_MASK;
}

static unsigned count_zeros(const unsigned char* data, size_t size, size_t pos) {
    const unsigned char* start = data + pos;
    const unsigned char* end = start + MAX_SUPPORTED_DEFLATE_LENGTH;
    end = std::min(end, data + size);
    data = start;
    while (data != end && *data == 0) {
        ++data;
    }
    /*subtracting two addresses returned as 32-bit number (max value is MAX_SUPPORTED_DEFLATE_LENGTH)*/
    return static_cast<unsigned>(data - start);
}

/*wpos = pos & (windowsize - 1)*/
static void update_hash_chain(Hash* hash, size_t wpos, unsigned hashval, unsigned short numzeros) {
    hash->val[wpos] = static_cast<int>(hashval);
    if (hash->head[hashval] != -1) {
        hash->chain[wpos] = hash->head[hashval];
    }
    hash->head[hashval] = wpos;

    hash->zeros[wpos] = numzeros;
    if (hash->headz[numzeros] != -1) {
        hash->chainz[wpos] = hash->headz[numzeros];
    }
    hash->headz[numzeros] = wpos;
}

/*
LZ77-encode the data. Return value is error code. The input are raw bytes, the output
is in the form of unsigned integers with codes representing for example literal bytes, or
length/distance pairs.
It uses a hash table technique to let it encode faster. When doing LZ77 encoding, a
sliding window (of windowsize) is used, and all past bytes in that window can be used as
the "dictionary". A brute force search through all possible distances would be slow, and
this hash technique is one out of several ways to speed this up.
*/
static unsigned encode_l_z77(uivector* out, Hash* hash, const unsigned char* in, size_t inpos, size_t insize, unsigned windowsize,
                             unsigned minmatch, unsigned nicematch, unsigned lazymatching) {
    size_t pos = 0;
    unsigned i = 0;
    unsigned error = 0;
    /*for large window lengths, assume the user wants no compression loss. Otherwise, max hash chain length speedup.*/
    unsigned const MAXCHAINLENGTH = windowsize >= 8192 ? windowsize : windowsize / 8;
    unsigned const MAXLAZYMATCH = windowsize >= 8192 ? MAX_SUPPORTED_DEFLATE_LENGTH : 64;

    unsigned const USEZEROS = 1; /*not sure if setting it to false for windowsize < 8192 is better or worse*/
    unsigned numzeros = 0;

    unsigned offset = 0; /*the offset represents the distance in LZ77 terminology*/
    unsigned length = 0;
    unsigned lazy = 0;
    unsigned lazylength = 0;
    unsigned lazyoffset = 0;
    unsigned hashval = 0;
    unsigned current_offset = 0;
    unsigned current_length = 0;
    unsigned prev_offset = 0;
    const unsigned char* lastptr = nullptr;
    const unsigned char* foreptr = nullptr;
    const unsigned char* backptr = nullptr;
    unsigned hashpos = 0;

    if (windowsize == 0 || windowsize > 32768) {
        return 60; /*error: windowsize smaller/larger than allowed*/
    }
    if ((windowsize & (windowsize - 1)) != 0) {
        return 90; /*error: must be power of two*/
    }

    nicematch = std::min<size_t>(nicematch, MAX_SUPPORTED_DEFLATE_LENGTH);

    for (pos = inpos; pos < insize; ++pos) {
        size_t wpos = pos & (windowsize - 1); /*position for in 'circular' hash buffers*/
        unsigned chainlength = 0;

        hashval = get_hash(in, insize, pos);

        if ((USEZEROS != 0U) && hashval == 0) {
            if (numzeros == 0) {
                numzeros = count_zeros(in, insize, pos);
            } else if (pos + numzeros > insize || in[pos + numzeros - 1] != 0) {
                --numzeros;
            }
        } else {
            numzeros = 0;
        }

        update_hash_chain(hash, wpos, hashval, numzeros);

        /*the length and offset found for the current position*/
        length = 0;
        offset = 0;

        hashpos = hash->chain[wpos];

        lastptr = &in[insize < pos + MAX_SUPPORTED_DEFLATE_LENGTH ? insize : pos + MAX_SUPPORTED_DEFLATE_LENGTH];

        /*search for the longest string*/
        prev_offset = 0;
        for (;;) {
            if (chainlength++ >= MAXCHAINLENGTH) {
                break;
            }
            current_offset = hashpos <= wpos ? wpos - hashpos : wpos - hashpos + windowsize;

            if (current_offset < prev_offset) {
                break; /*stop when went completely around the circular buffer*/
            }
            prev_offset = current_offset;
            if (current_offset > 0) {
                /*test the next characters*/
                foreptr = &in[pos];
                backptr = &in[pos - current_offset];

                /*common case in PNGs is lots of zeros. Quickly skip over them as a speedup*/
                if (numzeros >= 3) {
                    unsigned skip = hash->zeros[hashpos];
                    skip = std::min(skip, numzeros);
                    backptr += skip;
                    foreptr += skip;
                }

                while (foreptr != lastptr && *backptr == *foreptr) /*maximum supported length by deflate is max length*/
                {
                    ++backptr;
                    ++foreptr;
                }
                current_length = static_cast<unsigned>(foreptr - &in[pos]);

                if (current_length > length) {
                    length = current_length; /*the longest length*/
                    offset = current_offset; /*the offset that is related to this longest length*/
                    /*jump out once a length of max length is found (speed gain). This also jumps
                    out if length is MAX_SUPPORTED_DEFLATE_LENGTH*/
                    if (current_length >= nicematch) {
                        break;
                    }
                }
            }

            if (hashpos == hash->chain[hashpos]) {
                break;
            }

            if (numzeros >= 3 && length > numzeros) {
                hashpos = hash->chainz[hashpos];
                if (hash->zeros[hashpos] != numzeros) {
                    break;
                }
            } else {
                hashpos = hash->chain[hashpos];
                /*outdated hash value, happens if particular value was not encountered in whole last window*/
                if (std::cmp_not_equal(hash->val[hashpos], hashval)) {
                    break;
                }
            }
        }

        if (lazymatching != 0U) {
            if ((lazy == 0U) && length >= 3 && length <= MAXLAZYMATCH && length < MAX_SUPPORTED_DEFLATE_LENGTH) {
                lazy = 1;
                lazylength = length;
                lazyoffset = offset;
                continue; /*try the next byte*/
            }
            if (lazy != 0U) {
                lazy = 0;
                if (pos == 0) ERROR_BREAK(81);
                if (length > lazylength + 1) {
                    /*push the previous character as literal*/
                    if (uivector_push_back(out, in[pos - 1]) == 0U) ERROR_BREAK(83 /*alloc fail*/);
                } else {
                    length = lazylength;
                    offset = lazyoffset;
                    hash->head[hashval] = -1;   /*the same hashchain update will be done, this ensures no wrong alteration*/
                    hash->headz[numzeros] = -1; /*idem*/
                    --pos;
                }
            }
        }
        if (length >= 3 && offset > windowsize) ERROR_BREAK(86 /*too big (or overflown negative) offset*/);

        /*encode it as length/distance pair or literal value*/
        if (length < 3) /*only lengths of 3 or higher are supported as length/distance pair*/
        {
            if (uivector_push_back(out, in[pos]) == 0U) ERROR_BREAK(83 /*alloc fail*/);
        } else if (length < minmatch || (length == 3 && offset > 4096)) {
            /*compensate for the fact that longer offsets have more extra bits, a
            length of only 3 may be not worth it then*/
            if (uivector_push_back(out, in[pos]) == 0U) ERROR_BREAK(83 /*alloc fail*/);
        } else {
            add_length_distance(out, length, offset);
            for (i = 1; i < length; ++i) {
                ++pos;
                wpos = pos & (windowsize - 1);
                hashval = get_hash(in, insize, pos);
                if ((USEZEROS != 0U) && hashval == 0) {
                    if (numzeros == 0) {
                        numzeros = count_zeros(in, insize, pos);
                    } else if (pos + numzeros > insize || in[pos + numzeros - 1] != 0) {
                        --numzeros;
                    }
                } else {
                    numzeros = 0;
                }
                update_hash_chain(hash, wpos, hashval, numzeros);
            }
        }
    } /*end of the loop through each character of input*/

    return error;
}

/* /////////////////////////////////////////////////////////////////////////// */

static unsigned deflate_no_compression(ucvector* out, const unsigned char* data, size_t datasize) {
    /*non compressed deflate block data: 1 bit BFINAL,2 bits BTYPE,(5 bits): it jumps to start of next byte,
    2 bytes LEN, 2 bytes NLEN, LEN bytes literal DATA*/

    size_t i = 0;
    size_t j = 0;
    size_t const NUMDEFLATEBLOCKS = (datasize + 65534) / 65535;
    unsigned datapos = 0;
    for (i = 0; i != NUMDEFLATEBLOCKS; ++i) {
        unsigned bfinal = 0;
        unsigned btype = 0;
        unsigned len = 0;
        unsigned nlen = 0;
        unsigned char firstbyte = 0;

        bfinal = static_cast<unsigned int>(i == NUMDEFLATEBLOCKS - 1);
        btype = 0;

        firstbyte = static_cast<unsigned char>(bfinal + ((btype & 1) << 1) + ((btype & 2) << 1));
        ucvector_push_back(out, firstbyte);

        len = 65535;
        if (datasize - datapos < 65535) {
            len = static_cast<unsigned>(datasize) - datapos;
        }
        nlen = 65535 - len;

        ucvector_push_back(out, static_cast<unsigned char>(len % 256));
        ucvector_push_back(out, static_cast<unsigned char>(len / 256));
        ucvector_push_back(out, static_cast<unsigned char>(nlen % 256));
        ucvector_push_back(out, static_cast<unsigned char>(nlen / 256));

        /*Decompressed data*/
        for (j = 0; j < 65535 && datapos < datasize; ++j) {
            ucvector_push_back(out, data[datapos++]);
        }
    }

    return 0;
}

/*
write the lz77-encoded data, which has lit, len and dist codes, to compressed stream using huffman trees.
tree_ll: the tree for lit and len codes.
tree_d: the tree for distance codes.
*/
static void write_l_z77data(size_t* bp, ucvector* out, const uivector* lz77_encoded, const HuffmanTree* tree_ll,
                            const HuffmanTree* tree_d) {
    size_t i = 0;
    for (i = 0; i != lz77_encoded->size; ++i) {
        unsigned const VAL = lz77_encoded->data[i];
        add_huffman_symbol(bp, out, huffman_tree_get_code(tree_ll, VAL), huffman_tree_get_length(tree_ll, VAL));
        if (VAL > 256) /*for a length code, 3 more things have to be added*/
        {
            unsigned const LENGTH_INDEX = VAL - FIRST_LENGTH_CODE_INDEX;
            unsigned const N_LENGTH_EXTRA_BITS = LENGTHEXTRA[LENGTH_INDEX];
            unsigned const LENGTH_EXTRA_BITS = lz77_encoded->data[++i];

            unsigned const DISTANCE_CODE = lz77_encoded->data[++i];

            unsigned const DISTANCE_INDEX = DISTANCE_CODE;
            unsigned const N_DISTANCE_EXTRA_BITS = DISTANCEEXTRA[DISTANCE_INDEX];
            unsigned const DISTANCE_EXTRA_BITS = lz77_encoded->data[++i];

            add_bits_to_stream(bp, out, LENGTH_EXTRA_BITS, N_LENGTH_EXTRA_BITS);
            add_huffman_symbol(bp, out, huffman_tree_get_code(tree_d, DISTANCE_CODE), huffman_tree_get_length(tree_d, DISTANCE_CODE));
            add_bits_to_stream(bp, out, DISTANCE_EXTRA_BITS, N_DISTANCE_EXTRA_BITS);
        }
    }
}

/*Deflate for a block of type "dynamic", that is, with freely, optimally, created huffman trees*/
static unsigned deflate_dynamic(ucvector* out, size_t* bp, Hash* hash, const unsigned char* data, size_t datapos, size_t dataend,
                                const LodePNGCompressSettings* settings, unsigned final) {
    unsigned error = 0;

    /*
    A block is compressed as follows: The PNG data is lz77 encoded, resulting in
    literal bytes and length/distance pairs. This is then huffman compressed with
    two huffman trees. One huffman tree is used for the lit and len values ("ll"),
    another huffman tree is used for the dist values ("d"). These two trees are
    stored using their code lengths, and to compress even more these code lengths
    are also run-length encoded and huffman compressed. This gives a huffman tree
    of code lengths "cl". The code lenghts used to describe this third tree are
    the code length code lengths ("clcl").
    */

    /*The lz77 encoded data, represented with integers since there will also be length and distance codes in it*/
    uivector lz77_encoded;
    HuffmanTree tree_ll;     /*tree for lit,len values*/
    HuffmanTree tree_d;      /*tree for distance codes*/
    HuffmanTree tree_cl;     /*tree for encoding the code lengths representing tree_ll and tree_d*/
    uivector frequencies_ll; /*frequency of lit,len codes*/
    uivector frequencies_d;  /*frequency of dist codes*/
    uivector frequencies_cl; /*frequency of code length codes*/
    uivector bitlen_lld;     /*lit,len,dist code lenghts (int bits), literally (without repeat codes).*/
    uivector bitlen_lld_e;   /*bitlen_lld encoded with repeat codes (this is a rudemtary run length compression)*/
    /*bitlen_cl is the code length code lengths ("clcl"). The bit lengths of codes to represent tree_cl
    (these are written as is in the file, it would be crazy to compress these using yet another huffman
    tree that needs to be represented by yet another set of code lengths)*/
    uivector bitlen_cl;
    size_t const DATASIZE = dataend - datapos;

    /*
    Due to the huffman compression of huffman tree representations ("two levels"), there are some anologies:
    bitlen_lld is to tree_cl what data is to tree_ll and tree_d.
    bitlen_lld_e is to bitlen_lld what lz77_encoded is to data.
    bitlen_cl is to bitlen_lld_e what bitlen_lld is to lz77_encoded.
    */

    unsigned const BFINAL = final;
    size_t numcodes_ll = 0;
    size_t numcodes_d = 0;
    size_t i = 0;
    unsigned hlit = 0;
    unsigned hdist = 0;
    unsigned hclen = 0;

    uivector_init(&lz77_encoded);
    huffman_tree_init(&tree_ll);
    huffman_tree_init(&tree_d);
    huffman_tree_init(&tree_cl);
    uivector_init(&frequencies_ll);
    uivector_init(&frequencies_d);
    uivector_init(&frequencies_cl);
    uivector_init(&bitlen_lld);
    uivector_init(&bitlen_lld_e);
    uivector_init(&bitlen_cl);

    /*This while loop never loops due to a break at the end, it is here to
    allow breaking out of it to the cleanup phase on error conditions.*/
    while (error == 0U) {
        if (settings->use_lz77 != 0U) {
            error = encode_l_z77(&lz77_encoded, hash, data, datapos, dataend, settings->windowsize, settings->minmatch, settings->nicematch,
                                 settings->lazymatching);
            if (error != 0U) {
                break;
            }
        } else {
            if (uivector_resize(&lz77_encoded, DATASIZE) == 0U) ERROR_BREAK(83 /*alloc fail*/);
            for (i = datapos; i < dataend; ++i) {
                lz77_encoded.data[i] = data[i]; /*no LZ77, but still will be Huffman compressed*/
            }
        }

        if (uivector_resizev(&frequencies_ll, 286, 0) == 0U) ERROR_BREAK(83 /*alloc fail*/);
        if (uivector_resizev(&frequencies_d, 30, 0) == 0U) ERROR_BREAK(83 /*alloc fail*/);

        /*Count the frequencies of lit, len and dist codes*/
        for (i = 0; i != lz77_encoded.size; ++i) {
            unsigned const SYMBOL = lz77_encoded.data[i];
            ++frequencies_ll.data[SYMBOL];
            if (SYMBOL > 256) {
                unsigned const DIST = lz77_encoded.data[i + 2];
                ++frequencies_d.data[DIST];
                i += 3;
            }
        }
        frequencies_ll.data[256] = 1; /*there will be exactly 1 end code, at the end of the block*/

        /*Make both huffman trees, one for the lit and len codes, one for the dist codes*/
        error = huffman_tree_make_from_frequencies(&tree_ll, frequencies_ll.data, 257, frequencies_ll.size, 15);
        if (error != 0U) {
            break;
        }
        /*2, not 1, is chosen for mincodes: some buggy PNG decoders require at least 2 symbols in the dist tree*/
        error = huffman_tree_make_from_frequencies(&tree_d, frequencies_d.data, 2, frequencies_d.size, 15);
        if (error != 0U) {
            break;
        }

        numcodes_ll = tree_ll.numcodes;
        numcodes_ll = std::min<size_t>(numcodes_ll, 286);
        numcodes_d = tree_d.numcodes;
        numcodes_d = std::min<size_t>(numcodes_d, 30);
        /*store the code lengths of both generated trees in bitlen_lld*/
        for (i = 0; i != numcodes_ll; ++i) {
            uivector_push_back(&bitlen_lld, huffman_tree_get_length(&tree_ll, static_cast<unsigned>(i)));
        }
        for (i = 0; i != numcodes_d; ++i) {
            uivector_push_back(&bitlen_lld, huffman_tree_get_length(&tree_d, static_cast<unsigned>(i)));
        }

        /*run-length compress bitlen_ldd into bitlen_lld_e by using repeat codes 16 (copy length 3-6 times),
        17 (3-10 zeroes), 18 (11-138 zeroes)*/
        for (i = 0; i != static_cast<unsigned>(bitlen_lld.size); ++i) {
            unsigned j = 0; /*amount of repititions*/
            while (i + j + 1 < static_cast<unsigned>(bitlen_lld.size) && bitlen_lld.data[i + j + 1] == bitlen_lld.data[i]) {
                ++j;
            }

            if (bitlen_lld.data[i] == 0 && j >= 2) /*repeat code for zeroes*/
            {
                ++j;         /*include the first zero*/
                if (j <= 10) /*repeat code 17 supports max 10 zeroes*/
                {
                    uivector_push_back(&bitlen_lld_e, 17);
                    uivector_push_back(&bitlen_lld_e, j - 3);
                } else /*repeat code 18 supports max 138 zeroes*/
                {
                    j = std::min<unsigned int>(j, 138);
                    uivector_push_back(&bitlen_lld_e, 18);
                    uivector_push_back(&bitlen_lld_e, j - 11);
                }
                i += (j - 1);
            } else if (j >= 3) /*repeat code for value other than zero*/
            {
                size_t k = 0;
                unsigned const NUM = j / 6;
                unsigned const REST = j % 6;
                uivector_push_back(&bitlen_lld_e, bitlen_lld.data[i]);
                for (k = 0; k < NUM; ++k) {
                    uivector_push_back(&bitlen_lld_e, 16);
                    uivector_push_back(&bitlen_lld_e, 6 - 3);
                }
                if (REST >= 3) {
                    uivector_push_back(&bitlen_lld_e, 16);
                    uivector_push_back(&bitlen_lld_e, REST - 3);
                } else {
                    {
                        j -= REST;
                    }
                }
                i += j;
            } else /*too short to benefit from repeat code*/
            {
                uivector_push_back(&bitlen_lld_e, bitlen_lld.data[i]);
            }
        }

        /*generate tree_cl, the huffmantree of huffmantrees*/

        if (uivector_resizev(&frequencies_cl, NUM_CODE_LENGTH_CODES, 0) == 0U) ERROR_BREAK(83 /*alloc fail*/);
        for (i = 0; i != bitlen_lld_e.size; ++i) {
            ++frequencies_cl.data[bitlen_lld_e.data[i]];
            /*after a repeat code come the bits that specify the number of repetitions,
            those don't need to be in the frequencies_cl calculation*/
            if (bitlen_lld_e.data[i] >= 16) {
                ++i;
            }
        }

        error = huffman_tree_make_from_frequencies(&tree_cl, frequencies_cl.data, frequencies_cl.size, frequencies_cl.size, 7);
        if (error != 0U) {
            break;
        }

        if (uivector_resize(&bitlen_cl, tree_cl.numcodes) == 0U) ERROR_BREAK(83 /*alloc fail*/);
        for (i = 0; i != tree_cl.numcodes; ++i) {
            /*lenghts of code length tree is in the order as specified by deflate*/
            bitlen_cl.data[i] = huffman_tree_get_length(&tree_cl, CLCL_ORDER[i]);
        }
        while (bitlen_cl.data[bitlen_cl.size - 1] == 0 && bitlen_cl.size > 4) {
            /*remove zeros at the end, but minimum size must be 4*/
            if (uivector_resize(&bitlen_cl, bitlen_cl.size - 1) == 0U) ERROR_BREAK(83 /*alloc fail*/);
        }
        if (error != 0U) {
            break;
        }

        /*
        Write everything into the output

        After the BFINAL and BTYPE, the dynamic block consists out of the following:
        - 5 bits HLIT, 5 bits HDIST, 4 bits HCLEN
        - (HCLEN+4)*3 bits code lengths of code length alphabet
        - HLIT + 257 code lenghts of lit/length alphabet (encoded using the code length
          alphabet, + possible repetition codes 16, 17, 18)
        - HDIST + 1 code lengths of distance alphabet (encoded using the code length
          alphabet, + possible repetition codes 16, 17, 18)
        - compressed data
        - 256 (end code)
        */

        /*Write block type*/
        ADD_BIT_TO_STREAM(bp, out, BFINAL);
        ADD_BIT_TO_STREAM(bp, out, 0); /*first bit of BTYPE "dynamic"*/
        ADD_BIT_TO_STREAM(bp, out, 1); /*second bit of BTYPE "dynamic"*/

        /*write the HLIT, HDIST and HCLEN values*/
        hlit = static_cast<unsigned>(numcodes_ll - 257);
        hdist = static_cast<unsigned>(numcodes_d - 1);
        hclen = static_cast<unsigned>(bitlen_cl.size) - 4;
        /*trim zeroes for HCLEN. HLIT and HDIST were already trimmed at tree creation*/
        while ((bitlen_cl.data[hclen + 4 - 1] == 0U) && hclen > 0) {
            --hclen;
        }
        add_bits_to_stream(bp, out, hlit, 5);
        add_bits_to_stream(bp, out, hdist, 5);
        add_bits_to_stream(bp, out, hclen, 4);

        /*write the code lenghts of the code length alphabet*/
        for (i = 0; i != hclen + 4; ++i) {
            add_bits_to_stream(bp, out, bitlen_cl.data[i], 3);
        }

        /*write the lenghts of the lit/len AND the dist alphabet*/
        for (i = 0; i != bitlen_lld_e.size; ++i) {
            add_huffman_symbol(bp, out, huffman_tree_get_code(&tree_cl, bitlen_lld_e.data[i]),
                               huffman_tree_get_length(&tree_cl, bitlen_lld_e.data[i]));
            /*extra bits of repeat codes*/
            if (bitlen_lld_e.data[i] == 16) {
                add_bits_to_stream(bp, out, bitlen_lld_e.data[++i], 2);
            } else if (bitlen_lld_e.data[i] == 17) {
                add_bits_to_stream(bp, out, bitlen_lld_e.data[++i], 3);
            } else if (bitlen_lld_e.data[i] == 18) {
                add_bits_to_stream(bp, out, bitlen_lld_e.data[++i], 7);
            }
        }

        /*write the compressed data symbols*/
        write_l_z77data(bp, out, &lz77_encoded, &tree_ll, &tree_d);
        /*error: the length of the end code 256 must be larger than 0*/
        if (huffman_tree_get_length(&tree_ll, 256) == 0) ERROR_BREAK(64);

        /*write the end code*/
        add_huffman_symbol(bp, out, huffman_tree_get_code(&tree_ll, 256), huffman_tree_get_length(&tree_ll, 256));

        break; /*end of error-while*/
    }

    /*cleanup*/
    uivector_cleanup(&lz77_encoded);
    huffman_tree_cleanup(&tree_ll);
    huffman_tree_cleanup(&tree_d);
    huffman_tree_cleanup(&tree_cl);
    uivector_cleanup(&frequencies_ll);
    uivector_cleanup(&frequencies_d);
    uivector_cleanup(&frequencies_cl);
    uivector_cleanup(&bitlen_lld_e);
    uivector_cleanup(&bitlen_lld);
    uivector_cleanup(&bitlen_cl);

    return error;
}

static unsigned deflate_fixed(ucvector* out, size_t* bp, Hash* hash, const unsigned char* data, size_t datapos, size_t dataend,
                              const LodePNGCompressSettings* settings, unsigned final) {
    HuffmanTree tree_ll; /*tree for literal values and length codes*/
    HuffmanTree tree_d;  /*tree for distance codes*/

    unsigned const BFINAL = final;
    unsigned error = 0;
    size_t i = 0;

    huffman_tree_init(&tree_ll);
    huffman_tree_init(&tree_d);

    generate_fixed_lit_len_tree(&tree_ll);
    generate_fixed_distance_tree(&tree_d);

    ADD_BIT_TO_STREAM(bp, out, BFINAL);
    ADD_BIT_TO_STREAM(bp, out, 1); /*first bit of BTYPE*/
    ADD_BIT_TO_STREAM(bp, out, 0); /*second bit of BTYPE*/

    if (settings->use_lz77 != 0U) /*LZ77 encoded*/
    {
        uivector lz77_encoded;
        uivector_init(&lz77_encoded);
        error = encode_l_z77(&lz77_encoded, hash, data, datapos, dataend, settings->windowsize, settings->minmatch, settings->nicematch,
                             settings->lazymatching);
        if (error == 0U) {
            write_l_z77data(bp, out, &lz77_encoded, &tree_ll, &tree_d);
        }
        uivector_cleanup(&lz77_encoded);
    } else /*no LZ77, but still will be Huffman compressed*/
    {
        for (i = datapos; i < dataend; ++i) {
            add_huffman_symbol(bp, out, huffman_tree_get_code(&tree_ll, data[i]), huffman_tree_get_length(&tree_ll, data[i]));
        }
    }
    /*add END code*/
    if (error == 0U) {
        add_huffman_symbol(bp, out, huffman_tree_get_code(&tree_ll, 256), huffman_tree_get_length(&tree_ll, 256));
    }

    /*cleanup*/
    huffman_tree_cleanup(&tree_ll);
    huffman_tree_cleanup(&tree_d);

    return error;
}

static unsigned lodepng_deflatev(ucvector* out, const unsigned char* in, size_t insize, const LodePNGCompressSettings* settings) {
    unsigned error = 0;
    size_t i = 0;
    size_t blocksize = 0;
    size_t numdeflateblocks = 0;
    size_t bp = 0; /*the bit pointer*/
    Hash hash;

    if (settings->btype > 2) {
        {
            return 61;
        }
    }
    if (settings->btype == 0) {
        {
            return deflate_no_compression(out, in, insize);
        }
    }
    if (settings->btype == 1) {
        {
            blocksize = insize;
        }
    } else /*if(settings->btype == 2)*/
    {
        /*on PNGs, deflate blocks of 65-262k seem to give most dense encoding*/
        blocksize = (insize / 8) + 8;
        blocksize = std::max<size_t>(blocksize, 65536);
        blocksize = std::min<size_t>(blocksize, 262144);
    }

    numdeflateblocks = (insize + blocksize - 1) / blocksize;
    if (numdeflateblocks == 0) {
        numdeflateblocks = 1;
    }

    error = hash_init(&hash, settings->windowsize);
    if (error != 0U) {
        return error;
    }

    for (i = 0; i != numdeflateblocks && (error == 0U); ++i) {
        auto const FINAL = static_cast<unsigned int>(i == numdeflateblocks - 1);
        size_t const START = i * blocksize;
        size_t end = START + blocksize;
        end = std::min(end, insize);

        if (settings->btype == 1) {
            error = deflate_fixed(out, &bp, &hash, in, START, end, settings, FINAL);
        } else if (settings->btype == 2) {
            error = deflate_dynamic(out, &bp, &hash, in, START, end, settings, FINAL);
        }
    }

    hash_cleanup(&hash);

    return error;
}

unsigned lodepng_deflate(unsigned char** out, size_t* outsize, const unsigned char* in, size_t insize,
                         const LodePNGCompressSettings* settings) {
    unsigned error = 0;
    ucvector v;
    ucvector_init_buffer(&v, *out, *outsize);
    error = lodepng_deflatev(&v, in, insize, settings);
    *out = v.data;
    *outsize = v.size;
    return error;
}

static unsigned deflate(unsigned char** out, size_t* outsize, const unsigned char* in, size_t insize,
                        const LodePNGCompressSettings* settings) {
    if (settings->custom_deflate != nullptr) {
        return settings->custom_deflate(out, outsize, in, insize, settings);
    }
    return lodepng_deflate(out, outsize, in, insize, settings);
}

#endif /*LODEPNG_COMPILE_DECODER*/

/* ////////////////////////////////////////////////////////////////////////// */
/* / Adler32                                                                  */
/* ////////////////////////////////////////////////////////////////////////// */

static unsigned update_adler32(unsigned adler, const unsigned char* data, unsigned len) {
    unsigned s1 = adler & 0xffff;
    unsigned s2 = (adler >> 16) & 0xffff;

    while (len > 0) {
        /*at least 5550 sums can be done before the sums overflow, saving a lot of module divisions*/
        unsigned amount = len > 5550 ? 5550 : len;
        len -= amount;
        while (amount > 0) {
            s1 += (*data++);
            s2 += s1;
            --amount;
        }
        s1 %= 65521;
        s2 %= 65521;
    }

    return (s2 << 16) | s1;
}

/*Return the adler32 of the bytes data[0..len-1]*/
static unsigned adler32(const unsigned char* data, unsigned len) { return update_adler32(1L, data, len); }

/* ////////////////////////////////////////////////////////////////////////// */
/* / Zlib                                                                   / */
/* ////////////////////////////////////////////////////////////////////////// */

#ifdef LODEPNG_COMPILE_DECODER

unsigned lodepng_zlib_decompress(unsigned char** out, size_t* outsize, const unsigned char* in, size_t insize,
                                 const LodePNGDecompressSettings* settings) {
    unsigned error = 0;
    unsigned cm = 0;
    unsigned cinfo = 0;
    unsigned fdict = 0;

    if (insize < 2) {
        return 53; /*error, size of zlib data too small*/
    }
    /*read information from zlib header*/
    if (((in[0] * 256) + in[1]) % 31 != 0) {
        /*error: 256 * in[0] + in[1] must be a multiple of 31, the FCHECK value is supposed to be made that way*/
        return 24;
    }

    cm = in[0] & 15;
    cinfo = (in[0] >> 4) & 15;
    /*FCHECK = in[1] & 31;*/ /*FCHECK is already tested above*/
    fdict = (in[1] >> 5) & 1;
    /*FLEVEL = (in[1] >> 6) & 3;*/ /*FLEVEL is not used here*/

    if (cm != 8 || cinfo > 7) {
        /*error: only compression method 8: inflate with sliding window of 32k is supported by the PNG spec*/
        return 25;
    }
    if (fdict != 0) {
        /*error: the specification of PNG says about the zlib stream:
          "The additional flags shall not specify a preset dictionary."*/
        return 26;
    }

    error = inflate(out, outsize, in + 2, insize - 2, settings);
    if (error != 0U) {
        return error;
    }

    if (settings->ignore_adler32 == 0U) {
        unsigned const ADLE_R32 = lodepng_read32bit_int(&in[insize - 4]);
        unsigned const CHECKSUM = adler32(*out, static_cast<unsigned>(*outsize));
        if (CHECKSUM != ADLE_R32) {
            return 58; /*error, adler checksum not correct, data must be corrupted*/
        }
    }

    return 0; /*no error*/
}

static unsigned zlib_decompress(unsigned char** out, size_t* outsize, const unsigned char* in, size_t insize,
                                const LodePNGDecompressSettings* settings) {
    if (settings->custom_zlib != nullptr) {
        return settings->custom_zlib(out, outsize, in, insize, settings);
    }
    return lodepng_zlib_decompress(out, outsize, in, insize, settings);
}

#endif /*LODEPNG_COMPILE_DECODER*/

#ifdef LODEPNG_COMPILE_ENCODER

unsigned lodepng_zlib_compress(unsigned char** out, size_t* outsize, const unsigned char* in, size_t insize,
                               const LodePNGCompressSettings* settings) {
    /*initially, *out must be NULL and outsize 0, if you just give some random *out
    that's pointing to a non allocated buffer, this'll crash*/
    ucvector outv;
    size_t i = 0;
    unsigned error = 0;
    unsigned char* deflatedata = nullptr;
    size_t deflatesize = 0;

    /*zlib data: 1 byte CMF (CM+CINFO), 1 byte FLG, deflate data, 4 byte ADLER32 checksum of the Decompressed data*/
    unsigned const CMF = 120; /*0b01111000: CM 8, CINFO 7. With CINFO 7, any window size up to 32768 can be used.*/
    unsigned const FLEVEL = 0;
    unsigned const FDICT = 0;
    unsigned cmfflg = (256 * CMF) + (FDICT * 32) + (FLEVEL * 64);
    unsigned const FCHECK = 31 - (cmfflg % 31);
    cmfflg += FCHECK;

    /*ucvector-controlled version of the output buffer, for dynamic array*/
    ucvector_init_buffer(&outv, *out, *outsize);

    ucvector_push_back(&outv, static_cast<unsigned char>(cmfflg / 256));
    ucvector_push_back(&outv, static_cast<unsigned char>(cmfflg % 256));

    error = deflate(&deflatedata, &deflatesize, in, insize, settings);

    if (error == 0U) {
        unsigned const ADLE_R32 = adler32(in, static_cast<unsigned>(insize));
        for (i = 0; i != deflatesize; ++i) {
            ucvector_push_back(&outv, deflatedata[i]);
        }
        lodepng_free(deflatedata);
        lodepng_add32bit_int(&outv, ADLE_R32);
    }

    *out = outv.data;
    *outsize = outv.size;

    return error;
}

/* compress using the default or custom zlib function */
static unsigned zlib_compress(unsigned char** out, size_t* outsize, const unsigned char* in, size_t insize,
                              const LodePNGCompressSettings* settings) {
    if (settings->custom_zlib != nullptr) {
        return settings->custom_zlib(out, outsize, in, insize, settings);
    }
    return lodepng_zlib_compress(out, outsize, in, insize, settings);
}

#endif /*LODEPNG_COMPILE_ENCODER*/

#else /*no LODEPNG_COMPILE_ZLIB*/

#ifdef LODEPNG_COMPILE_DECODER
static unsigned zlib_decompress(unsigned char** out, size_t* outsize, const unsigned char* in, size_t insize,
                                const LodePNGDecompressSettings* settings) {
    if (!settings->custom_zlib) return 87; /*no custom zlib function provided */
    return settings->custom_zlib(out, outsize, in, insize, settings);
}
#endif /*LODEPNG_COMPILE_DECODER*/
#ifdef LODEPNG_COMPILE_ENCODER
static unsigned zlib_compress(unsigned char** out, size_t* outsize, const unsigned char* in, size_t insize,
                              const LodePNGCompressSettings* settings) {
    if (!settings->custom_zlib) return 87; /*no custom zlib function provided */
    return settings->custom_zlib(out, outsize, in, insize, settings);
}
#endif /*LODEPNG_COMPILE_ENCODER*/

#endif /*LODEPNG_COMPILE_ZLIB*/

/* ////////////////////////////////////////////////////////////////////////// */

#ifdef LODEPNG_COMPILE_ENCODER

/*this is a good tradeoff between speed and compression ratio*/
static constexpr unsigned DEFAULT_WINDOWSIZE = 2048;

void lodepng_compress_settings_init(LodePNGCompressSettings* settings) {
    /*compress with dynamic huffman tree (not in the mathematical sense, just not the predefined one)*/
    settings->btype = 2;
    settings->use_lz77 = 1;
    settings->windowsize = DEFAULT_WINDOWSIZE;
    settings->minmatch = 3;
    settings->nicematch = 128;
    settings->lazymatching = 1;

    settings->custom_zlib = nullptr;
    settings->custom_deflate = nullptr;
    settings->custom_context = nullptr;
}

const LodePNGCompressSettings LODEPNG_DEFAULT_COMPRESS_SETTINGS = {.btype = 2,
                                                                   .use_lz77 = 1,
                                                                   .windowsize = DEFAULT_WINDOWSIZE,
                                                                   .minmatch = 3,
                                                                   .nicematch = 128,
                                                                   .lazymatching = 1,
                                                                   .custom_zlib = nullptr,
                                                                   .custom_deflate = nullptr,
                                                                   .custom_context = nullptr};

#endif /*LODEPNG_COMPILE_ENCODER*/

#ifdef LODEPNG_COMPILE_DECODER

void lodepng_decompress_settings_init(LodePNGDecompressSettings* settings) {
    settings->ignore_adler32 = 0;

    settings->custom_zlib = nullptr;
    settings->custom_inflate = nullptr;
    settings->custom_context = nullptr;
}

const LodePNGDecompressSettings LODEPNG_DEFAULT_DECOMPRESS_SETTINGS = {
    .ignore_adler32 = 0, .custom_zlib = nullptr, .custom_inflate = nullptr, .custom_context = nullptr};

#endif /*LODEPNG_COMPILE_DECODER*/

/* ////////////////////////////////////////////////////////////////////////// */
/* ////////////////////////////////////////////////////////////////////////// */
/* // End of Zlib related code. Begin of PNG related code.                 // */
/* ////////////////////////////////////////////////////////////////////////// */
/* ////////////////////////////////////////////////////////////////////////// */

#ifdef LODEPNG_COMPILE_PNG

/* ////////////////////////////////////////////////////////////////////////// */
/* / CRC32                                                                  / */
/* ////////////////////////////////////////////////////////////////////////// */

#ifndef LODEPNG_NO_COMPILE_CRC
/* CRC polynomial: 0xedb88320 */
static unsigned lodepng_crc32_table[256] = {
    0U,          1996959894U, 3993919788U, 2567524794U, 124634137U,  1886057615U, 3915621685U, 2657392035U, 249268274U,  2044508324U,
    3772115230U, 2547177864U, 162941995U,  2125561021U, 3887607047U, 2428444049U, 498536548U,  1789927666U, 4089016648U, 2227061214U,
    450548861U,  1843258603U, 4107580753U, 2211677639U, 325883990U,  1684777152U, 4251122042U, 2321926636U, 335633487U,  1661365465U,
    4195302755U, 2366115317U, 997073096U,  1281953886U, 3579855332U, 2724688242U, 1006888145U, 1258607687U, 3524101629U, 2768942443U,
    901097722U,  1119000684U, 3686517206U, 2898065728U, 853044451U,  1172266101U, 3705015759U, 2882616665U, 651767980U,  1373503546U,
    3369554304U, 3218104598U, 565507253U,  1454621731U, 3485111705U, 3099436303U, 671266974U,  1594198024U, 3322730930U, 2970347812U,
    795835527U,  1483230225U, 3244367275U, 3060149565U, 1994146192U, 31158534U,   2563907772U, 4023717930U, 1907459465U, 112637215U,
    2680153253U, 3904427059U, 2013776290U, 251722036U,  2517215374U, 3775830040U, 2137656763U, 141376813U,  2439277719U, 3865271297U,
    1802195444U, 476864866U,  2238001368U, 4066508878U, 1812370925U, 453092731U,  2181625025U, 4111451223U, 1706088902U, 314042704U,
    2344532202U, 4240017532U, 1658658271U, 366619977U,  2362670323U, 4224994405U, 1303535960U, 984961486U,  2747007092U, 3569037538U,
    1256170817U, 1037604311U, 2765210733U, 3554079995U, 1131014506U, 879679996U,  2909243462U, 3663771856U, 1141124467U, 855842277U,
    2852801631U, 3708648649U, 1342533948U, 654459306U,  3188396048U, 3373015174U, 1466479909U, 544179635U,  3110523913U, 3462522015U,
    1591671054U, 702138776U,  2966460450U, 3352799412U, 1504918807U, 783551873U,  3082640443U, 3233442989U, 3988292384U, 2596254646U,
    62317068U,   1957810842U, 3939845945U, 2647816111U, 81470997U,   1943803523U, 3814918930U, 2489596804U, 225274430U,  2053790376U,
    3826175755U, 2466906013U, 167816743U,  2097651377U, 4027552580U, 2265490386U, 503444072U,  1762050814U, 4150417245U, 2154129355U,
    426522225U,  1852507879U, 4275313526U, 2312317920U, 282753626U,  1742555852U, 4189708143U, 2394877945U, 397917763U,  1622183637U,
    3604390888U, 2714866558U, 953729732U,  1340076626U, 3518719985U, 2797360999U, 1068828381U, 1219638859U, 3624741850U, 2936675148U,
    906185462U,  1090812512U, 3747672003U, 2825379669U, 829329135U,  1181335161U, 3412177804U, 3160834842U, 628085408U,  1382605366U,
    3423369109U, 3138078467U, 570562233U,  1426400815U, 3317316542U, 2998733608U, 733239954U,  1555261956U, 3268935591U, 3050360625U,
    752459403U,  1541320221U, 2607071920U, 3965973030U, 1969922972U, 40735498U,   2617837225U, 3943577151U, 1913087877U, 83908371U,
    2512341634U, 3803740692U, 2075208622U, 213261112U,  2463272603U, 3855990285U, 2094854071U, 198958881U,  2262029012U, 4057260610U,
    1759359992U, 534414190U,  2176718541U, 4139329115U, 1873836001U, 414664567U,  2282248934U, 4279200368U, 1711684554U, 285281116U,
    2405801727U, 4167216745U, 1634467795U, 376229701U,  2685067896U, 3608007406U, 1308918612U, 956543938U,  2808555105U, 3495958263U,
    1231636301U, 1047427035U, 2932959818U, 3654703836U, 1088359270U, 936918000U,  2847714899U, 3736837829U, 1202900863U, 817233897U,
    3183342108U, 3401237130U, 1404277552U, 615818150U,  3134207493U, 3453421203U, 1423857449U, 601450431U,  3009837614U, 3294710456U,
    1567103746U, 711928724U,  3020668471U, 3272380065U, 1510334235U, 755167117U};

/*Return the CRC of the bytes buf[0..len-1].*/
unsigned lodepng_crc32(const unsigned char* buf, size_t len) {
    unsigned c = 0xffffffffL;
    size_t n = 0;

    for (n = 0; n < len; ++n) {
        c = lodepng_crc32_table[(c ^ buf[n]) & 0xff] ^ (c >> 8);
    }
    return c ^ 0xffffffffL;
}
#endif /* !LODEPNG_NO_COMPILE_CRC */

/* ////////////////////////////////////////////////////////////////////////// */
/* / Reading and writing single bits and bytes from/to stream for LodePNG   / */
/* ////////////////////////////////////////////////////////////////////////// */

static unsigned char read_bit_from_reversed_stream(size_t* bitpointer, const unsigned char* bitstream) {
    auto const RESULT = static_cast<unsigned char>((bitstream[(*bitpointer) >> 3] >> (7 - ((*bitpointer) & 0x7))) & 1);
    ++(*bitpointer);
    return RESULT;
}

static unsigned read_bits_from_reversed_stream(size_t* bitpointer, const unsigned char* bitstream, size_t nbits) {
    unsigned result = 0;
    size_t i = 0;
    for (i = nbits - 1; i < nbits; --i) {
        result += static_cast<unsigned>(read_bit_from_reversed_stream(bitpointer, bitstream)) << i;
    }
    return result;
}

#ifdef LODEPNG_COMPILE_DECODER
static void set_bit_of_reversed_stream0(size_t* bitpointer, unsigned char* bitstream, unsigned char bit) {
    /*the current bit in bitstream must be 0 for this to work*/
    if (bit != 0U) {
        /*earlier bit of huffman code is in a lesser significant bit of an earlier byte*/
        bitstream[(*bitpointer) >> 3] |= (bit << (7 - ((*bitpointer) & 0x7)));
    }
    ++(*bitpointer);
}
#endif /*LODEPNG_COMPILE_DECODER*/

static void set_bit_of_reversed_stream(size_t* bitpointer, unsigned char* bitstream, unsigned char bit) {
    /*the current bit in bitstream may be 0 or 1 for this to work*/
    if (bit == 0) {
        bitstream[(*bitpointer) >> 3] &= static_cast<unsigned char>(~(1 << (7 - ((*bitpointer) & 0x7))));
    } else {
        bitstream[(*bitpointer) >> 3] |= (1 << (7 - ((*bitpointer) & 0x7)));
    }
    ++(*bitpointer);
}

/* ////////////////////////////////////////////////////////////////////////// */
/* / PNG chunks                                                             / */
/* ////////////////////////////////////////////////////////////////////////// */

unsigned lodepng_chunk_length(const unsigned char* chunk) { return lodepng_read32bit_int(&chunk[0]); }

void lodepng_chunk_type(char type[5], const unsigned char* chunk) {
    unsigned i = 0;
    for (i = 0; i != 4; ++i) {
        type[i] = static_cast<char>(chunk[4 + i]);
    }
    type[4] = 0; /*null termination char*/
}

unsigned char lodepng_chunk_type_equals(const unsigned char* chunk, const char* type) {
    if (strlen(type) != 4) {
        return 0;
    }
    return static_cast<unsigned char>(chunk[4] == type[0] && chunk[5] == type[1] && chunk[6] == type[2] && chunk[7] == type[3]);
}

unsigned char lodepng_chunk_ancillary(const unsigned char* chunk) { return static_cast<unsigned char>((chunk[4] & 32) != 0); }

unsigned char lodepng_chunk_private(const unsigned char* chunk) { return static_cast<unsigned char>((chunk[6] & 32) != 0); }

unsigned char lodepng_chunk_safetocopy(const unsigned char* chunk) { return static_cast<unsigned char>((chunk[7] & 32) != 0); }

unsigned char* lodepng_chunk_data(unsigned char* chunk) { return &chunk[8]; }

const unsigned char* lodepng_chunk_data_const(const unsigned char* chunk) { return &chunk[8]; }

unsigned lodepng_chunk_check_crc(const unsigned char* chunk) {
    unsigned const LENGTH = lodepng_chunk_length(chunk);
    unsigned const CRC = lodepng_read32bit_int(&chunk[LENGTH + 8]);
    /*the CRC is taken of the data and the 4 chunk type letters, not the length*/
    unsigned const CHECKSUM = lodepng_crc32(&chunk[4], LENGTH + 4);
    if (CRC != CHECKSUM) {
        return 1;
    }
    return 0;
}

void lodepng_chunk_generate_crc(unsigned char* chunk) {
    unsigned const LENGTH = lodepng_chunk_length(chunk);
    unsigned const CRC = lodepng_crc32(&chunk[4], LENGTH + 4);
    lodepng_set32bit_int(chunk + 8 + LENGTH, CRC);
}

unsigned char* lodepng_chunk_next(unsigned char* chunk) {
    unsigned const TOTAL_CHUNK_LENGTH = lodepng_chunk_length(chunk) + 12;
    return &chunk[TOTAL_CHUNK_LENGTH];
}

const unsigned char* lodepng_chunk_next_const(const unsigned char* chunk) {
    unsigned const TOTAL_CHUNK_LENGTH = lodepng_chunk_length(chunk) + 12;
    return &chunk[TOTAL_CHUNK_LENGTH];
}

unsigned lodepng_chunk_append(unsigned char** out, size_t* outlength, const unsigned char* chunk) {
    unsigned i = 0;
    unsigned const TOTAL_CHUNK_LENGTH = lodepng_chunk_length(chunk) + 12;
    unsigned char* chunk_start = nullptr;
    unsigned char* new_buffer = nullptr;
    size_t const NEW_LENGTH = (*outlength) + TOTAL_CHUNK_LENGTH;
    if (NEW_LENGTH < TOTAL_CHUNK_LENGTH || NEW_LENGTH < (*outlength)) {
        return 77; /*integer overflow happened*/
    }

    new_buffer = static_cast<unsigned char*>(lodepng_realloc(*out, NEW_LENGTH));
    if (new_buffer == nullptr) {
        return 83; /*alloc fail*/
    }
    (*out) = new_buffer;
    (*outlength) = NEW_LENGTH;
    chunk_start = &(*out)[NEW_LENGTH - TOTAL_CHUNK_LENGTH];

    for (i = 0; i != TOTAL_CHUNK_LENGTH; ++i) {
        chunk_start[i] = chunk[i];
    }

    return 0;
}

unsigned lodepng_chunk_create(unsigned char** out, size_t* outlength, unsigned length, const char* type, const unsigned char* data) {
    unsigned i = 0;
    unsigned char* chunk = nullptr;
    unsigned char* new_buffer = nullptr;
    size_t const NEW_LENGTH = (*outlength) + length + 12;
    if (NEW_LENGTH < length + 12 || NEW_LENGTH < (*outlength)) {
        return 77; /*integer overflow happened*/
    }
    new_buffer = static_cast<unsigned char*>(lodepng_realloc(*out, NEW_LENGTH));
    if (new_buffer == nullptr) {
        return 83; /*alloc fail*/
    }
    (*out) = new_buffer;
    (*outlength) = NEW_LENGTH;
    chunk = &(*out)[(*outlength) - length - 12];

    /*1: length*/
    lodepng_set32bit_int(chunk, length);

    /*2: chunk name (4 letters)*/
    chunk[4] = static_cast<unsigned char>(type[0]);
    chunk[5] = static_cast<unsigned char>(type[1]);
    chunk[6] = static_cast<unsigned char>(type[2]);
    chunk[7] = static_cast<unsigned char>(type[3]);

    /*3: the data*/
    for (i = 0; i != length; ++i) {
        chunk[8 + i] = data[i];
    }

    /*4: CRC (of the chunkname characters and the data)*/
    lodepng_chunk_generate_crc(chunk);

    return 0;
}

/* ////////////////////////////////////////////////////////////////////////// */
/* / Color types and such                                                   / */
/* ////////////////////////////////////////////////////////////////////////// */

/*return type is a LodePNG error code*/
static unsigned check_color_validity(LodePNGColorType colortype, unsigned bd) /*bd = bitdepth*/
{
    switch (colortype) {
        case 0:
            if (bd != 1 && bd != 2 && bd != 4 && bd != 8 && bd != 16) {
                return 37;
            }
            break; /*grey*/
        case 2:
            if (bd != 8 && bd != 16) {
                return 37;
            }
            break; /*RGB*/
        case 3:
            if (bd != 1 && bd != 2 && bd != 4 && bd != 8) {
                return 37;
            }
            break; /*palette*/
        case 4:
            if (bd != 8 && bd != 16) {
                return 37;
            }
            break; /*grey + alpha*/
        case 6:
            if (bd != 8 && bd != 16) {
                return 37;
            }
            break; /*RGBA*/
        default:
            return 31;
    }
    return 0; /*allowed color type / bits combination*/
}

static unsigned get_num_color_channels(LodePNGColorType colortype) {
    switch (colortype) {
        case 0:
            return 1; /*grey*/
        case 2:
            return 3; /*RGB*/
        case 3:
            return 1; /*palette*/
        case 4:
            return 2; /*grey + alpha*/
        case 6:
            return 4; /*RGBA*/
    }
    return 0; /*unexisting color type*/
}

static unsigned lodepng_get_bpp_lct(LodePNGColorType colortype, unsigned bitdepth) {
    /*bits per pixel is amount of channels * bits per channel*/
    return get_num_color_channels(colortype) * bitdepth;
}

/* ////////////////////////////////////////////////////////////////////////// */

void lodepng_color_mode_init(LodePNGColorMode* info) {
    info->key_defined = 0;
    info->key_r = info->key_g = info->key_b = 0;
    info->colortype = LCT_RGBA;
    info->bitdepth = 8;
    info->palette = nullptr;
    info->palettesize = 0;
}

void lodepng_color_mode_cleanup(LodePNGColorMode* info) { lodepng_palette_clear(info); }

unsigned lodepng_color_mode_copy(LodePNGColorMode* dest, const LodePNGColorMode* source) {
    size_t i = 0;
    lodepng_color_mode_cleanup(dest);
    *dest = *source;
    if (source->palette != nullptr) {
        dest->palette = static_cast<unsigned char*>(lodepng_malloc(1024));
        if ((dest->palette == nullptr) && (source->palettesize != 0U)) {
            return 83; /*alloc fail*/
        }
        for (i = 0; i != source->palettesize * 4; ++i) {
            dest->palette[i] = source->palette[i];
        }
    }
    return 0;
}

static int lodepng_color_mode_equal(const LodePNGColorMode* a, const LodePNGColorMode* b) {
    size_t i = 0;
    if (a->colortype != b->colortype) {
        return 0;
    }
    if (a->bitdepth != b->bitdepth) {
        return 0;
    }
    if (a->key_defined != b->key_defined) {
        return 0;
    }
    if (a->key_defined != 0U) {
        if (a->key_r != b->key_r) {
            return 0;
        }
        if (a->key_g != b->key_g) {
            return 0;
        }
        if (a->key_b != b->key_b) {
            return 0;
        }
    }
    if (a->palettesize != b->palettesize) {
        return 0;
    }
    for (i = 0; i != a->palettesize * 4; ++i) {
        if (a->palette[i] != b->palette[i]) {
            return 0;
        }
    }
    return 1;
}

void lodepng_palette_clear(LodePNGColorMode* info) {
    if (info->palette != nullptr) {
        lodepng_free(info->palette);
    }
    info->palette = nullptr;
    info->palettesize = 0;
}

unsigned lodepng_palette_add(LodePNGColorMode* info, unsigned char r, unsigned char g, unsigned char b, unsigned char a) {
    unsigned char* data = nullptr;
    /*the same resize technique as C++ std::vectors is used, and here it's made so that for a palette with
    the max of 256 colors, it'll have the exact alloc size*/
    if (info->palette == nullptr) /*allocate palette if empty*/
    {
        /*room for 256 colors with 4 bytes each*/
        data = static_cast<unsigned char*>(lodepng_realloc(info->palette, 1024));
        if (data == nullptr) {
            return 83; /*alloc fail*/
        }
        info->palette = data;
    }
    info->palette[(4 * info->palettesize) + 0] = r;
    info->palette[(4 * info->palettesize) + 1] = g;
    info->palette[(4 * info->palettesize) + 2] = b;
    info->palette[(4 * info->palettesize) + 3] = a;
    ++info->palettesize;
    return 0;
}

unsigned lodepng_get_bpp(const LodePNGColorMode* info) {
    /*calculate bits per pixel out of colortype and bitdepth*/
    return lodepng_get_bpp_lct(info->colortype, info->bitdepth);
}

unsigned lodepng_get_channels(const LodePNGColorMode* info) { return get_num_color_channels(info->colortype); }

unsigned lodepng_is_greyscale_type(const LodePNGColorMode* info) {
    return static_cast<unsigned int>(info->colortype == LCT_GREY || info->colortype == LCT_GREY_ALPHA);
}

unsigned lodepng_is_alpha_type(const LodePNGColorMode* info) { return static_cast<unsigned int>((info->colortype & 4) != 0); /*4 or 6*/ }

unsigned lodepng_is_palette_type(const LodePNGColorMode* info) { return static_cast<unsigned int>(info->colortype == LCT_PALETTE); }

unsigned lodepng_has_palette_alpha(const LodePNGColorMode* info) {
    size_t i = 0;
    for (i = 0; i != info->palettesize; ++i) {
        if (info->palette[(i * 4) + 3] < 255) {
            return 1;
        }
    }
    return 0;
}

unsigned lodepng_can_have_alpha(const LodePNGColorMode* info) {
    return static_cast<unsigned int>((info->key_defined != 0U) || (lodepng_is_alpha_type(info) != 0U) ||
                                     (lodepng_has_palette_alpha(info)) != 0U);
}

size_t lodepng_get_raw_size(unsigned w, unsigned h, const LodePNGColorMode* color) { return ((w * h * lodepng_get_bpp(color)) + 7) / 8; }

static size_t lodepng_get_raw_size_lct(unsigned w, unsigned h, LodePNGColorType colortype, unsigned bitdepth) {
    return ((w * h * lodepng_get_bpp_lct(colortype, bitdepth)) + 7) / 8;
}

#ifdef LODEPNG_COMPILE_PNG
#ifdef LODEPNG_COMPILE_DECODER
/*in an idat chunk, each scanline is a multiple of 8 bits, unlike the lodepng output buffer*/
static size_t lodepng_get_raw_size_idat(unsigned w, unsigned h, const LodePNGColorMode* color) {
    return h * (((w * lodepng_get_bpp(color)) + 7) / 8);
}
#endif /*LODEPNG_COMPILE_DECODER*/
#endif /*LODEPNG_COMPILE_PNG*/

#ifdef LODEPNG_COMPILE_ANCILLARY_CHUNKS

static void lode_png_unknown_chunks_init(LodePNGInfo* info) {
    unsigned i = 0;
    for (i = 0; i != 3; ++i) {
        info->unknown_chunks_data[i] = nullptr;
    }
    for (i = 0; i != 3; ++i) {
        info->unknown_chunks_size[i] = 0;
    }
}

static void lode_png_unknown_chunks_cleanup(LodePNGInfo* info) {
    unsigned i = 0;
    for (i = 0; i != 3; ++i) {
        lodepng_free(info->unknown_chunks_data[i]);
    }
}

static unsigned lode_png_unknown_chunks_copy(LodePNGInfo* dest, const LodePNGInfo* src) {
    unsigned i = 0;

    lode_png_unknown_chunks_cleanup(dest);

    for (i = 0; i != 3; ++i) {
        size_t j = 0;
        dest->unknown_chunks_size[i] = src->unknown_chunks_size[i];
        dest->unknown_chunks_data[i] = static_cast<unsigned char*>(lodepng_malloc(src->unknown_chunks_size[i]));
        if ((dest->unknown_chunks_data[i] == nullptr) && (dest->unknown_chunks_size[i] != 0U)) {
            return 83; /*alloc fail*/
        }
        for (j = 0; j < src->unknown_chunks_size[i]; ++j) {
            dest->unknown_chunks_data[i][j] = src->unknown_chunks_data[i][j];
        }
    }

    return 0;
}

/******************************************************************************/

static void lode_png_text_init(LodePNGInfo* info) {
    info->text_num = 0;
    info->text_keys = nullptr;
    info->text_strings = nullptr;
}

static void lode_png_text_cleanup(LodePNGInfo* info) {
    size_t i = 0;
    for (i = 0; i != info->text_num; ++i) {
        string_cleanup(&info->text_keys[i]);
        string_cleanup(&info->text_strings[i]);
    }
    lodepng_free(info->text_keys);
    lodepng_free(info->text_strings);
}

static unsigned lode_png_text_copy(LodePNGInfo* dest, const LodePNGInfo* source) {
    size_t i = 0;
    dest->text_keys = nullptr;
    dest->text_strings = nullptr;
    dest->text_num = 0;
    for (i = 0; i != source->text_num; ++i) {
        CERROR_TRY_RETURN(lodepng_add_text(dest, source->text_keys[i], source->text_strings[i]));
    }
    return 0;
}

void lodepng_clear_text(LodePNGInfo* info) { lode_png_text_cleanup(info); }

unsigned lodepng_add_text(LodePNGInfo* info, const char* key, const char* str) {
    char** new_keys = static_cast<char**>(lodepng_realloc(info->text_keys, sizeof(char*) * (info->text_num + 1)));
    char** new_strings = static_cast<char**>(lodepng_realloc(info->text_strings, sizeof(char*) * (info->text_num + 1)));
    if ((new_keys == nullptr) || (new_strings == nullptr)) {
        lodepng_free(new_keys);
        lodepng_free(new_strings);
        return 83; /*alloc fail*/
    }

    ++info->text_num;
    info->text_keys = new_keys;
    info->text_strings = new_strings;

    string_init(&info->text_keys[info->text_num - 1]);
    string_set(&info->text_keys[info->text_num - 1], key);

    string_init(&info->text_strings[info->text_num - 1]);
    string_set(&info->text_strings[info->text_num - 1], str);

    return 0;
}

/******************************************************************************/

static void lode_pngi_text_init(LodePNGInfo* info) {
    info->itext_num = 0;
    info->itext_keys = nullptr;
    info->itext_langtags = nullptr;
    info->itext_transkeys = nullptr;
    info->itext_strings = nullptr;
}

static void lode_pngi_text_cleanup(LodePNGInfo* info) {
    size_t i = 0;
    for (i = 0; i != info->itext_num; ++i) {
        string_cleanup(&info->itext_keys[i]);
        string_cleanup(&info->itext_langtags[i]);
        string_cleanup(&info->itext_transkeys[i]);
        string_cleanup(&info->itext_strings[i]);
    }
    lodepng_free(info->itext_keys);
    lodepng_free(info->itext_langtags);
    lodepng_free(info->itext_transkeys);
    lodepng_free(info->itext_strings);
}

static unsigned lode_pngi_text_copy(LodePNGInfo* dest, const LodePNGInfo* source) {
    size_t i = 0;
    dest->itext_keys = nullptr;
    dest->itext_langtags = nullptr;
    dest->itext_transkeys = nullptr;
    dest->itext_strings = nullptr;
    dest->itext_num = 0;
    for (i = 0; i != source->itext_num; ++i) {
        CERROR_TRY_RETURN(lodepng_add_itext(dest, source->itext_keys[i], source->itext_langtags[i], source->itext_transkeys[i],
                                            source->itext_strings[i]));
    }
    return 0;
}

void lodepng_clear_itext(LodePNGInfo* info) { lode_pngi_text_cleanup(info); }

unsigned lodepng_add_itext(LodePNGInfo* info, const char* key, const char* langtag, const char* transkey, const char* str) {
    char** new_keys = static_cast<char**>(lodepng_realloc(info->itext_keys, sizeof(char*) * (info->itext_num + 1)));
    char** new_langtags = static_cast<char**>(lodepng_realloc(info->itext_langtags, sizeof(char*) * (info->itext_num + 1)));
    char** new_transkeys = static_cast<char**>(lodepng_realloc(info->itext_transkeys, sizeof(char*) * (info->itext_num + 1)));
    char** new_strings = static_cast<char**>(lodepng_realloc(info->itext_strings, sizeof(char*) * (info->itext_num + 1)));
    if ((new_keys == nullptr) || (new_langtags == nullptr) || (new_transkeys == nullptr) || (new_strings == nullptr)) {
        lodepng_free(new_keys);
        lodepng_free(new_langtags);
        lodepng_free(new_transkeys);
        lodepng_free(new_strings);
        return 83; /*alloc fail*/
    }

    ++info->itext_num;
    info->itext_keys = new_keys;
    info->itext_langtags = new_langtags;
    info->itext_transkeys = new_transkeys;
    info->itext_strings = new_strings;

    string_init(&info->itext_keys[info->itext_num - 1]);
    string_set(&info->itext_keys[info->itext_num - 1], key);

    string_init(&info->itext_langtags[info->itext_num - 1]);
    string_set(&info->itext_langtags[info->itext_num - 1], langtag);

    string_init(&info->itext_transkeys[info->itext_num - 1]);
    string_set(&info->itext_transkeys[info->itext_num - 1], transkey);

    string_init(&info->itext_strings[info->itext_num - 1]);
    string_set(&info->itext_strings[info->itext_num - 1], str);

    return 0;
}
#endif /*LODEPNG_COMPILE_ANCILLARY_CHUNKS*/

void lodepng_info_init(LodePNGInfo* info) {
    lodepng_color_mode_init(&info->color);
    info->interlace_method = 0;
    info->compression_method = 0;
    info->filter_method = 0;
#ifdef LODEPNG_COMPILE_ANCILLARY_CHUNKS
    info->background_defined = 0;
    info->background_r = info->background_g = info->background_b = 0;

    lode_png_text_init(info);
    lode_pngi_text_init(info);

    info->time_defined = 0;
    info->phys_defined = 0;

    lode_png_unknown_chunks_init(info);
#endif /*LODEPNG_COMPILE_ANCILLARY_CHUNKS*/
}

void lodepng_info_cleanup(LodePNGInfo* info) {
    lodepng_color_mode_cleanup(&info->color);
#ifdef LODEPNG_COMPILE_ANCILLARY_CHUNKS
    lode_png_text_cleanup(info);
    lode_pngi_text_cleanup(info);

    lode_png_unknown_chunks_cleanup(info);
#endif /*LODEPNG_COMPILE_ANCILLARY_CHUNKS*/
}

unsigned lodepng_info_copy(LodePNGInfo* dest, const LodePNGInfo* source) {
    lodepng_info_cleanup(dest);
    *dest = *source;
    lodepng_color_mode_init(&dest->color);
    CERROR_TRY_RETURN(lodepng_color_mode_copy(&dest->color, &source->color));

#ifdef LODEPNG_COMPILE_ANCILLARY_CHUNKS
    CERROR_TRY_RETURN(lode_png_text_copy(dest, source));
    CERROR_TRY_RETURN(lode_pngi_text_copy(dest, source));

    lode_png_unknown_chunks_init(dest);
    CERROR_TRY_RETURN(lode_png_unknown_chunks_copy(dest, source));
#endif /*LODEPNG_COMPILE_ANCILLARY_CHUNKS*/
    return 0;
}
/* ////////////////////////////////////////////////////////////////////////// */

/*index: bitgroup index, bits: bitgroup size(1, 2 or 4), in: bitgroup value, out: octet array to add bits to*/
static void add_color_bits(unsigned char* out, size_t index, unsigned bits, unsigned in) {
    unsigned const M = bits == 1 ? 7 : bits == 2 ? 3 : 1; /*8 / bits - 1*/
    /*p = the partial index in the byte, e.g. with 4 palettebits it is 0 for first half or 1 for second half*/
    unsigned const P = index & M;
    in &= (1U << bits) - 1U; /*filter out any other bits of the input value*/
    in = in << (bits * (M - P));
    if (P == 0) {
        out[index * bits / 8] = in;
    } else {
        out[index * bits / 8] |= in;
    }
}

using ColorTree = struct ColorTree;

/*
One node of a color tree
This is the data structure used to count the number of unique colors and to get a palette
index for a color. It's like an octree, but because the alpha channel is used too, each
node has 16 instead of 8 children.
*/
struct ColorTree {
    ColorTree* children[16]; /*up to 16 pointers to ColorTree of next level*/
    int index;               /*the payload. Only has a meaningful value if this is in the last level*/
};

static void color_tree_init(ColorTree* tree) {
    int i = 0;
    for (i = 0; i != 16; ++i) {
        tree->children[i] = nullptr;
    }
    tree->index = -1;
}

static void color_tree_cleanup(ColorTree* tree) {
    int i = 0;
    for (i = 0; i != 16; ++i) {
        if (tree->children[i] != nullptr) {
            color_tree_cleanup(tree->children[i]);
            lodepng_free(tree->children[i]);
        }
    }
}

/*returns -1 if color not present, its index otherwise*/
static int color_tree_get(ColorTree* tree, unsigned char r, unsigned char g, unsigned char b, unsigned char a) {
    int bit = 0;
    for (bit = 0; bit < 8; ++bit) {
        int const I = (8 * ((r >> bit) & 1)) + (4 * ((g >> bit) & 1)) + (2 * ((b >> bit) & 1)) + (1 * ((a >> bit) & 1));
        if (tree->children[I] == nullptr) {
            return -1;
        }
        tree = tree->children[I];
    }
    return (tree != nullptr) ? tree->index : -1;
}

#ifdef LODEPNG_COMPILE_ENCODER
static int color_tree_has(ColorTree* tree, unsigned char r, unsigned char g, unsigned char b, unsigned char a) {
    return static_cast<int>(color_tree_get(tree, r, g, b, a) >= 0);
}
#endif /*LODEPNG_COMPILE_ENCODER*/

/*color is not allowed to already exist.
Index should be >= 0 (it's signed to be compatible with using -1 for "doesn't exist")*/
static void color_tree_add(ColorTree* tree, unsigned char r, unsigned char g, unsigned char b, unsigned char a, unsigned index) {
    int bit = 0;
    for (bit = 0; bit < 8; ++bit) {
        int const I = (8 * ((r >> bit) & 1)) + (4 * ((g >> bit) & 1)) + (2 * ((b >> bit) & 1)) + (1 * ((a >> bit) & 1));
        if (tree->children[I] == nullptr) {
            tree->children[I] = static_cast<ColorTree*>(lodepng_malloc(sizeof(ColorTree)));
            color_tree_init(tree->children[I]);
        }
        tree = tree->children[I];
    }
    tree->index = static_cast<int>(index);
}

/*put a pixel, given its RGBA color, into image of any color type*/
static unsigned rgba8_to_pixel(unsigned char* out, size_t i, const LodePNGColorMode* mode, ColorTree* tree /*for palette*/, unsigned char r,
                               unsigned char g, unsigned char b, unsigned char a) {
    if (mode->colortype == LCT_GREY) {
        unsigned char grey = r; /*((unsigned short)r + g + b) / 3*/
        ;
        if (mode->bitdepth == 8) {
            {
                out[i] = grey;
            }
        } else if (mode->bitdepth == 16) {
            {
                out[(i * 2) + 0] = out[(i * 2) + 1] = grey;
            }
        } else {
            /*take the most significant bits of grey*/
            grey = (grey >> (8 - mode->bitdepth)) & ((1 << mode->bitdepth) - 1);
            add_color_bits(out, i, mode->bitdepth, grey);
        }
    } else if (mode->colortype == LCT_RGB) {
        if (mode->bitdepth == 8) {
            out[(i * 3) + 0] = r;
            out[(i * 3) + 1] = g;
            out[(i * 3) + 2] = b;
        } else {
            out[(i * 6) + 0] = out[(i * 6) + 1] = r;
            out[(i * 6) + 2] = out[(i * 6) + 3] = g;
            out[(i * 6) + 4] = out[(i * 6) + 5] = b;
        }
    } else if (mode->colortype == LCT_PALETTE) {
        int const INDEX = color_tree_get(tree, r, g, b, a);
        if (INDEX < 0) {
            return 82; /*color not in palette*/
        }
        if (mode->bitdepth == 8) {
            out[i] = INDEX;
        } else {
            add_color_bits(out, i, mode->bitdepth, static_cast<unsigned>(INDEX));
        }
    } else if (mode->colortype == LCT_GREY_ALPHA) {
        unsigned char const GREY = r; /*((unsigned short)r + g + b) / 3*/
        ;
        if (mode->bitdepth == 8) {
            out[(i * 2) + 0] = GREY;
            out[(i * 2) + 1] = a;
        } else if (mode->bitdepth == 16) {
            out[(i * 4) + 0] = out[(i * 4) + 1] = GREY;
            out[(i * 4) + 2] = out[(i * 4) + 3] = a;
        }
    } else if (mode->colortype == LCT_RGBA) {
        if (mode->bitdepth == 8) {
            out[(i * 4) + 0] = r;
            out[(i * 4) + 1] = g;
            out[(i * 4) + 2] = b;
            out[(i * 4) + 3] = a;
        } else {
            out[(i * 8) + 0] = out[(i * 8) + 1] = r;
            out[(i * 8) + 2] = out[(i * 8) + 3] = g;
            out[(i * 8) + 4] = out[(i * 8) + 5] = b;
            out[(i * 8) + 6] = out[(i * 8) + 7] = a;
        }
    }

    return 0; /*no error*/
}

/*put a pixel, given its RGBA16 color, into image of any color 16-bitdepth type*/
static void rgba16_to_pixel(unsigned char* out, size_t i, const LodePNGColorMode* mode, unsigned short r, unsigned short g,
                            unsigned short b, unsigned short a) {
    if (mode->colortype == LCT_GREY) {
        unsigned short const GREY = r; /*((unsigned)r + g + b) / 3*/
        ;
        out[(i * 2) + 0] = (GREY >> 8) & 255;
        out[(i * 2) + 1] = GREY & 255;
    } else if (mode->colortype == LCT_RGB) {
        out[(i * 6) + 0] = (r >> 8) & 255;
        out[(i * 6) + 1] = r & 255;
        out[(i * 6) + 2] = (g >> 8) & 255;
        out[(i * 6) + 3] = g & 255;
        out[(i * 6) + 4] = (b >> 8) & 255;
        out[(i * 6) + 5] = b & 255;
    } else if (mode->colortype == LCT_GREY_ALPHA) {
        unsigned short const GREY = r; /*((unsigned)r + g + b) / 3*/
        ;
        out[(i * 4) + 0] = (GREY >> 8) & 255;
        out[(i * 4) + 1] = GREY & 255;
        out[(i * 4) + 2] = (a >> 8) & 255;
        out[(i * 4) + 3] = a & 255;
    } else if (mode->colortype == LCT_RGBA) {
        out[(i * 8) + 0] = (r >> 8) & 255;
        out[(i * 8) + 1] = r & 255;
        out[(i * 8) + 2] = (g >> 8) & 255;
        out[(i * 8) + 3] = g & 255;
        out[(i * 8) + 4] = (b >> 8) & 255;
        out[(i * 8) + 5] = b & 255;
        out[(i * 8) + 6] = (a >> 8) & 255;
        out[(i * 8) + 7] = a & 255;
    }
}

/*Get RGBA8 color of pixel with index i (y * width + x) from the raw image with given color type.*/
static void get_pixel_color_rgb_a8(unsigned char* r, unsigned char* g, unsigned char* b, unsigned char* a, const unsigned char* in,
                                   size_t i, const LodePNGColorMode* mode) {
    if (mode->colortype == LCT_GREY) {
        if (mode->bitdepth == 8) {
            *r = *g = *b = in[i];
            if ((mode->key_defined != 0U) && *r == mode->key_r) {
                *a = 0;
            } else {
                *a = 255;
            }
        } else if (mode->bitdepth == 16) {
            *r = *g = *b = in[(i * 2) + 0];
            if ((mode->key_defined != 0U) && (256U * in[(i * 2) + 0]) + in[(i * 2) + 1] == mode->key_r) {
                *a = 0;
            } else {
                *a = 255;
            }
        } else {
            unsigned const HIGHEST = ((1U << mode->bitdepth) - 1U); /*highest possible value for this bit depth*/
            size_t j = i * mode->bitdepth;
            unsigned const VALUE = read_bits_from_reversed_stream(&j, in, mode->bitdepth);
            *r = *g = *b = (VALUE * 255) / HIGHEST;
            if ((mode->key_defined != 0U) && VALUE == mode->key_r) {
                *a = 0;
            } else {
                *a = 255;
            }
        }
    } else if (mode->colortype == LCT_RGB) {
        if (mode->bitdepth == 8) {
            *r = in[(i * 3) + 0];
            *g = in[(i * 3) + 1];
            *b = in[(i * 3) + 2];
            if ((mode->key_defined != 0U) && *r == mode->key_r && *g == mode->key_g && *b == mode->key_b) {
                *a = 0;
            } else {
                *a = 255;
            }
        } else {
            *r = in[(i * 6) + 0];
            *g = in[(i * 6) + 2];
            *b = in[(i * 6) + 4];
            if ((mode->key_defined != 0U) && (256U * in[(i * 6) + 0]) + in[(i * 6) + 1] == mode->key_r &&
                (256U * in[(i * 6) + 2]) + in[(i * 6) + 3] == mode->key_g && (256U * in[(i * 6) + 4]) + in[(i * 6) + 5] == mode->key_b) {
                *a = 0;
            } else {
                *a = 255;
            }
        }
    } else if (mode->colortype == LCT_PALETTE) {
        unsigned index = 0;
        if (mode->bitdepth == 8) {
            {
                index = in[i];
            }
        } else {
            size_t j = i * mode->bitdepth;
            index = read_bits_from_reversed_stream(&j, in, mode->bitdepth);
        }

        if (index >= mode->palettesize) {
            /*This is an error according to the PNG spec, but common PNG decoders make it black instead.
            Done here too, slightly faster due to no error handling needed.*/
            *r = *g = *b = 0;
            *a = 255;
        } else {
            *r = mode->palette[(index * 4) + 0];
            *g = mode->palette[(index * 4) + 1];
            *b = mode->palette[(index * 4) + 2];
            *a = mode->palette[(index * 4) + 3];
        }
    } else if (mode->colortype == LCT_GREY_ALPHA) {
        if (mode->bitdepth == 8) {
            *r = *g = *b = in[(i * 2) + 0];
            *a = in[(i * 2) + 1];
        } else {
            *r = *g = *b = in[(i * 4) + 0];
            *a = in[(i * 4) + 2];
        }
    } else if (mode->colortype == LCT_RGBA) {
        if (mode->bitdepth == 8) {
            *r = in[(i * 4) + 0];
            *g = in[(i * 4) + 1];
            *b = in[(i * 4) + 2];
            *a = in[(i * 4) + 3];
        } else {
            *r = in[(i * 8) + 0];
            *g = in[(i * 8) + 2];
            *b = in[(i * 8) + 4];
            *a = in[(i * 8) + 6];
        }
    }
}

/*Similar to getPixelColorRGBA8, but with all the for loops inside of the color
mode test cases, optimized to convert the colors much faster, when converting
to RGBA or RGB with 8 bit per cannel. buffer must be RGBA or RGB output with
enough memory, if has_alpha is true the output is RGBA. mode has the color mode
of the input buffer.*/
static void get_pixel_colors_rgb_a8(unsigned char* buffer, size_t numpixels, unsigned has_alpha, const unsigned char* in,
                                    const LodePNGColorMode* mode) {
    unsigned const NUM_CHANNELS = (has_alpha != 0U) ? 4 : 3;
    size_t i = 0;
    if (mode->colortype == LCT_GREY) {
        if (mode->bitdepth == 8) {
            for (i = 0; i != numpixels; ++i, buffer += NUM_CHANNELS) {
                buffer[0] = buffer[1] = buffer[2] = in[i];
                if (has_alpha != 0U) {
                    buffer[3] = (mode->key_defined != 0U) && in[i] == mode->key_r ? 0 : 255;
                }
            }
        } else if (mode->bitdepth == 16) {
            for (i = 0; i != numpixels; ++i, buffer += NUM_CHANNELS) {
                buffer[0] = buffer[1] = buffer[2] = in[i * 2];
                if (has_alpha != 0U) {
                    buffer[3] = (mode->key_defined != 0U) && (256U * in[(i * 2) + 0]) + in[(i * 2) + 1] == mode->key_r ? 0 : 255;
                }
            }
        } else {
            unsigned const HIGHEST = ((1U << mode->bitdepth) - 1U); /*highest possible value for this bit depth*/
            size_t j = 0;
            for (i = 0; i != numpixels; ++i, buffer += NUM_CHANNELS) {
                unsigned const VALUE = read_bits_from_reversed_stream(&j, in, mode->bitdepth);
                buffer[0] = buffer[1] = buffer[2] = (VALUE * 255) / HIGHEST;
                if (has_alpha != 0U) {
                    buffer[3] = (mode->key_defined != 0U) && VALUE == mode->key_r ? 0 : 255;
                }
            }
        }
    } else if (mode->colortype == LCT_RGB) {
        if (mode->bitdepth == 8) {
            for (i = 0; i != numpixels; ++i, buffer += NUM_CHANNELS) {
                buffer[0] = in[(i * 3) + 0];
                buffer[1] = in[(i * 3) + 1];
                buffer[2] = in[(i * 3) + 2];
                if (has_alpha != 0U) {
                    buffer[3] =
                        (mode->key_defined != 0U) && buffer[0] == mode->key_r && buffer[1] == mode->key_g && buffer[2] == mode->key_b ? 0
                                                                                                                                      : 255;
                }
            }
        } else {
            for (i = 0; i != numpixels; ++i, buffer += NUM_CHANNELS) {
                buffer[0] = in[(i * 6) + 0];
                buffer[1] = in[(i * 6) + 2];
                buffer[2] = in[(i * 6) + 4];
                if (has_alpha != 0U) {
                    buffer[3] = (mode->key_defined != 0U) && (256U * in[(i * 6) + 0]) + in[(i * 6) + 1] == mode->key_r &&
                                        (256U * in[(i * 6) + 2]) + in[(i * 6) + 3] == mode->key_g &&
                                        (256U * in[(i * 6) + 4]) + in[(i * 6) + 5] == mode->key_b
                                    ? 0
                                    : 255;
                }
            }
        }
    } else if (mode->colortype == LCT_PALETTE) {
        unsigned index = 0;
        size_t j = 0;
        for (i = 0; i != numpixels; ++i, buffer += NUM_CHANNELS) {
            if (mode->bitdepth == 8) {
                index = in[i];
            } else {
                index = read_bits_from_reversed_stream(&j, in, mode->bitdepth);
            }

            if (index >= mode->palettesize) {
                /*This is an error according to the PNG spec, but most PNG decoders make it black instead.
                Done here too, slightly faster due to no error handling needed.*/
                buffer[0] = buffer[1] = buffer[2] = 0;
                if (has_alpha != 0U) {
                    buffer[3] = 255;
                }
            } else {
                buffer[0] = mode->palette[(index * 4) + 0];
                buffer[1] = mode->palette[(index * 4) + 1];
                buffer[2] = mode->palette[(index * 4) + 2];
                if (has_alpha != 0U) {
                    buffer[3] = mode->palette[(index * 4) + 3];
                }
            }
        }
    } else if (mode->colortype == LCT_GREY_ALPHA) {
        if (mode->bitdepth == 8) {
            for (i = 0; i != numpixels; ++i, buffer += NUM_CHANNELS) {
                buffer[0] = buffer[1] = buffer[2] = in[(i * 2) + 0];
                if (has_alpha != 0U) {
                    buffer[3] = in[(i * 2) + 1];
                }
            }
        } else {
            for (i = 0; i != numpixels; ++i, buffer += NUM_CHANNELS) {
                buffer[0] = buffer[1] = buffer[2] = in[(i * 4) + 0];
                if (has_alpha != 0U) {
                    buffer[3] = in[(i * 4) + 2];
                }
            }
        }
    } else if (mode->colortype == LCT_RGBA) {
        if (mode->bitdepth == 8) {
            for (i = 0; i != numpixels; ++i, buffer += NUM_CHANNELS) {
                buffer[0] = in[(i * 4) + 0];
                buffer[1] = in[(i * 4) + 1];
                buffer[2] = in[(i * 4) + 2];
                if (has_alpha != 0U) {
                    buffer[3] = in[(i * 4) + 3];
                }
            }
        } else {
            for (i = 0; i != numpixels; ++i, buffer += NUM_CHANNELS) {
                buffer[0] = in[(i * 8) + 0];
                buffer[1] = in[(i * 8) + 2];
                buffer[2] = in[(i * 8) + 4];
                if (has_alpha != 0U) {
                    buffer[3] = in[(i * 8) + 6];
                }
            }
        }
    }
}

/*Get RGBA16 color of pixel with index i (y * width + x) from the raw image with
given color type, but the given color type must be 16-bit itself.*/
static void get_pixel_color_rgb_a16(unsigned short* r, unsigned short* g, unsigned short* b, unsigned short* a, const unsigned char* in,
                                    size_t i, const LodePNGColorMode* mode) {
    if (mode->colortype == LCT_GREY) {
        *r = *g = *b = (256 * in[(i * 2) + 0]) + in[(i * 2) + 1];
        if ((mode->key_defined != 0U) && (256U * in[(i * 2) + 0]) + in[(i * 2) + 1] == mode->key_r) {
            *a = 0;
        } else {
            *a = 65535;
        }
    } else if (mode->colortype == LCT_RGB) {
        *r = (256 * in[(i * 6) + 0]) + in[(i * 6) + 1];
        *g = (256 * in[(i * 6) + 2]) + in[(i * 6) + 3];
        *b = (256 * in[(i * 6) + 4]) + in[(i * 6) + 5];
        if ((mode->key_defined != 0U) && (256U * in[(i * 6) + 0]) + in[(i * 6) + 1] == mode->key_r &&
            (256U * in[(i * 6) + 2]) + in[(i * 6) + 3] == mode->key_g && (256U * in[(i * 6) + 4]) + in[(i * 6) + 5] == mode->key_b) {
            *a = 0;
        } else {
            *a = 65535;
        }
    } else if (mode->colortype == LCT_GREY_ALPHA) {
        *r = *g = *b = (256 * in[(i * 4) + 0]) + in[(i * 4) + 1];
        *a = (256 * in[(i * 4) + 2]) + in[(i * 4) + 3];
    } else if (mode->colortype == LCT_RGBA) {
        *r = (256 * in[(i * 8) + 0]) + in[(i * 8) + 1];
        *g = (256 * in[(i * 8) + 2]) + in[(i * 8) + 3];
        *b = (256 * in[(i * 8) + 4]) + in[(i * 8) + 5];
        *a = (256 * in[(i * 8) + 6]) + in[(i * 8) + 7];
    }
}

unsigned lodepng_convert(unsigned char* out, const unsigned char* in, LodePNGColorMode* mode_out, const LodePNGColorMode* mode_in,
                         unsigned w, unsigned h) {
    size_t i = 0;
    ColorTree tree;
    size_t const NUMPIXELS = w * h;

    if (lodepng_color_mode_equal(mode_out, mode_in) != 0) {
        size_t const NUMBYTES = lodepng_get_raw_size(w, h, mode_in);
        for (i = 0; i != NUMBYTES; ++i) {
            out[i] = in[i];
        }
        return 0;
    }

    if (mode_out->colortype == LCT_PALETTE) {
        size_t palsize = 1U << mode_out->bitdepth;
        palsize = std::min(mode_out->palettesize, palsize);
        color_tree_init(&tree);
        for (i = 0; i != palsize; ++i) {
            unsigned char const* p = &mode_out->palette[i * 4];
            color_tree_add(&tree, p[0], p[1], p[2], p[3], i);
        }
    }

    if (mode_in->bitdepth == 16 && mode_out->bitdepth == 16) {
        for (i = 0; i != NUMPIXELS; ++i) {
            unsigned short r = 0;
            unsigned short g = 0;
            unsigned short b = 0;
            unsigned short a = 0;
            get_pixel_color_rgb_a16(&r, &g, &b, &a, in, i, mode_in);
            rgba16_to_pixel(out, i, mode_out, r, g, b, a);
        }
    } else if (mode_out->bitdepth == 8 && mode_out->colortype == LCT_RGBA) {
        get_pixel_colors_rgb_a8(out, NUMPIXELS, 1, in, mode_in);
    } else if (mode_out->bitdepth == 8 && mode_out->colortype == LCT_RGB) {
        get_pixel_colors_rgb_a8(out, NUMPIXELS, 0, in, mode_in);
    } else {
        unsigned char r = 0;
        unsigned char g = 0;
        unsigned char b = 0;
        unsigned char a = 0;
        for (i = 0; i != NUMPIXELS; ++i) {
            get_pixel_color_rgb_a8(&r, &g, &b, &a, in, i, mode_in);
            rgba8_to_pixel(out, i, mode_out, &tree, r, g, b, a);
        }
    }

    if (mode_out->colortype == LCT_PALETTE) {
        color_tree_cleanup(&tree);
    }

    return 0; /*no error (this function currently never has one, but maybe OOM detection added later.)*/
}

#ifdef LODEPNG_COMPILE_ENCODER

void lodepng_color_profile_init(LodePNGColorProfile* profile) {
    profile->colored = 0;
    profile->key = 0;
    profile->alpha = 0;
    profile->key_r = profile->key_g = profile->key_b = 0;
    profile->numcolors = 0;
    profile->bits = 1;
}

/*function used for debug purposes with C++*/
/*void printColorProfile(LodePNGColorProfile* p)
{
  std::cout << "colored: " << (int)p->colored << ", ";
  std::cout << "key: " << (int)p->key << ", ";
  std::cout << "key_r: " << (int)p->key_r << ", ";
  std::cout << "key_g: " << (int)p->key_g << ", ";
  std::cout << "key_b: " << (int)p->key_b << ", ";
  std::cout << "alpha: " << (int)p->alpha << ", ";
  std::cout << "numcolors: " << (int)p->numcolors << ", ";
  std::cout << "bits: " << (int)p->bits << std::endl;
}*/

/*Returns how many bits needed to represent given value (max 8 bit)*/
static unsigned get_value_required_bits(unsigned char value) {
    if (value == 0 || value == 255) {
        return 1;
    }
    /*The scaling of 2-bit and 4-bit values uses multiples of 85 and 17*/
    if (value % 17 == 0) {
        return value % 85 == 0 ? 2 : 4;
    }
    return 8;
}

/*profile must already have been inited with mode.
It's ok to set some parameters of profile to done already.*/
unsigned lodepng_get_color_profile(LodePNGColorProfile* profile, const unsigned char* in, unsigned w, unsigned h,
                                   const LodePNGColorMode* mode) {
    unsigned const ERROR = 0;
    size_t i = 0;
    ColorTree tree;
    size_t const NUMPIXELS = w * h;

    unsigned colored_done = (lodepng_is_greyscale_type(mode) != 0U) ? 1 : 0;
    unsigned alpha_done = (lodepng_can_have_alpha(mode) != 0U) ? 0 : 1;
    unsigned numcolors_done = 0;
    unsigned const BPP = lodepng_get_bpp(mode);
    unsigned bits_done = BPP == 1 ? 1 : 0;
    unsigned maxnumcolors = 257;
    unsigned sixteen = 0;
    if (BPP <= 8) {
        maxnumcolors = BPP == 1 ? 2 : (BPP == 2 ? 4 : (BPP == 4 ? 16 : 256));
    }

    color_tree_init(&tree);

    /*Check if the 16-bit input is truly 16-bit*/
    if (mode->bitdepth == 16) {
        unsigned short r = 0;
        unsigned short g = 0;
        unsigned short b = 0;
        unsigned short a = 0;
        for (i = 0; i != NUMPIXELS; ++i) {
            get_pixel_color_rgb_a16(&r, &g, &b, &a, in, i, mode);
            if ((r & 255) != ((r >> 8) & 255) || (g & 255) != ((g >> 8) & 255) || (b & 255) != ((b >> 8) & 255) ||
                (a & 255) != ((a >> 8) & 255)) /*first and second byte differ*/
            {
                sixteen = 1;
                break;
            }
        }
    }

    if (sixteen != 0U) {
        unsigned short r = 0;
        unsigned short g = 0;
        unsigned short b = 0;
        unsigned short a = 0;
        profile->bits = 16;
        bits_done = numcolors_done = 1; /*counting colors no longer useful, palette doesn't support 16-bit*/

        for (i = 0; i != NUMPIXELS; ++i) {
            get_pixel_color_rgb_a16(&r, &g, &b, &a, in, i, mode);

            if ((colored_done == 0U) && (r != g || r != b)) {
                profile->colored = 1;
                colored_done = 1;
            }

            if (alpha_done == 0U) {
                auto const MATCHKEY = static_cast<unsigned int>(r == profile->key_r && g == profile->key_g && b == profile->key_b);
                if (a != 65535 && (a != 0 || ((profile->key != 0U) && (MATCHKEY == 0U)))) {
                    profile->alpha = 1;
                    alpha_done = 1;
                    profile->bits =
                        std::max<unsigned int>(profile->bits, 8); /*PNG has no alphachannel modes with less than 8-bit per channel*/
                } else if (a == 0 && (profile->alpha == 0U) && (profile->key == 0U)) {
                    profile->key = 1;
                    profile->key_r = r;
                    profile->key_g = g;
                    profile->key_b = b;
                } else if (a == 65535 && (profile->key != 0U) && (MATCHKEY != 0U)) {
                    /* Color key cannot be used if an opaque pixel also has that RGB color. */
                    profile->alpha = 1;
                    alpha_done = 1;
                }
            }

            if ((alpha_done != 0U) && (numcolors_done != 0U) && (colored_done != 0U) && (bits_done != 0U)) {
                break;
            }
        }
    } else /* < 16-bit */
    {
        for (i = 0; i != NUMPIXELS; ++i) {
            unsigned char r = 0;
            unsigned char g = 0;
            unsigned char b = 0;
            unsigned char a = 0;
            get_pixel_color_rgb_a8(&r, &g, &b, &a, in, i, mode);

            if ((bits_done == 0U) && profile->bits < 8) {
                /*only r is checked, < 8 bits is only relevant for greyscale*/
                unsigned const BITS = get_value_required_bits(r);
                profile->bits = std::max(BITS, profile->bits);
            }
            bits_done = static_cast<unsigned int>(profile->bits >= BPP);

            if ((colored_done == 0U) && (r != g || r != b)) {
                profile->colored = 1;
                colored_done = 1;
                profile->bits = std::max<unsigned int>(profile->bits, 8); /*PNG has no colored modes with less than 8-bit per channel*/
            }

            if (alpha_done == 0U) {
                auto const MATCHKEY = static_cast<unsigned int>(r == profile->key_r && g == profile->key_g && b == profile->key_b);
                if (a != 255 && (a != 0 || ((profile->key != 0U) && (MATCHKEY == 0U)))) {
                    profile->alpha = 1;
                    alpha_done = 1;
                    profile->bits =
                        std::max<unsigned int>(profile->bits, 8); /*PNG has no alphachannel modes with less than 8-bit per channel*/
                } else if (a == 0 && (profile->alpha == 0U) && (profile->key == 0U)) {
                    profile->key = 1;
                    profile->key_r = r;
                    profile->key_g = g;
                    profile->key_b = b;
                } else if (a == 255 && (profile->key != 0U) && (MATCHKEY != 0U)) {
                    /* Color key cannot be used if an opaque pixel also has that RGB color. */
                    profile->alpha = 1;
                    alpha_done = 1;
                    profile->bits =
                        std::max<unsigned int>(profile->bits, 8); /*PNG has no alphachannel modes with less than 8-bit per channel*/
                }
            }

            if (numcolors_done == 0U) {
                if (color_tree_has(&tree, r, g, b, a) == 0) {
                    color_tree_add(&tree, r, g, b, a, profile->numcolors);
                    if (profile->numcolors < 256) {
                        unsigned char* p = profile->palette;
                        unsigned const N = profile->numcolors;
                        p[(N * 4) + 0] = r;
                        p[(N * 4) + 1] = g;
                        p[(N * 4) + 2] = b;
                        p[(N * 4) + 3] = a;
                    }
                    ++profile->numcolors;
                    numcolors_done = static_cast<unsigned int>(profile->numcolors >= maxnumcolors);
                }
            }

            if ((alpha_done != 0U) && (numcolors_done != 0U) && (colored_done != 0U) && (bits_done != 0U)) {
                break;
            }
        }

        /*make the profile's key always 16-bit for consistency - repeat each byte twice*/
        profile->key_r += (profile->key_r << 8);
        profile->key_g += (profile->key_g << 8);
        profile->key_b += (profile->key_b << 8);
    }

    color_tree_cleanup(&tree);
    return ERROR;
}

/*Automatically chooses color type that gives smallest amount of bits in the
output image, e.g. grey if there are only greyscale pixels, palette if there
are less than 256 colors, ...
Updates values of mode with a potentially smaller color model. mode_out should
contain the user chosen color model, but will be overwritten with the new chosen one.*/
unsigned lodepng_auto_choose_color(LodePNGColorMode* mode_out, const unsigned char* image, unsigned w, unsigned h,
                                   const LodePNGColorMode* mode_in) {
    LodePNGColorProfile prof;
    unsigned error = 0;
    unsigned i = 0;
    unsigned n = 0;
    unsigned palettebits = 0;
    unsigned grey_ok = 0;
    unsigned palette_ok = 0;

    lodepng_color_profile_init(&prof);
    error = lodepng_get_color_profile(&prof, image, w, h, mode_in);
    if (error != 0U) {
        return error;
    }
    mode_out->key_defined = 0;

    if ((prof.key != 0U) && w * h <= 16) {
        prof.alpha = 1;                                   /*too few pixels to justify tRNS chunk overhead*/
        prof.bits = std::max<unsigned int>(prof.bits, 8); /*PNG has no alphachannel modes with less than 8-bit per channel*/
    }
    grey_ok = static_cast<unsigned int>((prof.colored == 0U) && (prof.alpha) == 0U); /*grey without alpha, with potentially low bits*/
    n = prof.numcolors;
    palettebits = n <= 2 ? 1 : (n <= 4 ? 2 : (n <= 16 ? 4 : 8));
    palette_ok = static_cast<unsigned int>(n <= 256 && (n * 2 < w * h) && prof.bits <= 8);
    if (w * h < n * 2) {
        palette_ok = 0; /*don't add palette overhead if image has only a few pixels*/
    }
    if ((grey_ok != 0U) && prof.bits <= palettebits) {
        palette_ok = 0; /*grey is less overhead*/
    }

    if (palette_ok != 0U) {
        unsigned char const* p = prof.palette;
        lodepng_palette_clear(mode_out); /*remove potential earlier palette*/
        for (i = 0; i != prof.numcolors; ++i) {
            error = lodepng_palette_add(mode_out, p[(i * 4) + 0], p[(i * 4) + 1], p[(i * 4) + 2], p[(i * 4) + 3]);
            if (error != 0U) {
                break;
            }
        }

        mode_out->colortype = LCT_PALETTE;
        mode_out->bitdepth = palettebits;

        if (mode_in->colortype == LCT_PALETTE && mode_in->palettesize >= mode_out->palettesize && mode_in->bitdepth == mode_out->bitdepth) {
            /*If input should have same palette colors, keep original to preserve its order and prevent conversion*/
            lodepng_color_mode_cleanup(mode_out);
            lodepng_color_mode_copy(mode_out, mode_in);
        }
    } else /*8-bit or 16-bit per channel*/
    {
        mode_out->bitdepth = prof.bits;
        mode_out->colortype =
            (prof.alpha != 0U) ? ((prof.colored != 0U) ? LCT_RGBA : LCT_GREY_ALPHA) : ((prof.colored != 0U) ? LCT_RGB : LCT_GREY);

        if ((prof.key != 0U) && (prof.alpha == 0U)) {
            unsigned const MASK = (1U << mode_out->bitdepth) - 1U; /*profile always uses 16-bit, mask converts it*/
            mode_out->key_r = prof.key_r & MASK;
            mode_out->key_g = prof.key_g & MASK;
            mode_out->key_b = prof.key_b & MASK;
            mode_out->key_defined = 1;
        }
    }

    return error;
}

#endif /* #ifdef LODEPNG_COMPILE_ENCODER */

/*
Paeth predicter, used by PNG filter type 4
The parameters are of type short, but should come from unsigned chars, the shorts
are only needed to make the paeth calculation correct.
*/
static unsigned char paeth_predictor(short a, short b, short c) {
    short const PA = abs(b - c);
    short const PB = abs(a - c);
    short const PC = abs(a + b - c - c);

    if (PC < PA && PC < PB) {
        return static_cast<unsigned char>(c);
    }
    if (PB < PA) {
        return static_cast<unsigned char>(b);
    }
    return static_cast<unsigned char>(a);
}

/*shared values used by multiple Adam7 related functions*/

static constexpr std::array<unsigned, 7> ADAM7_IX = {0, 4, 0, 2, 0, 1, 0}; /*x start values*/
static constexpr std::array<unsigned, 7> ADAM7_IY = {0, 0, 4, 0, 2, 0, 1}; /*y start values*/
static constexpr std::array<unsigned, 7> ADAM7_DX = {8, 8, 4, 4, 2, 2, 1}; /*x delta values*/
static constexpr std::array<unsigned, 7> ADAM7_DY = {8, 8, 8, 4, 4, 2, 2}; /*y delta values*/

static size_t scanline_byte_width(unsigned pixel_width, unsigned bpp) { return ((static_cast<size_t>(pixel_width) * bpp) + 7) / 8; }

/*
Outputs various dimensions and positions in the image related to the Adam7 reduced images.
passw: output containing the width of the 7 passes
passh: output containing the height of the 7 passes
filter_passstart: output containing the index of the start and end of each
 reduced image with filter bytes
padded_passstart output containing the index of the start and end of each
 reduced image when without filter bytes but with padded scanlines
passstart: output containing the index of the start and end of each reduced
 image without padding between scanlines, but still padding between the images
w, h: width and height of non-interlaced image
bpp: bits per pixel
"padded" is only relevant if bpp is less than 8 and a scanline or image does not
 end at a full byte
*/
static void adam7_getpassvalues(std::array<unsigned, 7>& passw, std::array<unsigned, 7>& passh, std::array<size_t, 8>& filter_passstart,
                                std::array<size_t, 8>& padded_passstart, std::array<size_t, 8>& passstart, unsigned w, unsigned h,
                                unsigned bpp) {
    /*the passstart values have 8 values: the 8th one indicates the byte after the end of the 7th (= last) pass*/
    unsigned i = 0;

    /*calculate width and height in pixels of each pass*/
    for (i = 0; i != 7; ++i) {
        passw[i] = (w + ADAM7_DX[i] - ADAM7_IX[i] - 1) / ADAM7_DX[i];
        passh[i] = (h + ADAM7_DY[i] - ADAM7_IY[i] - 1) / ADAM7_DY[i];
        if (passw[i] == 0) {
            passh[i] = 0;
        }
        if (passh[i] == 0) {
            passw[i] = 0;
        }
    }

    filter_passstart[0] = padded_passstart[0] = passstart[0] = 0;
    for (i = 0; i != 7; ++i) {
        size_t const PASS_LINEBYTES = scanline_byte_width(passw[i], bpp);
        size_t const PASS_BITS = static_cast<size_t>(passw[i]) * bpp;
        /*if passw[i] is 0, it's 0 bytes, not 1 (no filtertype-byte)*/
        filter_passstart[i + 1] =
            filter_passstart[i] + (((passw[i] != 0U) && (passh[i] != 0U)) ? static_cast<size_t>(passh[i]) * (1 + PASS_LINEBYTES) : 0);
        /*bits padded if needed to fill full byte at end of each scanline*/
        padded_passstart[i + 1] = padded_passstart[i] + (static_cast<size_t>(passh[i]) * PASS_LINEBYTES);
        /*only padded at end of reduced image*/
        passstart[i + 1] = passstart[i] + (((static_cast<size_t>(passh[i]) * PASS_BITS) + 7) / 8);
    }
}

#ifdef LODEPNG_COMPILE_DECODER

/* ////////////////////////////////////////////////////////////////////////// */
/* / PNG Decoder                                                            / */
/* ////////////////////////////////////////////////////////////////////////// */

/*read the information from the header and store it in the LodePNGInfo. return value is error*/
unsigned lodepng_inspect(unsigned* w, unsigned* h, LodePNGState* state, const unsigned char* in, size_t insize) {
    LodePNGInfo* info = &state->info_png;
    if (insize == 0 || in == nullptr) {
        CERROR_RETURN_ERROR(state->error, 48); /*error: the given data is empty*/
    }
    if (insize < 33) {
        CERROR_RETURN_ERROR(state->error, 27); /*error: the data length is smaller than the length of a PNG header*/
    }

    /*when decoding a new PNG image, make sure all parameters created after previous decoding are reset*/
    lodepng_info_cleanup(info);
    lodepng_info_init(info);

    if (in[0] != 137 || in[1] != 80 || in[2] != 78 || in[3] != 71 || in[4] != 13 || in[5] != 10 || in[6] != 26 || in[7] != 10) {
        CERROR_RETURN_ERROR(state->error, 28); /*error: the first 8 bytes are not the correct PNG signature*/
    }
    if (in[12] != 'I' || in[13] != 'H' || in[14] != 'D' || in[15] != 'R') {
        CERROR_RETURN_ERROR(state->error, 29); /*error: it doesn't start with a IHDR chunk!*/
    }

    /*read the values given in the header*/
    *w = lodepng_read32bit_int(&in[16]);
    *h = lodepng_read32bit_int(&in[20]);
    info->color.bitdepth = in[24];
    info->color.colortype = static_cast<LodePNGColorType>(in[25]);
    info->compression_method = in[26];
    info->filter_method = in[27];
    info->interlace_method = in[28];

    if (*w == 0 || *h == 0) {
        CERROR_RETURN_ERROR(state->error, 93);
    }

    if (state->decoder.ignore_crc == 0U) {
        unsigned const CRC = lodepng_read32bit_int(&in[29]);
        unsigned const CHECKSUM = lodepng_crc32(&in[12], 17);
        if (CRC != CHECKSUM) {
            CERROR_RETURN_ERROR(state->error, 57); /*invalid CRC*/
        }
    }

    /*error: only compression method 0 is allowed in the specification*/
    if (info->compression_method != 0) CERROR_RETURN_ERROR(state->error, 32);
    /*error: only filter method 0 is allowed in the specification*/
    if (info->filter_method != 0) CERROR_RETURN_ERROR(state->error, 33);
    /*error: only interlace methods 0 and 1 exist in the specification*/
    if (info->interlace_method > 1) CERROR_RETURN_ERROR(state->error, 34);

    state->error = check_color_validity(info->color.colortype, info->color.bitdepth);
    return state->error;
}

static unsigned unfilter_scanline(unsigned char* recon, const unsigned char* scanline, const unsigned char* precon, size_t bytewidth,
                                  unsigned char filter_type, size_t length) {
    /*
    For PNG filter method 0
    unfilter a PNG image scanline by scanline. when the pixels are smaller than 1 byte,
    the filter works byte per byte (bytewidth = 1)
    precon is the previous unfiltered scanline, recon the result, scanline the current one
    the incoming scanlines do NOT include the filtertype byte, that one is given in the parameter filterType instead
    recon and scanline MAY be the same memory address! precon must be disjoint.
    */

    size_t i = 0;
    switch (filter_type) {
        case 0:
            for (i = 0; i != length; ++i) {
                recon[i] = scanline[i];
            }
            break;
        case 1:
            for (i = 0; i != bytewidth; ++i) {
                recon[i] = scanline[i];
            }
            for (i = bytewidth; i < length; ++i) {
                recon[i] = scanline[i] + recon[i - bytewidth];
            }
            break;
        case 2:
            if (precon != nullptr) {
                for (i = 0; i != length; ++i) {
                    recon[i] = scanline[i] + precon[i];
                }
            } else {
                for (i = 0; i != length; ++i) {
                    recon[i] = scanline[i];
                }
            }
            break;
        case 3:
            if (precon != nullptr) {
                for (i = 0; i != bytewidth; ++i) {
                    recon[i] = scanline[i] + (precon[i] / 2);
                }
                for (i = bytewidth; i < length; ++i) {
                    recon[i] = scanline[i] + ((recon[i - bytewidth] + precon[i]) / 2);
                }
            } else {
                for (i = 0; i != bytewidth; ++i) {
                    recon[i] = scanline[i];
                }
                for (i = bytewidth; i < length; ++i) {
                    recon[i] = scanline[i] + (recon[i - bytewidth] / 2);
                }
            }
            break;
        case 4:
            if (precon != nullptr) {
                for (i = 0; i != bytewidth; ++i) {
                    recon[i] = (scanline[i] + precon[i]); /*paethPredictor(0, precon[i], 0) is always precon[i]*/
                }
                for (i = bytewidth; i < length; ++i) {
                    recon[i] = (scanline[i] + paeth_predictor(recon[i - bytewidth], precon[i], precon[i - bytewidth]));
                }
            } else {
                for (i = 0; i != bytewidth; ++i) {
                    recon[i] = scanline[i];
                }
                for (i = bytewidth; i < length; ++i) {
                    /*paethPredictor(recon[i - bytewidth], 0, 0) is always recon[i - bytewidth]*/
                    recon[i] = (scanline[i] + recon[i - bytewidth]);
                }
            }
            break;
        default:
            return 36; /*error: unexisting filter type given*/
    }
    return 0;
}

static unsigned unfilter(unsigned char* out, const unsigned char* in, unsigned w, unsigned h, unsigned bpp) {
    /*
    For PNG filter method 0
    this function unfilters a single image (e.g. without interlacing this is called once, with Adam7 seven times)
    out must have enough bytes allocated already, in must have the scanlines + 1 filtertype byte per scanline
    w and h are image dimensions or dimensions of reduced image, bpp is bits per pixel
    in and out are allowed to be the same memory address (but aren't the same size since in has the extra filter bytes)
    */

    unsigned y = 0;
    unsigned char const* prevline = nullptr;

    /*bytewidth is used for filtering, is 1 when bpp < 8, number of bytes per pixel otherwise*/
    size_t const BYTEWIDTH = (bpp + 7) / 8;
    size_t const LINEBYTES = ((w * bpp) + 7) / 8;

    for (y = 0; y < h; ++y) {
        size_t const OUTINDEX = LINEBYTES * y;
        size_t const ININDEX = (1 + LINEBYTES) * y; /*the extra filterbyte added to each row*/
        unsigned char const FILTER_TYPE = in[ININDEX];

        CERROR_TRY_RETURN(unfilter_scanline(&out[OUTINDEX], &in[ININDEX + 1], prevline, BYTEWIDTH, FILTER_TYPE, LINEBYTES));

        prevline = &out[OUTINDEX];
    }

    return 0;
}

/*
in: Adam7 interlaced image, with no padding bits between scanlines, but between
 reduced images so that each reduced image starts at a byte.
out: the same pixels, but re-ordered so that they're now a non-interlaced image with size w*h
bpp: bits per pixel
out has the following size in bits: w * h * bpp.
in is possibly bigger due to padding bits between reduced images.
out must be big enough AND must be 0 everywhere if bpp < 8 in the current implementation
(because that's likely a little bit faster)
NOTE: comments about padding bits are only relevant if bpp < 8
*/
static void adam7_deinterlace(unsigned char* out, const unsigned char* in, unsigned w, unsigned h, unsigned bpp) {
    std::array<unsigned, 7> passw{};
    std::array<unsigned, 7> passh{};
    std::array<size_t, 8> filter_passstart{};
    std::array<size_t, 8> padded_passstart{};
    std::array<size_t, 8> passstart{};
    unsigned i = 0;

    adam7_getpassvalues(passw, passh, filter_passstart, padded_passstart, passstart, w, h, bpp);

    if (bpp >= 8) {
        for (i = 0; i != 7; ++i) {
            unsigned x = 0;
            unsigned y = 0;
            unsigned b = 0;
            size_t const BYTEWIDTH = bpp / 8;
            for (y = 0; y < passh[i]; ++y) {
                for (x = 0; x < passw[i]; ++x) {
                    size_t const PIXELINSTART = passstart[i] + (((y * passw[i]) + x) * BYTEWIDTH);
                    size_t const PIXELOUTSTART = (((ADAM7_IY[i] + (y * ADAM7_DY[i])) * w) + ADAM7_IX[i] + (x * ADAM7_DX[i])) * BYTEWIDTH;
                    for (b = 0; b < BYTEWIDTH; ++b) {
                        out[PIXELOUTSTART + b] = in[PIXELINSTART + b];
                    }
                }
            }
        }
    } else /*bpp < 8: Adam7 with pixels < 8 bit is a bit trickier: with bit pointers*/
    {
        for (i = 0; i != 7; ++i) {
            unsigned x = 0;
            unsigned y = 0;
            unsigned b = 0;
            unsigned const ILINEBITS = bpp * passw[i];
            unsigned const OLINEBITS = bpp * w;
            size_t obp = 0;
            size_t ibp = 0; /*bit pointers (for out and in buffer)*/
            for (y = 0; y < passh[i]; ++y) {
                for (x = 0; x < passw[i]; ++x) {
                    ibp = (8 * passstart[i]) + ((y * ILINEBITS) + (x * bpp));
                    obp = ((ADAM7_IY[i] + (y * ADAM7_DY[i])) * OLINEBITS) + ((ADAM7_IX[i] + (x * ADAM7_DX[i])) * bpp);
                    for (b = 0; b < bpp; ++b) {
                        unsigned char const BIT = read_bit_from_reversed_stream(&ibp, in);
                        /*note that this function assumes the out buffer is completely 0, use setBitOfReversedStream otherwise*/
                        set_bit_of_reversed_stream0(&obp, out, BIT);
                    }
                }
            }
        }
    }
}

static void remove_padding_bits(unsigned char* out, const unsigned char* in, size_t olinebits, size_t ilinebits, unsigned h) {
    /*
    After filtering there are still padding bits if scanlines have non multiple of 8 bit amounts. They need
    to be removed (except at last scanline of (Adam7-reduced) image) before working with pure image buffers
    for the Adam7 code, the color convert code and the output to the user.
    in and out are allowed to be the same buffer, in may also be higher but still overlapping; in must
    have >= ilinebits*h bits, out must have >= olinebits*h bits, olinebits must be <= ilinebits
    also used to move bits after earlier such operations happened, e.g. in a sequence of reduced images from Adam7
    only useful if (ilinebits - olinebits) is a value in the range 1..7
    */
    unsigned y = 0;
    size_t const DIFF = ilinebits - olinebits;
    size_t ibp = 0;
    size_t obp = 0; /*input and output bit pointers*/
    for (y = 0; y < h; ++y) {
        size_t x = 0;
        for (x = 0; x < olinebits; ++x) {
            unsigned char const BIT = read_bit_from_reversed_stream(&ibp, in);
            set_bit_of_reversed_stream(&obp, out, BIT);
        }
        ibp += DIFF;
    }
}

/*out must be buffer big enough to contain full image, and in must contain the full decompressed data from
the IDAT chunks (with filter index bytes and possible padding bits)
return value is error*/
static unsigned post_process_scanlines(unsigned char* out, unsigned char* in, unsigned w, unsigned h, const LodePNGInfo* info_png) {
    /*
    This function converts the filtered-padded-interlaced data into pure 2D image buffer with the PNG's colortype.
    Steps:
    *) if no Adam7: 1) unfilter 2) remove padding bits (= posible extra bits per scanline if bpp < 8)
    *) if adam7: 1) 7x unfilter 2) 7x remove padding bits 3) Adam7_deinterlace
    NOTE: the in buffer will be overwritten with intermediate data!
    */
    unsigned const BPP = lodepng_get_bpp(&info_png->color);
    if (BPP == 0) {
        return 31; /*error: invalid colortype*/
    }

    if (info_png->interlace_method == 0) {
        if (BPP < 8 && w * BPP != (((w * BPP) + 7) / 8) * 8) {
            CERROR_TRY_RETURN(unfilter(in, in, w, h, BPP));
            remove_padding_bits(out, in, w * BPP, (((w * BPP) + 7) / 8) * 8, h);
        }
        /*we can immediatly filter into the out buffer, no other steps needed*/
        else
            CERROR_TRY_RETURN(unfilter(out, in, w, h, BPP));
    } else /*interlace_method is 1 (Adam7)*/
    {
        std::array<unsigned, 7> passw{};
        std::array<unsigned, 7> passh{};
        std::array<size_t, 8> filter_passstart{};
        std::array<size_t, 8> padded_passstart{};
        std::array<size_t, 8> passstart{};
        unsigned i = 0;

        adam7_getpassvalues(passw, passh, filter_passstart, padded_passstart, passstart, w, h, BPP);

        for (i = 0; i != 7; ++i) {
            CERROR_TRY_RETURN(unfilter(&in[padded_passstart[i]], &in[filter_passstart[i]], passw[i], passh[i], BPP));
            /*TODO: possible efficiency improvement: if in this reduced image the bits fit nicely in 1 scanline,
            move bytes instead of bits or move not at all*/
            if (BPP < 8) {
                /*remove padding bits in scanlines; after this there still may be padding
                bits between the different reduced images: each reduced image still starts nicely at a byte*/
                remove_padding_bits(&in[passstart[i]], &in[padded_passstart[i]], passw[i] * BPP, (((passw[i] * BPP) + 7) / 8) * 8,
                                    passh[i]);
            }
        }

        adam7_deinterlace(out, in, w, h, BPP);
    }

    return 0;
}

static unsigned read_chunk_plte(LodePNGColorMode* color, const unsigned char* data, size_t chunk_length) {
    unsigned pos = 0;
    unsigned i = 0;
    if (color->palette != nullptr) {
        lodepng_free(color->palette);
    }
    color->palettesize = chunk_length / 3;
    color->palette = static_cast<unsigned char*>(lodepng_malloc(4 * color->palettesize));
    if ((color->palette == nullptr) && (color->palettesize != 0U)) {
        color->palettesize = 0;
        return 83; /*alloc fail*/
    }
    if (color->palettesize > 256) {
        return 38; /*error: palette too big*/
    }

    for (i = 0; i != color->palettesize; ++i) {
        color->palette[(4 * i) + 0] = data[pos++]; /*R*/
        color->palette[(4 * i) + 1] = data[pos++]; /*G*/
        color->palette[(4 * i) + 2] = data[pos++]; /*B*/
        color->palette[(4 * i) + 3] = 255;         /*alpha*/
    }

    return 0; /* OK */
}

static unsigned read_chunk_t_rns(LodePNGColorMode* color, const unsigned char* data, size_t chunk_length) {
    unsigned i = 0;
    if (color->colortype == LCT_PALETTE) {
        /*error: more alpha values given than there are palette entries*/
        if (chunk_length > color->palettesize) {
            return 38;
        }

        for (i = 0; i != chunk_length; ++i) {
            color->palette[(4 * i) + 3] = data[i];
        }
    } else if (color->colortype == LCT_GREY) {
        /*error: this chunk must be 2 bytes for greyscale image*/
        if (chunk_length != 2) {
            return 30;
        }

        color->key_defined = 1;
        color->key_r = color->key_g = color->key_b = (256U * data[0]) + data[1];
    } else if (color->colortype == LCT_RGB) {
        /*error: this chunk must be 6 bytes for RGB image*/
        if (chunk_length != 6) {
            return 41;
        }

        color->key_defined = 1;
        color->key_r = (256U * data[0]) + data[1];
        color->key_g = (256U * data[2]) + data[3];
        color->key_b = (256U * data[4]) + data[5];
    } else {
        {
            return 42; /*error: tRNS chunk not allowed for other color models*/
        }
    }

    return 0; /* OK */
}

#ifdef LODEPNG_COMPILE_ANCILLARY_CHUNKS
/*background color chunk (bKGD)*/
static unsigned read_chunk_b_kgd(LodePNGInfo* info, const unsigned char* data, size_t chunk_length) {
    if (info->color.colortype == LCT_PALETTE) {
        /*error: this chunk must be 1 byte for indexed color image*/
        if (chunk_length != 1) {
            return 43;
        }

        info->background_defined = 1;
        info->background_r = info->background_g = info->background_b = data[0];
    } else if (info->color.colortype == LCT_GREY || info->color.colortype == LCT_GREY_ALPHA) {
        /*error: this chunk must be 2 bytes for greyscale image*/
        if (chunk_length != 2) {
            return 44;
        }

        info->background_defined = 1;
        info->background_r = info->background_g = info->background_b = (256U * data[0]) + data[1];
    } else if (info->color.colortype == LCT_RGB || info->color.colortype == LCT_RGBA) {
        /*error: this chunk must be 6 bytes for greyscale image*/
        if (chunk_length != 6) {
            return 45;
        }

        info->background_defined = 1;
        info->background_r = (256U * data[0]) + data[1];
        info->background_g = (256U * data[2]) + data[3];
        info->background_b = (256U * data[4]) + data[5];
    }

    return 0; /* OK */
}

/*text chunk (tEXt)*/
static unsigned read_chunk_t_e_xt(LodePNGInfo* info, const unsigned char* data, size_t chunk_length) {
    unsigned error = 0;
    char* key = nullptr;
    char* str = nullptr;
    unsigned i = 0;

    while (error == 0U) /*not really a while loop, only used to break on error*/
    {
        unsigned length = 0;
        unsigned string2_begin = 0;

        length = 0;
        while (length < chunk_length && data[length] != 0) {
            ++length;
        }
        /*even though it's not allowed by the standard, no error is thrown if
        there's no null termination char, if the text is empty*/
        if (length < 1 || length > 79) CERROR_BREAK(error, 89); /*keyword too short or long*/

        key = static_cast<char*>(lodepng_malloc(length + 1));
        if (key == nullptr) CERROR_BREAK(error, 83); /*alloc fail*/

        key[length] = 0;
        for (i = 0; i != length; ++i) {
            key[i] = static_cast<char>(data[i]);
        }

        string2_begin = length + 1; /*skip keyword null terminator*/

        length = chunk_length < string2_begin ? 0 : chunk_length - string2_begin;
        str = static_cast<char*>(lodepng_malloc(length + 1));
        if (str == nullptr) CERROR_BREAK(error, 83); /*alloc fail*/

        str[length] = 0;
        for (i = 0; i != length; ++i) {
            str[i] = static_cast<char>(data[string2_begin + i]);
        }

        error = lodepng_add_text(info, key, str);

        break;
    }

    lodepng_free(key);
    lodepng_free(str);

    return error;
}

/*compressed text chunk (zTXt)*/
static unsigned read_chunk_z_t_xt(LodePNGInfo* info, const LodePNGDecompressSettings* zlibsettings, const unsigned char* data,
                                  size_t chunk_length) {
    unsigned error = 0;
    unsigned i = 0;

    unsigned length = 0;
    unsigned string2_begin = 0;
    char* key = nullptr;
    ucvector decoded;

    ucvector_init(&decoded);

    while (error == 0U) /*not really a while loop, only used to break on error*/
    {
        for (length = 0; length < chunk_length && data[length] != 0; ++length) {
            ;
        }
        if (length + 2 >= chunk_length) CERROR_BREAK(error, 75); /*no null termination, corrupt?*/
        if (length < 1 || length > 79) CERROR_BREAK(error, 89);  /*keyword too short or long*/

        key = static_cast<char*>(lodepng_malloc(length + 1));
        if (key == nullptr) CERROR_BREAK(error, 83); /*alloc fail*/

        key[length] = 0;
        for (i = 0; i != length; ++i) {
            key[i] = static_cast<char>(data[i]);
        }

        if (data[length + 1] != 0) CERROR_BREAK(error, 72); /*the 0 byte indicating compression must be 0*/

        string2_begin = length + 2;
        if (string2_begin > chunk_length) CERROR_BREAK(error, 75); /*no null termination, corrupt?*/

        length = chunk_length - string2_begin;
        /*will fail if zlib error, e.g. if length is too small*/
        error = zlib_decompress(&decoded.data, &decoded.size, const_cast<unsigned char*>(&data[string2_begin]), length, zlibsettings);
        if (error != 0U) {
            break;
        }
        ucvector_push_back(&decoded, 0);

        error = lodepng_add_text(info, key, reinterpret_cast<char*>(decoded.data));

        break;
    }

    lodepng_free(key);
    ucvector_cleanup(&decoded);

    return error;
}

/*international text chunk (iTXt)*/
static unsigned read_chunk_i_t_xt(LodePNGInfo* info, const LodePNGDecompressSettings* zlibsettings, const unsigned char* data,
                                  size_t chunk_length) {
    unsigned error = 0;
    unsigned i = 0;

    unsigned length = 0;
    unsigned begin = 0;
    unsigned compressed = 0;
    char* key = nullptr;
    char* langtag = nullptr;
    char* transkey = nullptr;
    ucvector decoded;
    ucvector_init(&decoded);

    while (error == 0U) /*not really a while loop, only used to break on error*/
    {
        /*Quick check if the chunk length isn't too small. Even without check
        it'd still fail with other error checks below if it's too short. This just gives a different error code.*/
        if (chunk_length < 5) CERROR_BREAK(error, 30); /*iTXt chunk too short*/

        /*read the key*/
        for (length = 0; length < chunk_length && data[length] != 0; ++length) {
            ;
        }
        if (length + 3 >= chunk_length) CERROR_BREAK(error, 75); /*no null termination char, corrupt?*/
        if (length < 1 || length > 79) CERROR_BREAK(error, 89);  /*keyword too short or long*/

        key = static_cast<char*>(lodepng_malloc(length + 1));
        if (key == nullptr) CERROR_BREAK(error, 83); /*alloc fail*/

        key[length] = 0;
        for (i = 0; i != length; ++i) {
            key[i] = static_cast<char>(data[i]);
        }

        /*read the compression method*/
        compressed = data[length + 1];
        if (data[length + 2] != 0) CERROR_BREAK(error, 72); /*the 0 byte indicating compression must be 0*/

        /*even though it's not allowed by the standard, no error is thrown if
        there's no null termination char, if the text is empty for the next 3 texts*/

        /*read the langtag*/
        begin = length + 3;
        length = 0;
        for (i = begin; i < chunk_length && data[i] != 0; ++i) {
            ++length;
        }

        langtag = static_cast<char*>(lodepng_malloc(length + 1));
        if (langtag == nullptr) CERROR_BREAK(error, 83); /*alloc fail*/

        langtag[length] = 0;
        for (i = 0; i != length; ++i) {
            langtag[i] = static_cast<char>(data[begin + i]);
        }

        /*read the transkey*/
        begin += length + 1;
        length = 0;
        for (i = begin; i < chunk_length && data[i] != 0; ++i) {
            ++length;
        }

        transkey = static_cast<char*>(lodepng_malloc(length + 1));
        if (transkey == nullptr) CERROR_BREAK(error, 83); /*alloc fail*/

        transkey[length] = 0;
        for (i = 0; i != length; ++i) {
            transkey[i] = static_cast<char>(data[begin + i]);
        }

        /*read the actual text*/
        begin += length + 1;

        length = chunk_length < begin ? 0 : chunk_length - begin;

        if (compressed != 0U) {
            /*will fail if zlib error, e.g. if length is too small*/
            error = zlib_decompress(&decoded.data, &decoded.size, const_cast<unsigned char*>(&data[begin]), length, zlibsettings);
            if (error != 0U) {
                break;
            }
            decoded.allocsize = std::max(decoded.allocsize, decoded.size);
            ucvector_push_back(&decoded, 0);
        } else {
            if (ucvector_resize(&decoded, length + 1) == 0U) CERROR_BREAK(error, 83 /*alloc fail*/);

            decoded.data[length] = 0;
            for (i = 0; i != length; ++i) {
                decoded.data[i] = data[begin + i];
            }
        }

        error = lodepng_add_itext(info, key, langtag, transkey, reinterpret_cast<char*>(decoded.data));

        break;
    }

    lodepng_free(key);
    lodepng_free(langtag);
    lodepng_free(transkey);
    ucvector_cleanup(&decoded);

    return error;
}

static unsigned read_chunk_t_ime(LodePNGInfo* info, const unsigned char* data, size_t chunk_length) {
    if (chunk_length != 7) {
        return 73; /*invalid tIME chunk size*/
    }

    info->time_defined = 1;
    info->time.year = (256U * data[0]) + data[1];
    info->time.month = data[2];
    info->time.day = data[3];
    info->time.hour = data[4];
    info->time.minute = data[5];
    info->time.second = data[6];

    return 0; /* OK */
}

static unsigned read_chunk_p_h_ys(LodePNGInfo* info, const unsigned char* data, size_t chunk_length) {
    if (chunk_length != 9) {
        return 74; /*invalid pHYs chunk size*/
    }

    info->phys_defined = 1;
    info->phys_x = (16777216U * data[0]) + (65536U * data[1]) + (256U * data[2]) + data[3];
    info->phys_y = (16777216U * data[4]) + (65536U * data[5]) + (256U * data[6]) + data[7];
    info->phys_unit = data[8];

    return 0; /* OK */
}
#endif /*LODEPNG_COMPILE_ANCILLARY_CHUNKS*/

/*read a PNG, the result will be in the same color type as the PNG (hence "generic")*/
static void decode_generic(unsigned char** out, unsigned* w, unsigned* h, LodePNGState* state, const unsigned char* in, size_t insize) {
    unsigned char iend = 0;
    const unsigned char* chunk = nullptr;
    size_t i = 0;
    ucvector idat; /*the data from idat chunks*/
    ucvector scanlines;
    size_t predict = 0;
    size_t numpixels = 0;

    /*for unknown chunk order*/
    unsigned unknown = 0;
#ifdef LODEPNG_COMPILE_ANCILLARY_CHUNKS
    unsigned critical_pos = 1; /*1 = after IHDR, 2 = after PLTE, 3 = after IDAT*/
#endif                         /*LODEPNG_COMPILE_ANCILLARY_CHUNKS*/

    /*provide some proper output values if error will happen*/
    *out = nullptr;

    state->error = lodepng_inspect(w, h, state, in, insize); /*reads header and resets other parameters in state->info_png*/
    if (state->error != 0U) {
        return;
    }

    numpixels = *w * *h;

    /*multiplication overflow*/
    if (*h != 0 && numpixels / *h != *w) CERROR_RETURN(state->error, 92);
    /*multiplication overflow possible further below. Allows up to 2^31-1 pixel
    bytes with 16-bit RGBA, the rest is room for filter bytes.*/
    if (numpixels > 268435455) CERROR_RETURN(state->error, 92);

    ucvector_init(&idat);
    chunk = &in[33]; /*first byte of the first chunk after the header*/

    /*loop through the chunks, ignoring unknown chunks and stopping at IEND chunk.
    IDAT data is put at the start of the in buffer*/
    while ((iend == 0U) && (state->error == 0U)) {
        unsigned chunk_length = 0;
        const unsigned char* data = nullptr; /*the data in the chunk*/

        /*error: size of the in buffer too small to contain next chunk*/
        if (std::cmp_greater(((chunk - in) + 12), insize) || chunk < in) CERROR_BREAK(state->error, 30);

        /*length of the data of the chunk, excluding the length bytes, chunk type and CRC bytes*/
        chunk_length = lodepng_chunk_length(chunk);
        /*error: chunk length larger than the max PNG chunk size*/
        if (chunk_length > 2147483647) CERROR_BREAK(state->error, 63);

        if (std::cmp_greater(((chunk - in) + chunk_length + 12), insize) || (chunk + chunk_length + 12) < in) {
            CERROR_BREAK(state->error, 64); /*error: size of the in buffer too small to contain next chunk*/
        }

        data = lodepng_chunk_data_const(chunk);

        /*IDAT chunk, containing compressed image data*/
        if (lodepng_chunk_type_equals(chunk, "IDAT") != 0U) {
            size_t const OLDSIZE = idat.size;
            if (ucvector_resize(&idat, OLDSIZE + chunk_length) == 0U) CERROR_BREAK(state->error, 83 /*alloc fail*/);
            for (i = 0; i != chunk_length; ++i) {
                idat.data[OLDSIZE + i] = data[i];
            }
#ifdef LODEPNG_COMPILE_ANCILLARY_CHUNKS
            critical_pos = 3;
#endif /*LODEPNG_COMPILE_ANCILLARY_CHUNKS*/
        }
        /*IEND chunk*/
        else if (lodepng_chunk_type_equals(chunk, "IEND") != 0U) {
            iend = 1;
        }
        /*palette chunk (PLTE)*/
        else if (lodepng_chunk_type_equals(chunk, "PLTE") != 0U) {
            state->error = read_chunk_plte(&state->info_png.color, data, chunk_length);
            if (state->error != 0U) {
                break;
            }
#ifdef LODEPNG_COMPILE_ANCILLARY_CHUNKS
            critical_pos = 2;
#endif /*LODEPNG_COMPILE_ANCILLARY_CHUNKS*/
        }
        /*palette transparency chunk (tRNS)*/
        else if (lodepng_chunk_type_equals(chunk, "tRNS") != 0U) {
            state->error = read_chunk_t_rns(&state->info_png.color, data, chunk_length);
            if (state->error != 0U) {
                break;
            }
        }
#ifdef LODEPNG_COMPILE_ANCILLARY_CHUNKS
        /*background color chunk (bKGD)*/
        else if (lodepng_chunk_type_equals(chunk, "bKGD") != 0U) {
            state->error = read_chunk_b_kgd(&state->info_png, data, chunk_length);
            if (state->error != 0U) {
                break;
            }
        }
        /*text chunk (tEXt)*/
        else if (lodepng_chunk_type_equals(chunk, "tEXt") != 0U) {
            if (state->decoder.read_text_chunks != 0U) {
                state->error = read_chunk_t_e_xt(&state->info_png, data, chunk_length);
                if (state->error != 0U) {
                    break;
                }
            }
        }
        /*compressed text chunk (zTXt)*/
        else if (lodepng_chunk_type_equals(chunk, "zTXt") != 0U) {
            if (state->decoder.read_text_chunks != 0U) {
                state->error = read_chunk_z_t_xt(&state->info_png, &state->decoder.zlibsettings, data, chunk_length);
                if (state->error != 0U) {
                    break;
                }
            }
        }
        /*international text chunk (iTXt)*/
        else if (lodepng_chunk_type_equals(chunk, "iTXt") != 0U) {
            if (state->decoder.read_text_chunks != 0U) {
                state->error = read_chunk_i_t_xt(&state->info_png, &state->decoder.zlibsettings, data, chunk_length);
                if (state->error != 0U) {
                    break;
                }
            }
        } else if (lodepng_chunk_type_equals(chunk, "tIME") != 0U) {
            state->error = read_chunk_t_ime(&state->info_png, data, chunk_length);
            if (state->error != 0U) {
                break;
            }
        } else if (lodepng_chunk_type_equals(chunk, "pHYs") != 0U) {
            state->error = read_chunk_p_h_ys(&state->info_png, data, chunk_length);
            if (state->error != 0U) {
                break;
            }
        }
#endif       /*LODEPNG_COMPILE_ANCILLARY_CHUNKS*/
        else /*it's not an implemented chunk type, so ignore it: skip over the data*/
        {
            /*error: unknown critical chunk (5th bit of first byte of chunk type is 0)*/
            if (lodepng_chunk_ancillary(chunk) == 0U) CERROR_BREAK(state->error, 69);

            unknown = 1;
#ifdef LODEPNG_COMPILE_ANCILLARY_CHUNKS
            if (state->decoder.remember_unknown_chunks != 0U) {
                state->error = lodepng_chunk_append(&state->info_png.unknown_chunks_data[critical_pos - 1],
                                                    &state->info_png.unknown_chunks_size[critical_pos - 1], chunk);
                if (state->error != 0U) {
                    break;
                }
            }
#endif /*LODEPNG_COMPILE_ANCILLARY_CHUNKS*/
        }

        if ((state->decoder.ignore_crc == 0U) && (unknown == 0U)) /*check CRC if wanted, only on known chunk types*/
        {
            if (lodepng_chunk_check_crc(chunk) != 0U) CERROR_BREAK(state->error, 57); /*invalid CRC*/
        }

        if (iend == 0U) {
            chunk = lodepng_chunk_next_const(chunk);
        }
    }

    ucvector_init(&scanlines);
    /*predict output size, to allocate exact size for output buffer to avoid more dynamic allocation.
    If the decompressed size does not match the prediction, the image must be corrupt.*/
    if (state->info_png.interlace_method == 0) {
        /*The extra *h is added because this are the filter bytes every scanline starts with*/
        predict = lodepng_get_raw_size_idat(*w, *h, &state->info_png.color) + *h;
    } else {
        /*Adam-7 interlaced: predicted size is the sum of the 7 sub-images sizes*/
        const LodePNGColorMode* color = &state->info_png.color;
        predict = 0;
        predict += lodepng_get_raw_size_idat((*w + 7) / 8, (*h + 7) / 8, color) + ((*h + 7) / 8);
        if (*w > 4) {
            predict += lodepng_get_raw_size_idat((*w + 3) / 8, (*h + 7) / 8, color) + ((*h + 7) / 8);
        }
        predict += lodepng_get_raw_size_idat((*w + 3) / 4, (*h + 3) / 8, color) + ((*h + 3) / 8);
        if (*w > 2) {
            predict += lodepng_get_raw_size_idat((*w + 1) / 4, (*h + 3) / 4, color) + ((*h + 3) / 4);
        }
        predict += lodepng_get_raw_size_idat((*w + 1) / 2, (*h + 1) / 4, color) + ((*h + 1) / 4);
        if (*w > 1) {
            predict += lodepng_get_raw_size_idat((*w + 0) / 2, (*h + 1) / 2, color) + ((*h + 1) / 2);
        }
        predict += lodepng_get_raw_size_idat((*w + 0) / 1, (*h + 0) / 2, color) + ((*h + 0) / 2);
    }
    if ((state->error == 0U) && (ucvector_reserve(&scanlines, predict) == 0U)) {
        state->error = 83; /*alloc fail*/
    }
    if (state->error == 0U) {
        state->error = zlib_decompress(&scanlines.data, &scanlines.size, idat.data, idat.size, &state->decoder.zlibsettings);
        if ((state->error == 0U) && scanlines.size != predict) {
            state->error = 91; /*decompressed size doesn't match prediction*/
        }
    }
    ucvector_cleanup(&idat);

    if (state->error == 0U) {
        size_t const OUTSIZE = lodepng_get_raw_size(*w, *h, &state->info_png.color);
        ucvector outv;
        ucvector_init(&outv);
        if (ucvector_resizev(&outv, OUTSIZE, 0) == 0U) {
            state->error = 83; /*alloc fail*/
        }
        if (state->error == 0U) {
            state->error = post_process_scanlines(outv.data, scanlines.data, *w, *h, &state->info_png);
        }
        *out = outv.data;
    }
    ucvector_cleanup(&scanlines);
}

unsigned lodepng_decode(unsigned char** out, unsigned* w, unsigned* h, LodePNGState* state, const unsigned char* in, size_t insize) {
    *out = nullptr;
    decode_generic(out, w, h, state, in, insize);
    if (state->error != 0U) {
        return state->error;
    }
    if ((state->decoder.color_convert == 0U) || (lodepng_color_mode_equal(&state->info_raw, &state->info_png.color) != 0)) {
        /*same color type, no copying or converting of data needed*/
        /*store the info_png color settings on the info_raw so that the info_raw still reflects what colortype
        the raw image has to the end user*/
        if (state->decoder.color_convert == 0U) {
            state->error = lodepng_color_mode_copy(&state->info_raw, &state->info_png.color);
            if (state->error != 0U) {
                return state->error;
            }
        }
    } else {
        /*color conversion needed; sort of copy of the data*/
        unsigned char* data = *out;
        size_t outsize = 0;

        /*TODO: check if this works according to the statement in the documentation: "The converter can convert
        from greyscale input color type, to 8-bit greyscale or greyscale with alpha"*/
        if (state->info_raw.colortype != LCT_RGB && state->info_raw.colortype != LCT_RGBA && !(state->info_raw.bitdepth == 8)) {
            return 56; /*unsupported color mode conversion*/
        }

        outsize = lodepng_get_raw_size(*w, *h, &state->info_raw);
        *out = static_cast<unsigned char*>(lodepng_malloc(outsize));
        if ((*out) == nullptr) {
            state->error = 83; /*alloc fail*/
        } else {
            {
                state->error = lodepng_convert(*out, data, &state->info_raw, &state->info_png.color, *w, *h);
            }
        }
        lodepng_free(data);
    }
    return state->error;
}

unsigned lodepng_decode_memory(unsigned char** out, unsigned* w, unsigned* h, const unsigned char* in, size_t insize,
                               LodePNGColorType colortype, unsigned bitdepth) {
    unsigned error = 0;
    LodePNGState state;
    lodepng_state_init(&state);
    state.info_raw.colortype = colortype;
    state.info_raw.bitdepth = bitdepth;
    error = lodepng_decode(out, w, h, &state, in, insize);
    lodepng_state_cleanup(&state);
    return error;
}

unsigned lodepng_decode32(unsigned char** out, unsigned* w, unsigned* h, const unsigned char* in, size_t insize) {
    return lodepng_decode_memory(out, w, h, in, insize, LCT_RGBA, 8);
}

unsigned lodepng_decode24(unsigned char** out, unsigned* w, unsigned* h, const unsigned char* in, size_t insize) {
    return lodepng_decode_memory(out, w, h, in, insize, LCT_RGB, 8);
}

#ifdef LODEPNG_COMPILE_DISK
unsigned lodepng_decode_file(unsigned char** out, unsigned* w, unsigned* h, const char* filename, LodePNGColorType colortype,
                             unsigned bitdepth) {
    unsigned char* buffer = nullptr;
    size_t buffersize = 0;
    unsigned error = 0;
    error = lodepng_load_file(&buffer, &buffersize, filename);
    if (error == 0U) {
        error = lodepng_decode_memory(out, w, h, buffer, buffersize, colortype, bitdepth);
    }
    lodepng_free(buffer);
    return error;
}

unsigned lodepng_decode32_file(unsigned char** out, unsigned* w, unsigned* h, const char* filename) {
    return lodepng_decode_file(out, w, h, filename, LCT_RGBA, 8);
}

unsigned lodepng_decode24_file(unsigned char** out, unsigned* w, unsigned* h, const char* filename) {
    return lodepng_decode_file(out, w, h, filename, LCT_RGB, 8);
}
#endif /*LODEPNG_COMPILE_DISK*/

void lodepng_decoder_settings_init(LodePNGDecoderSettings* settings) {
    settings->color_convert = 1;
#ifdef LODEPNG_COMPILE_ANCILLARY_CHUNKS
    settings->read_text_chunks = 1;
    settings->remember_unknown_chunks = 0;
#endif /*LODEPNG_COMPILE_ANCILLARY_CHUNKS*/
    settings->ignore_crc = 0;
    lodepng_decompress_settings_init(&settings->zlibsettings);
}

#endif /*LODEPNG_COMPILE_DECODER*/

#if defined(LODEPNG_COMPILE_DECODER) || defined(LODEPNG_COMPILE_ENCODER)

void lodepng_state_init(LodePNGState* state) {
#ifdef LODEPNG_COMPILE_DECODER
    lodepng_decoder_settings_init(&state->decoder);
#endif /*LODEPNG_COMPILE_DECODER*/
#ifdef LODEPNG_COMPILE_ENCODER
    lodepng_encoder_settings_init(&state->encoder);
#endif /*LODEPNG_COMPILE_ENCODER*/
    lodepng_color_mode_init(&state->info_raw);
    lodepng_info_init(&state->info_png);
    state->error = 1;
}

void lodepng_state_cleanup(LodePNGState* state) {
    lodepng_color_mode_cleanup(&state->info_raw);
    lodepng_info_cleanup(&state->info_png);
}

void lodepng_state_copy(LodePNGState* dest, const LodePNGState* source) {
    lodepng_state_cleanup(dest);
    *dest = *source;
    lodepng_color_mode_init(&dest->info_raw);
    lodepng_info_init(&dest->info_png);
    dest->error = lodepng_color_mode_copy(&dest->info_raw, &source->info_raw);
    if (dest->error != 0U) {
        return;
    }
    dest->error = lodepng_info_copy(&dest->info_png, &source->info_png);
    if (dest->error != 0U) {
        return;
    }
}

#endif /* defined(LODEPNG_COMPILE_DECODER) || defined(LODEPNG_COMPILE_ENCODER) */

#ifdef LODEPNG_COMPILE_ENCODER

/* ////////////////////////////////////////////////////////////////////////// */
/* / PNG Encoder                                                            / */
/* ////////////////////////////////////////////////////////////////////////// */

/*chunkName must be string of 4 characters*/
static unsigned add_chunk(ucvector* out, const char* chunk_name, const unsigned char* data, size_t length) {
    CERROR_TRY_RETURN(lodepng_chunk_create(&out->data, &out->size, static_cast<unsigned>(length), chunk_name, data));
    out->allocsize = out->size; /*fix the allocsize again*/
    return 0;
}

static void write_signature(ucvector* out) {
    /*8 bytes PNG signature, aka the magic bytes*/
    ucvector_push_back(out, 137);
    ucvector_push_back(out, 80);
    ucvector_push_back(out, 78);
    ucvector_push_back(out, 71);
    ucvector_push_back(out, 13);
    ucvector_push_back(out, 10);
    ucvector_push_back(out, 26);
    ucvector_push_back(out, 10);
}

static unsigned add_chunk_ihdr(ucvector* out, unsigned w, unsigned h, LodePNGColorType colortype, unsigned bitdepth,
                               unsigned interlace_method) {
    unsigned error = 0;
    ucvector header;
    ucvector_init(&header);

    lodepng_add32bit_int(&header, w);                                   /*width*/
    lodepng_add32bit_int(&header, h);                                   /*height*/
    ucvector_push_back(&header, static_cast<unsigned char>(bitdepth));  /*bit depth*/
    ucvector_push_back(&header, static_cast<unsigned char>(colortype)); /*color type*/
    ucvector_push_back(&header, 0);                                     /*compression method*/
    ucvector_push_back(&header, 0);                                     /*filter method*/
    ucvector_push_back(&header, interlace_method);                      /*interlace method*/

    error = add_chunk(out, "IHDR", header.data, header.size);
    ucvector_cleanup(&header);

    return error;
}

static unsigned add_chunk_plte(ucvector* out, const LodePNGColorMode* info) {
    unsigned error = 0;
    size_t i = 0;
    ucvector plte;
    ucvector_init(&plte);
    for (i = 0; i != info->palettesize * 4; ++i) {
        /*add all channels except alpha channel*/
        if (i % 4 != 3) {
            ucvector_push_back(&plte, info->palette[i]);
        }
    }
    error = add_chunk(out, "PLTE", plte.data, plte.size);
    ucvector_cleanup(&plte);

    return error;
}

static unsigned add_chunk_t_rns(ucvector* out, const LodePNGColorMode* info) {
    unsigned error = 0;
    size_t i = 0;
    ucvector t_rns;
    ucvector_init(&t_rns);
    if (info->colortype == LCT_PALETTE) {
        size_t amount = info->palettesize;
        /*the tail of palette values that all have 255 as alpha, does not have to be encoded*/
        for (i = info->palettesize; i != 0; --i) {
            if (info->palette[(4 * (i - 1)) + 3] == 255) {
                --amount;
            } else {
                break;
            }
        }
        /*add only alpha channel*/
        for (i = 0; i != amount; ++i) {
            ucvector_push_back(&t_rns, info->palette[(4 * i) + 3]);
        }
    } else if (info->colortype == LCT_GREY) {
        if (info->key_defined != 0U) {
            ucvector_push_back(&t_rns, static_cast<unsigned char>(info->key_r / 256));
            ucvector_push_back(&t_rns, static_cast<unsigned char>(info->key_r % 256));
        }
    } else if (info->colortype == LCT_RGB) {
        if (info->key_defined != 0U) {
            ucvector_push_back(&t_rns, static_cast<unsigned char>(info->key_r / 256));
            ucvector_push_back(&t_rns, static_cast<unsigned char>(info->key_r % 256));
            ucvector_push_back(&t_rns, static_cast<unsigned char>(info->key_g / 256));
            ucvector_push_back(&t_rns, static_cast<unsigned char>(info->key_g % 256));
            ucvector_push_back(&t_rns, static_cast<unsigned char>(info->key_b / 256));
            ucvector_push_back(&t_rns, static_cast<unsigned char>(info->key_b % 256));
        }
    }

    error = add_chunk(out, "tRNS", t_rns.data, t_rns.size);
    ucvector_cleanup(&t_rns);

    return error;
}

static unsigned add_chunk_idat(ucvector* out, const unsigned char* data, size_t datasize, LodePNGCompressSettings* zlibsettings) {
    ucvector zlibdata;
    unsigned error = 0;

    /*compress with the Zlib compressor*/
    ucvector_init(&zlibdata);
    error = zlib_compress(&zlibdata.data, &zlibdata.size, data, datasize, zlibsettings);
    if (error == 0U) {
        error = add_chunk(out, "IDAT", zlibdata.data, zlibdata.size);
    }
    ucvector_cleanup(&zlibdata);

    return error;
}

static unsigned add_chunk_iend(ucvector* out) {
    unsigned error = 0;
    error = add_chunk(out, "IEND", nullptr, 0);
    return error;
}

#ifdef LODEPNG_COMPILE_ANCILLARY_CHUNKS

static unsigned add_chunk_t_e_xt(ucvector* out, const char* keyword, const char* textstring) {
    unsigned error = 0;
    size_t i = 0;
    ucvector text;
    ucvector_init(&text);
    for (i = 0; keyword[i] != 0; ++i) {
        ucvector_push_back(&text, static_cast<unsigned char>(keyword[i]));
    }
    if (i < 1 || i > 79) {
        return 89; /*error: invalid keyword size*/
    }
    ucvector_push_back(&text, 0); /*0 termination char*/
    for (i = 0; textstring[i] != 0; ++i) {
        ucvector_push_back(&text, static_cast<unsigned char>(textstring[i]));
    }
    error = add_chunk(out, "tEXt", text.data, text.size);
    ucvector_cleanup(&text);

    return error;
}

static unsigned add_chunk_z_t_xt(ucvector* out, const char* keyword, const char* textstring, LodePNGCompressSettings* zlibsettings) {
    unsigned error = 0;
    ucvector data;
    ucvector compressed;
    size_t i = 0;
    size_t const TEXTSIZE = strlen(textstring);

    ucvector_init(&data);
    ucvector_init(&compressed);
    for (i = 0; keyword[i] != 0; ++i) {
        ucvector_push_back(&data, static_cast<unsigned char>(keyword[i]));
    }
    if (i < 1 || i > 79) {
        return 89; /*error: invalid keyword size*/
    }
    ucvector_push_back(&data, 0); /*0 termination char*/
    ucvector_push_back(&data, 0); /*compression method: 0*/

    error = zlib_compress(&compressed.data, &compressed.size, reinterpret_cast<const unsigned char*>(textstring), TEXTSIZE, zlibsettings);
    if (error == 0U) {
        for (i = 0; i != compressed.size; ++i) {
            ucvector_push_back(&data, compressed.data[i]);
        }
        error = add_chunk(out, "zTXt", data.data, data.size);
    }

    ucvector_cleanup(&compressed);
    ucvector_cleanup(&data);
    return error;
}

static unsigned add_chunk_i_t_xt(ucvector* out, unsigned compressed, const char* keyword, const char* langtag, const char* transkey,
                                 const char* textstring, LodePNGCompressSettings* zlibsettings) {
    unsigned error = 0;
    ucvector data;
    size_t i = 0;
    size_t const TEXTSIZE = strlen(textstring);

    ucvector_init(&data);

    for (i = 0; keyword[i] != 0; ++i) {
        ucvector_push_back(&data, static_cast<unsigned char>(keyword[i]));
    }
    if (i < 1 || i > 79) {
        return 89; /*error: invalid keyword size*/
    }
    ucvector_push_back(&data, 0);                          /*null termination char*/
    ucvector_push_back(&data, (compressed != 0U) ? 1 : 0); /*compression flag*/
    ucvector_push_back(&data, 0);                          /*compression method*/
    for (i = 0; langtag[i] != 0; ++i) {
        ucvector_push_back(&data, static_cast<unsigned char>(langtag[i]));
    }
    ucvector_push_back(&data, 0); /*null termination char*/
    for (i = 0; transkey[i] != 0; ++i) {
        ucvector_push_back(&data, static_cast<unsigned char>(transkey[i]));
    }
    ucvector_push_back(&data, 0); /*null termination char*/

    if (compressed != 0U) {
        ucvector compressed_data;
        ucvector_init(&compressed_data);
        error = zlib_compress(&compressed_data.data, &compressed_data.size, reinterpret_cast<const unsigned char*>(textstring), TEXTSIZE,
                              zlibsettings);
        if (error == 0U) {
            for (i = 0; i != compressed_data.size; ++i) {
                ucvector_push_back(&data, compressed_data.data[i]);
            }
        }
        ucvector_cleanup(&compressed_data);
    } else /*not compressed*/
    {
        for (i = 0; textstring[i] != 0; ++i) {
            ucvector_push_back(&data, static_cast<unsigned char>(textstring[i]));
        }
    }

    if (error == 0U) {
        error = add_chunk(out, "iTXt", data.data, data.size);
    }
    ucvector_cleanup(&data);
    return error;
}

static unsigned add_chunk_b_kgd(ucvector* out, const LodePNGInfo* info) {
    unsigned error = 0;
    ucvector b_kgd;
    ucvector_init(&b_kgd);
    if (info->color.colortype == LCT_GREY || info->color.colortype == LCT_GREY_ALPHA) {
        ucvector_push_back(&b_kgd, static_cast<unsigned char>(info->background_r / 256));
        ucvector_push_back(&b_kgd, static_cast<unsigned char>(info->background_r % 256));
    } else if (info->color.colortype == LCT_RGB || info->color.colortype == LCT_RGBA) {
        ucvector_push_back(&b_kgd, static_cast<unsigned char>(info->background_r / 256));
        ucvector_push_back(&b_kgd, static_cast<unsigned char>(info->background_r % 256));
        ucvector_push_back(&b_kgd, static_cast<unsigned char>(info->background_g / 256));
        ucvector_push_back(&b_kgd, static_cast<unsigned char>(info->background_g % 256));
        ucvector_push_back(&b_kgd, static_cast<unsigned char>(info->background_b / 256));
        ucvector_push_back(&b_kgd, static_cast<unsigned char>(info->background_b % 256));
    } else if (info->color.colortype == LCT_PALETTE) {
        ucvector_push_back(&b_kgd, static_cast<unsigned char>(info->background_r % 256)); /*palette index*/
    }

    error = add_chunk(out, "bKGD", b_kgd.data, b_kgd.size);
    ucvector_cleanup(&b_kgd);

    return error;
}

static unsigned add_chunk_t_ime(ucvector* out, const LodePNGTime* time) {
    unsigned error = 0;
    auto* data = static_cast<unsigned char*>(lodepng_malloc(7));
    if (data == nullptr) {
        return 83; /*alloc fail*/
    }
    data[0] = static_cast<unsigned char>(time->year / 256);
    data[1] = static_cast<unsigned char>(time->year % 256);
    data[2] = static_cast<unsigned char>(time->month);
    data[3] = static_cast<unsigned char>(time->day);
    data[4] = static_cast<unsigned char>(time->hour);
    data[5] = static_cast<unsigned char>(time->minute);
    data[6] = static_cast<unsigned char>(time->second);
    error = add_chunk(out, "tIME", data, 7);
    lodepng_free(data);
    return error;
}

static unsigned add_chunk_p_h_ys(ucvector* out, const LodePNGInfo* info) {
    unsigned error = 0;
    ucvector data;
    ucvector_init(&data);

    lodepng_add32bit_int(&data, info->phys_x);
    lodepng_add32bit_int(&data, info->phys_y);
    ucvector_push_back(&data, info->phys_unit);

    error = add_chunk(out, "pHYs", data.data, data.size);
    ucvector_cleanup(&data);

    return error;
}

#endif /*LODEPNG_COMPILE_ANCILLARY_CHUNKS*/

static void filter_scanline(unsigned char* out, const unsigned char* scanline, const unsigned char* prevline, size_t length,
                            size_t bytewidth, unsigned char filter_type) {
    size_t i = 0;
    switch (filter_type) {
        case 0: /*None*/
            for (i = 0; i != length; ++i) {
                out[i] = scanline[i];
            }
            break;
        case 1: /*Sub*/
            for (i = 0; i != bytewidth; ++i) {
                out[i] = scanline[i];
            }
            for (i = bytewidth; i < length; ++i) {
                out[i] = scanline[i] - scanline[i - bytewidth];
            }
            break;
        case 2: /*Up*/
            if (prevline != nullptr) {
                for (i = 0; i != length; ++i) {
                    out[i] = scanline[i] - prevline[i];
                }
            } else {
                for (i = 0; i != length; ++i) {
                    out[i] = scanline[i];
                }
            }
            break;
        case 3: /*Average*/
            if (prevline != nullptr) {
                for (i = 0; i != bytewidth; ++i) {
                    out[i] = scanline[i] - (prevline[i] / 2);
                }
                for (i = bytewidth; i < length; ++i) {
                    out[i] = scanline[i] - ((scanline[i - bytewidth] + prevline[i]) / 2);
                }
            } else {
                for (i = 0; i != bytewidth; ++i) {
                    out[i] = scanline[i];
                }
                for (i = bytewidth; i < length; ++i) {
                    out[i] = scanline[i] - (scanline[i - bytewidth] / 2);
                }
            }
            break;
        case 4: /*Paeth*/
            if (prevline != nullptr) {
                /*paethPredictor(0, prevline[i], 0) is always prevline[i]*/
                for (i = 0; i != bytewidth; ++i) {
                    out[i] = (scanline[i] - prevline[i]);
                }
                for (i = bytewidth; i < length; ++i) {
                    out[i] = (scanline[i] - paeth_predictor(scanline[i - bytewidth], prevline[i], prevline[i - bytewidth]));
                }
            } else {
                for (i = 0; i != bytewidth; ++i) {
                    out[i] = scanline[i];
                }
                /*paethPredictor(scanline[i - bytewidth], 0, 0) is always scanline[i - bytewidth]*/
                for (i = bytewidth; i < length; ++i) {
                    out[i] = (scanline[i] - scanline[i - bytewidth]);
                }
            }
            break;
        default:
            return; /*unexisting filter type given*/
    }
}

/* log2 approximation. A slight bit faster than std::log. */
static float flog2(float f) {
    float result = 0;
    while (f > 32) {
        result += 4;
        f /= 16;
    }
    while (f > 2) {
        ++result;
        f /= 2;
    }
    return result + (std::numbers::log2e_v<float> * ((f * f * f / 3) - (3 * f * f / 2) + (3 * f) - 1.83333F));
}

static unsigned filter(unsigned char* out, const unsigned char* in, unsigned w, unsigned h, const LodePNGColorMode* info,
                       const LodePNGEncoderSettings* settings) {
    /*
    For PNG filter method 0
    out must be a buffer with as size: h + (w * h * bpp + 7) / 8, because there are
    the scanlines with 1 extra byte per scanline
    */

    unsigned const BPP = lodepng_get_bpp(info);
    /*the width of a scanline in bytes, not including the filter type*/
    size_t const LINEBYTES = ((w * BPP) + 7) / 8;
    /*bytewidth is used for filtering, is 1 when bpp < 8, number of bytes per pixel otherwise*/
    size_t const BYTEWIDTH = (BPP + 7) / 8;
    const unsigned char* prevline = nullptr;
    unsigned x = 0;
    unsigned y = 0;
    unsigned const ERROR = 0;
    LodePNGFilterStrategy strategy = settings->filter_strategy;

    /*
    There is a heuristic called the minimum sum of absolute differences heuristic, suggested by the PNG standard:
     *  If the image type is Palette, or the bit depth is smaller than 8, then do not filter the image (i.e.
        use fixed filtering, with the filter None).
     * (The other case) If the image type is Grayscale or RGB (with or without Alpha), and the bit depth is
       not smaller than 8, then use adaptive filtering heuristic as follows: independently for each row, apply
       all five filters and select the filter that produces the smallest sum of absolute values per row.
    This heuristic is used if filter strategy is LFS_MINSUM and filter_palette_zero is true.

    If filter_palette_zero is true and filter_strategy is not LFS_MINSUM, the above heuristic is followed,
    but for "the other case", whatever strategy filter_strategy is set to instead of the minimum sum
    heuristic is used.
    */
    if ((settings->filter_palette_zero != 0U) && (info->colortype == LCT_PALETTE || info->bitdepth < 8)) {
        strategy = LFS_ZERO;
    }

    if (BPP == 0) {
        return 31; /*error: invalid color type*/
    }

    if (strategy == LFS_ZERO) {
        for (y = 0; y != h; ++y) {
            size_t const OUTINDEX = (1 + LINEBYTES) * y; /*the extra filterbyte added to each row*/
            size_t const ININDEX = LINEBYTES * y;
            out[OUTINDEX] = 0; /*filter type byte*/
            filter_scanline(&out[OUTINDEX + 1], &in[ININDEX], prevline, LINEBYTES, BYTEWIDTH, 0);
            prevline = &in[ININDEX];
        }
    } else if (strategy == LFS_MINSUM) {
        /*adaptive filtering*/
        size_t sum[5];
        ucvector attempt[5]; /*five filtering attempts, one for each filter type*/
        size_t smallest = 0;
        unsigned char type = 0;
        unsigned char best_type = 0;

        for (type = 0; type != 5; ++type) {
            ucvector_init(&attempt[type]);
            if (ucvector_resize(&attempt[type], LINEBYTES) == 0U) {
                return 83; /*alloc fail*/
            }
        }

        if (ERROR == 0U) {
            for (y = 0; y != h; ++y) {
                /*try the 5 filter types*/
                for (type = 0; type != 5; ++type) {
                    filter_scanline(attempt[type].data, &in[y * LINEBYTES], prevline, LINEBYTES, BYTEWIDTH, type);

                    /*calculate the sum of the result*/
                    sum[type] = 0;
                    if (type == 0) {
                        for (x = 0; x != LINEBYTES; ++x) {
                            sum[type] += attempt[type].data[x];
                        }
                    } else {
                        for (x = 0; x != LINEBYTES; ++x) {
                            /*For differences, each byte should be treated as signed, values above 127 are negative
                            (converted to signed char). Filtertype 0 isn't a difference though, so use unsigned there.
                            This means filtertype 0 is almost never chosen, but that is justified.*/
                            unsigned char const S = attempt[type].data[x];
                            sum[type] += S < 128 ? S : (255U - S);
                        }
                    }

                    /*check if this is smallest sum (or if type == 0 it's the first case so always store the values)*/
                    if (type == 0 || sum[type] < smallest) {
                        best_type = type;
                        smallest = sum[type];
                    }
                }

                prevline = &in[y * LINEBYTES];

                /*now fill the out values*/
                out[y * (LINEBYTES + 1)] = best_type; /*the first byte of a scanline will be the filter type*/
                for (x = 0; x != LINEBYTES; ++x) {
                    out[(y * (LINEBYTES + 1)) + 1 + x] = attempt[best_type].data[x];
                }
            }
        }

        for (type = 0; type != 5; ++type) {
            ucvector_cleanup(&attempt[type]);
        }
    } else if (strategy == LFS_ENTROPY) {
        float sum[5];
        ucvector attempt[5]; /*five filtering attempts, one for each filter type*/
        float smallest = 0;
        unsigned type = 0;
        unsigned best_type = 0;
        unsigned count[256];

        for (type = 0; type != 5; ++type) {
            ucvector_init(&attempt[type]);
            if (ucvector_resize(&attempt[type], LINEBYTES) == 0U) {
                return 83; /*alloc fail*/
            }
        }

        for (y = 0; y != h; ++y) {
            /*try the 5 filter types*/
            for (type = 0; type != 5; ++type) {
                filter_scanline(attempt[type].data, &in[y * LINEBYTES], prevline, LINEBYTES, BYTEWIDTH, type);
                for (x = 0; x != 256; ++x) {
                    count[x] = 0;
                }
                for (x = 0; x != LINEBYTES; ++x) {
                    ++count[attempt[type].data[x]];
                }
                ++count[type]; /*the filter type itself is part of the scanline*/
                sum[type] = 0;
                for (x = 0; x != 256; ++x) {
                    float const P = count[x] / static_cast<float>(LINEBYTES + 1);
                    sum[type] += count[x] == 0 ? 0 : flog2(1 / P) * P;
                }
                /*check if this is smallest sum (or if type == 0 it's the first case so always store the values)*/
                if (type == 0 || sum[type] < smallest) {
                    best_type = type;
                    smallest = sum[type];
                }
            }

            prevline = &in[y * LINEBYTES];

            /*now fill the out values*/
            out[y * (LINEBYTES + 1)] = best_type; /*the first byte of a scanline will be the filter type*/
            for (x = 0; x != LINEBYTES; ++x) {
                out[(y * (LINEBYTES + 1)) + 1 + x] = attempt[best_type].data[x];
            }
        }

        for (type = 0; type != 5; ++type) {
            ucvector_cleanup(&attempt[type]);
        }
    } else if (strategy == LFS_PREDEFINED) {
        for (y = 0; y != h; ++y) {
            size_t const OUTINDEX = (1 + LINEBYTES) * y; /*the extra filterbyte added to each row*/
            size_t const ININDEX = LINEBYTES * y;
            unsigned char const TYPE = settings->predefined_filters[y];
            out[OUTINDEX] = TYPE; /*filter type byte*/
            filter_scanline(&out[OUTINDEX + 1], &in[ININDEX], prevline, LINEBYTES, BYTEWIDTH, TYPE);
            prevline = &in[ININDEX];
        }
    } else if (strategy == LFS_BRUTE_FORCE) {
        /*brute force filter chooser.
        deflate the scanline after every filter attempt to see which one deflates best.
        This is very slow and gives only slightly smaller, sometimes even larger, result*/
        size_t size[5];
        ucvector attempt[5]; /*five filtering attempts, one for each filter type*/
        size_t smallest = 0;
        unsigned type = 0;
        unsigned best_type = 0;
        unsigned char* dummy = nullptr;
        LodePNGCompressSettings zlibsettings = settings->zlibsettings;
        /*use fixed tree on the attempts so that the tree is not adapted to the filtertype on purpose,
        to simulate the true case where the tree is the same for the whole image. Sometimes it gives
        better result with dynamic tree anyway. Using the fixed tree sometimes gives worse, but in rare
        cases better compression. It does make this a bit less slow, so it's worth doing this.*/
        zlibsettings.btype = 1;
        /*a custom encoder likely doesn't read the btype setting and is optimized for complete PNG
        images only, so disable it*/
        zlibsettings.custom_zlib = nullptr;
        zlibsettings.custom_deflate = nullptr;
        for (type = 0; type != 5; ++type) {
            ucvector_init(&attempt[type]);
            ucvector_resize(&attempt[type], LINEBYTES); /*todo: give error if resize failed*/
        }
        for (y = 0; y != h; ++y) /*try the 5 filter types*/
        {
            for (type = 0; type != 5; ++type) {
                unsigned const TESTSIZE = attempt[type].size;
                /*if(testsize > 8) testsize /= 8;*/ /*it already works good enough by testing a part of the row*/

                filter_scanline(attempt[type].data, &in[y * LINEBYTES], prevline, LINEBYTES, BYTEWIDTH, type);
                size[type] = 0;
                dummy = nullptr;
                zlib_compress(&dummy, &size[type], attempt[type].data, TESTSIZE, &zlibsettings);
                lodepng_free(dummy);
                /*check if this is smallest size (or if type == 0 it's the first case so always store the values)*/
                if (type == 0 || size[type] < smallest) {
                    best_type = type;
                    smallest = size[type];
                }
            }
            prevline = &in[y * LINEBYTES];
            out[y * (LINEBYTES + 1)] = best_type; /*the first byte of a scanline will be the filter type*/
            for (x = 0; x != LINEBYTES; ++x) {
                out[(y * (LINEBYTES + 1)) + 1 + x] = attempt[best_type].data[x];
            }
        }
        for (type = 0; type != 5; ++type) {
            ucvector_cleanup(&attempt[type]);
        }
    } else {
        {
            return 88; /* unknown filter strategy */
        }
    }

    return ERROR;
}

static void add_padding_bits(unsigned char* out, const unsigned char* in, size_t olinebits, size_t ilinebits, unsigned h) {
    /*The opposite of the removePaddingBits function
    olinebits must be >= ilinebits*/
    unsigned y = 0;
    size_t const DIFF = olinebits - ilinebits;
    size_t obp = 0;
    size_t ibp = 0; /*bit pointers*/
    for (y = 0; y != h; ++y) {
        size_t x = 0;
        for (x = 0; x < ilinebits; ++x) {
            unsigned char const BIT = read_bit_from_reversed_stream(&ibp, in);
            set_bit_of_reversed_stream(&obp, out, BIT);
        }
        /*obp += diff; --> no, fill in some value in the padding bits too, to avoid
        "Use of uninitialised value of size ###" warning from valgrind*/
        for (x = 0; x != DIFF; ++x) {
            set_bit_of_reversed_stream(&obp, out, 0);
        }
    }
}

/*
in: non-interlaced image with size w*h
out: the same pixels, but re-ordered according to PNG's Adam7 interlacing, with
 no padding bits between scanlines, but between reduced images so that each
 reduced image starts at a byte.
bpp: bits per pixel
there are no padding bits, not between scanlines, not between reduced images
in has the following size in bits: w * h * bpp.
out is possibly bigger due to padding bits between reduced images
NOTE: comments about padding bits are only relevant if bpp < 8
*/
static void adam7_interlace(unsigned char* out, const unsigned char* in, unsigned w, unsigned h, unsigned bpp) {
    std::array<unsigned, 7> passw{};
    std::array<unsigned, 7> passh{};
    std::array<size_t, 8> filter_passstart{};
    std::array<size_t, 8> padded_passstart{};
    std::array<size_t, 8> passstart{};
    unsigned i = 0;

    adam7_getpassvalues(passw, passh, filter_passstart, padded_passstart, passstart, w, h, bpp);

    if (bpp >= 8) {
        for (i = 0; i != 7; ++i) {
            unsigned x = 0;
            unsigned y = 0;
            unsigned b = 0;
            size_t const BYTEWIDTH = bpp / 8;
            for (y = 0; y < passh[i]; ++y) {
                for (x = 0; x < passw[i]; ++x) {
                    size_t const PIXELINSTART = (((ADAM7_IY[i] + (y * ADAM7_DY[i])) * w) + ADAM7_IX[i] + (x * ADAM7_DX[i])) * BYTEWIDTH;
                    size_t const PIXELOUTSTART = passstart[i] + (((y * passw[i]) + x) * BYTEWIDTH);
                    for (b = 0; b < BYTEWIDTH; ++b) {
                        out[PIXELOUTSTART + b] = in[PIXELINSTART + b];
                    }
                }
            }
        }
    } else /*bpp < 8: Adam7 with pixels < 8 bit is a bit trickier: with bit pointers*/
    {
        for (i = 0; i != 7; ++i) {
            unsigned x = 0;
            unsigned y = 0;
            unsigned b = 0;
            unsigned const ILINEBITS = bpp * passw[i];
            unsigned const OLINEBITS = bpp * w;
            size_t obp = 0;
            size_t ibp = 0; /*bit pointers (for out and in buffer)*/
            for (y = 0; y < passh[i]; ++y) {
                for (x = 0; x < passw[i]; ++x) {
                    ibp = ((ADAM7_IY[i] + (y * ADAM7_DY[i])) * OLINEBITS) + ((ADAM7_IX[i] + (x * ADAM7_DX[i])) * bpp);
                    obp = (8 * passstart[i]) + ((y * ILINEBITS) + (x * bpp));
                    for (b = 0; b < bpp; ++b) {
                        unsigned char const BIT = read_bit_from_reversed_stream(&ibp, in);
                        set_bit_of_reversed_stream(&obp, out, BIT);
                    }
                }
            }
        }
    }
}

/*out must be buffer big enough to contain uncompressed IDAT chunk data, and in must contain the full image.
return value is error**/
static unsigned pre_process_scanlines(unsigned char** out, size_t* outsize, const unsigned char* in, unsigned w, unsigned h,
                                      const LodePNGInfo* info_png, const LodePNGEncoderSettings* settings) {
    /*
    This function converts the pure 2D image with the PNG's colortype, into filtered-padded-interlaced data. Steps:
    *) if no Adam7: 1) add padding bits (= posible extra bits per scanline if bpp < 8) 2) filter
    *) if adam7: 1) Adam7_interlace 2) 7x add padding bits 3) 7x filter
    */
    unsigned const BPP = lodepng_get_bpp(&info_png->color);
    unsigned error = 0;

    if (info_png->interlace_method == 0) {
        size_t const LINEBYTES = scanline_byte_width(w, BPP);
        *outsize = static_cast<size_t>(h) + (static_cast<size_t>(h) * LINEBYTES); /*image size plus one filter byte per scanline*/
        *out = static_cast<unsigned char*>(lodepng_malloc(*outsize));
        if (((*out) == nullptr) && ((*outsize) != 0U)) {
            error = 83; /*alloc fail*/
        }

        if (error == 0U) {
            /*non multiple of 8 bits per scanline, padding bits needed per scanline*/
            if (BPP < 8 && static_cast<size_t>(w) * BPP != LINEBYTES * 8) {
                auto* padded = static_cast<unsigned char*>(lodepng_malloc(static_cast<size_t>(h) * LINEBYTES));
                if (padded == nullptr) {
                    error = 83; /*alloc fail*/
                }
                if (error == 0U) {
                    add_padding_bits(padded, in, LINEBYTES * 8, static_cast<size_t>(w) * BPP, h);
                    error = filter(*out, padded, w, h, &info_png->color, settings);
                }
                lodepng_free(padded);
            } else {
                /*we can immediatly filter into the out buffer, no other steps needed*/
                error = filter(*out, in, w, h, &info_png->color, settings);
            }
        }
    } else /*interlace_method is 1 (Adam7)*/
    {
        std::array<unsigned, 7> passw{};
        std::array<unsigned, 7> passh{};
        std::array<size_t, 8> filter_passstart{};
        std::array<size_t, 8> padded_passstart{};
        std::array<size_t, 8> passstart{};
        unsigned char* adam7 = nullptr;

        adam7_getpassvalues(passw, passh, filter_passstart, padded_passstart, passstart, w, h, BPP);

        *outsize = filter_passstart[7]; /*image size plus an extra byte per scanline + possible padding bits*/
        *out = static_cast<unsigned char*>(lodepng_malloc(*outsize));
        if ((*out) == nullptr) {
            error = 83; /*alloc fail*/
        }

        adam7 = static_cast<unsigned char*>(lodepng_malloc(passstart[7]));
        if ((adam7 == nullptr) && (passstart[7] != 0U)) {
            error = 83; /*alloc fail*/
        }

        if (error == 0U) {
            unsigned i = 0;

            adam7_interlace(adam7, in, w, h, BPP);
            for (i = 0; i != 7; ++i) {
                if (BPP < 8) {
                    auto* padded = static_cast<unsigned char*>(lodepng_malloc(padded_passstart[i + 1] - padded_passstart[i]));
                    if (padded == nullptr) ERROR_BREAK(83); /*alloc fail*/
                    size_t const PASS_LINEBYTES = scanline_byte_width(passw[i], BPP);
                    add_padding_bits(padded, &adam7[passstart[i]], PASS_LINEBYTES * 8, static_cast<size_t>(passw[i]) * BPP, passh[i]);
                    error = filter(&(*out)[filter_passstart[i]], padded, passw[i], passh[i], &info_png->color, settings);
                    lodepng_free(padded);
                } else {
                    error =
                        filter(&(*out)[filter_passstart[i]], &adam7[padded_passstart[i]], passw[i], passh[i], &info_png->color, settings);
                }

                if (error != 0U) {
                    break;
                }
            }
        }

        lodepng_free(adam7);
    }

    return error;
}

/*
palette must have 4 * palettesize bytes allocated, and given in format RGBARGBARGBARGBA...
returns 0 if the palette is opaque,
returns 1 if the palette has a single color with alpha 0 ==> color key
returns 2 if the palette is semi-translucent.
*/
static unsigned get_palette_translucency(const unsigned char* palette, size_t palettesize) {
    size_t i = 0;
    unsigned key = 0;
    unsigned r = 0;
    unsigned g = 0;
    unsigned b = 0; /*the value of the color with alpha 0, so long as color keying is possible*/
    for (i = 0; i != palettesize; ++i) {
        if ((key == 0U) && palette[(4 * i) + 3] == 0) {
            r = palette[(4 * i) + 0];
            g = palette[(4 * i) + 1];
            b = palette[(4 * i) + 2];
            key = 1;
            i = static_cast<size_t>(-1); /*restart from beginning, to detect earlier opaque colors with key's value*/
        } else if (palette[(4 * i) + 3] != 255) {
            {
                return 2;
                /*when key, no opaque RGB may have key's RGB*/
            }
        } else if ((key != 0U) && r == palette[(i * 4) + 0] && g == palette[(i * 4) + 1] && b == palette[(i * 4) + 2]) {
            {
                return 2;
            }
        }
    }
    return key;
}

#ifdef LODEPNG_COMPILE_ANCILLARY_CHUNKS
static unsigned add_unknown_chunks(ucvector* out, unsigned char* data, size_t datasize) {
    unsigned char* inchunk = data;
    while (std::cmp_less((inchunk - data), datasize)) {
        CERROR_TRY_RETURN(lodepng_chunk_append(&out->data, &out->size, inchunk));
        out->allocsize = out->size; /*fix the allocsize again*/
        inchunk = lodepng_chunk_next(inchunk);
    }
    return 0;
}
#endif /*LODEPNG_COMPILE_ANCILLARY_CHUNKS*/

unsigned lodepng_encode(unsigned char** out, size_t* outsize, const unsigned char* image, unsigned w, unsigned h, LodePNGState* state) {
    LodePNGInfo info;
    ucvector outv;
    unsigned char* data = nullptr; /*uncompressed version of the IDAT chunk data*/
    size_t datasize = 0;

    /*provide some proper output values if error will happen*/
    *out = nullptr;
    *outsize = 0;
    state->error = 0;

    lodepng_info_init(&info);
    lodepng_info_copy(&info, &state->info_png);

    if ((info.color.colortype == LCT_PALETTE || (state->encoder.force_palette != 0U)) &&
        (info.color.palettesize == 0 || info.color.palettesize > 256)) {
        state->error = 68; /*invalid palette size, it is only allowed to be 1-256*/
        return state->error;
    }

    if (state->encoder.auto_convert != 0U) {
        state->error = lodepng_auto_choose_color(&info.color, image, w, h, &state->info_raw);
    }
    if (state->error != 0U) {
        return state->error;
    }

    if (state->encoder.zlibsettings.btype > 2) {
        CERROR_RETURN_ERROR(state->error, 61); /*error: unexisting btype*/
    }
    if (state->info_png.interlace_method > 1) {
        CERROR_RETURN_ERROR(state->error, 71); /*error: unexisting interlace mode*/
    }

    state->error = check_color_validity(info.color.colortype, info.color.bitdepth);
    if (state->error != 0U) {
        return state->error; /*error: unexisting color type given*/
    }
    state->error = check_color_validity(state->info_raw.colortype, state->info_raw.bitdepth);
    if (state->error != 0U) {
        return state->error; /*error: unexisting color type given*/
    }

    if (lodepng_color_mode_equal(&state->info_raw, &info.color) == 0) {
        unsigned char* converted = nullptr;
        size_t const SIZE = ((w * h * lodepng_get_bpp(&info.color)) + 7) / 8;

        converted = static_cast<unsigned char*>(lodepng_malloc(SIZE));
        if ((converted == nullptr) && (SIZE != 0U)) {
            state->error = 83; /*alloc fail*/
        }
        if (state->error == 0U) {
            state->error = lodepng_convert(converted, image, &info.color, &state->info_raw, w, h);
        }
        if (state->error == 0U) {
            pre_process_scanlines(&data, &datasize, converted, w, h, &info, &state->encoder);
        }
        lodepng_free(converted);
    } else {
        {
            pre_process_scanlines(&data, &datasize, image, w, h, &info, &state->encoder);
        }
    }

    ucvector_init(&outv);
    while (state->error == 0U) /*while only executed once, to break on error*/
    {
#ifdef LODEPNG_COMPILE_ANCILLARY_CHUNKS
        size_t i = 0;
#endif /*LODEPNG_COMPILE_ANCILLARY_CHUNKS*/
        /*write signature and chunks*/
        write_signature(&outv);
        /*IHDR*/
        add_chunk_ihdr(&outv, w, h, info.color.colortype, info.color.bitdepth, info.interlace_method);
#ifdef LODEPNG_COMPILE_ANCILLARY_CHUNKS
        /*unknown chunks between IHDR and PLTE*/
        if (info.unknown_chunks_data[0] != nullptr) {
            state->error = add_unknown_chunks(&outv, info.unknown_chunks_data[0], info.unknown_chunks_size[0]);
            if (state->error != 0U) {
                break;
            }
        }
#endif /*LODEPNG_COMPILE_ANCILLARY_CHUNKS*/
        /*PLTE*/
        if (info.color.colortype == LCT_PALETTE) {
            add_chunk_plte(&outv, &info.color);
        }
        if ((state->encoder.force_palette != 0U) && (info.color.colortype == LCT_RGB || info.color.colortype == LCT_RGBA)) {
            add_chunk_plte(&outv, &info.color);
        }
        /*tRNS*/
        if (info.color.colortype == LCT_PALETTE && get_palette_translucency(info.color.palette, info.color.palettesize) != 0) {
            add_chunk_t_rns(&outv, &info.color);
        }
        if ((info.color.colortype == LCT_GREY || info.color.colortype == LCT_RGB) && (info.color.key_defined != 0U)) {
            add_chunk_t_rns(&outv, &info.color);
        }
#ifdef LODEPNG_COMPILE_ANCILLARY_CHUNKS
        /*bKGD (must come between PLTE and the IDAt chunks*/
        if (info.background_defined != 0U) {
            add_chunk_b_kgd(&outv, &info);
        }
        /*pHYs (must come before the IDAT chunks)*/
        if (info.phys_defined != 0U) {
            add_chunk_p_h_ys(&outv, &info);
        }

        /*unknown chunks between PLTE and IDAT*/
        if (info.unknown_chunks_data[1] != nullptr) {
            state->error = add_unknown_chunks(&outv, info.unknown_chunks_data[1], info.unknown_chunks_size[1]);
            if (state->error != 0U) {
                break;
            }
        }
#endif /*LODEPNG_COMPILE_ANCILLARY_CHUNKS*/
        /*IDAT (multiple IDAT chunks must be consecutive)*/
        state->error = add_chunk_idat(&outv, data, datasize, &state->encoder.zlibsettings);
        if (state->error != 0U) {
            break;
        }
#ifdef LODEPNG_COMPILE_ANCILLARY_CHUNKS
        /*tIME*/
        if (info.time_defined != 0U) {
            add_chunk_t_ime(&outv, &info.time);
        }
        /*tEXt and/or zTXt*/
        for (i = 0; i != info.text_num; ++i) {
            if (strlen(info.text_keys[i]) > 79) {
                state->error = 66; /*text chunk too large*/
                break;
            }
            if (strlen(info.text_keys[i]) < 1) {
                state->error = 67; /*text chunk too small*/
                break;
            }
            if (state->encoder.text_compression != 0U) {
                add_chunk_z_t_xt(&outv, info.text_keys[i], info.text_strings[i], &state->encoder.zlibsettings);
            } else {
                add_chunk_t_e_xt(&outv, info.text_keys[i], info.text_strings[i]);
            }
        }
        /*LodePNG version id in text chunk*/
        if (state->encoder.add_id != 0U) {
            unsigned alread_added_id_text = 0;
            for (i = 0; i != info.text_num; ++i) {
                if (strcmp(info.text_keys[i], "LodePNG") == 0) {
                    alread_added_id_text = 1;
                    break;
                }
            }
            if (alread_added_id_text == 0) {
                add_chunk_t_e_xt(&outv, "LodePNG", lodepng_version_string); /*it's shorter as tEXt than as zTXt chunk*/
            }
        }
        /*iTXt*/
        for (i = 0; i != info.itext_num; ++i) {
            if (strlen(info.itext_keys[i]) > 79) {
                state->error = 66; /*text chunk too large*/
                break;
            }
            if (strlen(info.itext_keys[i]) < 1) {
                state->error = 67; /*text chunk too small*/
                break;
            }
            add_chunk_i_t_xt(&outv, state->encoder.text_compression, info.itext_keys[i], info.itext_langtags[i], info.itext_transkeys[i],
                             info.itext_strings[i], &state->encoder.zlibsettings);
        }

        /*unknown chunks between IDAT and IEND*/
        if (info.unknown_chunks_data[2] != nullptr) {
            state->error = add_unknown_chunks(&outv, info.unknown_chunks_data[2], info.unknown_chunks_size[2]);
            if (state->error != 0U) {
                break;
            }
        }
#endif /*LODEPNG_COMPILE_ANCILLARY_CHUNKS*/
        add_chunk_iend(&outv);

        break; /*this isn't really a while loop; no error happened so break out now!*/
    }

    lodepng_info_cleanup(&info);
    lodepng_free(data);
    /*instead of cleaning the vector up, give it to the output*/
    *out = outv.data;
    *outsize = outv.size;

    return state->error;
}

unsigned lodepng_encode_memory(unsigned char** out, size_t* outsize, const unsigned char* image, unsigned w, unsigned h,
                               LodePNGColorType colortype, unsigned bitdepth) {
    unsigned error = 0;
    LodePNGState state;
    lodepng_state_init(&state);
    state.info_raw.colortype = colortype;
    state.info_raw.bitdepth = bitdepth;
    state.info_png.color.colortype = colortype;
    state.info_png.color.bitdepth = bitdepth;
    lodepng_encode(out, outsize, image, w, h, &state);
    error = state.error;
    lodepng_state_cleanup(&state);
    return error;
}

unsigned lodepng_encode32(unsigned char** out, size_t* outsize, const unsigned char* image, unsigned w, unsigned h) {
    return lodepng_encode_memory(out, outsize, image, w, h, LCT_RGBA, 8);
}

unsigned lodepng_encode24(unsigned char** out, size_t* outsize, const unsigned char* image, unsigned w, unsigned h) {
    return lodepng_encode_memory(out, outsize, image, w, h, LCT_RGB, 8);
}

#ifdef LODEPNG_COMPILE_DISK
unsigned lodepng_encode_file(const char* filename, const unsigned char* image, unsigned w, unsigned h, LodePNGColorType colortype,
                             unsigned bitdepth) {
    unsigned char* buffer = nullptr;
    size_t buffersize = 0;
    unsigned error = lodepng_encode_memory(&buffer, &buffersize, image, w, h, colortype, bitdepth);
    if (error == 0U) {
        error = lodepng_save_file(buffer, buffersize, filename);
    }
    lodepng_free(buffer);
    return error;
}

unsigned lodepng_encode32_file(const char* filename, const unsigned char* image, unsigned w, unsigned h) {
    return lodepng_encode_file(filename, image, w, h, LCT_RGBA, 8);
}

unsigned lodepng_encode24_file(const char* filename, const unsigned char* image, unsigned w, unsigned h) {
    return lodepng_encode_file(filename, image, w, h, LCT_RGB, 8);
}
#endif /*LODEPNG_COMPILE_DISK*/

void lodepng_encoder_settings_init(LodePNGEncoderSettings* settings) {
    lodepng_compress_settings_init(&settings->zlibsettings);
    settings->filter_palette_zero = 1;
    settings->filter_strategy = LFS_MINSUM;
    settings->auto_convert = 1;
    settings->force_palette = 0;
    settings->predefined_filters = nullptr;
#ifdef LODEPNG_COMPILE_ANCILLARY_CHUNKS
    settings->add_id = 0;
    settings->text_compression = 1;
#endif /*LODEPNG_COMPILE_ANCILLARY_CHUNKS*/
}

#endif /*LODEPNG_COMPILE_ENCODER*/
#endif /*LODEPNG_COMPILE_PNG*/

#ifdef LODEPNG_COMPILE_ERROR_TEXT
/*
This returns the description of a numerical error code in English. This is also
the documentation of all the error codes.
*/
const char* lodepng_error_text(unsigned code) {
    switch (code) {
        case 0:
            return "no error, everything went ok";
        case 1:
            return "nothing done yet"; /*the Encoder/Decoder has done nothing yet, error checking makes no sense yet*/
        case 10:
            return "end of input memory reached without huffman end code"; /*while huffman decoding*/
        case 11:
            return "error in code tree made it jump outside of huffman tree"; /*while huffman decoding*/
        case 13:
            return "problem while processing dynamic deflate block";
        case 14:
            return "problem while processing dynamic deflate block";
        case 15:
            return "problem while processing dynamic deflate block";
        case 16:
            return "unexisting code while processing dynamic deflate block";
        case 17:
            return "end of out buffer memory reached while inflating";
        case 18:
            return "invalid distance code while inflating";
        case 19:
            return "end of out buffer memory reached while inflating";
        case 20:
            return "invalid deflate block BTYPE encountered while decoding";
        case 21:
            return "NLEN is not ones complement of LEN in a deflate block";
            /*end of out buffer memory reached while inflating:
            This can happen if the inflated deflate data is longer than the amount of bytes required to fill up
            all the pixels of the image, given the color depth and image dimensions. Something that doesn't
            happen in a normal, well encoded, PNG image.*/
        case 22:
            return "end of out buffer memory reached while inflating";
        case 23:
            return "end of in buffer memory reached while inflating";
        case 24:
            return "invalid FCHECK in zlib header";
        case 25:
            return "invalid compression method in zlib header";
        case 26:
            return "FDICT encountered in zlib header while it's not used for PNG";
        case 27:
            return "PNG file is smaller than a PNG header";
        /*Checks the magic file header, the first 8 bytes of the PNG file*/
        case 28:
            return "incorrect PNG signature, it's no PNG or corrupted";
        case 29:
            return "first chunk is not the header chunk";
        case 30:
            return "chunk length too large, chunk broken off at end of file";
        case 31:
            return "illegal PNG color type or bpp";
        case 32:
            return "illegal PNG compression method";
        case 33:
            return "illegal PNG filter method";
        case 34:
            return "illegal PNG interlace method";
        case 35:
            return "chunk length of a chunk is too large or the chunk too small";
        case 36:
            return "illegal PNG filter type encountered";
        case 37:
            return "illegal bit depth for this color type given";
        case 38:
            return "the palette is too big"; /*more than 256 colors*/
        case 39:
            return "more palette alpha values given in tRNS chunk than there are colors in the palette";
        case 40:
            return "tRNS chunk has wrong size for greyscale image";
        case 41:
            return "tRNS chunk has wrong size for RGB image";
        case 42:
            return "tRNS chunk appeared while it was not allowed for this color type";
        case 43:
            return "bKGD chunk has wrong size for palette image";
        case 44:
            return "bKGD chunk has wrong size for greyscale image";
        case 45:
            return "bKGD chunk has wrong size for RGB image";
        /*the input data is empty, maybe a PNG file doesn't exist or is in the wrong path*/
        case 48:
            return "empty input or file doesn't exist";
        case 49:
            return "jumped past memory while generating dynamic huffman tree";
        case 50:
            return "jumped past memory while generating dynamic huffman tree";
        case 51:
            return "jumped past memory while inflating huffman block";
        case 52:
            return "jumped past memory while inflating";
        case 53:
            return "size of zlib data too small";
        case 54:
            return "repeat symbol in tree while there was no value symbol yet";
        /*jumped past tree while generating huffman tree, this could be when the
        tree will have more leaves than symbols after generating it out of the
        given lenghts. They call this an oversubscribed dynamic bit lengths tree in zlib.*/
        case 55:
            return "jumped past tree while generating huffman tree";
        case 56:
            return "given output image colortype or bitdepth not supported for color conversion";
        case 57:
            return "invalid CRC encountered (checking CRC can be disabled)";
        case 58:
            return "invalid ADLER32 encountered (checking ADLER32 can be disabled)";
        case 59:
            return "requested color conversion not supported";
        case 60:
            return "invalid window size given in the settings of the encoder (must be 0-32768)";
        case 61:
            return "invalid BTYPE given in the settings of the encoder (only 0, 1 and 2 are allowed)";
        /*LodePNG leaves the choice of RGB to greyscale conversion formula to the user.*/
        case 62:
            return "conversion from color to greyscale not supported";
        case 63:
            return "length of a chunk too long, max allowed for PNG is 2147483647 bytes per chunk"; /*(2^31-1)*/
        /*this would result in the inability of a deflated block to ever contain an end code. It must be at least 1.*/
        case 64:
            return "the length of the END symbol 256 in the Huffman tree is 0";
        case 66:
            return "the length of a text chunk keyword given to the encoder is longer than the maximum of 79 bytes";
        case 67:
            return "the length of a text chunk keyword given to the encoder is smaller than the minimum of 1 byte";
        case 68:
            return "tried to encode a PLTE chunk with a palette that has less than 1 or more than 256 colors";
        case 69:
            return "unknown chunk type with 'critical' flag encountered by the decoder";
        case 71:
            return "unexisting interlace mode given to encoder (must be 0 or 1)";
        case 72:
            return "while decoding, unexisting compression method encountering in zTXt or iTXt chunk (it must be 0)";
        case 73:
            return "invalid tIME chunk size";
        case 74:
            return "invalid pHYs chunk size";
        /*length could be wrong, or data chopped off*/
        case 75:
            return "no null termination char found while decoding text chunk";
        case 76:
            return "iTXt chunk too short to contain required bytes";
        case 77:
            return "integer overflow in buffer size";
        case 78:
            return "failed to open file for reading"; /*file doesn't exist or couldn't be opened for reading*/
        case 79:
            return "failed to open file for writing";
        case 80:
            return "tried creating a tree of 0 symbols";
        case 81:
            return "lazy matching at pos 0 is impossible";
        case 82:
            return "color conversion to palette requested while a color isn't in palette";
        case 83:
            return "memory allocation failed";
        case 84:
            return "given image too small to contain all pixels to be encoded";
        case 86:
            return "impossible offset in lz77 encoding (internal bug)";
        case 87:
            return "must provide custom zlib function pointer if LODEPNG_COMPILE_ZLIB is not defined";
        case 88:
            return "invalid filter strategy given for LodePNGEncoderSettings.filter_strategy";
        case 89:
            return "text chunk keyword too short or long: must have size 1-79";
        /*the windowsize in the LodePNGCompressSettings. Requiring POT(==> & instead of %) makes encoding 12% faster.*/
        case 90:
            return "windowsize must be a power of two";
        case 91:
            return "invalid decompressed idat size";
        case 92:
            return "too many pixels, not supported";
        case 93:
            return "zero width or height is invalid";
    }
    return "unknown error code";
}
#endif /*LODEPNG_COMPILE_ERROR_TEXT*/

/* ////////////////////////////////////////////////////////////////////////// */
/* ////////////////////////////////////////////////////////////////////////// */
/* // C++ Wrapper                                                          // */
/* ////////////////////////////////////////////////////////////////////////// */
/* ////////////////////////////////////////////////////////////////////////// */

#ifdef LODEPNG_COMPILE_CPP
namespace lodepng {

#ifdef LODEPNG_COMPILE_DISK
void load_file(std::vector<unsigned char>& buffer, const std::string& filename) {
    std::ifstream file(filename.c_str(), std::ios::in | std::ios::binary | std::ios::ate);

    /*get filesize*/
    std::streamsize size = 0;
    if (file.seekg(0, std::ios::end).good()) {
        size = file.tellg();
    }
    if (file.seekg(0, std::ios::beg).good()) {
        size -= file.tellg();
    }

    /*read contents of the file into the vector*/
    buffer.resize(static_cast<size_t>(size));
    if (size > 0) {
        file.read(reinterpret_cast<char*>(buffer.data()), size);
    }
}

/*write given buffer to the file, overwriting the file, it doesn't append to it.*/
unsigned save_file(const std::vector<unsigned char>& buffer, const std::string& filename) {
    std::ofstream file(filename.c_str(), std::ios::out | std::ios::binary);
    if (!file) {
        return 79;
    }
    file.write(buffer.empty() ? nullptr : reinterpret_cast<const char*>(buffer.data()), static_cast<std::streamsize>(buffer.size()));
    return 0;
}
#endif /* LODEPNG_COMPILE_DISK */

#ifdef LODEPNG_COMPILE_ZLIB
#ifdef LODEPNG_COMPILE_DECODER
unsigned decompress(std::vector<unsigned char>& out, const unsigned char* in, size_t insize, const LodePNGDecompressSettings& settings) {
    unsigned char* buffer = nullptr;
    size_t buffersize = 0;
    unsigned const ERROR = zlib_decompress(&buffer, &buffersize, in, insize, &settings);
    if (buffer != nullptr) {
        out.insert(out.end(), &buffer[0], &buffer[buffersize]);
        lodepng_free(buffer);
    }
    return ERROR;
}

unsigned decompress(std::vector<unsigned char>& out, const std::vector<unsigned char>& in, const LodePNGDecompressSettings& settings) {
    return decompress(out, in.empty() ? nullptr : in.data(), in.size(), settings);
}
#endif /* LODEPNG_COMPILE_DECODER */

#ifdef LODEPNG_COMPILE_ENCODER
unsigned compress(std::vector<unsigned char>& out, const unsigned char* in, size_t insize, const LodePNGCompressSettings& settings) {
    unsigned char* buffer = nullptr;
    size_t buffersize = 0;
    unsigned const ERROR = zlib_compress(&buffer, &buffersize, in, insize, &settings);
    if (buffer != nullptr) {
        out.insert(out.end(), &buffer[0], &buffer[buffersize]);
        lodepng_free(buffer);
    }
    return ERROR;
}

unsigned compress(std::vector<unsigned char>& out, const std::vector<unsigned char>& in, const LodePNGCompressSettings& settings) {
    return compress(out, in.empty() ? nullptr : in.data(), in.size(), settings);
}
#endif /* LODEPNG_COMPILE_ENCODER */
#endif /* LODEPNG_COMPILE_ZLIB */

#ifdef LODEPNG_COMPILE_PNG

State::State() { lodepng_state_init(this); }

State::State(const State& other) : LodePNGState(other) {
    lodepng_state_init(this);
    lodepng_state_copy(this, &other);
}

State::~State() { lodepng_state_cleanup(this); }

State& State::operator=(const State& other) {
    lodepng_state_copy(this, &other);
    return *this;
}

#ifdef LODEPNG_COMPILE_DECODER

unsigned decode(std::vector<unsigned char>& out, unsigned& w, unsigned& h, const unsigned char* in, size_t insize,
                LodePNGColorType colortype, unsigned bitdepth) {
    unsigned char* buffer = nullptr;
    unsigned const ERROR = lodepng_decode_memory(&buffer, &w, &h, in, insize, colortype, bitdepth);
    if ((buffer != nullptr) && (ERROR == 0U)) {
        State state;
        state.info_raw.colortype = colortype;
        state.info_raw.bitdepth = bitdepth;
        size_t const BUFFERSIZE = lodepng_get_raw_size(w, h, &state.info_raw);
        out.insert(out.end(), &buffer[0], &buffer[BUFFERSIZE]);
        lodepng_free(buffer);
    }
    return ERROR;
}

unsigned decode(std::vector<unsigned char>& out, unsigned& w, unsigned& h, const std::vector<unsigned char>& in, LodePNGColorType colortype,
                unsigned bitdepth) {
    return decode(out, w, h, in.empty() ? nullptr : in.data(), static_cast<unsigned>(in.size()), colortype, bitdepth);
}

unsigned decode(std::vector<unsigned char>& out, unsigned& w, unsigned& h, State& state, const unsigned char* in, size_t insize) {
    unsigned char* buffer = nullptr;
    unsigned const ERROR = lodepng_decode(&buffer, &w, &h, &state, in, insize);
    if ((buffer != nullptr) && (ERROR == 0U)) {
        size_t const BUFFERSIZE = lodepng_get_raw_size(w, h, &state.info_raw);
        out.insert(out.end(), &buffer[0], &buffer[BUFFERSIZE]);
    }
    lodepng_free(buffer);
    return ERROR;
}

unsigned decode(std::vector<unsigned char>& out, unsigned& w, unsigned& h, State& state, const std::vector<unsigned char>& in) {
    return decode(out, w, h, state, in.empty() ? nullptr : in.data(), in.size());
}

#ifdef LODEPNG_COMPILE_DISK
unsigned decode(std::vector<unsigned char>& out, unsigned& w, unsigned& h, const std::string& filename, LodePNGColorType colortype,
                unsigned bitdepth) {
    std::vector<unsigned char> buffer;
    load_file(buffer, filename);
    return decode(out, w, h, buffer, colortype, bitdepth);
}
#endif /* LODEPNG_COMPILE_DECODER */
#endif /* LODEPNG_COMPILE_DISK */

#ifdef LODEPNG_COMPILE_ENCODER
unsigned encode(std::vector<unsigned char>& out, const unsigned char* in, unsigned w, unsigned h, LodePNGColorType colortype,
                unsigned bitdepth) {
    unsigned char* buffer = nullptr;
    size_t buffersize = 0;
    unsigned const ERROR = lodepng_encode_memory(&buffer, &buffersize, in, w, h, colortype, bitdepth);
    if (buffer != nullptr) {
        out.insert(out.end(), &buffer[0], &buffer[buffersize]);
        lodepng_free(buffer);
    }
    return ERROR;
}

unsigned encode(std::vector<unsigned char>& out, const std::vector<unsigned char>& in, unsigned w, unsigned h, LodePNGColorType colortype,
                unsigned bitdepth) {
    if (lodepng_get_raw_size_lct(w, h, colortype, bitdepth) > in.size()) {
        return 84;
    }
    return encode(out, in.empty() ? nullptr : in.data(), w, h, colortype, bitdepth);
}

unsigned encode(std::vector<unsigned char>& out, const unsigned char* in, unsigned w, unsigned h, State& state) {
    unsigned char* buffer = nullptr;
    size_t buffersize = 0;
    unsigned const ERROR = lodepng_encode(&buffer, &buffersize, in, w, h, &state);
    if (buffer != nullptr) {
        out.insert(out.end(), &buffer[0], &buffer[buffersize]);
        lodepng_free(buffer);
    }
    return ERROR;
}

unsigned encode(std::vector<unsigned char>& out, const std::vector<unsigned char>& in, unsigned w, unsigned h, State& state) {
    if (lodepng_get_raw_size(w, h, &state.info_raw) > in.size()) {
        return 84;
    }
    return encode(out, in.empty() ? nullptr : in.data(), w, h, state);
}

#ifdef LODEPNG_COMPILE_DISK
unsigned encode(const std::string& filename, const unsigned char* in, unsigned w, unsigned h, LodePNGColorType colortype,
                unsigned bitdepth) {
    std::vector<unsigned char> buffer;
    unsigned error = encode(buffer, in, w, h, colortype, bitdepth);
    if (error == 0U) {
        error = save_file(buffer, filename);
    }
    return error;
}

unsigned encode(const std::string& filename, const std::vector<unsigned char>& in, unsigned w, unsigned h, LodePNGColorType colortype,
                unsigned bitdepth) {
    if (lodepng_get_raw_size_lct(w, h, colortype, bitdepth) > in.size()) {
        return 84;
    }
    return encode(filename, in.empty() ? nullptr : in.data(), w, h, colortype, bitdepth);
}
#endif /* LODEPNG_COMPILE_DISK */
#endif /* LODEPNG_COMPILE_ENCODER */
#endif /* LODEPNG_COMPILE_PNG */
} /* namespace lodepng */
#endif /*LODEPNG_COMPILE_CPP*/

// NOLINTEND(bugprone-branch-clone, bugprone-implicit-widening-of-multiplication-result, bugprone-narrowing-conversions,
// NOLINTNEXTLINE(readability-comment-starts-with-space)
// bugprone-switch-missing-default-case, clang-analyzer-security.ArrayBound, clang-analyzer-unix.Errno,
// NOLINTNEXTLINE(readability-comment-starts-with-space)
// cppcoreguidelines-avoid-c-arrays, cppcoreguidelines-macro-usage, cppcoreguidelines-narrowing-conversions,
// NOLINTNEXTLINE(readability-comment-starts-with-space)
// cppcoreguidelines-no-malloc, cppcoreguidelines-pro-bounds-array-to-pointer-decay,
// NOLINTNEXTLINE(readability-comment-starts-with-space)
// cppcoreguidelines-pro-bounds-avoid-unchecked-container-access, misc-const-correctness, misc-use-anonymous-namespace,
// NOLINTNEXTLINE(readability-comment-starts-with-space)
// misc-use-internal-linkage, modernize-avoid-c-arrays)
