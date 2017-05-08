#ifndef _BLOOM_H
#define _BLOOM_H
#include <stddef.h>
#include <stdbool.h>


typedef unsigned int (*hash_function)(const void *data);
typedef struct bloom_filter * bloom_t;
/* Creates a new bloom filter with no hash functions and size * 8 bits. */
bloom_t bloom_create(size_t size);
/* Frees a bloom filter. */
void bloom_free(bloom_t filter);
/* Adds a hashing function to the bloom filter. You should add all of the
 * functions you intend to use before you add any items. */
void bloom_add_hash(bloom_t filter, hash_function func);
/* Adds an item to the bloom filter. */
void bloom_add(bloom_t filter, const void *item);
/* Tests if an item is in the bloom filter.
 *
 * Returns false if the item has definitely not been added before. Returns true
 * if the item was probably added before. */
bool bloom_test(bloom_t filter, const void *item);
#endif

struct lsm_bloom {
    hash_function func;
    struct lsm_bloom *next;
};
struct bloom_filter {
    struct lsm_bloom *func;
    void *bits;
    size_t size;
};

bloom_t bloom_create(size_t size) {
    bloom_t res = calloc(1, sizeof(struct bloom_filter));
    res->size = size;
    res->bits = malloc(size);
    return res;
}

void bloom_add_hash(bloom_t filter, hash_function func) {
    struct lsm_bloom *hkey = calloc(1, sizeof(struct lsm_bloom));
    hkey->func = func;
    struct lsm_bloom *last = filter->func;
    while (last && last->next) {
        last = last->next;
    }
    if (last) {
        last->next = hkey;
    } else {
        filter->func = hkey;
    }
}

void bloom_add(bloom_t filter, const void *item) {
    struct lsm_bloom *hkey = filter->func;
    uint8_t *bits = filter->bits;
    while (hkey) {
        unsigned int hash = hkey->func(item);
        hash %= filter->size * 8;
        bits[hash / 8] |= 1 << hash % 8;
        hkey = hkey->next;
    }
}

void bloom_free(bloom_t filter) {
    if (filter) {
        /*while (filter->func) {
            struct lsm_bloom *hkey;
            //filter->func = hkey->next; //chg
            //free(hkey); //chg
        }*/
        free(filter->bits);
        free(filter);
    }
}

bool bloom_test(bloom_t filter, const void *item) {
    struct lsm_bloom *hkey = filter->func;
    uint8_t *bits = filter->bits;
    while (hkey) {
        unsigned int hash = hkey->func(item);
        hash %= filter->size * 8;
        if (!(bits[hash / 8] & 1 << hash % 8)) {
            return false;
        }
        hkey = hkey->next;
    }
    return true;
}