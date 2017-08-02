//
// Created by martynas on 7/24/17.
//

#ifndef FINGERPRINT_DB_HASHTABLE_H
#define FINGERPRINT_DB_HASHTABLE_H

#include <stdint.h>

#define HASHTABLE_SIZE 16777216
#define ROW_SLOTS_MAX 256

#define MAX_TEXT_LEN 8192
#define MAX_ID 134217727
#define FINGERPRINTS_NUM 10
#define NGRAM_LEN 6

#define MAX_LOOKUP_TEXT_LEN 10204

#define MAX_NGRAMS MAX_LOOKUP_TEXT_LEN/NGRAM_LEN+1


typedef struct stats {
    uint32_t used_hashes;
    uint64_t used_slots;
    uint8_t max_slots;
    uint8_t slots_dist[ROW_SLOTS_MAX + 1];
} stats_t;

typedef struct slot {
    uint8_t data[6];
} slot_t;

typedef struct row {
    slot_t *slots;
    uint8_t len;
    uint8_t updated;
} row_t;

typedef struct token {
    uint32_t start;
    uint32_t len;
} token_t;

typedef struct result {
    uint32_t id;
    uint8_t count;
} result_t;

uint32_t get_good_sequences(uint8_t *text, token_t *tokens, uint32_t tokens_len, uint64_t *hashes);

int index_text(uint32_t id, uint8_t *text, uint32_t len);

int identify(uint8_t *text, uint32_t text_len, result_t *results, uint32_t *result_len);

int load();

stats_t get_stats();

#endif //FINGERPRINT_DB_HASHTABLE_H
