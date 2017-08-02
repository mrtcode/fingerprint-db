/*
 ***** BEGIN LICENSE BLOCK *****

 Copyright © 2017 Zotero
 https://www.zotero.org

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.

 ***** END LICENSE BLOCK *****
 */

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unicode/ustdio.h>
#include <unicode/ubrk.h>

#define XXH_STATIC_LINKING_ONLY

#include "xxhash.h"

#include "hashtable.h"
#include "db.h"

row_t rows[HASHTABLE_SIZE] = {0};
struct timeval t_updated = {0};

uint32_t tokenize(uint8_t *text, uint32_t text_len, token_t *tokens, uint32_t *len) {
    uint32_t tokens_len = 0;
    UText *ut = NULL;
    UBreakIterator *bi = NULL;
    UErrorCode status = U_ZERO_ERROR;

    ut = utext_openUTF8(ut, text, text_len, &status);
    bi = ubrk_open(UBRK_WORD, 0, NULL, 0, &status);

    ubrk_setUText(bi, ut, &status);
    uint32_t start = 0, pos;
    while ((pos = ubrk_next(bi)) != UBRK_DONE) {
        if (ubrk_getRuleStatus(bi) != UBRK_WORD_NONE) {
            tokens[tokens_len].start = start;
            tokens[tokens_len].len = pos - start;
            tokens_len++;
        }
        start = pos;
    }
    utext_close(ut);
    ubrk_close(bi);

    *len = tokens_len;

    return tokens_len;
}

uint64_t get_ngram_hash(uint8_t *text, token_t *tokens, uint32_t start, uint32_t len) {
    XXH64_state_t state64;
    XXH64_reset(&state64, 0);

    for (int i = start; i < start + len; i++) {
        token_t token = tokens[i];
        XXH64_update(&state64, text + token.start, token.len);
    }
    return (XXH64_digest(&state64)) & 0x1FFFFFFFFFFF;
}

uint32_t get_hash(uint64_t hash) {
    uint32_t hash24 = (uint32_t) (hash >> 21);
    row_t *row = &rows[hash24];
    if (row->len) {
        for (int k = 0; k < row->len; k++) {
            if (*((uint16_t *) row->slots[k].data) == (uint16_t) (hash >> 5)
                && ((uint32_t) hash & 0x1F) == ((*(uint32_t *) (row->slots[k].data + 2)) >> 27)) {
                return (*(uint32_t *) (row->slots[k].data + 2)) & ~(0x1F << 27);
            }
        }
    }
    return 0;
}

uint32_t add_hash(uint64_t hash, uint32_t id) {
    uint32_t hash24 = (uint32_t) (hash >> 21);
    row_t *row = &rows[hash24];

    if (row->len) {
        if (row->len >= 256) {
            printf("Slot error");
            return 0;
        }
        if (!(row->slots = realloc(row->slots, sizeof(slot_t) * (row->len + 1)))) {
            printf("err");
            return 0;
        };
        row->updated = 1;
    } else {
        if (!(row->slots = malloc(sizeof(slot_t)))) {
            printf("err");
            return 0;
        }
        row->updated = 1;
    }

    slot_t *slot = row->slots + row->len;
    *((uint16_t *) slot->data) = (uint16_t) (hash >> 5);
    *((uint32_t *) (slot->data + 2)) = (uint32_t) ((hash & 0x1F) << 27) | id;

    row->len++;
    return 1;
}

uint32_t get_good_sequences(uint8_t *text, token_t *tokens, uint32_t tokens_len, uint64_t *hashes) {

    static uint32_t ngrams[MAX_TEXT_LEN / NGRAM_LEN + 1];
    uint32_t ngrams_len = 0;

    if (tokens_len < NGRAM_LEN) return 0;
    for (int i = 0; i < tokens_len - NGRAM_LEN; i++) {
        uint32_t start = i;
        uint32_t ng_len = 0;
        for (int j = start; j < start + NGRAM_LEN; j++) {
            token_t *tk = &tokens[j];
            ng_len += tk->len;
        }

        if (ng_len < 10 || ng_len > 120) {
            continue;
        }

        uint64_t hash = get_ngram_hash(text, tokens, start, NGRAM_LEN);
        uint32_t id = get_hash(hash);

        if (id) {
            //print_ngram(text, tokens, start, max_tokens);
            continue;
        }

        ngrams[ngrams_len++] = start;
    }

    int ntt = ngrams_len / FINGERPRINTS_NUM;

    if (!ntt) ntt = 1;

    uint32_t hashes_n = 0;

    for (int i = 0; hashes_n < FINGERPRINTS_NUM && i < ngrams_len; i += ntt) {
        uint32_t start = ngrams[i];
        uint64_t hash = get_ngram_hash(text, tokens, start, NGRAM_LEN);
        if (get_hash(hash)) {
            continue;
        }
        hashes[hashes_n++] = hash;
    }

    return hashes_n;
}

int indexed = 0;

int index_text(uint32_t id, uint8_t *text, uint32_t len) {
    static token_t tokens[MAX_TEXT_LEN];

    if (!id) {
        return 0;
    }

    if (id > MAX_ID) {
        return 0;
    }

    if (len > MAX_TEXT_LEN) len = MAX_TEXT_LEN;

    uint32_t tokens_len;
    tokenize(text, len, tokens, &tokens_len);

    uint64_t hashes[FINGERPRINTS_NUM];
    uint32_t hashes_n = get_good_sequences(text, tokens, tokens_len, hashes);

    if (hashes_n < 2) {
        return 0;
    }

    for (int i = 0; i < hashes_n; i++) {
        uint64_t hash = hashes[i];
        add_hash(hash, id);
    }

    gettimeofday(&t_updated, NULL);
    printf("indexed: %d\n", indexed++);
}

stats_t get_stats() {
    stats_t stats = {0};
    for (int i = 0; i < HASHTABLE_SIZE; i++) {
        if (rows[i].len) stats.used_hashes++;
        stats.used_slots += rows[i].len;
        if (stats.max_slots < rows[i].len) stats.max_slots = rows[i].len;
    }

    for (int i = 0; i < HASHTABLE_SIZE; i++) {
        stats.slots_dist[rows[i].len]++;
    }

    return stats;
}

int compare_results(const void *a, const void *b) {
    return ((result_t *) b)->count - ((result_t *) a)->count;
}

int identify(uint8_t *text, uint32_t text_len, result_t *results, uint32_t *results_len) {
    token_t tokens[MAX_LOOKUP_TEXT_LEN];
    uint32_t tokens_len;

    uint64_t detected_hashes[MAX_NGRAMS];
    uint32_t detected_hashes_len = 0;

    *results_len = 0;

    int found = 0;

    if (text_len > MAX_LOOKUP_TEXT_LEN) text_len = MAX_LOOKUP_TEXT_LEN;
    tokenize(text, text_len, tokens, &tokens_len);

    for (int i = 0; i < tokens_len; i++) {
        uint64_t hash = get_ngram_hash(text, tokens, i, NGRAM_LEN);

        found = 0;
        for (int j = 0; j < detected_hashes_len; j++) {
            if (detected_hashes[j] == hash) {
                found = 1;
                break;
            }
        }
        if (found) continue;

        uint32_t id = get_hash(hash);
        if (id) {
            detected_hashes[detected_hashes_len++] = hash;
            found = 0;
            for (int j = 0; j < *results_len; j++) {
                if (results[j].id == id) {
                    results[j].count++;
                    found = 1;
                    break;
                }
            }

            if (!found) {
                results[*results_len].id = id;
                results[*results_len].count = 1;
                (*results_len)++;
            }
        }
    }

    qsort(results, *results_len, sizeof(result_t), compare_results);
    return 0;
}

int load() {
    load_hashtable(rows);
}
