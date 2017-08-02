
#ifndef FINGERPRINT_DB_DB_H
#define FINGERPRINT_DB_DB_H

#define COPY_ROWS_LEN 100000

typedef struct copy_row {
    uint32_t id;
    uint32_t buf_len;
    uint8_t *buf;
} copy_row_t;

int init_db();

int save_hashtable(copy_row_t *copy_rows, uint32_t copy_rows_len);

int load_hashtable(row_t *rows);

#endif //FINGERPRINT_DB_DB_H
