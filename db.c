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

#include <sqlite3.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include "hashtable.h"
#include "db.h"

#define SQLITE_PATH "db.sqlite"

sqlite3 *sqlite;

int init_db() {
    int rc;
    char *err_msg = 0;
    char path[] = SQLITE_PATH;

    if ((rc = sqlite3_open(path, &sqlite)) != SQLITE_OK) {
        fprintf(stderr, "Cannot open database %s (%d): %s\n", path, rc, sqlite3_errmsg(sqlite));
        sqlite3_close(sqlite);
        return 1;
    }

    char *sql = "CREATE TABLE IF NOT EXISTS hashtable (id INTEGER PRIMARY KEY, data BLOB);";
    if ((rc = sqlite3_exec(sqlite, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        fprintf(stderr, "SQL error (%d): %s\n", rc, err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(sqlite);
        return 1;
    }

    return 0;
}

int save_hashtable(copy_row_t *copy_rows, uint32_t copy_rows_len) {
    char *err_msg = 0;

    int rc;
    const char sql[] = "INSERT OR REPLACE INTO hashtable (id, data) VALUES (?,?);";
    sqlite3_stmt *stmt = NULL;
    if ((rc = sqlite3_prepare(sqlite, sql, -1, &stmt, 0)) != SQLITE_OK) {
        fprintf(stderr, "Can't prepare statement %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite));
        sqlite3_close(sqlite);
        return 1;
    }

    if (sqlite3_exec(sqlite, "BEGIN TRANSACTION", NULL, NULL, &err_msg) != SQLITE_OK) {
        printf("Transaction error.\n");
        return 1;
    }

    for (int i = 0; i < copy_rows_len; i++) {
        copy_row_t *copy_row = &copy_rows[i];

        if (sqlite3_bind_int(stmt, 1, copy_row->id) != SQLITE_OK) {
            printf("Could not bind text.\n");
            return 1;
        }

        if (sqlite3_bind_blob(stmt, 2, copy_row->buf, copy_row->buf_len, SQLITE_STATIC) != SQLITE_OK) {
            printf("Could not bind text.\n");
            return 1;
        }

        sqlite3_step(stmt);
        sqlite3_clear_bindings(stmt);
        sqlite3_reset(stmt);
    }

    if (sqlite3_exec(sqlite, "END TRANSACTION", NULL, NULL, &err_msg) != SQLITE_OK) {
        printf("Transaction error2.\n");
        return 1;
    }

    sqlite3_finalize(stmt);
}

int load_hashtable(row_t *rows) {
    int rc;
    const char sql[] = "SELECT id, data FROM hashtable";
    sqlite3_stmt *stmt = NULL;

    if ((rc = sqlite3_prepare_v2(sqlite, sql, -1, &stmt, NULL)) != SQLITE_OK) {
        fprintf(stderr, "Can't prepare select statement %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite));
        sqlite3_close(sqlite);
        return 1;
    }

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        if (sqlite3_column_count(stmt) == 2) {
            uint32_t id = sqlite3_column_int(stmt, 0);
            uint8_t *data = sqlite3_column_blob(stmt, 1);
            int len = sqlite3_column_bytes(stmt, 1);

            if (id >= HASHTABLE_SIZE) continue;

            rows[id].len = len / sizeof(slot_t);
            rows[id].slots = malloc(len);
            memcpy(rows[id].slots, data, len);
        }
    }
    if (SQLITE_DONE != rc) {
        fprintf(stderr, "Select statement didn't finish with DONE (%i): %s\n", rc, sqlite3_errmsg(sqlite));
    }

    sqlite3_finalize(stmt);
    return 0;
}
