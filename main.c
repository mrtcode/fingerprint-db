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
#include <sys/time.h>
#include <signal.h>
#include <pthread.h>
#include <onion/onion.h>
#include <onion/block.h>
#include <jansson.h>
#include <unicode/ustdio.h>
#include <string.h>
#include "hashtable.h"
#include "db.h"

#define HTTP_PORT "8001"

extern row_t rows[HASHTABLE_SIZE];
extern struct timeval t_updated;
copy_row_t copy_rows[COPY_ROWS_LEN];

onion *on = NULL;

pthread_rwlock_t rwlock;

onion_connection_status url_identify(void *_, onion_request *req, onion_response *res) {
    if (!(onion_request_get_flags(req) & OR_POST)) {
        return OCS_PROCESSED;
    }

    const onion_block *dreq = onion_request_get_data(req);

    if (!dreq) return OCS_PROCESSED;

    const char *data = onion_block_data(dreq);

    json_t *root;
    json_error_t error;
    root = json_loads(data, 0, &error);

    if (!root || !json_is_object(root)) {
        return OCS_PROCESSED;
    }

    json_t *json_text = json_object_get(root, "text");

    if (!json_is_string(json_text)) {
        return OCS_PROCESSED;;
    }

    uint8_t *text = json_string_value(json_text);
    uint32_t text_len = json_string_length(json_text);

    result_t results[MAX_NGRAMS];
    uint32_t results_len;

    struct timeval st, et;

    pthread_rwlock_rdlock(&rwlock);

    gettimeofday(&st, NULL);
    identify(text, text_len, results, &results_len);
    gettimeofday(&et, NULL);

    pthread_rwlock_unlock(&rwlock);

    uint32_t elapsed = ((et.tv_sec - st.tv_sec) * 1000000) + (et.tv_usec - st.tv_usec);

    json_t *obj = json_object();
    json_object_set_new(obj, "us_identify", json_integer(elapsed));
    json_t *array = json_array();

    for (int i = 0; i < results_len; i++) {
        json_t *item = json_object();
        json_object_set(item, "id", json_integer(results[i].id));
        json_object_set(item, "count", json_integer(results[i].count));
        json_array_append_new(array, item);
    }

    json_object_set_new(obj, "ids", array);

    char *str = json_dumps(obj, JSON_INDENT(1) | JSON_PRESERVE_ORDER);
    json_decref(obj);

    onion_response_set_header(res, "Content-Type", "application/json; charset=utf-8");
    onion_response_printf(res, str);
    free(str);

    return OCS_PROCESSED;
}

onion_connection_status url_index(void *_, onion_request *req, onion_response *res) {
    if (!(onion_request_get_flags(req) & OR_POST)) {
        return OCS_PROCESSED;
    }

    const onion_block *dreq = onion_request_get_data(req);

    if (!dreq) return OCS_PROCESSED;

    const char *data = onion_block_data(dreq);

    json_t *root;
    json_error_t error;
    root = json_loads(data, 0, &error);

    if (!root) {
        return OCS_PROCESSED;
    }

    pthread_rwlock_wrlock(&rwlock);
    if (json_is_array(root)) {
        uint32_t n = (uint32_t) json_array_size(root);
        int i;
        for (i = 0; i < n; i++) {
            json_t *el = json_array_get(root, i);
            if (json_is_object(el)) {
                json_t *json_id = json_object_get(el, "id");
                json_t *json_text = json_object_get(el, "text");
                uint32_t id = json_integer_value(json_id);
                uint8_t *text = json_string_value(json_text);
                uint32_t text_len = json_string_length(json_text);
                index_text(id, text, text_len);
            }
        }
    }
    pthread_rwlock_unlock(&rwlock);


    json_decref(root);
    return OCS_KEEP_ALIVE;
}

onion_connection_status url_stats(void *_, onion_request *req, onion_response *res) {
    char buf[256];
    stats_t stats = get_stats();
    sprintf(buf, "used_hashes: %d, used_slots: %d, max_slots: %d\n",
            stats.used_hashes, stats.used_slots, stats.max_slots);
    onion_response_printf(res, buf);
    return OCS_PROCESSED;
}

void *saver_thread(void *arg) {
    uint32_t cursor = 0;
    uint32_t copy_rows_len = 0;
    uint32_t buf_size = 0;
    uint8_t *buf;
    struct timeval t_current;

    while (1) {
        copy_rows_len = 0;
        buf_size = 0;
        usleep(10000);

        if (!t_updated.tv_sec) {
            continue;
        }

        gettimeofday(&t_current, NULL);

        if (t_current.tv_sec - t_updated.tv_sec < 2) {
            continue;
        }

        pthread_rwlock_wrlock(&rwlock);
        uint32_t i;
        for (i = 0; i < HASHTABLE_SIZE && copy_rows_len < COPY_ROWS_LEN; i++, cursor++) {
            if (cursor >= HASHTABLE_SIZE) cursor = 0;
            if (rows[cursor].updated) {
                rows[cursor].updated = 0;
                copy_rows[copy_rows_len].id = cursor;
                copy_rows[copy_rows_len].buf_len = rows[cursor].len * sizeof(slot_t);
                buf_size += copy_rows[copy_rows_len].buf_len;
                copy_rows_len++;
            }
        }
        if (i == HASHTABLE_SIZE) {
            t_updated.tv_sec = 0;
            t_updated.tv_usec = 0;
        }

        printf("Saving data: %u\n", copy_rows_len);

        if (copy_rows_len) {
            buf = malloc(buf_size);
            uint8_t *p = buf;
            for (uint32_t i = 0; i < copy_rows_len; i++) {
                copy_row_t *copy_row = &copy_rows[i];
                row_t *row = &rows[copy_row->id];
                copy_row->buf = p;
                memcpy(copy_row->buf, row->slots, copy_row->buf_len);
                p += copy_row->buf_len;
            }
            pthread_rwlock_unlock(&rwlock);

            save_hashtable(copy_rows, copy_rows_len);
            free(buf);
        } else {
            pthread_rwlock_unlock(&rwlock);
        }
    }
}

void exit(int signum) {
    if (on)
        onion_listen_stop(on);
}

int main(int argc, char **argv) {

    struct sigaction action;
    memset(&action, 0, sizeof(struct sigaction));
    action.sa_handler = exit;
    sigaction(SIGINT, &action, NULL);
    sigaction(SIGTERM, &action, NULL);

    setenv("ONION_LOG", "noinfo", 1);
    pthread_rwlock_init(&rwlock, 0);

    init_db();
    load();
    stats_t stats = get_stats();

    printf("used_hashes: %d, used_slots: %d, max_slots: %d\n",
           stats.used_hashes, stats.used_slots, stats.max_slots);

    pthread_t tid;
    pthread_create(&tid, NULL, saver_thread, 0);

    on = onion_new(O_ONE_LOOP);
    onion_set_port(on, HTTP_PORT);
    onion_set_max_post_size(on, 50000000);

    onion_url *urls = onion_root_url(on);

    onion_url_add(urls, "identify", url_identify);
    onion_url_add(urls, "index", url_index);
    onion_url_add(urls, "stats", url_stats);

    onion_listen(on);

    onion_free(on);
    return 0;
}
