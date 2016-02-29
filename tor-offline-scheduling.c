#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>

//#define debug(...) fprintf(stderr, ##__VA_ARGS__)
#define g_debug(...)

#define FOREACH(head, v) for(GQueue *iter = head, v = head->data; iter; iter = g_list_next(iter), v = iter->data)

typedef struct circuit_s {
    gchar *guard;
    gchar *middle;
    gchar *exit;
    gdouble bandwidth;
    gchar *client;
    gdouble start_time;
    gdouble end_time;
} circuit_t;

typedef struct download_s {
    gchar *client;
    gint start_time;
    gint end_time;
    gdouble bandwidth;
    gchar *bottleneck;
    GQueue *circuits;
    gint total_circuit_bandwidth;
    circuit_t **circuit_list;
    circuit_t **weighted_circuit_list;
} download_t;

typedef struct experiment_t {
    GHashTable *circuit_selection;
    gint score;
} experiment_t;

typedef struct experiment_info_t {
    GQueue *downloads;
    GHashTable *relays;
    GHashTable *downloads_by_tick;
    GQueue *ticks;
    GTimer *round_timer;
} experiment_info_t;



GLogLevelFlags min_log_level = G_LOG_LEVEL_MESSAGE;

/*
 * Logging functions
 */

static const gchar *log_level_to_string (GLogLevelFlags level) {
    switch (level) {
        case G_LOG_LEVEL_ERROR: return "ERROR";
        case G_LOG_LEVEL_CRITICAL: return "CRITICAL";
        case G_LOG_LEVEL_WARNING: return "WARNING";
        case G_LOG_LEVEL_MESSAGE: return "MESSAGE";
        case G_LOG_LEVEL_INFO: return "INFO";
        case G_LOG_LEVEL_DEBUG: return "DEBUG";
        default: return "UNKNOWN";
    }
}

static void log_handler_cb(const gchar *log_domain, GLogLevelFlags log_level, const gchar *message, gpointer user_data) {
    const gchar *log_level_str = log_level_to_string(log_level & G_LOG_LEVEL_MASK);

    if(log_level > min_log_level) {
        return;
    }

    if(log_level <= G_LOG_LEVEL_WARNING) {
        g_printerr("[%s] %s\n", log_level_str, message);
    } else {
        g_print("[%s] %s\n", log_level_str, message);
    }
}

/*
 * Helper functions
 */

static int compare_int(gconstpointer p1, gconstpointer p2, gpointer user_data) {
    gint a = GPOINTER_TO_INT(p1);
    gint b = GPOINTER_TO_INT(p2);
    return a - b;
}

static int compare_download_by_start(gconstpointer p1, gconstpointer p2, gpointer user_data) {
    download_t *download1 = (download_t *)p1;
    download_t *download2 = (download_t *)p2;
    return download1->start_time - download2->start_time;
}

static int compare_download_by_end(gconstpointer p1, gconstpointer p2, gpointer user_data) {
    download_t *download1 = (download_t *)p1;
    download_t *download2 = (download_t *)p2;
    return download1->end_time - download2->end_time;
}

static int compare_download_by_length(gconstpointer p1, gconstpointer p2, gpointer user_data) {
    download_t *download1 = (download_t *)p1;
    download_t *download2 = (download_t *)p2;
    gint length1 = download1->end_time - download1->start_time;
    gint length2 = download2->end_time - download2->start_time;
    return length2 - length1;
}

static int compare_download_by_bandwidth(gconstpointer p1, gconstpointer p2, gpointer user_data) {
    download_t *download1 = (download_t *)p1;
    download_t *download2 = (download_t *)p2;
    return download2->bandwidth - download1->bandwidth;
}

void free_download(gpointer data) {
    download_t *download = (download_t *)data;
    g_free(download->client);
    g_free(download);
}

gchar **get_file_lines(gchar *filename) {
    gchar *content;
    gsize length;
    GError *error = NULL;

    /* read in download start and end times */
    gboolean success = g_file_get_contents(filename, &content, &length, &error);
    if(!success) {
        g_error("[ERROR] g_file_get_contents: %s", error->message);
        g_error_free(error);
        return NULL;
    }

    gchar **lines = g_strsplit(content, "\n", 0);
    g_free(content);

    return lines;
}

GHashTable *read_downloads(gchar *filename) {
    gchar **lines = get_file_lines(filename);
    if(!lines) {
        return NULL;
    }

    GHashTable *downloads = g_hash_table_new(g_str_hash, g_str_equal);
    for(gint idx = 0; lines[idx]; idx++) {
        if(!g_ascii_strcasecmp(lines[idx], "")) {
            continue;
        }

        gchar **parts = g_strsplit(lines[idx], " ", 0);
        if(!parts[0] || !parts[1] || !parts[2]) {
            g_warning("missing start time, stop time, or client hostname: '%s'", lines[idx]);
            continue;
        }

        download_t *download = g_new0(download_t, 1);
        download->start_time = (gint)(g_ascii_strtod(parts[0], NULL) * 10) * 100;
        download->end_time = (gint)(g_ascii_strtod(parts[1], NULL) * 10) * 100;
        download->client = g_strdup(parts[2]);

        GQueue *client_downloads = g_hash_table_lookup(downloads, download->client);
        if(!client_downloads) {
            client_downloads = g_queue_new();
            g_hash_table_insert(downloads, download->client, client_downloads);
        }
        g_queue_push_tail(client_downloads, download);

        g_strfreev(parts);
    }
    g_strfreev(lines);

    return downloads;
}

GHashTable *read_relays(gchar *filename) {
    gchar **lines = get_file_lines(filename);
    if(!lines) {
        return NULL;
    }

    GHashTable *relays = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
    for(gint idx = 0; lines[idx]; idx++) {
        if(!g_ascii_strcasecmp(lines[idx], "")) {
            continue;
        }

        gchar **relay_info = g_strsplit(lines[idx], " ", 0);
        if(!relay_info[0] || !relay_info[1]) {
            g_warning("no relay and bandwidth: '%s'", lines[idx]);
            continue;
        }

        gchar *relay = g_strdup(relay_info[0]);
        gint bandwidth = g_ascii_strtoull(relay_info[1], NULL, 10);

        g_hash_table_insert(relays, relay, GINT_TO_POINTER(bandwidth));

        g_strfreev(relay_info);
    }
    g_strfreev(lines);

    return relays;
}

GQueue *read_circuits(gchar *filename, GHashTable *client_downloads) {
    gchar **lines = get_file_lines(filename);
    if(!lines) {
        return NULL;
    }

    GQueue *circuits = g_queue_new();
    for(gint idx = 0; lines[idx]; idx++) {
        if(!g_ascii_strcasecmp(lines[idx], "")) {
            continue;
        }

        gchar **parts = g_strsplit(lines[idx], " ", 0);

        gint nparts = 0;
        while(parts[nparts]) {
            nparts++;
        }

        if(nparts < 3) {
            g_warning("missing guard, middle, or exit: '%s'", lines[idx]);
            continue;
        }

        circuit_t *circuit = g_new0(circuit_t, 1);
        circuit->guard = g_strdup(parts[0]);
        circuit->middle = g_strdup(parts[1]);
        circuit->exit = g_strdup(parts[2]);

        if(nparts > 3) {
            circuit->client = g_strdup(parts[3]);
        }

        if(nparts > 4) {
            circuit->start_time = g_ascii_strtod(parts[4], NULL) * 1000;
        }

        if(nparts > 5) {
            circuit->end_time = g_ascii_strtod(parts[5], NULL) * 1000;
        }

        /* if the circuit is assigned to a specific client, find all the downloads
         * which can potentially use the circuit based on start/end times */
        if(circuit->client) {
            GQueue *downloads = g_hash_table_lookup(client_downloads, circuit->client);
            if(!downloads) {
                g_warning("no downloads for client %s", circuit->client);
                continue;
            }

            for(GList *iter = g_queue_peek_head_link(downloads); iter; iter = g_list_next(iter)) {
                download_t *download = (download_t *)iter->data;
                if((!circuit->start_time || circuit->start_time <= download->start_time) &&
                   (!circuit->end_time || circuit->end_time >= download->end_time)) {

                    if(!download->circuits) {
                        download->circuits = g_queue_new();
                    }
                    g_queue_push_tail(download->circuits, circuit);
                }
            }
        }

        g_queue_push_tail(circuits, circuit);

        g_strfreev(parts);
    }
    g_strfreev(lines);

    return circuits;
}

GQueue *get_all_downloads(GHashTable *client_downloads) {
    g_assert(client_downloads);

    GQueue *downloads = g_queue_new();

    GList *download_lists = g_hash_table_get_values(client_downloads);
    for(GList *iter = download_lists; iter; iter = g_list_next(iter)) {
        GQueue *list = (GQueue*)iter->data;
        for(GList *iter2 = g_queue_peek_head_link(list); iter2; iter2 = g_list_next(iter2)) {
            download_t *download = (download_t *)iter2->data;
            g_queue_push_tail(downloads, download);
        }
    }
    g_list_free(download_lists);

    return downloads;
}

GQueue *build_all_circuits(GHashTable *relays) {
    g_assert(relays);

    GList *relay_list = g_hash_table_get_keys(relays);
    gint nrelays = g_hash_table_size(relays);

    /* construct a list of all circuits */
    GQueue *circuits = g_queue_new();
    for(gint i = 0; i < nrelays - 2; i++) {
        for(gint j = i + 1; j < nrelays - 1; j++) {
            for(gint k = j + 1; k < nrelays; k++) {
                gchar *relay1 = (gchar *)g_list_nth_data(relay_list, i);
                gchar *relay2 = (gchar *)g_list_nth_data(relay_list, j);
                gchar *relay3 = (gchar *)g_list_nth_data(relay_list, k);
                gint bw1 = GPOINTER_TO_INT(g_hash_table_lookup(relays, relay1));
                gint bw2 = GPOINTER_TO_INT(g_hash_table_lookup(relays, relay2));
                gint bw3 = GPOINTER_TO_INT(g_hash_table_lookup(relays, relay3));

                circuit_t *circuit = g_new0(circuit_t, 1);
                circuit->bandwidth = MIN(bw1, MIN(bw2, bw3));

                /* make sure one of the relay_list is an exit */
                if(g_strstr_len(relay3, -1, "exit")) {
                    circuit->guard = relay1;
                    circuit->middle = relay2;
                    circuit->exit = relay3;
                    g_queue_push_tail(circuits, circuit);
                } else if(g_strstr_len(relay2, -1, "exit")) {
                    circuit->guard = relay1;
                    circuit->middle = relay3;
                    circuit->exit = relay2;
                    g_queue_push_tail(circuits, circuit);
                } else if(g_strstr_len(relay1, -1, "exit")) {
                    circuit->guard = relay2;
                    circuit->middle = relay3;
                    circuit->exit = relay1;
                    g_queue_push_tail(circuits, circuit);
                } else {
                    g_free(circuit);
                }
            }
        }
    }

    g_list_free(relay_list);

    return circuits;
}

static gint compare_relay_by_bw(gconstpointer a, gconstpointer b, gpointer user_data) {
    gchar *relay1 = (gchar *)a;
    gchar *relay2 = (gchar *)b;

    GHashTable *relay_bandwidth = (GHashTable *)user_data;
    gint bw1 = GPOINTER_TO_INT(g_hash_table_lookup(relay_bandwidth, relay1));
    gint bw2 = GPOINTER_TO_INT(g_hash_table_lookup(relay_bandwidth, relay2));

    return bw2 - bw1;
}

GQueue *build_pruned_circuits(GHashTable *relays) {
    g_assert(relays);

    GHashTable *relay_bandwidth = g_hash_table_new(g_str_hash, g_str_equal);
    GQueue *relays_remaining = g_queue_new();

    GHashTableIter iter;
    gpointer key,value;

    g_hash_table_iter_init(&iter, relays);
    while(g_hash_table_iter_next(&iter, &key, &value)) {
        g_hash_table_insert(relay_bandwidth, key, value);
        g_queue_push_head(relays_remaining, key);
    }

    GQueue *circuits = g_queue_new();
    while(g_queue_get_length(relays_remaining) >= 3) {
        g_queue_sort(relays_remaining, (GCompareDataFunc)compare_relay_by_bw, relay_bandwidth);

        gchar *relay1 = g_queue_pop_head(relays_remaining);
        gchar *relay2 = g_queue_pop_head(relays_remaining);

        gchar *relay3 = NULL;
        if(g_strstr_len(relay1, -1, "exit") || g_strstr_len(relay2, -1, "exit")) {
            relay3 = g_queue_pop_head(relays_remaining);
        } else {
            gint idx = 0;
            for(GList *iter = g_queue_peek_head_link(relays_remaining); iter && !relay3; iter = g_list_next(iter)) {
                gchar *relay = iter->data;
                if(g_strstr_len(relay, -1, "exit")) {
                    relay3 = g_queue_pop_nth(relays_remaining, idx);
                }
                idx++;
            }
        }

        if(!relay3) {
            break;
        }

        gint bw1 = GPOINTER_TO_INT(g_hash_table_lookup(relay_bandwidth, relay1));
        gint bw2 = GPOINTER_TO_INT(g_hash_table_lookup(relay_bandwidth, relay2));
        gint bw3 = GPOINTER_TO_INT(g_hash_table_lookup(relay_bandwidth, relay3));

        circuit_t *circuit = g_new0(circuit_t, 1);
        circuit->bandwidth = MIN(bw1, MIN(bw2, bw3));

        g_hash_table_insert(relay_bandwidth, relay1, GINT_TO_POINTER(bw1 - circuit->bandwidth));
        g_hash_table_insert(relay_bandwidth, relay2, GINT_TO_POINTER(bw2 - circuit->bandwidth));
        g_hash_table_insert(relay_bandwidth, relay3, GINT_TO_POINTER(bw3 - circuit->bandwidth));

        /* make sure one of the relay_list is an exit */
        if(g_strstr_len(relay3, -1, "exit")) {
            circuit->guard = relay1;
            circuit->middle = relay2;
            circuit->exit = relay3;
            g_queue_push_tail(circuits, circuit);
        } else if(g_strstr_len(relay2, -1, "exit")) {
            circuit->guard = relay1;
            circuit->middle = relay3;
            circuit->exit = relay2;
            g_queue_push_tail(circuits, circuit);
        } else if(g_strstr_len(relay1, -1, "exit")) {
            circuit->guard = relay2;
            circuit->middle = relay3;
            circuit->exit = relay1;
            g_queue_push_tail(circuits, circuit);
        } else {
            g_warning("should be exit, none found with relays %s %s %s", relay1, relay2, relay3);
        }
    }

    g_hash_table_destroy(relay_bandwidth);
    g_queue_free(relays_remaining);

    return circuits;
}

GQueue *get_download_ticks(GQueue *downloads) {
    GQueue *ticks = g_queue_new();
    for(GList *iter = g_queue_peek_head_link(downloads); iter; iter = g_list_next(iter)) {
        download_t *download = (download_t *)iter->data;
        g_queue_push_tail(ticks, GINT_TO_POINTER(download->start_time));
        g_queue_push_tail(ticks, GINT_TO_POINTER(download->end_time));
    }
    g_queue_sort(ticks, (GCompareDataFunc)compare_int, NULL);
    return ticks;
}

GHashTable *generate_downloads_by_tick(GQueue *downloads) {
    /* generate mapping of tick to download list */
    GHashTable *downloads_by_tick = g_hash_table_new(g_direct_hash, g_direct_equal);
    for(GList *iter = g_queue_peek_head_link(downloads); iter; iter = g_list_next(iter)) {
        download_t *download = (download_t *)iter->data;

        GQueue *tick_downloads;

        tick_downloads = g_hash_table_lookup(downloads_by_tick, GINT_TO_POINTER(download->start_time));
        if(!tick_downloads) {
            tick_downloads = g_queue_new();
            g_hash_table_insert(downloads_by_tick, GINT_TO_POINTER(download->start_time), tick_downloads);
        }
        g_queue_push_tail(tick_downloads, download);

        tick_downloads = g_hash_table_lookup(downloads_by_tick, GINT_TO_POINTER(download->end_time));
        if(!tick_downloads) {
            tick_downloads = g_queue_new();
            g_hash_table_insert(downloads_by_tick, GINT_TO_POINTER(download->end_time), tick_downloads);
        }
        g_queue_push_tail(tick_downloads, download);
    }

    return downloads_by_tick;
}

void generate_circuit_lists(GQueue *circuits, circuit_t ***circuit_list, 
        circuit_t ***weighted_circuit_list, gint *total_circuit_bandwidth) {
    g_assert(circuits);

    *circuit_list = (circuit_t **)g_new0(gpointer, g_queue_get_length(circuits));
    circuit_t **list = *circuit_list;

    *total_circuit_bandwidth = 0;

    gint idx = 0;
    for(GList *iter = g_queue_peek_head_link(circuits); iter; iter = g_list_next(iter)) {
        circuit_t *circuit = iter->data;
        list[idx] = circuit;
        idx++;

        gint bw = MAX((gint)(circuit->bandwidth / 1024.0), 1);
        *total_circuit_bandwidth += bw;
    }

    /*g_message("total circuit bandwidth %d", *total_circuit_bandwidth);*/

    *weighted_circuit_list = (circuit_t **)g_new0(gpointer, *total_circuit_bandwidth);
    circuit_t **weighted_list = *weighted_circuit_list;

    idx = 0;
    for(gint i = 0; i < g_queue_get_length(circuits); i++) {
        gint bw = MAX((gint)(list[i]->bandwidth / 1024.0), 1);
        for(gint j = 0; j < bw; j++) {
            weighted_list[idx++] = list[i];
        }
    }
}

void write_circuits_to_file(GQueue *downloads, GHashTable *circuit_selection, gchar *filename) {
    GString *content = g_string_new("");
    for(GList *iter = g_queue_peek_head_link(downloads); iter; iter = g_list_next(iter)) {
        download_t *download = iter->data;
        circuit_t *circuit = g_hash_table_lookup(circuit_selection, download);
        g_string_append_printf(content, "%s %f %f %s %s %s\n", download->client,
                download->start_time / 1000.0, download->end_time / 1000.0,
                circuit->guard, circuit->middle, circuit->exit);
    }

    GError *error = NULL;
    if(!g_file_set_contents(filename, content->str, content->len, &error)) {
        g_error("[ERROR] g_file_set_contents: %s", error->message);
        g_error_free(error);
    }

    g_string_free(content, TRUE);
}


/*
 * Calculate bandwidth of each circuit
 */

void update_active_relay(GHashTable *relay_downloads, GHashTable *active_relays, gchar *relay, download_t *download, gint bandwidth) {
    g_assert(relay_downloads);
    g_assert(active_relays);
    g_assert(relay);

    GHashTable *downloads = g_hash_table_lookup(relay_downloads, relay);
    if(!downloads) {
        downloads = g_hash_table_new(g_direct_hash, g_direct_equal);
        g_hash_table_insert(relay_downloads, relay, downloads);
    }
    g_hash_table_insert(downloads, download, GINT_TO_POINTER(TRUE));

    if(!g_hash_table_lookup(active_relays, relay)) {
        gdouble *bw = g_new0(gdouble, 1);
        *bw = (gdouble)bandwidth;
        g_hash_table_insert(active_relays, relay, bw);
    }
}

void update_relays(GHashTable *active_relays, gchar *relay, gdouble download_bandwidth) {
    g_assert(active_relays);
    g_assert(relay);

    gdouble *relays = (gdouble *)(g_hash_table_lookup(active_relays, relay));
    *relays -= download_bandwidth;
    if(*relays < 0.000001) {
        g_debug("removing relay %s from list with bandwidth %f", relay, *relays);
        g_hash_table_remove(active_relays, relay);
    }
}

void remove_download_from_relay(GHashTable *relay_downloads, gchar *relay, download_t *download) {
    g_assert(relay_downloads);
    g_assert(relay);
    g_assert(download);

    GHashTable *downloads = g_hash_table_lookup(relay_downloads, relay);
    if(!download) {
        g_error("relay %s had no download list", relay);
        return;
    }

    if(!g_hash_table_remove(downloads, download)) {
        g_error("error removing download %p from relay %s", download, relay);
    }

    if(g_hash_table_size(downloads) == 0) {
        g_debug("removing %s from downloads", relay);
        g_hash_table_destroy(downloads);
        g_hash_table_remove(relay_downloads, relay);
    }
}

gdouble compute_download_bandwidths(GHashTable *active_downloads, GHashTable *relays, GHashTable *circuit_selection, GHashTable *weights, GHashTable *available_bandwidth) {
    g_assert(relays);

    GHashTableIter iter;
    gpointer key,value;

    GHashTable *active_relays = g_hash_table_new_full(g_str_hash, g_str_equal, NULL, g_free);
    GHashTable *relay_downloads = g_hash_table_new(g_str_hash, g_str_equal);

    /* 1. Build mapping of relay and all active downloads */
    g_hash_table_iter_init(&iter, active_downloads);
    while(g_hash_table_iter_next(&iter, &key, &value)) {
        download_t *download = (download_t *)key;
        circuit_t *circuit = g_hash_table_lookup(circuit_selection, download);
        g_debug("download from %f to %f active on circuit %s,%s,%s",
                download->start_time / 1000.0, download->end_time / 1000.0,
                circuit->guard, circuit->middle, circuit->exit);

        gint bandwidth;

        bandwidth = GPOINTER_TO_INT(g_hash_table_lookup(relays, circuit->guard));
        update_active_relay(relay_downloads, active_relays, circuit->guard, download, bandwidth);

        bandwidth = GPOINTER_TO_INT(g_hash_table_lookup(relays, circuit->middle));
        update_active_relay(relay_downloads, active_relays, circuit->middle, download, bandwidth);

        bandwidth = GPOINTER_TO_INT(g_hash_table_lookup(relays, circuit->exit));
        update_active_relay(relay_downloads, active_relays, circuit->exit, download, bandwidth);
    }

    if(available_bandwidth) {
        g_hash_table_iter_init(&iter, relays);
        while(g_hash_table_iter_next(&iter, &key, &value)) {
            gchar *relay = (gchar *)key;
            if(!g_hash_table_lookup(active_relays, relay)) {
                g_hash_table_insert(available_bandwidth, relay, value);
            }
        }
    }

    gdouble total_bandwidth = 0;

    gint ncircuits = 0;

    /* loop through all relays until there are no longer
     * any active relays or active downloads */
    while(g_hash_table_size(active_relays) > 0 && g_hash_table_size(relay_downloads) > 0) {
        gchar *bottleneck_relay = NULL;
        gdouble bottleneck_bandwidth = G_MAXINT32;
        gdouble download_bandwidth = G_MAXINT32;


        /* 2. find relay with smallest per download bandwidth */
        GHashTableIter iter;
        gpointer key,value;
        g_hash_table_iter_init(&iter, active_relays);
        while(g_hash_table_iter_next(&iter, &key, &value)) {
            gchar *relay = (gchar *)key;
            gdouble bandwidth = *((gdouble *)value);
            if(!bandwidth) {
                g_warning("relay %s has 0 bandwidth, should not be in active list", relay);
                continue;
            }
            GHashTable *relay_download_list = g_hash_table_lookup(relay_downloads, relay);
            if(!relay_download_list) {
                continue;
            }

            gint ndownloads = g_hash_table_size(relay_download_list);
            if(bandwidth / ndownloads < download_bandwidth) {
                bottleneck_relay = relay;
                bottleneck_bandwidth = bandwidth;
                download_bandwidth = bandwidth / ndownloads;
            }
        }

        if(!bottleneck_relay) {
            g_error("[ERROR] no bottleneck relay found somehow, must be done");
            continue;
        }

        GHashTable *downloads = g_hash_table_lookup(relay_downloads, bottleneck_relay);
        GList *download_list = g_hash_table_get_keys(downloads);

        /*g_debug("bottleneck relay is %s (%f) with %d downloads and %f per download bandwidth",*/
                /*bottleneck_relay, bottleneck_bandwidth, g_list_length(download_list), download_bandwidth);*/

        ncircuits += g_hash_table_size(downloads);

        gdouble *bw = g_hash_table_lookup(active_relays, bottleneck_relay);
        *bw = download_bandwidth * g_hash_table_size(downloads);

        /* if there is a weight hash table, update the DWC weight */
        if(weights) {
            gdouble *weight = g_hash_table_lookup(weights, bottleneck_relay);
            if(!weight) {
                weight = g_new0(gdouble, 1);
                g_hash_table_insert(weights, bottleneck_relay, weight);
            }
            *weight = (1.0 / download_bandwidth) * g_hash_table_size(downloads);
        }


        /* 3. go through all relay downloads, assign them the bottleneck bandwidth,
         * and decrement the bandwidth of the relays on the download circuit */
        for(GList *iter = download_list; iter; iter = g_list_next(iter)) {
            download_t *download = (download_t *)iter->data;
            circuit_t *circuit = g_hash_table_lookup(circuit_selection, download);

            download->bandwidth = download_bandwidth;
            download->bottleneck = bottleneck_relay;
            total_bandwidth += download_bandwidth;

            g_debug("updating download with circuit %s,%s,%s",
                    circuit->guard, circuit->middle, circuit->exit);
    
            /* update bandwidth of relays on the circuit */
            update_relays(active_relays, circuit->guard, download_bandwidth);
            update_relays(active_relays, circuit->middle, download_bandwidth);
            update_relays(active_relays, circuit->exit, download_bandwidth);

            /* remove download from relay download lists */
            remove_download_from_relay(relay_downloads, circuit->guard, download);
            remove_download_from_relay(relay_downloads, circuit->middle, download);
            remove_download_from_relay(relay_downloads, circuit->exit, download);
        }
        g_list_free(download_list);

        if(g_hash_table_lookup(active_relays, bottleneck_relay)) {
            g_error("bottleneck relay %s still has bandwidth %d available", bottleneck_relay,
                    GPOINTER_TO_INT(g_hash_table_lookup(active_relays, bottleneck_relay)));
        }
        if(g_hash_table_lookup(relay_downloads, bottleneck_relay)) {
            g_error("bottleneck relay %s still has downloads", bottleneck_relay);
        }
    }

    /*g_debug("we saw %d circuits, %d active relays left, %d active downloads left", ncircuits,*/
            /*g_hash_table_size(active_relays), g_hash_table_size(relay_downloads));*/

    GList *download_lists = g_hash_table_get_values(relay_downloads);
    for(GList *iter = download_lists; iter; iter = g_list_next(iter)) {
        g_queue_free((GQueue *)iter->data);
    }
    g_list_free(download_lists);

    if(available_bandwidth) {
        g_hash_table_iter_init(&iter, active_relays);
        while(g_hash_table_iter_next(&iter, &key, &value)) {
            gchar *relay = (gchar *)key;
            gdouble *bandwidth = (gdouble *)value;
            g_hash_table_insert(available_bandwidth, relay, GINT_TO_POINTER((gint)(*bandwidth)));
        }
    }

    g_hash_table_destroy(active_relays);
    g_hash_table_destroy(relay_downloads);

    return total_bandwidth;
}

gdouble compute_total_bandwidth(GQueue *downloads, GHashTable *relays, GHashTable *circuit_selection, GHashTable *downloads_by_tick, GQueue *ticks) {
    g_assert(downloads);
    g_assert(relays);
    g_assert(downloads_by_tick);

    GHashTable *active_downloads = g_hash_table_new(g_direct_hash, g_direct_equal);

    gdouble total_bandwidth = 0;
    gint last_tick = -1;
    gint last_bandwidth;
    for(GList *iter = g_queue_peek_head_link(ticks); iter; iter = g_list_next(iter)) {
        gint tick = GPOINTER_TO_INT(iter->data);
        GQueue *tick_downloads = g_hash_table_lookup(downloads_by_tick, GINT_TO_POINTER(tick));

        for(GList *diter = g_queue_peek_head_link(tick_downloads); diter; diter = g_list_next(diter)) {
            download_t *download = diter->data;

            if(g_hash_table_lookup(circuit_selection, download)) {
                if(download->start_time == tick) {
                    g_hash_table_insert(active_downloads, download, GINT_TO_POINTER(TRUE));
                } else if(download->end_time == tick) {
                    g_hash_table_remove(active_downloads, download);
                } else {
                    g_error("download from %d to %d in list for tick %d", download->start_time, download->end_time, tick);
                }
            }
        }

        gdouble bandwidth = compute_download_bandwidths(active_downloads, relays, circuit_selection, NULL, NULL);

        if(last_tick != -1) {
            total_bandwidth += last_bandwidth * (tick - last_tick) / 1000.0;
        }

        g_debug("[%f] %d downloads, bandwidth %f MBps (total %f)", tick / 1000.0, 
                g_hash_table_size(active_downloads), bandwidth / 1024.0, total_bandwidth / 1024.0);

        last_tick = tick;
        last_bandwidth = bandwidth;
    }

    g_hash_table_destroy(active_downloads);

    return total_bandwidth;
}

/*
 * Genetic Algorithm functions
 */

experiment_t **generate_initial_experiments(GQueue *downloads, gdouble weighted, gint n) {
    experiment_t **experiments = (experiment_t **)g_new0(gpointer, n);
    for(gint i = 0; i < n; i++) {
        experiments[i] = g_new0(experiment_t, 1);
    }


    for(gint i = 0; i < n; i++) {
        experiments[i]->circuit_selection = g_hash_table_new(g_direct_hash, g_direct_equal);

        for(GList *iter = g_queue_peek_head_link(downloads); iter; iter = g_list_next(iter)) {
            download_t *download = (download_t *)iter->data;
            
            circuit_t **list = download->circuit_list;
            gint ncircuits = g_queue_get_length(download->circuits);

            if(weighted) {
                list = download->weighted_circuit_list;
                ncircuits = download->total_circuit_bandwidth;
            }

            gint idx = rand() % ncircuits;
            circuit_t *circuit = list[idx];
            g_hash_table_insert(experiments[i]->circuit_selection, download, circuit);
        }
    }

    return experiments;
}

experiment_t *select_parent(experiment_t **experiments, gint nexperiments, gdouble breed_percentile,
        gboolean breed_weighted) {
    g_assert(experiments);

    gint breed_size = nexperiments * breed_percentile;
    experiment_t **breed_experiments = (experiment_t **)g_new0(gpointer, breed_size);

    /* go through and get the top breed percentile experiments */
    for(gint i = 0; i < breed_size; i++) {
        breed_experiments[i] = experiments[i];
    }
    for(gint i = breed_size; i < nexperiments; i++) {
        experiment_t *iter = experiments[i];
        for(gint j = 0; j < breed_size; j++) {
            if(iter->score > breed_experiments[j]->score) {
                experiment_t *t = breed_experiments[j];
                breed_experiments[j] = iter;
                iter = t;
            }
        }
    }

    experiment_t *parent;

    if(!breed_weighted) {
        gint idx = rand() % breed_size;
        parent = breed_experiments[idx];
    } else {
        gint total_score = 0;
        for(gint i = 0; i < breed_size; i++) {
            total_score += (gint)(breed_experiments[i]->score / 1024.0);
        }

        experiment_t **weighted_experiments = (experiment_t **)g_new0(gpointer, total_score);

        gint weighted_idx = 0;
        for(gint i = 0; i < breed_size; i++) {
            for(gint j = 0; j < (gint)(breed_experiments[i]->score / 1024.0); j++) {
                weighted_experiments[weighted_idx++] = breed_experiments[i];
            }
        }

        gint idx = rand() % total_score;
        parent = weighted_experiments[idx];

        g_free(weighted_experiments);
    }

    g_free(breed_experiments);
        
    return parent;
}

void breed(experiment_t **experiments, gint nexperiments, GQueue *downloads, gdouble breed_percentile, 
        gboolean breed_weighted, gdouble elite_percentile, gdouble mutation_probability) {
    g_assert(experiments);

    experiment_t **new_experiments = (experiment_t **)g_new0(gpointer, nexperiments);

    gint nelite = nexperiments * elite_percentile;

    for(gint i = 0; i < nelite; i++) {
        new_experiments[i] = experiments[i];
    }
    for(gint i = nelite; i < nexperiments; i++) {
        experiment_t *iter = experiments[i];
        for(gint j = 0; j < nelite; j++) {
            if(iter->score > new_experiments[j]->score) {
                experiment_t *t = new_experiments[j];
                new_experiments[j] = iter;
                iter = t;
            }
        }
    }

    for(gint i = 0; i < nelite; i++) {
        experiment_t *experiment = new_experiments[i];
        
        new_experiments[i] = g_new0(experiment_t, 1);
        new_experiments[i]->circuit_selection = g_hash_table_new(g_direct_hash, g_direct_equal);

        GHashTableIter iter;
        gpointer key,value;

        g_hash_table_iter_init(&iter, experiment->circuit_selection);
        while(g_hash_table_iter_next(&iter, &key, &value)) {
            g_hash_table_insert(new_experiments[i]->circuit_selection, key, value);
        }
    }

    for(gint i = nelite; i < nexperiments; i++) {
        new_experiments[i] = g_new0(experiment_t, 1);
        new_experiments[i]->circuit_selection = g_hash_table_new(g_direct_hash, g_direct_equal);
        
        experiment_t *child = new_experiments[i];
        experiment_t *parent1 = select_parent(experiments, nexperiments, breed_percentile, breed_weighted);
        experiment_t *parent2 = select_parent(experiments, nexperiments, breed_percentile, breed_weighted);

        for(GList *iter = g_queue_peek_head_link(downloads); iter; iter = g_list_next(iter)) {
            download_t *download = iter->data;
            circuit_t *circuit1 = g_hash_table_lookup(parent1->circuit_selection, download);
            circuit_t *circuit2 = g_hash_table_lookup(parent2->circuit_selection, download);

            g_assert(circuit1);
            g_assert(circuit2);

            gdouble r = (gdouble)rand() / RAND_MAX;

            if(r < mutation_probability) {
                gint idx = rand() % g_queue_get_length(download->circuits);
                g_hash_table_insert(child->circuit_selection, download, download->circuit_list[idx]);
            } else {
                r = (gdouble)rand() / RAND_MAX;
                if(r < 0.5) {
                    g_hash_table_insert(child->circuit_selection, download, circuit1);
                } else {
                    g_hash_table_insert(child->circuit_selection, download, circuit2);
                }
            }
        }
    }

    for(gint i = 0; i < nexperiments; i++) {
        g_hash_table_destroy(experiments[i]->circuit_selection);
        experiments[i]->circuit_selection = new_experiments[i]->circuit_selection;
        g_free(new_experiments[i]);
    }

    g_free(new_experiments);
}

void genetic_worker(experiment_t *experiment, gpointer user_data) {
    g_assert(experiment);
    g_assert(user_data);

    experiment_info_t *experiment_info = (experiment_info_t *)user_data;
    gdouble start = g_timer_elapsed(experiment_info->round_timer, NULL);

    /*g_usleep(G_USEC_PER_SEC);*/

    experiment->score = compute_total_bandwidth(experiment_info->downloads, 
            experiment_info->relays, experiment->circuit_selection, 
            experiment_info->downloads_by_tick, experiment_info->ticks);

    gdouble end = g_timer_elapsed(experiment_info->round_timer, NULL);
    g_message("[%f] [%f] experiment returned bandwidth of %f MB/s", end,
            end - start, experiment->score / 1024.0 / 1024.0);
}

void run_genetic_algorithm(GQueue *downloads, GHashTable *relays, gint nexperiments, 
        gboolean initial_weighted, gdouble breed_percentile, gboolean breed_weighted, 
        gdouble elite_percentile, gdouble mutate_probability, gint nthreads) {
    g_assert(downloads);
    g_assert(relays);

    g_message("Generating initial experiment of size %d", nexperiments);

    experiment_info_t *experiment_info = g_new0(experiment_info_t, 1);
    experiment_info->downloads = downloads;
    experiment_info->relays = relays;
    experiment_info->downloads_by_tick = generate_downloads_by_tick(downloads);
    experiment_info->ticks = g_queue_new();

    GList *tick_list = g_hash_table_get_keys(experiment_info->downloads_by_tick);
    tick_list = g_list_sort(tick_list, (GCompareFunc)compare_int);
    for(GList *iter = tick_list; iter; iter = g_list_next(iter)) {
        g_queue_push_tail(experiment_info->ticks, iter->data);
    }
    g_list_free(tick_list);

    experiment_t **experiments = generate_initial_experiments(downloads, initial_weighted, 
            nexperiments);

    gint roundnum = 1;
    while(TRUE) {
        g_message("Starting round %d", roundnum);

        experiment_info->round_timer = g_timer_new();
        GThreadPool *thread_pool = g_thread_pool_new((GFunc)genetic_worker,
            experiment_info, nthreads, TRUE, NULL);

        for(gint i = 0; i < nexperiments; i++) {
            g_thread_pool_push(thread_pool, experiments[i], NULL);
        }

        g_thread_pool_free(thread_pool, FALSE, TRUE);
        g_timer_destroy(experiment_info->round_timer);
            
            
        gint max_bandwidth_idx = 0;
        gdouble total_score = 0;
        for(gint i = 0; i < nexperiments; i++) {
            total_score += experiments[i]->score;

            if(experiments[i]->score > experiments[max_bandwidth_idx]->score) {
                max_bandwidth_idx = i;
            }
        }

        g_message("[round %d] average total bandwidth %f", roundnum, (total_score / nexperiments) / 1024.0);

        g_message("[round %d] best circuit selection at %d with bandwidth %f, saving it", roundnum, max_bandwidth_idx + 1,
                experiments[max_bandwidth_idx]->score / 1024.0 / 1024.0);


        gchar filename[1024];
        sprintf(filename, "circuits/round%d.txt", roundnum);
        write_circuits_to_file(downloads, experiments[max_bandwidth_idx]->circuit_selection, filename);
    
        breed(experiments, nexperiments, downloads, breed_percentile, breed_weighted, 
                elite_percentile, mutate_probability);

        roundnum++;
    }
}

/*
 * Greedy circuit selection algorithms
 */

void greedy_circuit_selection(GQueue *downloads, GHashTable *relays) {
    g_assert(downloads);
    g_assert(relays);

    GTimer *timer = g_timer_new();
    gdouble last_time_elapsed = 0;
    gdouble times_elapsed[10];
    for(gint i = 0; i < 10; i++) {
        times_elapsed[i] = 0;
    }
    gint elapsed_idx = 0;

    /*GHashTable *downloads_by_tick = generate_downloads_by_tick(downloads);*/
    /*GQueue *ticks = g_list_sort(g_hash_table_get_keys(downloads_by_tick), (GCompareFunc)compare_int);*/

    GHashTable *downloads_by_tick = g_hash_table_new(g_direct_hash, g_direct_equal);
    GHashTable *circuit_selection = g_hash_table_new(g_direct_hash, g_direct_equal);

    gint n = 1;
    for(GList *dliter = g_queue_peek_head_link(downloads); dliter; dliter = g_list_next(dliter)) {
        download_t *download = dliter->data;

        GQueue *tick_downloads;

        tick_downloads = g_hash_table_lookup(downloads_by_tick, GINT_TO_POINTER(download->start_time));
        if(!tick_downloads) {
            tick_downloads = g_queue_new();
            g_hash_table_insert(downloads_by_tick, tick_downloads, GINT_TO_POINTER(download->start_time));
        }
        g_queue_push_tail(tick_downloads, download);

        tick_downloads = g_hash_table_lookup(downloads_by_tick, GINT_TO_POINTER(download->end_time));
        if(!tick_downloads) {
            tick_downloads = g_queue_new();
            g_hash_table_insert(downloads_by_tick, tick_downloads, GINT_TO_POINTER(download->end_time));
        }
        g_queue_push_tail(tick_downloads, download);

        GList *tick_list = g_list_sort(g_hash_table_get_keys(downloads_by_tick), (GCompareFunc)compare_int);
        GQueue *ticks = g_queue_new();
        for(GList *iter = tick_list; iter; iter = g_list_next(iter)) {
            g_queue_push_tail(ticks, iter->data);
        }
        g_list_free(tick_list);
                    

        circuit_t *best_circuit;
        gdouble best_circuit_bandwidth = 0;

        for(GList *circiter = g_queue_peek_head_link(download->circuits); circiter; circiter = g_list_next(circiter)) {
            circuit_t *circuit = circiter->data;
            g_hash_table_insert(circuit_selection, download, circuit);

            gdouble bandwidth = compute_total_bandwidth(downloads, relays, circuit_selection, downloads_by_tick, ticks);
            if(bandwidth > best_circuit_bandwidth) {
                best_circuit = circuit;
                best_circuit_bandwidth = bandwidth;
            }
        }

        g_queue_free(ticks);

        gdouble elapsed = g_timer_elapsed(timer, NULL);
        times_elapsed[elapsed_idx] = elapsed - last_time_elapsed;
        gdouble time_per_download = 0;
        for(gint i = 0; i < 10; i++) {
            time_per_download += times_elapsed[i];
        }
        time_per_download /= 10;
        gdouble time_remaining = (g_queue_get_length(downloads) - n) * time_per_download;
        last_time_elapsed = elapsed;
        elapsed_idx = (elapsed_idx + 1) % 10;

        g_hash_table_insert(circuit_selection, download, best_circuit);
        g_message("[%f] [%d/%d] selected circuit %s %s %s with bw %f for download %f - %f (%f) on %s (estimated %f seconds left)", elapsed, n, g_queue_get_length(downloads),
                best_circuit->guard, best_circuit->middle, best_circuit->exit, best_circuit_bandwidth, 
                download->start_time / 1000.0, download->end_time / 1000.0, (download->end_time - download->start_time) / 1000.0,
                download->client, time_remaining);

        n++;

    }

    g_hash_table_destroy(circuit_selection);
    g_hash_table_destroy(downloads_by_tick);
}

void run_greedy_algorithm(GQueue *downloads, GHashTable *relays, gchar *selection) {
    g_assert(downloads);
    g_assert(relays);

    if(!g_ascii_strcasecmp(selection, "inorder")) {
        g_queue_sort(downloads, (GCompareDataFunc)compare_download_by_end, NULL);
    } else if(!g_ascii_strcasecmp(selection, "longest")) {
        g_queue_sort(downloads, (GCompareDataFunc)compare_download_by_length, NULL);
    } else if(!g_ascii_strcasecmp(selection, "shortest")) {
        g_queue_sort(downloads, (GCompareDataFunc)compare_download_by_length, NULL);
        g_queue_reverse(downloads);
    } else {
        g_warning("no selection mode '%s', defaulting to inorder", selection);
        g_queue_sort(downloads, (GCompareDataFunc)compare_download_by_end, NULL);
    }

    greedy_circuit_selection(downloads, relays);
}

/*
 * Run the DWC algorithm offline, but still processing downloads in an "online" manor
 **/

typedef struct dwc_data_s {
    GHashTable *relays;
    GHashTable *active_downloads;
    GHashTable *circuit_selection;
    GHashTable *relay_weights;
    GHashTable *available_bandwidth;
    download_t *download;
    circuit_t *best_circuit;
    gdouble best_circuit_weight;
    gint best_circuit_bandwidth;
    gint start_idx;
    gint end_idx;
} dwc_data_t;

void dwc_worker(dwc_data_t *dwc_data, gpointer user_data) {
    GHashTable *circuit_selection = g_hash_table_new(g_direct_hash, g_direct_equal);
    GHashTable *relay_weights = g_hash_table_new_full(g_str_hash, g_str_equal, NULL, g_free);
    GHashTable *available_bandwidth = g_hash_table_new(g_str_hash, g_str_equal);

    GHashTableIter iter;
    gpointer key,value;
    g_hash_table_iter_init(&iter, dwc_data->circuit_selection);
    while(g_hash_table_iter_next(&iter, &key, &value)) {
        g_hash_table_insert(circuit_selection, key, value);
    }

    dwc_data->best_circuit = NULL;
    dwc_data->best_circuit_weight = G_MAXDOUBLE;
    dwc_data->best_circuit_bandwidth = G_MININT;

    for(gint i = dwc_data->start_idx; i < dwc_data->end_idx; i++) {
        circuit_t *circuit = dwc_data->download->circuit_list[i];

        GHashTable *weights;
        GHashTable *bandwidths;
        if(dwc_data->relay_weights) {
            weights = dwc_data->relay_weights;
            bandwidths = dwc_data->available_bandwidth;
        } else {
            g_hash_table_insert(circuit_selection, dwc_data->download, circuit);
            compute_download_bandwidths(dwc_data->active_downloads, dwc_data->relays, circuit_selection, relay_weights, available_bandwidth);
            weights = relay_weights;
            bandwidths = available_bandwidth;
        }

        gint circuit_bandwidth = GPOINTER_TO_INT(g_hash_table_lookup(bandwidths, circuit->guard));
        circuit_bandwidth = MIN(circuit_bandwidth, GPOINTER_TO_INT(g_hash_table_lookup(bandwidths, circuit->middle)));
        circuit_bandwidth = MIN(circuit_bandwidth, GPOINTER_TO_INT(g_hash_table_lookup(bandwidths, circuit->exit)));

        gdouble circuit_weight = 0;
        gdouble *weight;

        weight = (gdouble *)g_hash_table_lookup(weights, circuit->guard);
        if(weight) {
            circuit_weight += *weight;
        }
        weight = (gdouble *)g_hash_table_lookup(weights, circuit->middle);
        if(weight) {
            circuit_weight += *weight;
        }
        weight = (gdouble *)g_hash_table_lookup(weights, circuit->exit);
        if(weight) {
            circuit_weight += *weight;
        }

        if(circuit_weight < dwc_data->best_circuit_weight || (circuit_weight == dwc_data->best_circuit_weight && circuit_bandwidth > dwc_data->best_circuit_bandwidth)) {
            dwc_data->best_circuit = circuit;
            dwc_data->best_circuit_weight = circuit_weight;
            dwc_data->best_circuit_bandwidth = circuit_bandwidth;
        }
    }

    g_hash_table_destroy(relay_weights);
    g_hash_table_destroy(circuit_selection);
}

GHashTable* run_dwc_algorithm(GQueue *downloads, GHashTable *relays, gint nthreads) {
    g_assert(downloads);
    g_assert(relays);

    GHashTable *downloads_by_tick = generate_downloads_by_tick(downloads);
    GQueue *ticks = g_queue_new();

    GList *tick_list = g_hash_table_get_keys(downloads_by_tick);
    tick_list = g_list_sort(tick_list, (GCompareFunc)compare_int);
    for(GList *iter = tick_list; iter; iter = g_list_next(iter)) {
        g_queue_push_tail(ticks, iter->data);
    }
    g_list_free(tick_list);

    GHashTable *active_downloads = g_hash_table_new(g_direct_hash, g_direct_equal);
    GHashTable *circuit_selection = g_hash_table_new(g_direct_hash, g_direct_equal);

    dwc_data_t **dwc_data = (dwc_data_t **)g_new0(gpointer, nthreads);

    for(gint i = 0; i < nthreads; i++) {
        dwc_data[i] = g_new0(dwc_data_t, 1);
        dwc_data[i]->relays = relays;
        dwc_data[i]->active_downloads = active_downloads;
        dwc_data[i]->circuit_selection = circuit_selection;
    }

    gint n = 0;
    gint ndownloads = g_queue_get_length(downloads);
    GTimer *timer = g_timer_new();
    gdouble last_elapsed = 0;

    for(GList *iter = g_queue_peek_head_link(ticks); iter; iter = g_list_next(iter)) {
        gint tick = GPOINTER_TO_INT(iter->data);
        GQueue *tick_downloads = g_hash_table_lookup(downloads_by_tick, GINT_TO_POINTER(tick));

        /* remove all downloads that have ended from active map */
        for(GList *diter = g_queue_peek_head_link(tick_downloads); diter; diter = g_list_next(diter)) {
            download_t *download = diter->data;

            if(download->end_time == tick) {
                g_hash_table_remove(active_downloads, download);
            }
        }

        /* for all downloads that started, use DWC to pick circuit */
        for(GList *diter = g_queue_peek_head_link(tick_downloads); diter; diter = g_list_next(diter)) {
            download_t *download = diter->data;

            /*compute_download_bandwidths(active_downloads, relays, circuit_selection, relay_weights, available_bandwidth);*/

            if(download->start_time == tick) {
                circuit_t *best_circuit = NULL;
                gdouble best_circuit_weight = G_MAXDOUBLE;
                gint best_circuit_bandwidth = G_MININT;

                GHashTable *relay_weights = NULL;
                GHashTable *available_bandwidth = NULL;

                /*g_hash_table_insert(active_downloads, download, GINT_TO_POINTER(TRUE));*/

                relay_weights = g_hash_table_new_full(g_str_hash, g_str_equal, NULL, g_free);
                available_bandwidth = g_hash_table_new_full(g_str_hash, g_str_equal, NULL, NULL);
                compute_download_bandwidths(active_downloads, relays, circuit_selection, relay_weights, available_bandwidth);

                gint interval = g_queue_get_length(download->circuits) / nthreads;
                for(gint i = 0; i < nthreads; i++) {
                    dwc_data[i]->relay_weights = relay_weights;
                    dwc_data[i]->available_bandwidth = available_bandwidth;
                    dwc_data[i]->download = download;
                    dwc_data[i]->best_circuit = NULL;
                    dwc_data[i]->best_circuit_weight = G_MAXDOUBLE;
                    dwc_data[i]->best_circuit_bandwidth = G_MININT;
                    dwc_data[i]->start_idx = i * interval;
                    dwc_data[i]->end_idx = (i+1) * interval;
                }
                dwc_data[nthreads - 1]->end_idx = g_queue_get_length(download->circuits);

                GThreadPool *thread_pool = g_thread_pool_new((GFunc)dwc_worker, NULL, nthreads, TRUE, NULL);
                for(gint i = 0; i < nthreads; i++) {
                    g_thread_pool_push(thread_pool, dwc_data[i], NULL);
                }
                g_thread_pool_free(thread_pool, FALSE, TRUE);

                for(gint i = 0; i < nthreads; i++) {
                    /*g_message("[thread-%d] best weight %f", i+1, dwc_data[i]->best_circuit_weight);*/
                    if(best_circuit_weight > dwc_data[i]->best_circuit_weight || (best_circuit_weight == dwc_data[i]->best_circuit_weight && best_circuit_bandwidth < dwc_data[i]->best_circuit_bandwidth)) {
                        best_circuit = dwc_data[i]->best_circuit;
                        best_circuit_weight = dwc_data[i]->best_circuit_weight;
                        best_circuit_bandwidth = dwc_data[i]->best_circuit_bandwidth;
                    }
                }

                g_hash_table_destroy(relay_weights);
                g_hash_table_destroy(available_bandwidth);

                g_hash_table_insert(active_downloads, download, GINT_TO_POINTER(TRUE));
                g_hash_table_insert(circuit_selection, download, best_circuit);

                gint total_bandwidth = compute_download_bandwidths(active_downloads, relays, circuit_selection, NULL, NULL);

                n++;

                gdouble elapsed = g_timer_elapsed(timer, NULL);
                gdouble time_left = (elapsed - last_elapsed) * (ndownloads - n);
                last_elapsed = elapsed;

                g_message("[%f] [%f MB/s] [%d/%d] [%s] download %f-%f assigned circuit %s,%s,%s (weight %f bw %d) (%d active) (time left %f)", elapsed, total_bandwidth / 1024.0, n, ndownloads,
                        download->client, download->start_time / 1000.0, download->end_time / 1000.0,
                        best_circuit->guard, best_circuit->middle, best_circuit->exit, best_circuit_weight, best_circuit_bandwidth, g_hash_table_size(active_downloads), time_left);
            }
        }

    }

    gdouble total_bandwidth = compute_total_bandwidth(downloads, relays, circuit_selection, downloads_by_tick, ticks);
    g_message("Total bandwidth calculation %f", total_bandwidth / 1024.0 / 1024.0);

    g_hash_table_destroy(downloads_by_tick);
    g_queue_free(ticks);

    return circuit_selection;
}

/*
 * Estimate maximum bandwidth of Tor network
 **/
void estimate_max_bandwidth(GQueue *circuits, GHashTable *relays) {
    g_assert(circuits);
    g_assert(relays);

    GHashTable *circuit_selection = g_hash_table_new(g_direct_hash, g_direct_equal);
    GHashTable *downloads = g_hash_table_new_full(g_direct_hash, g_direct_equal, g_free, NULL);
    for(GList *iter = g_queue_peek_head_link(circuits); iter; iter = g_list_next(iter)) {
        circuit_t *circuit = iter->data;
        download_t *download = g_new0(download_t, 1);
        download->start_time = 0;
        download->end_time = 100;
        g_hash_table_insert(downloads, download, GINT_TO_POINTER(TRUE));
        g_hash_table_insert(circuit_selection, download, circuit);
    }

    gdouble bandwidth = compute_download_bandwidths(downloads, relays, circuit_selection, NULL, NULL);
    g_message("maximum bandwidth is %f", bandwidth);
}


/*
 * Main
 */

int main(int argc, char **argv) {
    GError *error = NULL;
    GOptionContext *context = NULL;

    context = g_option_context_new("<downloads.txt> <relays.txt> <genetic|greedy|maxbw|dwc>");
    g_option_context_set_summary(context, "Tor circuit selection simulator");

    gboolean pruned_circuits = FALSE;
    gchar *circuits_filename = NULL;
    gchar *output_directory = NULL;
    gchar *log_level = NULL;

    GOptionGroup *mainGroup = g_option_group_new("main", "Main Options", "Primary simulator options", NULL, NULL);
    const GOptionEntry mainEntries[] =  
    {
        { "circuits", 'c', 0, G_OPTION_ARG_FILENAME, &circuits_filename, 
            "List of circuits to consider.  If none provided full circuit list is generated and used.", "FILENAME"},
        { "pruned", 'p', 0, G_OPTION_ARG_NONE, &pruned_circuits,
            "Use pruned set of circuits instead of all possible combinations", NULL},
        { "output", 'o', 0, G_OPTION_ARG_STRING, &output_directory, 
            "Output where any circuits generated will be saved [circuits]", "DIRECTORY"},
        { "log", 'l', 0, G_OPTION_ARG_STRING, &log_level, 
            "Log level to print out messages ('debug', 'info', 'message', 'warning', 'error') ['message']", "LOGLEVEL"},
        { NULL }
    };
    g_option_group_add_entries(mainGroup, mainEntries);
    g_option_context_set_main_group(context, mainGroup);

    gint population_size = 100;
    gboolean initial_unweighted = FALSE;
    gdouble breed_percentile = 0.2;
    gboolean breed_unweighted = FALSE;
    gdouble elite_percentile = 0.1;
    gdouble mutate_probability = 0.01;
    gint nthreads = 4;

    GOptionGroup *geneticGroup = g_option_group_new("genetic", "Genetic Algorithm Options", "Genetic algorithm parameters", NULL, NULL);
    const GOptionEntry geneticEntries[] =  
    {
        { "population", 's', 0, G_OPTION_ARG_INT, &population_size, 
            "Size of population running on [100]", "N"},
        { "initial-unweighted", 0, 0, G_OPTION_ARG_NONE, &initial_unweighted,
            "Initial circuits selected uniformly at random instead of weighted by their bandwidth", NULL},
        { "breed-percentile", 'b', 0, G_OPTION_ARG_DOUBLE, &breed_percentile, 
            "Top percent of population to draw from when breeding [0.2]", "f"},
        { "breed-unweighted", 0, 0, G_OPTION_ARG_NONE, &breed_unweighted,
            "Breed parents selected uniformly at random instead of weighted by their bandwidth", NULL},
        { "elite-percentile", 'b', 0, G_OPTION_ARG_DOUBLE, &elite_percentile, 
            "Top percent of parents to keep in new population [0.1]", "f"},
        { "mutate", 'm', 0, G_OPTION_ARG_DOUBLE, &mutate_probability, 
            "Probability of mutating any single download [0.01]", "f"},
        { "threads", 't', 0, G_OPTION_ARG_INT, &nthreads, 
            "Number of threads to use for calculating population bandwidth [4]", "N"},
        { NULL }
    };
    g_option_group_add_entries(geneticGroup, geneticEntries);
    g_option_context_add_group(context, geneticGroup);

    gchar *greedy_selection = NULL;

    GOptionGroup *greedyGroup = g_option_group_new("greedy", "Greedy Algorithm Options", "Greedy algorithm parameters", NULL, NULL);
    const GOptionEntry greedyEntries[] =  
    {
        { "selection", 's', 0, G_OPTION_ARG_STRING, &population_size, "Selection stategy used during greedy algorithm ('inorder', 'longest', 'shortest') ['inorder']", "SELECTION"},
        { NULL }
    };
    g_option_group_add_entries(greedyGroup, greedyEntries);
    g_option_context_add_group(context, greedyGroup);

    /* parse options */
    if(!g_option_context_parse(context, &argc, &argv, &error)) {
        g_printerr("** %s **\n", error->message);
        gchar *helpString = g_option_context_get_help(context, TRUE, NULL);
        g_printerr("%s", helpString);
        g_free(helpString);
        return 0;
    }

    if(argc < 3) {
        g_printerr("** Please provide the required parameters **\n");
        gchar *helpString = g_option_context_get_help(context, TRUE, NULL);
        g_printerr("%s", helpString);
        g_free(helpString);
        return 0;
    }

    /* set defaults */
    if(!output_directory) {
        output_directory = g_strdup("circuits");
    }
    if(!log_level) {
        log_level = g_strdup("message");
    }
    if(!greedy_selection) {
        greedy_selection = g_strdup("inorder");
    }

    if(!g_ascii_strcasecmp(log_level, "debug")) {
        min_log_level = G_LOG_LEVEL_DEBUG;
    } else if(!g_ascii_strcasecmp(log_level, "info")) {
        min_log_level = G_LOG_LEVEL_INFO;
    } else if(!g_ascii_strcasecmp(log_level, "message")) {
        min_log_level = G_LOG_LEVEL_MESSAGE;
    } else if(!g_ascii_strcasecmp(log_level, "warning")) {
        min_log_level = G_LOG_LEVEL_WARNING;
    } else if(!g_ascii_strcasecmp(log_level, "error")) {
        min_log_level = G_LOG_LEVEL_ERROR;
    } 

    g_log_set_handler(NULL, G_LOG_LEVEL_MASK | G_LOG_FLAG_FATAL | G_LOG_FLAG_RECURSION, log_handler_cb, NULL);

    g_message("Reading list of downloads");
    GHashTable *client_downloads = read_downloads(argv[1]);
    if(!client_downloads) {
        g_error("could not read in download list");
        return -1;
    }
    GQueue *downloads = get_all_downloads(client_downloads);

    g_message("Reading list of relays");
    GHashTable *relays = read_relays(argv[2]);
    if(!relays) {
        g_error("could not read in relay list");
        return -1;
    }

    GQueue *circuits = NULL;
    circuit_t **circuit_list = NULL;
    circuit_t **weighted_circuit_list;
    gint total_circuit_bandwidth;

    if(circuits_filename) {
        g_message("Reading list of circuits");
        circuits = read_circuits(circuits_filename, client_downloads);
    } else if(pruned_circuits) {
        g_message("Building set of pruned circuits");
        circuits = build_pruned_circuits(relays);
    } else {
        g_message("Building list of all potential circuits");
        circuits = build_all_circuits(relays);
    }
    generate_circuit_lists(circuits, &circuit_list, &weighted_circuit_list,
            &total_circuit_bandwidth);

    /* go through the downloads, any one that has no circuits assigned use global list */
    for(GList *iter = g_queue_peek_head_link(downloads); iter; iter = g_list_next(iter)) {
        download_t *download = iter->data;
        if(!download->circuits) {
            download->circuits = circuits;
            download->circuit_list = circuit_list;
            download->weighted_circuit_list = weighted_circuit_list;
            download->total_circuit_bandwidth = total_circuit_bandwidth;
        } else {
            generate_circuit_lists(download->circuits, &download->circuit_list, 
                    &download->weighted_circuit_list, &download->total_circuit_bandwidth);
        }
    }


    /* create the output directory */
    if(!g_file_test(output_directory, (G_FILE_TEST_EXISTS | G_FILE_TEST_IS_DIR))) {
        if(g_mkdir_with_parents(output_directory, 0777) < -1) {
            g_critical("cannot create circuits directory");
            return -1;
        }
    }

    gint ndownloads = g_queue_get_length(downloads);
    gint nrelays = g_hash_table_size(relays);
    gint ncircuits = g_queue_get_length(circuits);

    g_message("There are %d downloads, %d relays, and %d circuits", ndownloads, nrelays, ncircuits);

    g_message("Running simulator in '%s' mode", argv[3]);

    GHashTable *circuit_selection = NULL;

    if(!g_ascii_strcasecmp(argv[3], "genetic")) {
        run_genetic_algorithm(downloads, relays, population_size, !initial_unweighted,
                breed_percentile, !breed_unweighted, elite_percentile, mutate_probability, nthreads);
    } else if(!g_ascii_strcasecmp(argv[3], "greedy")) {
        run_greedy_algorithm(downloads, relays, greedy_selection);
    } else if(!g_ascii_strcasecmp(argv[3], "maxbw")) {
        estimate_max_bandwidth(circuits, relays);
    } else if(!g_ascii_strcasecmp(argv[3], "dwc")) {
        circuit_selection = run_dwc_algorithm(downloads, relays, nthreads);
    } else {
        g_error("Did not recognize mode '%s'", argv[3]);
    }

    if(circuit_selection) {
        GHashTable *downloads_by_client = g_hash_table_new(g_str_hash, g_str_equal);
        for(GList *iter = g_queue_peek_head_link(downloads); iter; iter = g_list_next(iter)) {
            download_t *download = iter->data;
            GList *download_list = g_hash_table_lookup(downloads_by_client, download->client);
            download_list = g_list_append(download_list, download);
            g_hash_table_insert(downloads_by_client, download->client, download_list);
        }

        GHashTableIter iter;
        gpointer key,value;
        g_hash_table_iter_init(&iter, downloads_by_client);
        while(g_hash_table_iter_next(&iter, &key, &value)) {
            gchar *client = (gchar *)key;
            GList *download_list = (GList *)value;

            GString *buffer = g_string_new("");
            for(GList *iter = download_list; iter; iter = g_list_next(iter)) {
                download_t *download = iter->data;
                circuit_t *circuit = g_hash_table_lookup(circuit_selection, download);
                if(!circuit) {
                    g_warning("no circuit selected for download %s at time %f", client, download->start_time / 1000.0);
                } else {
                    g_string_append_printf(buffer, "%f %s,%s,%s\n", download->start_time / 1000.0,
                            circuit->guard, circuit->middle, circuit->exit);
                }
            }


            GString *filename = g_string_new("");
            g_string_printf(filename, "%s/%s.txt", output_directory, client);
            g_file_set_contents(filename->str, buffer->str, -1, NULL);

            g_string_free(filename, TRUE);
            g_string_free(buffer, TRUE);

        }

        g_hash_table_destroy(downloads_by_client);
        g_hash_table_destroy(circuit_selection);
    }

    g_free(output_directory);
    g_free(log_level);
    g_free(greedy_selection);

    g_queue_free_full(downloads, (GDestroyNotify)free_download);
    g_queue_free_full(circuits, g_free);
    g_hash_table_destroy(relays);

    return 0;
}
