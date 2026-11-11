// zpico: a small zenoh-pico driver for tiny-zenoh-go's interop tests
// (zenoh/pico_test.go). It is test tooling only and is never linked into the
// Go library. It prints one JSON object per event on stdout.
//
//	zpico [-e connect]... [-l listen]... [-m client|peer] [-s scout-locator] CMD ARGS...
//
//	sub KE                 subscriber, prints samples
//	put KE PAYLOAD [ENC] [ATT]
//	del KE
//	pub KE N SIZE          declared publisher, N puts of SIZE pattern bytes (SIZE 0: "pico-<i>")
//	get SELECTOR [PAYLOAD] prints replies, then {"done":true}
//	qbl KE [REPLYKEY]      queryable; replies "pico:<params>:<payload>" (fail=1: error "nope", big=N: N pattern bytes)
//	token KE               liveliness token
//	lsub KE                liveliness subscriber with history
//	lget KE                liveliness get
//	querier KE N           declared querier, N sequential gets
//
// Long-lived commands print {"ready":true,...} and run until stdin closes.
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <zenoh-pico.h>

static pthread_mutex_t out_mu = PTHREAD_MUTEX_INITIALIZER;
static atomic_int done_flag;
static const char *reply_key;

static uint32_t fnv32(const uint8_t *p, size_t n) {
    uint32_t h = 2166136261u;
    for (size_t i = 0; i < n; i++) {
        h ^= p[i];
        h *= 16777619u;
    }
    return h;
}

static void json_str(const char *name, const uint8_t *p, size_t n) {
    printf(",\"%s\":\"", name);
    for (size_t i = 0; i < n; i++) {
        unsigned c = p[i];
        if (c == '"' || c == '\\') {
            printf("\\%c", c);
        } else if (c < 0x20 || c >= 0x7f) {
            printf("\\u%04x", c);
        } else {
            putchar((int)c);
        }
    }
    putchar('"');
}

// bytes_field prints NAME (escaped, if short) plus size and fnv32 sum.
static void bytes_field(const char *name, const z_loaned_bytes_t *b, int with_sum) {
    z_owned_slice_t sl;
    z_bytes_to_slice(b, &sl);
    const uint8_t *p = z_slice_data(z_loan(sl));
    size_t n = z_slice_len(z_loan(sl));
    if (n <= 512) {
        json_str(name, p, n);
    }
    if (with_sum) {
        printf(",\"size\":%zu,\"sum\":\"%08x\"", n, fnv32(p, n));
    }
    z_drop(z_move(sl));
}

static void print_sample(const char *tag, const z_loaned_sample_t *s) {
    z_view_string_t k;
    z_keyexpr_as_view_string(z_sample_keyexpr(s), &k);
    z_owned_string_t enc;
    z_encoding_to_string(z_sample_encoding(s), &enc);
    printf("{\"ev\":\"%s\",\"kind\":\"%s\"", tag, z_sample_kind(s) == Z_SAMPLE_KIND_PUT ? "PUT" : "DELETE");
    json_str("key", (const uint8_t *)z_string_data(z_loan(k)), z_string_len(z_loan(k)));
    json_str("encoding", (const uint8_t *)z_string_data(z_loan(enc)), z_string_len(z_loan(enc)));
    bytes_field("payload", z_sample_payload(s), 1);
    const z_loaned_bytes_t *att = z_sample_attachment(s);
    if (att != NULL && z_bytes_len(att) > 0) {
        bytes_field("attachment", att, 0);
    }
    printf("}\n");
    z_drop(z_move(enc));
}

static void on_sample(z_loaned_sample_t *s, void *ctx) {
    pthread_mutex_lock(&out_mu);
    print_sample((const char *)ctx, s);
    fflush(stdout);
    pthread_mutex_unlock(&out_mu);
}

static void on_reply(z_loaned_reply_t *r, void *ctx) {
    (void)ctx;
    pthread_mutex_lock(&out_mu);
    if (z_reply_is_ok(r)) {
        print_sample("reply", z_reply_ok(r));
    } else {
        printf("{\"ev\":\"err\"");
        bytes_field("payload", z_reply_err_payload(z_reply_err(r)), 0);
        printf("}\n");
    }
    fflush(stdout);
    pthread_mutex_unlock(&out_mu);
}

static void on_reply_drop(void *ctx) {
    (void)ctx;
    atomic_store(&done_flag, 1);
}

static void pattern(uint8_t *p, size_t n, int seed) {
    for (size_t j = 0; j < n; j++) {
        p[j] = (uint8_t)(j * 7 + (size_t)seed);
    }
}

static void on_query(z_loaned_query_t *q, void *ctx) {
    (void)ctx;
    z_view_string_t params;
    z_query_parameters(q, &params);
    const char *pd = z_string_data(z_loan(params));
    size_t pn = z_string_len(z_loan(params));
    char pstr[256];
    snprintf(pstr, sizeof pstr, "%.*s", (int)pn, pd);

    pthread_mutex_lock(&out_mu);
    z_view_string_t k;
    z_keyexpr_as_view_string(z_query_keyexpr(q), &k);
    printf("{\"ev\":\"query\"");
    json_str("key", (const uint8_t *)z_string_data(z_loan(k)), z_string_len(z_loan(k)));
    json_str("params", (const uint8_t *)pstr, strlen(pstr));
    printf("}\n");
    fflush(stdout);
    pthread_mutex_unlock(&out_mu);

    if (strstr(pstr, "fail=1") != NULL) {
        z_owned_bytes_t b;
        z_bytes_copy_from_str(&b, "nope");
        z_query_reply_err(q, z_move(b), NULL);
        return;
    }
    z_view_keyexpr_t rk;
    const z_loaned_keyexpr_t *key = z_query_keyexpr(q);
    if (reply_key != NULL) {
        z_view_keyexpr_from_str(&rk, reply_key);
        key = z_loan(rk);
    }
    z_owned_bytes_t b;
    const char *big = strstr(pstr, "big=");
    if (big != NULL) {
        size_t n = (size_t)atoi(big + 4);
        uint8_t *buf = malloc(n);
        pattern(buf, n, 0);
        z_bytes_copy_from_buf(&b, buf, n);
        free(buf);
    } else {
        char body[1024];
        int bn = snprintf(body, sizeof body, "pico:%s:", pstr);
        const z_loaned_bytes_t *qp = z_query_payload(q);
        if (qp != NULL) {
            z_owned_slice_t sl;
            z_bytes_to_slice(qp, &sl);
            size_t n = z_slice_len(z_loan(sl));
            if (n > sizeof body - (size_t)bn - 1) n = sizeof body - (size_t)bn - 1;
            memcpy(body + bn, z_slice_data(z_loan(sl)), n);
            bn += (int)n;
            z_drop(z_move(sl));
        }
        z_bytes_copy_from_buf(&b, (const uint8_t *)body, (size_t)bn);
    }
    z_query_reply_options_t opts;
    z_query_reply_options_default(&opts);
    z_owned_encoding_t enc;
    z_encoding_from_str(&enc, "text/plain");
    opts.encoding = z_move(enc);
    const z_loaned_bytes_t *qa = z_query_attachment(q);
    z_owned_bytes_t att;
    if (qa != NULL && z_bytes_len(qa) > 0) {
        z_bytes_clone(&att, qa);
        opts.attachment = z_move(att);
    }
    if (z_query_reply(q, key, z_move(b), &opts) < 0) {
        pthread_mutex_lock(&out_mu);
        printf("{\"ev\":\"reply_failed\"}\n");
        fflush(stdout);
        pthread_mutex_unlock(&out_mu);
    }
}

static void ready(const z_loaned_session_t *s) {
    z_id_t id = z_info_zid(s);
    z_owned_string_t zs;
    z_id_to_string(&id, &zs);
    pthread_mutex_lock(&out_mu);
    printf("{\"ev\":\"ready\",\"zid\":\"%.*s\"}\n", (int)z_string_len(z_loan(zs)), z_string_data(z_loan(zs)));
    fflush(stdout);
    pthread_mutex_unlock(&out_mu);
    z_drop(z_move(zs));
}

static void done(void) {
    pthread_mutex_lock(&out_mu);
    printf("{\"ev\":\"done\"}\n");
    fflush(stdout);
    pthread_mutex_unlock(&out_mu);
}

static void fail(const char *what) {
    pthread_mutex_lock(&out_mu);
    printf("{\"ev\":\"fail\",\"what\":\"%s\"}\n", what);
    fflush(stdout);
    pthread_mutex_unlock(&out_mu);
}

static void wait_stdin(void) {
    while (getchar() != EOF) {
    }
}

static void wait_done(void) {
    for (int i = 0; i < 500 && !atomic_load(&done_flag); i++) {
        usleep(10000);
    }
    atomic_store(&done_flag, 0);
}

static void split_selector(const char *sel, char *ke, size_t kn, const char **params) {
    const char *q = strchr(sel, '?');
    size_t n = q ? (size_t)(q - sel) : strlen(sel);
    if (n >= kn) n = kn - 1;
    memcpy(ke, sel, n);
    ke[n] = 0;
    *params = q ? q + 1 : "";
}

int main(int argc, char **argv) {
    z_owned_config_t config;
    z_config_default(&config);
    int opt;
    while ((opt = getopt(argc, argv, "+e:l:m:s:")) != -1) {
        switch (opt) {
            case 'e':
                zp_config_insert(z_loan_mut(config), Z_CONFIG_CONNECT_KEY, optarg);
                break;
            case 'l':
                zp_config_insert(z_loan_mut(config), Z_CONFIG_LISTEN_KEY, optarg);
                break;
            case 'm':
                zp_config_insert(z_loan_mut(config), Z_CONFIG_MODE_KEY, optarg);
                break;
            case 's':
                zp_config_insert(z_loan_mut(config), Z_CONFIG_MULTICAST_LOCATOR_KEY, optarg);
                break;
            default:
                return 2;
        }
    }
    argc -= optind;
    argv += optind;
    if (argc < 2) {
        fprintf(stderr, "usage: zpico [-e loc] [-l loc] [-m mode] CMD KE ...\n");
        return 2;
    }
    const char *cmd = argv[0];

    z_owned_session_t s;
    if (z_open(&s, z_move(config), NULL) < 0) {
        fail("open");
        return 1;
    }
    char keybuf[512];
    const char *params = "";
    split_selector(argv[1], keybuf, sizeof keybuf, &params);
    z_view_keyexpr_t ke;
    if (z_view_keyexpr_from_str(&ke, keybuf) < 0) {
        fail("keyexpr");
        return 1;
    }
    int rc = 0;

    if (strcmp(cmd, "sub") == 0) {
        z_owned_closure_sample_t cb;
        z_closure(&cb, on_sample, NULL, (void *)"sample");
        z_owned_subscriber_t sub;
        if (z_declare_subscriber(z_loan(s), &sub, z_loan(ke), z_move(cb), NULL) < 0) {
            fail("declare_subscriber");
            return 1;
        }
        ready(z_loan(s));
        wait_stdin();
        z_drop(z_move(sub));
    } else if (strcmp(cmd, "put") == 0 && argc >= 3) {
        z_put_options_t o;
        z_put_options_default(&o);
        o.congestion_control = Z_CONGESTION_CONTROL_BLOCK;  // DROP fails while pico's TX lock is busy
        z_owned_encoding_t enc;
        z_owned_bytes_t att;
        if (argc >= 4 && argv[3][0]) {
            z_encoding_from_str(&enc, argv[3]);
            o.encoding = z_move(enc);
        }
        if (argc >= 5) {
            z_bytes_copy_from_str(&att, argv[4]);
            o.attachment = z_move(att);
        }
        z_owned_bytes_t b;
        z_bytes_copy_from_str(&b, argv[2]);
        if (z_put(z_loan(s), z_loan(ke), z_move(b), &o) < 0) {
            fail("put");
            rc = 1;
        }
        done();
    } else if (strcmp(cmd, "del") == 0) {
        z_delete_options_t o;
        z_delete_options_default(&o);
        o.congestion_control = Z_CONGESTION_CONTROL_BLOCK;
        if (z_delete(z_loan(s), z_loan(ke), &o) < 0) {
            fail("delete");
            rc = 1;
        }
        done();
    } else if (strcmp(cmd, "pub") == 0 && argc >= 4) {
        int n = atoi(argv[2]);
        size_t size = (size_t)atoi(argv[3]);
        z_publisher_options_t po;
        z_publisher_options_default(&po);
        po.congestion_control = Z_CONGESTION_CONTROL_BLOCK;
        z_owned_publisher_t pub;
        if (z_declare_publisher(z_loan(s), &pub, z_loan(ke), &po) < 0) {
            fail("declare_publisher");
            return 1;
        }
        usleep(500000);  // let the publisher's write filter learn about subscribers
        ready(z_loan(s));
        uint8_t *buf = malloc(size + 32);
        for (int i = 0; i < n; i++) {
            z_owned_bytes_t b;
            if (size == 0) {
                int bn = snprintf((char *)buf, 32, "pico-%d", i);
                z_bytes_copy_from_buf(&b, buf, (size_t)bn);
            } else {
                pattern(buf, size, i);
                z_bytes_copy_from_buf(&b, buf, size);
            }
            if (z_publisher_put(z_loan(pub), z_move(b), NULL) < 0) {
                fail("publisher_put");
                rc = 1;
            }
        }
        free(buf);
        done();
        wait_stdin();
        z_drop(z_move(pub));
    } else if (strcmp(cmd, "get") == 0) {
        z_get_options_t o;
        z_get_options_default(&o);
        o.timeout_ms = 3000;
        z_owned_bytes_t b;
        if (argc >= 3) {
            z_bytes_copy_from_str(&b, argv[2]);
            o.payload = z_move(b);
        }
        z_owned_closure_reply_t cb;
        z_closure(&cb, on_reply, on_reply_drop, NULL);
        if (z_get(z_loan(s), z_loan(ke), params, z_move(cb), &o) < 0) {
            fail("get");
            rc = 1;
        }
        wait_done();
        done();
    } else if (strcmp(cmd, "qbl") == 0) {
        if (argc >= 3) reply_key = argv[2];
        z_owned_closure_query_t cb;
        z_closure(&cb, on_query, NULL, NULL);
        z_queryable_options_t o;
        z_queryable_options_default(&o);
        o.complete = true;
        z_owned_queryable_t q;
        if (z_declare_queryable(z_loan(s), &q, z_loan(ke), z_move(cb), &o) < 0) {
            fail("declare_queryable");
            return 1;
        }
        ready(z_loan(s));
        wait_stdin();
        z_drop(z_move(q));
    } else if (strcmp(cmd, "token") == 0) {
        z_owned_liveliness_token_t tok;
        if (z_liveliness_declare_token(z_loan(s), &tok, z_loan(ke), NULL) < 0) {
            fail("declare_token");
            return 1;
        }
        ready(z_loan(s));
        wait_stdin();
        z_drop(z_move(tok));
    } else if (strcmp(cmd, "lsub") == 0) {
        z_owned_closure_sample_t cb;
        z_closure(&cb, on_sample, NULL, (void *)"token");
        z_liveliness_subscriber_options_t o;
        z_liveliness_subscriber_options_default(&o);
        o.history = true;
        z_owned_subscriber_t sub;
        if (z_liveliness_declare_subscriber(z_loan(s), &sub, z_loan(ke), z_move(cb), &o) < 0) {
            fail("declare_liveliness_subscriber");
            return 1;
        }
        ready(z_loan(s));
        wait_stdin();
        z_drop(z_move(sub));
    } else if (strcmp(cmd, "lget") == 0) {
        z_owned_closure_reply_t cb;
        z_closure(&cb, on_reply, on_reply_drop, NULL);
        z_liveliness_get_options_t o;
        z_liveliness_get_options_default(&o);
        o.timeout_ms = 3000;
        if (z_liveliness_get(z_loan(s), z_loan(ke), z_move(cb), &o) < 0) {
            fail("liveliness_get");
            rc = 1;
        }
        wait_done();
        done();
    } else if (strcmp(cmd, "querier") == 0 && argc >= 3) {
        int n = atoi(argv[2]);
        z_owned_querier_t qr;
        if (z_declare_querier(z_loan(s), &qr, z_loan(ke), NULL) < 0) {
            fail("declare_querier");
            return 1;
        }
        usleep(500000);  // let the querier's write filter learn about queryables
        ready(z_loan(s));
        for (int i = 0; i < n; i++) {
            char p[32];
            snprintf(p, sizeof p, "i=%d", i);
            z_owned_closure_reply_t cb;
            z_closure(&cb, on_reply, on_reply_drop, NULL);
            if (z_querier_get(z_loan(qr), p, z_move(cb), NULL) < 0) {
                fail("querier_get");
                rc = 1;
            }
            wait_done();
        }
        done();
        z_drop(z_move(qr));
    } else {
        fprintf(stderr, "unknown command %s\n", cmd);
        rc = 2;
    }
    z_drop(z_move(s));
    return rc;
}
