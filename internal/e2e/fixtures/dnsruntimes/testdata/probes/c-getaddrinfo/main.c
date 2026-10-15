// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

static long long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000LL + (long long)(ts.tv_nsec / 1000000LL);
}

static const char *eai_code_name(int code) {
    switch (code) {
        case EAI_AGAIN:
            return "EAI_AGAIN";
        case EAI_BADFLAGS:
            return "EAI_BADFLAGS";
        case EAI_FAIL:
            return "EAI_FAIL";
        case EAI_FAMILY:
            return "EAI_FAMILY";
        case EAI_MEMORY:
            return "EAI_MEMORY";
        case EAI_NONAME:
            return "EAI_NONAME";
        case EAI_SERVICE:
            return "EAI_SERVICE";
        case EAI_SOCKTYPE:
            return "EAI_SOCKTYPE";
        case EAI_SYSTEM:
            return "EAI_SYSTEM";
#ifdef EAI_NODATA
#if EAI_NODATA != EAI_NONAME
        case EAI_NODATA:
            return "EAI_NODATA";
#endif
#endif
        default:
            return "EAI_UNKNOWN";
    }
}

// Minimal JSON string field extractor for flat JSON objects.
static void extract_json_string(const char *json, const char *key, char *out, size_t out_len) {
    out[0] = '\0';
    char pattern[128];
    snprintf(pattern, sizeof(pattern), "\"%s\"", key);
    const char *p = strstr(json, pattern);
    if (!p) {
        return;
    }
    p += strlen(pattern);
    while (*p == ' ' || *p == '\t' || *p == ':') {
        p++;
    }
    if (*p != '"') {
        return;
    }
    p++;
    size_t i = 0;
    while (*p && *p != '"' && i + 1 < out_len) {
        if (*p == '\\' && *(p + 1)) {
            p++;
        }
        out[i++] = *p++;
    }
    out[i] = '\0';
}

static int extract_json_int(const char *json, const char *key, int default_val) {
    char pattern[128];
    snprintf(pattern, sizeof(pattern), "\"%s\"", key);
    const char *p = strstr(json, pattern);
    if (!p) {
        return default_val;
    }
    p += strlen(pattern);
    while (*p == ' ' || *p == '\t' || *p == ':') {
        p++;
    }
    if (*p < '0' || *p > '9') {
        return default_val;
    }
    return atoi(p);
}

static void print_json_escaped(const char *s) {
    putchar('"');
    for (; *s; s++) {
        if (*s == '"' || *s == '\\') {
            putchar('\\');
            putchar(*s);
        } else if (*s == '\n') {
            fputs("\\n", stdout);
        } else if (*s == '\r') {
            fputs("\\r", stdout);
        } else {
            putchar(*s);
        }
    }
    putchar('"');
}

// Parse host, port, and path from http://<host>[:<port>]/<path>.
static void parse_http_url(const char *url, char *host, size_t host_len, char *port, size_t port_len, char *path, size_t path_len) {
    const char *p = url;
    if (strncmp(p, "http://", 7) == 0) {
        p += 7;
    }
    const char *slash = strchr(p, '/');
    const char *colon = strchr(p, ':');
    if (colon && (!slash || colon < slash)) {
        size_t hlen = (size_t)(colon - p);
        if (hlen >= host_len) hlen = host_len - 1;
        memcpy(host, p, hlen);
        host[hlen] = '\0';

        const char *port_start = colon + 1;
        size_t plen = slash ? (size_t)(slash - port_start) : strlen(port_start);
        if (plen >= port_len) plen = port_len - 1;
        memcpy(port, port_start, plen);
        port[plen] = '\0';
    } else {
        size_t hlen = slash ? (size_t)(slash - p) : strlen(p);
        if (hlen >= host_len) hlen = host_len - 1;
        memcpy(host, p, hlen);
        host[hlen] = '\0';
        snprintf(port, port_len, "80");
    }
    if (slash) {
        snprintf(path, path_len, "%s", slash);
    } else {
        snprintf(path, path_len, "/");
    }
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <json-request>\n", argv[0]);
        return 1;
    }
    const char *req = argv[1];
    char op[64];
    char host[256];
    char url[512];
    extract_json_string(req, "op", op, sizeof(op));
    extract_json_string(req, "host", host, sizeof(host));
    extract_json_string(req, "url", url, sizeof(url));
    int attempts = extract_json_int(req, "attempts", 1);
    int interval_ms = extract_json_int(req, "intervalMs", 0);
    if (attempts <= 0) {
        attempts = 1;
    }

    fputs("{\"attempts\":[", stdout);
    for (int i = 0; i < attempts; i++) {
        if (i > 0) {
            putchar(',');
            if (interval_ms > 0) {
                struct timespec req_ts;
                req_ts.tv_sec = interval_ms / 1000;
                req_ts.tv_nsec = (long)(interval_ms % 1000) * 1000000L;
                nanosleep(&req_ts, NULL);
            }
        }
        long long start = now_ms();
        if (strcmp(op, "http_get") == 0) {
            char uhost[256], uport[32], upath[256];
            parse_http_url(url, uhost, sizeof(uhost), uport, sizeof(uport), upath, sizeof(upath));

            struct addrinfo hints;
            memset(&hints, 0, sizeof(hints));
            hints.ai_family = AF_INET;
            hints.ai_socktype = SOCK_STREAM;

            struct addrinfo *res = NULL;
            int rc = getaddrinfo(uhost, uport, &hints, &res);
            long long dur = now_ms() - start;
            if (rc != 0) {
                fputs("{\"success\":false,\"errorCode\":", stdout);
                print_json_escaped(eai_code_name(rc));
                fputs(",\"error\":", stdout);
                char msg[512];
                snprintf(msg, sizeof(msg), "%s: %s", eai_code_name(rc), gai_strerror(rc));
                print_json_escaped(msg);
                printf(",\"durationMs\":%lld}", dur);
                continue;
            }

            int sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
            if (sock < 0) {
                freeaddrinfo(res);
                fputs("{\"success\":false,\"errorCode\":\"SOCKET_ERR\",\"error\":\"socket failed\"", stdout);
                printf(",\"durationMs\":%lld}", now_ms() - start);
                continue;
            }
            if (connect(sock, res->ai_addr, res->ai_addrlen) < 0) {
                int err = errno;
                close(sock);
                freeaddrinfo(res);
                fputs("{\"success\":false,\"errorCode\":\"CONNECT_ERR\",\"error\":", stdout);
                print_json_escaped(strerror(err));
                printf(",\"durationMs\":%lld}", now_ms() - start);
                continue;
            }
            freeaddrinfo(res);

            char http_req[1024];
            int req_len = snprintf(http_req, sizeof(http_req),
                                   "GET %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n",
                                   upath, uhost);
            (void)write(sock, http_req, (size_t)req_len);
            char resp_buf[512];
            ssize_t n = read(sock, resp_buf, sizeof(resp_buf) - 1);
            close(sock);
            dur = now_ms() - start;
            int status_code = 0;
            if (n > 0) {
                resp_buf[n] = '\0';
                const char *sp = strchr(resp_buf, ' ');
                if (sp) {
                    status_code = atoi(sp + 1);
                }
            }
            if (status_code >= 200 && status_code < 400) {
                printf("{\"success\":true,\"statusCode\":%d,\"durationMs\":%lld}", status_code, dur);
            } else {
                printf("{\"success\":false,\"statusCode\":%d,\"error\":\"http status %d\",\"durationMs\":%lld}",
                       status_code, status_code, dur);
            }
        } else {
            struct addrinfo hints;
            memset(&hints, 0, sizeof(hints));
            hints.ai_family = AF_INET;
            hints.ai_socktype = SOCK_STREAM;

            struct addrinfo *res = NULL;
            int rc = getaddrinfo(host, NULL, &hints, &res);
            long long dur = now_ms() - start;
            if (rc != 0) {
                fputs("{\"success\":false,\"errorCode\":", stdout);
                print_json_escaped(eai_code_name(rc));
                fputs(",\"error\":", stdout);
                char msg[512];
                snprintf(msg, sizeof(msg), "%s: %s", eai_code_name(rc), gai_strerror(rc));
                print_json_escaped(msg);
                printf(",\"durationMs\":%lld}", dur);
            } else {
                fputs("{\"success\":true,\"addresses\":[", stdout);
                int first = 1;
                for (struct addrinfo *p = res; p != NULL; p = p->ai_next) {
                    if (p->ai_family == AF_INET) {
                        struct sockaddr_in * ipv4 = (struct sockaddr_in *)p->ai_addr;
                        char ipstr[INET_ADDRSTRLEN];
                        inet_ntop(AF_INET, &(ipv4->sin_addr), ipstr, sizeof(ipstr));
                        if (!first) putchar(',');
                        first = 0;
                        print_json_escaped(ipstr);
                    }
                }
                freeaddrinfo(res);
                printf("],\"durationMs\":%lld}", dur);
            }
        }
    }
    fputs("]}\n", stdout);
    return 0;
}
