//
// Created by vic on 16-6-9.
//

#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include "uv.h"
#include "protocol.h"
#include "uv_help.h"


#define SERVER_HOST "0.0.0.0"
#define SERVER_PORT 7001
#define BACKLOG_LEN 128


typedef struct octel_connection_s octel_connection_t;

typedef struct {

    uv_timer_t timer;
    uv_loop_t *raft_loop;
    octel_connection_t *conns;
    uv_tcp_t server;

} octel_server_t;


struct octel_connection_s {
    struct sockaddr_in addr;
    uv_stream_t *stream;
    uv_loop_t *loop;
};


octel_server_t *octelServer;

static void __raft_periodic(uv_timer_t *handle) {
    fprintf(stdout, "raft_periodic\n");
    if (octelServer->conns) {
        uv_buf_t wrbuf = uv_buf_init("ok\n", 3);
        int e = uv_try_write(octelServer->conns->stream, &wrbuf, 1);
        if (e < 0) uv_fatal(e);
    }

}


void alloc_buffer(uv_handle_t *handle, size_t sug_size, uv_buf_t *buf) {
    buf->base = (char *) malloc(sug_size);
    buf->len = sug_size;
}

void on_read(uv_stream_t *client, ssize_t nread, const uv_buf_t *buf) {

    if (0 > nread) {
        if (nread != UV_EOF)
            fprintf(stderr, "Read error %s\n", uv_err_name(nread));
        uv_close((uv_handle_t *) client, NULL);
    } else {
        uv_buf_t wrbuf = uv_buf_init(buf->base, nread);
        int e = uv_try_write(client, &wrbuf, 1);
        if (e < 0) uv_fatal(e);
        char *tmp = malloc(nread);
        memcpy(tmp, buf->base, nread);
        fprintf(stdout, "%s", tmp);
        free(tmp);
    }
    if (buf->base)
        free(buf->base);
}

void on_new_connection(uv_stream_t *server, int status) {
    if (0 > status) {
        fprintf(stderr, "New connection error %s \n", uv_strerror(status));
        return;
    }

    fprintf(stdout, "connection status %d\n", status);

    uv_tcp_t *client = (uv_tcp_t *) malloc(sizeof(uv_tcp_t));

    uv_tcp_init(server->loop, client);

    octel_connection_t *connt = calloc(1, sizeof(octel_connection_t));
    connt->loop = server->loop;
    connt->stream = (uv_stream_t *) client;
    //client->data = connt;
    octelServer->conns = connt;

    if (uv_accept(server, (uv_stream_t *) client) == 0) {
        uv_read_start((uv_stream_t *) client, alloc_buffer, on_read);
    } else {
        uv_close((uv_handle_t *) client, NULL);
        free(client);
    }
}


int main(int argc, char **argv) {
    octelServer = (octel_server_t *) malloc(sizeof(octel_server_t));
    octelServer->raft_loop = uv_loop_new();
    uv_bind_listen_socket(&(octelServer->server), SERVER_HOST, SERVER_PORT, octelServer->raft_loop);
    int e = uv_listen((uv_stream_t *) &(octelServer->server), BACKLOG_LEN, on_new_connection);
    if (0 != e) uv_fatal(e);

    return uv_run(octelServer->raft_loop, UV_RUN_DEFAULT);
}
