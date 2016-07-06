//
// Created by vic on 16-7-5.
//

#ifndef OCTEL_UV_HELP_H
#define OCTEL_UV_HELP_H

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "uv.h"

#define uv_fatal(e) { \
        assert(0 != e); \
        fprintf(stderr, "%s:%d - err:%s: %s\n", \
                __FILE__, __LINE__, uv_err_name((e)), uv_strerror((e))); \
        exit(1); }

void uv_bind_listen_socket(uv_tcp_t* listen, const char* host, const int port, uv_loop_t* loop)
{
    int e;
    e = uv_tcp_init(loop, listen);
    if (e != 0)
        uv_fatal(e);

    struct sockaddr_in addr;
    e = uv_ip4_addr(host, port, &addr);
    switch (e)
    {
        case 0:
            break;
        case EINVAL:
            fprintf(stderr, "Invalid address/port: %s %d\n", host, port);
            abort();
        default:
            uv_fatal(e);
    }

    e = uv_tcp_bind(listen, (const struct sockaddr *)&addr, 0);
    if (e != 0)
        uv_fatal(e);
}



#endif //OCTEL_UV_HELP_H
