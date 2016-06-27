//
// Created by vic on 16-6-9.
//

#include <stdio.h>
#include <stdlib.h>
#include "uv.h"

typedef struct {
    uv_timer_t timer;
    uv_loop_t *raft_loop;
} octel_server_t;

octel_server_t *octelServer;

static void __raft_periodic(uv_timer_t *handle)
{
    fprintf(stdout,"raft_periodic\n");
}

int main(void)
{

    octelServer = (octel_server_t*)malloc(sizeof(octel_server_t));

    octelServer->raft_loop = uv_loop_new();

    uv_timer_init(octelServer->raft_loop, &octelServer->timer);

    uv_timer_start(&octelServer->timer, __raft_periodic, 0, 1000);

    return uv_run(octelServer->raft_loop, UV_RUN_DEFAULT);
}
