//
// Created by vic on 16-6-9.
//

#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include "uv.h"
int n=0;
static void timer_callback(uv_timer_t *handle)
{
    printf("%d\n",n++);
}


int main()
{
    int r;
    uv_timer_t timer;
    r = uv_timer_init(uv_default_loop(),&timer);
    assert(r == 0);

    r = uv_timer_start(&timer,timer_callback,1000,2000);
    assert(r == 0);
    uv_run(uv_default_loop(), UV_RUN_DEFAULT);
    //raft_server_t *raft = raft_new();
    return 0;
}
