//
// Created by vic on 16-6-9.
//

//#include <stdlib.h>
//#include <assert.h>
//#include <stdio.h>
//#include "uv.h"
//
//typedef struct {
//
//    uv_timer_t *timer_loop;
//
//} octel_server;
//
//int n=0;
//static void timer_callback(uv_timer_t *handle)
//{
//    printf("%d\n",n++);
//}
//
//
//int main()
//{
//    int r;
//    uv_timer_t timer;
//    r = uv_timer_init(uv_default_loop(),&timer);
//    assert(r == 0);
//
//    r = uv_timer_start(&timer,timer_callback,1000,2000);
//    assert(r == 0);
//    uv_run(uv_default_loop(), UV_RUN_DEFAULT);
//    //raft_server_t *raft = raft_new();
//    return 0;
//}
#include <stdio.h>

#include <uv.h>

uv_loop_t *loop;
uv_timer_t gc_req;
uv_timer_t fake_job_req;

void gc(uv_timer_t *handle) {
    fprintf(stderr, "Freeing unused objects\n");
}

void fake_job(uv_timer_t *handle) {
    fprintf(stdout, "Fake job done\n");
}

int main() {
    loop = uv_default_loop();

    uv_timer_init(loop, &gc_req);
    uv_unref((uv_handle_t*) &gc_req);

    uv_timer_start(&gc_req, gc, 0, 2000);

    // could actually be a TCP download or something
    uv_timer_init(loop, &fake_job_req);
    uv_timer_start(&fake_job_req, fake_job, 9000, 0);
    return uv_run(loop, UV_RUN_DEFAULT);
}
