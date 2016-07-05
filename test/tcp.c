//
// Created by vic on 16-6-26.
//


#include <uv.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#define uv_fatal(e) { \
        assert(0 != e); \
        fprintf(stderr, "%s:%d - err:%s: %s\n", \
                __FILE__, __LINE__, uv_err_name((e)), uv_strerror((e))); \
        exit(1); }

uv_loop_t *loop;

struct sockaddr_in addr;


void alloc_buffer(uv_handle_t *handle,size_t sug_size,uv_buf_t *buf)
{
    buf->base = (char *)malloc(sug_size);
    buf->len = sug_size;
}

void on_read(uv_stream_t *client,ssize_t nread,const uv_buf_t *buf)
{

    if (0 > nread){
        if (nread != UV_EOF)
            fprintf(stderr,"Read error %s\n",uv_err_name(nread));
        uv_close((uv_handle_t*)client,NULL);
    } else{
        uv_buf_t wrbuf = uv_buf_init(buf->base,nread);
        int e = uv_try_write(client,&wrbuf,1);
        if (e < 0)
            uv_fatal(e);
    }
    if (buf->base)
        free(buf->base);
}

void on_new_connection(uv_stream_t* server,int status)
{
    if (0 > status){
        fprintf(stderr,"New connection error %s \n",uv_strerror(status));
        return;
    }

    fprintf(stdout,"connection status %d\n",status);

    uv_tcp_t *client = (uv_tcp_t *)malloc(sizeof(uv_tcp_t));

    uv_tcp_init(loop,client);

    if (uv_accept(server,(uv_stream_t *)client) == 0){
        uv_read_start((uv_stream_t*)client,alloc_buffer,on_read);
    } else{
        uv_close((uv_handle_t*)client,NULL);
        free(client);
    }
}


int main()
{
    loop = uv_loop_new();
    uv_tcp_t server;

    uv_tcp_init(loop,&server);


    uv_ip4_addr("0.0.0.0",7001,&addr);

    uv_tcp_bind(&server,(const struct sockaddr*)&addr,0);
    int r = uv_listen((uv_stream_t *)&server,128,on_new_connection);

    if (r){
        fprintf(stderr,"Listen error %s\n",uv_strerror(r));
        return 1;
    }
    return uv_run(loop,UV_RUN_DEFAULT);
}