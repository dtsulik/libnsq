#include "nsq.h"
#include <pthread.h>

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

#define NSQ_HOST "127.0.0.1"
#define NSQ_HOST2 "10.10.134.237"
#define NS 1000000000

int sent_counter = 0;
int rcv_counter = 0;

static void message_handler(struct NSQReader *rdr, struct NSQDConnection *conn, struct NSQMessage *msg, void *ctx)
{
    _DEBUG("%s: %ld, %d, %s, %lu, %.*s\n", __FUNCTION__, msg->timestamp, msg->attempts, msg->id,
        msg->body_length, (int)msg->body_length, msg->body);
    int ret = 0;

    buffer_reset(conn->command_buf);

    if(ret < 0){
        nsq_requeue(conn->command_buf, msg->id, 100);
    }else{
        nsq_finish(conn->command_buf, msg->id);
    }
    buffered_socket_write_buffer(conn->bs, conn->command_buf);

    buffer_reset(conn->command_buf);
    nsq_ready(conn->command_buf, 50);
    buffered_socket_write_buffer(conn->bs, conn->command_buf);
    rcv_counter++;
    printf("recvd %d\n", rcv_counter);

    free_nsq_message(msg);
}

static void pub_error_handler(struct NSQDUnbufferedCon *ucon, void *arg)
{
    _DEBUG("%s: handle this %s\n", __FUNCTION__, (char *)arg);

}

static void pub_conn_handler(struct NSQDUnbufferedCon *ucon, void *arg)
{
    _DEBUG("%s: handle this %s\n", __FUNCTION__, (char *)arg);
}

void *writer(void *p){
    struct NSQDUnbufferedCon *primary = nsq_new_unbuffered_pub(NSQ_HOST2, 4150,
        pub_conn_handler, pub_error_handler, NSQ_HOST2);

    struct NSQDUnbufferedCon *secondary = nsq_new_unbuffered_pub(NSQ_HOST, 4150,
        pub_conn_handler, pub_error_handler, NSQ_HOST);

    int i;
    for(i = 0; i < 10000; i++){
        nsq_upub(primary, secondary, "test2", "pingpong", 8);
        sleep(10);
    }

    return NULL;
}

void *reader(void *p){
    char *host_str = (char*)p;
    struct NSQReader *rdr;

    struct ev_loop *loop = ev_loop_new(0);

    // reader
    rdr = new_nsq_reader(loop, "test2", "ch", NULL,
        NULL, NULL, NULL, message_handler);

    rdr->max_in_flight = 50;
    nsq_reader_connect_to_nsqd(rdr, host_str, 4150);
    // ev loop run
    nsq_run(loop);
    return NULL;
}

int main(int argc, char **argv)
{
// buffered
    struct NSQReader *rdr;
    struct ev_loop *loop;
    void *ctx = NULL; //(void *)(new TestNsqMsgContext());

    loop = ev_default_loop(0);
    rdr = new_nsq_reader(loop, "test", "ch", (void *)ctx,
        NULL, NULL, NULL, message_handler);

    nsq_reader_connect_to_nsqd(rdr, "127.0.0.1", 4150);
    nsq_run(loop);

// unbuffered 

    // pthread_t t;
    // pthread_attr_t t_attr;
    // pthread_attr_init(&t_attr);
    // pthread_attr_setdetachstate(&t_attr, PTHREAD_CREATE_DETACHED);

    // pthread_create(&t, &t_attr, writer, NSQ_HOST);
    // pthread_create(&t, &t_attr, writer, NSQ_HOST2);

    // pthread_create(&t, &t_attr, reader, NSQ_HOST);
    // pthread_create(&t, &t_attr, reader, NSQ_HOST2);

    // while(1){
    //     sleep(1);
    // }

    return 0;
}
