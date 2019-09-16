#include "nsq.h"
#include <pthread.h>

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

int sent_counter = 0;
int rcv_counter = 0;

static void message_handler(struct NSQReader *rdr, struct NSQDConnection *conn, struct NSQMessage *msg, void *ctx)
{
    _DEBUG("%s: %lld, %d, %s, %lu, %.*s\n", __FUNCTION__, msg->timestamp, msg->attempts, msg->id,
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
    nsq_ready(conn->command_buf, rdr->max_in_flight);
    buffered_socket_write_buffer(conn->bs, conn->command_buf);
    rcv_counter++;
    printf("recvd %d\n", rcv_counter);

    free_nsq_message(msg);
}

static void response_handler(struct NSQPublisher *pub, struct NSQDConnection *conn, struct NSQMessage *msg, void *ctx)
{
    _DEBUG("%s: %lld, %d, %s, %lu, %.*s\n", __FUNCTION__, msg->timestamp, msg->attempts, msg->id,
        msg->body_length, (int)msg->body_length, msg->body);

    free_nsq_message(msg);
}

static void pub_error_handler(struct NSQPublisher *pub, struct NSQDConnection *conn, void *arg)
{
    _DEBUG("%s: handle this\n", __FUNCTION__);

}

static void pub_success_handler(struct NSQPublisher *pub, struct NSQDConnection *conn, void *arg)
{
    _DEBUG("%s: handle this\n", __FUNCTION__);

}

void *writer(void *p){
    struct NSQPublisher *pub = (struct NSQPublisher *) p;
    struct NSQDConnection *conn;

    sleep(4);
    struct timespec spc;
    spc.tv_sec = 0;
    spc.tv_nsec = 500000000;

    printf("hello from writer\n");

    LL_FOREACH(pub->conns, conn) {
        printf("%p\n", conn);
        if(conn){
        	nsq_unbuffered_publish(conn, "test2", "asdftest", 8, 0);
        }
        break;
    }

    return NULL;
}

#define NSQ_HOST "192.168.122.250"

int main(int argc, char **argv)
{
    struct NSQPublisher *pub;
    struct NSQPublisher *rdr;
    struct ev_loop *loop;
    void *ctx = NULL;

    loop = ev_default_loop(0);

    rdr = new_nsq_reader(loop, "test2", "ch", (void *)ctx,
        NULL, NULL, NULL, message_handler);

    nsq_reader_connect_to_nsqd(rdr, NSQ_HOST, 4150);

    pub = new_nsq_publisher(loop, "test2", "ch", (void *)ctx,
        NULL, NULL, NULL, pub_success_handler, pub_error_handler, response_handler);

    nsq_publisher_connect_to_nsqd(pub, NSQ_HOST, 4150);

    pthread_t t;
    pthread_attr_t t_attr;
    pthread_attr_init(&t_attr);
    pthread_attr_setdetachstate(&t_attr, PTHREAD_CREATE_DETACHED);
    pthread_create(&t, &t_attr, writer, pub);

    nsq_run(loop);

    return 0;
}
