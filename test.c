#include "nsq.h"
#include <pthread.h>

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

#define NSQ_HOST "10.10.134.124"

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
    nsq_ready(conn->command_buf, 5);
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
    printf("hello from writer thread\n");
    struct NSQDUnbufferedCon *ucon = nsq_new_unbuffered_pub(NSQ_HOST, 4150);

    printf("conn %p now sleeping\n", ucon);
    sleep(4);
    if(ucon == NULL){
        return NULL;
    }

    int rc = nsq_unbuffered_publish(ucon, "test2", "asdftest", 8, 5);
    printf("writer test thr rc %d\n", rc);

    return NULL;
}

int main(int argc, char **argv)
{
    struct NSQPublisher *pub;
    struct NSQReader *rdr;
    struct NSQDConnection *conn = NULL;
    struct ev_loop *loop;
    void *ctx = NULL;

    pthread_t t;
    pthread_attr_t t_attr;
    pthread_attr_init(&t_attr);
    pthread_attr_setdetachstate(&t_attr, PTHREAD_CREATE_DETACHED);

    // // ev loop init
    // loop = ev_default_loop(0);

    // // reader
    // rdr = new_nsq_reader(loop, "test2", "ch", (void *)ctx,
    //     NULL, NULL, NULL, message_handler);

    // rdr->max_in_flight = 5;
    // nsq_reader_connect_to_nsqd(rdr, NSQ_HOST, 4150);

    // publisher
    // pub = new_nsq_publisher(loop, "test2", "ch", (void *)ctx,
    //     NULL, NULL, NULL, pub_success_handler, pub_error_handler, response_handler);

    // nsq_publisher_connect_to_nsqd(pub, NSQ_HOST, 4150, &conn);

    // direct pub thread
    pthread_create(&t, &t_attr, writer, NULL);

    // ev loop run
    // nsq_run(loop);

    while(1){
        sleep(1);
    }

    return 0;
}
