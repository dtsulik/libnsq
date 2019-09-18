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
    struct NSQPublisher *pub = (struct NSQPublisher *) p;
    struct NSQDConnection *conn;

    struct timespec spc;
    spc.tv_sec = 0;
    spc.tv_nsec = 500000000;

    sleep(4);


    char **msgs = malloc(5 * sizeof(char *));
    int **sizes = malloc(5 * sizeof(int *));
    int one_size_to_rule_them_all = 4;

    char msg1[] = {"msg1"};
    char msg2[] = {"msg2"};
    char msg3[] = {"msg3"};
    char msg4[] = {"msg4"};
    char msg5[] = {"msg5"};
    msgs[0] = msg1;
    msgs[1] = msg2;
    msgs[2] = msg3;
    msgs[3] = msg4;
    msgs[4] = msg5;
    sizes[0] = &one_size_to_rule_them_all;
    sizes[1] = &one_size_to_rule_them_all;
    sizes[2] = &one_size_to_rule_them_all;
    sizes[3] = &one_size_to_rule_them_all;
    sizes[4] = &one_size_to_rule_them_all;

    printf("hello from writer\n");

    int i = 0;
    LL_FOREACH(pub->conns, conn) {
        printf("%p\n", conn);

        // for(i = 0; i < 5; i++){
        //     if(conn){
        //         nsq_unbuffered_publish(conn, "test2", "asdftest", 8, 0);
        //     }
        // }

        nsq_unbuffered_mpublish(conn, "test2", msgs, sizes, 5, 20, 0);

        break;
    }

    return NULL;
}

#define NSQ_HOST "10.10.134.124"

int main(int argc, char **argv)
{
    struct NSQPublisher *pub;
    struct NSQReader *rdr;
    struct ev_loop *loop;
    void *ctx = NULL;

    pthread_t t;
    pthread_attr_t t_attr;
    pthread_attr_init(&t_attr);
    pthread_attr_setdetachstate(&t_attr, PTHREAD_CREATE_DETACHED);


    // ev loop init
    loop = ev_default_loop(0);

    // reader
    rdr = new_nsq_reader(loop, "test2", "ch", (void *)ctx,
        NULL, NULL, NULL, message_handler);

    rdr->max_in_flight = 5;
    nsq_reader_connect_to_nsqd(rdr, NSQ_HOST, 4150);

    // publisher
    pub = new_nsq_publisher(loop, "test2", "ch", (void *)ctx,
        NULL, NULL, NULL, pub_success_handler, pub_error_handler, response_handler);

    nsq_publisher_connect_to_nsqd(pub, NSQ_HOST, 4150);

    // direct pub thread
    pthread_create(&t, &t_attr, writer, pub);

    // ev loop run
    nsq_run(loop);

    return 0;
}
