#include "nsq.h"
#include <pthread.h>

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

#define NSQ_LOCAL "127.0.0.1"
// #define NSQ_HOST "10.10.134.124"
#define NSQ_HOST "1.1.1.1"
#define NSQ_HOST2 "10.10.134.237"
#define NS 1000000000

int sent_counter = 0;
int rcv_counter = 0;
int erroed =0;

static void connect_callback(struct NSQPublisher *pub, struct NSQDConnection *conn){
    _DEBUG("%s: handler\n", __FUNCTION__);
}
static void close_callback(struct NSQPublisher *pub, struct NSQDConnection *conn){
    _DEBUG("%s: handler\n", __FUNCTION__);
}
static void success_callback(struct NSQPublisher *pub, struct NSQDConnection *conn, void *arg){
    _DEBUG("%s: handler\n", __FUNCTION__);
}
static void error_callback(struct NSQPublisher *pub, struct NSQDConnection *conn, void *arg){
    _DEBUG("%s: handler\n", __FUNCTION__);
    erroed++;
}
static void async_write_callback(struct BufferedSocket *bs, void *arg){
    struct NSQDConnection *conn = (struct NSQDConnection *)arg;
    struct NSQPublisher *pub = conn->arg;

    struct Buffer *buf;    
    buf = new_buffer(256, 1024);

    char b[1024];
    size_t n;
    n = sprintf(b, "GET / HTTP/1.0\r\n\r\n");

    buffer_reset(buf);
    buffer_add(buf, b, n);
    buffered_socket_write_buffer(bs, buf);

    // buffer_reset(conn->command_buf);
    // nsq_pub(conn->command_buf, pub->topic, msg, size);

    printf("%s: cbarg %s\n", __FUNCTION__, (char *)pub->ctx);
}
static void msg_callback(struct NSQPublisher *pub, struct NSQDConnection *conn, struct NSQMessage *msg, void *ctx){
    _DEBUG("%s: handler\n", __FUNCTION__);
}

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
    _DEBUG("%s: handle this %s; errno %d\n", __FUNCTION__, (char *)arg, errno);
    erroed++;
    // exit(1);
}

static void pub_conn_handler(struct NSQDUnbufferedCon *ucon, void *arg)
{
    _DEBUG("%s: handle this %s\n", __FUNCTION__, (char *)arg);
}

void *writer(void *p){
    struct NSQDUnbufferedCon *primary = nsq_new_unbuffered_pub(NSQ_HOST, 4150,
        pub_conn_handler, pub_error_handler, NSQ_HOST, 1.);

    printf("connected ? %p %s", primary, NSQ_HOST);

    // struct NSQDUnbufferedCon *secondary = nsq_new_unbuffered_pub(NSQ_LOCAL, 4150,
    //     pub_conn_handler, pub_error_handler, NSQ_HOST2);
    int i;
    struct timespec s;
    s.tv_sec = 0;
    s.tv_nsec = 1000000;
    for(i = 0; i < 10000; i++){
        int rc = nsq_upub(primary, NULL, "spam", "pingpong", 8);
        // printf("rc = %d\n", rc);
        nanosleep(&s, NULL);
    }

    printf("done %d\n", erroed);
    free_unbuffered_pub(primary);
    return NULL;
}

struct NSQReader *rdrglob = NULL;
void *reader(void *p){
    sleep(5);
    printf("breaking loop\n");
    free_nsq_reader(rdrglob);
    return NULL;
}

void *puber(void *p){
    struct NSQPublisher **pub = (struct NSQPublisher **)p;

    struct ev_loop *loop = ev_loop_new(0);

    // reader
    *pub = new_nsq_publisher(loop, "spam", "ch", NULL,
        NULL, connect_callback, close_callback, success_callback,
        error_callback, async_write_callback, msg_callback);

    struct NSQPublisher *pub_p = *pub;
    char userdata[] = {"userdata"};

    pub_p->max_in_flight = 50;
    pub_p->ctx = userdata;

    nsq_publisher_connect_to_nsqd(*pub, NSQ_HOST, 4150);
    // nsq_publisher_connect_to_nsqd(*pub, NSQ_HOST2, 4150);
    // ev loop run
    nsq_run(loop);
    return NULL;
}

int main(int argc, char **argv)
{
    pthread_t t;
    pthread_attr_t t_attr;
    pthread_attr_init(&t_attr);
    pthread_attr_setdetachstate(&t_attr, PTHREAD_CREATE_DETACHED);

    // struct NSQPublisher *pub = NULL;
    // pthread_create(&t, &t_attr, puber, &pub);

    pthread_create(&t, &t_attr, writer, "1.1.1.1");
    // pthread_create(&t, &t_attr, writer, NSQ_HOST2);

    // pthread_create(&t, &t_attr, reader, NSQ_HOST);
    // pthread_create(&t, &t_attr, reader, NSQ_HOST2);

    // printf("trying to delete topic\n");
    // int rc = nsq_delete_topic(NSQ_HOST, 4151, "spam");
    // if(rc < 0){
    //     printf("error deleting topic\n");
    // }
    // printf("topic gone\n");


    // struct NSQReader *rdr;

    // struct ev_loop *loop = ev_loop_new(0);

    // // reader
    // rdr = new_nsq_reader(loop, "spam", "ch", NULL,
    //     NULL, NULL, NULL, message_handler);

    // rdrglob = rdr;
    // rdr->max_in_flight = 50;
    // char config_json[] = {"{\"output_buffer_timeout\": -1}"};
    // rdr->conn_cfg = config_json;

    // nsq_reader_connect_to_nsqd(rdr, NSQ_HOST, 4150);
    // // ev loop run
    // nsq_run(loop);
    // printf("reader done\n");
    // ev_loop_destroy(loop);


    while(1){
        sleep(1);
    }
    return 0;
}
