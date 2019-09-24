#include "nsq.h"
#include <pthread.h>

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

#define NSQ_HOST "10.10.134.127"
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

static void response_handler(struct NSQPublisher *pub, struct NSQDConnection *conn, struct NSQMessage *msg, void *ctx)
{
    _DEBUG("%s: %ld, %d, %s, %lu, %.*s\n", __FUNCTION__, msg->timestamp, msg->attempts, msg->id,
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

void *publisher(void *p){
    struct NSQDUnbufferedCon *ucon = nsq_new_unbuffered_pub(NSQ_HOST, 4150);

    if(ucon == NULL){
        return NULL;
    }

    int i;

    struct timespec cpu_time_1;
    struct timespec real_time_1;
    struct timespec cpu_time_2;
    struct timespec real_time_2;

    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpu_time_1);
    clock_gettime(CLOCK_REALTIME, &real_time_1);

    for(i = 0; i< 10000; i++){
        nsq_unbuffered_publish(ucon, "test2", "asdftest", 8, 5, 0);
    }

    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpu_time_2);
    clock_gettime(CLOCK_REALTIME, &real_time_2);

    long long cpu_nanosec_1 = (cpu_time_1.tv_sec * NS) + cpu_time_1.tv_nsec;
    long long cpu_nanosec_2 = (cpu_time_2.tv_sec * NS) + cpu_time_2.tv_nsec;
    long long real_nanosec_1 = (real_time_1.tv_sec * NS) + real_time_1.tv_nsec;
    long long real_nanosec_2 = (real_time_2.tv_sec * NS) + real_time_2.tv_nsec;

    long long cpu_nsec = (cpu_nanosec_2 - cpu_nanosec_1);
    long long real_nsec = (real_nanosec_2 - real_nanosec_1);

    printf("task finished in %llds.%lldns; cpu time %llds.%lldns\n",
        real_nsec/NS, real_nsec % NS, cpu_nsec/NS, cpu_nsec % NS);

    return NULL;
}

void *writer(void *p){
    struct NSQDUnbufferedCon *ucon = nsq_new_unbuffered_pub(NSQ_HOST, 4150);

    if(ucon == NULL){
        return NULL;
    }

    int i;

    struct timespec cpu_time_1;
    struct timespec real_time_1;
    struct timespec cpu_time_2;
    struct timespec real_time_2;

    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpu_time_1);
    clock_gettime(CLOCK_REALTIME, &real_time_1);

    for(i = 0; i< 100000; i++){
        nsq_unbuffered_publish(ucon, "test2", "asdftest", 8, 5, 0);
    }

    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpu_time_2);
    clock_gettime(CLOCK_REALTIME, &real_time_2);

    long long cpu_nanosec_1 = (cpu_time_1.tv_sec * NS) + cpu_time_1.tv_nsec;
    long long cpu_nanosec_2 = (cpu_time_2.tv_sec * NS) + cpu_time_2.tv_nsec;
    long long real_nanosec_1 = (real_time_1.tv_sec * NS) + real_time_1.tv_nsec;
    long long real_nanosec_2 = (real_time_2.tv_sec * NS) + real_time_2.tv_nsec;

    long long cpu_nsec = (cpu_nanosec_2 - cpu_nanosec_1);
    long long real_nsec = (real_nanosec_2 - real_nanosec_1);

    printf("task finished in %llds.%lldns; cpu time %llds.%lldns\n",
        real_nsec/NS, real_nsec % NS, cpu_nsec/NS, cpu_nsec % NS);

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

    // ev loop init
    loop = ev_default_loop(0);

    // reader
    rdr = new_nsq_reader(loop, "test2", "ch", (void *)ctx,
        NULL, NULL, NULL, message_handler);

    rdr->max_in_flight = 50;
    nsq_reader_connect_to_nsqd(rdr, NSQ_HOST, 4150);

    // publisher
    pub = new_nsq_publisher(loop, "test2", "ch", (void *)ctx,
        NULL, NULL, NULL, pub_success_handler, pub_error_handler, response_handler);

    nsq_publisher_connect_to_nsqd(pub, NSQ_HOST, 4150, &conn);

    // direct pub thread
    pthread_create(&t, &t_attr, writer, NULL);

    // ev loop run
    nsq_run(loop);

    // while(1){
    //     sleep(1);
    // }

    return 0;
}
