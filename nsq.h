#ifndef __nsq_h
#define __nsq_h

#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <time.h>
#include <ev.h>
#include <evbuffsock.h>

#include "utlist.h"

#define DEFAULT_LOOKUPD_INTERVAL     5.
#define DEFAULT_COMMAND_BUF_LEN      4096
#define DEFAULT_COMMAND_BUF_CAPACITY 4096
#define DEFAULT_READ_BUF_LEN         16 * 1024
#define DEFAULT_READ_BUF_CAPACITY    16 * 1024
#define DEFAULT_WRITE_BUF_LEN        16 * 1024
#define DEFAULT_WRITE_BUF_CAPACITY   16 * 1024

typedef enum {NSQ_FRAME_TYPE_RESPONSE, NSQ_FRAME_TYPE_ERROR, NSQ_FRAME_TYPE_MESSAGE} frame_type;
struct NSQDConnection;
struct NSQMessage;

struct NSQReaderCfg {
    ev_tstamp lookupd_interval;
    size_t command_buf_len;
    size_t command_buf_capacity;
    size_t read_buf_len;
    size_t read_buf_capacity;
    size_t write_buf_len;
    size_t write_buf_capacity;
};

struct NSQPublisherCfg {
    ev_tstamp lookupd_interval;
    size_t command_buf_len;
    size_t command_buf_capacity;
    size_t read_buf_len;
    size_t read_buf_capacity;
    size_t write_buf_len;
    size_t write_buf_capacity;
};

struct NSQReader {
    char *topic;
    char *channel;
    void *ctx; //context for call back
    int max_in_flight;
    struct NSQDConnection *conns;
    struct NSQDConnInfo *infos;
    struct NSQLookupdEndpoint *lookupd;
    struct ev_timer lookupd_poll_timer;
    struct ev_loop *loop;
    struct NSQReaderCfg *cfg;
    void *httpc;
    void (*connect_callback)(struct NSQReader *rdr, struct NSQDConnection *conn);
    void (*close_callback)(struct NSQReader *rdr, struct NSQDConnection *conn);
    void (*msg_callback)(struct NSQReader *rdr, struct NSQDConnection *conn, struct NSQMessage *msg, void *ctx);
};

struct NSQPublisher {
    char *topic;
    char *channel;
    void *ctx; //context for call back
    int max_in_flight;
    struct NSQDConnection *conns;
    struct NSQDConnInfo *infos;
    struct NSQLookupdEndpoint *lookupd;
    struct ev_timer lookupd_poll_timer;
    struct ev_loop *loop;
    struct NSQPublisherCfg *cfg;
    void *httpc;
    void (*connect_callback)(struct NSQPublisher *pub, struct NSQDConnection *conn);
    void (*close_callback)(struct NSQPublisher *pub, struct NSQDConnection *conn);
    void (*msg_callback)(struct NSQPublisher *pub, struct NSQDConnection *conn, struct NSQMessage *msg, void *ctx);
};

struct NSQReader *new_nsq_reader(struct ev_loop *loop, const char *topic, const char *channel, void *ctx,
    struct NSQReaderCfg *cfg,
    void (*connect_callback)(struct NSQReader *rdr, struct NSQDConnection *conn),
    void (*close_callback)(struct NSQReader *rdr, struct NSQDConnection *conn),
    void (*msg_callback)(struct NSQReader *rdr, struct NSQDConnection *conn, struct NSQMessage *msg, void *ctx));
void free_nsq_reader(struct NSQReader *rdr);
int nsq_reader_connect_to_nsqd(struct NSQReader *rdr, const char *address, int port);
int nsq_reader_connect_to_nsqlookupd(struct NSQReader *rdr);
int nsq_reader_add_nsqlookupd_endpoint(struct NSQReader *rdr, const char *address, int port);
void nsq_reader_set_loop(struct NSQReader *rdr, struct ev_loop *loop);

struct NSQPublisher *new_nsq_publisher(struct ev_loop *loop, const char *topic, const char *channel, void *ctx,
    struct NSQPublisherCfg *cfg,
    void (*connect_callback)(struct NSQPublisher *pub, struct NSQDConnection *conn),
    void (*close_callback)(struct NSQPublisher *pub, struct NSQDConnection *conn),
    void (*msg_callback)(struct NSQPublisher *pub, struct NSQDConnection *conn, struct NSQMessage *msg, void *ctx));
void free_nsq_publisher(struct NSQPublisher *pub);
int nsq_publisher_connect_to_nsqd(struct NSQPublisher *pub, const char *address, int port);
int nsq_publisher_connect_to_nsqlookupd(struct NSQPublisher *pub);
int nsq_publisher_add_nsqlookupd_endpoint(struct NSQPublisher *pub, const char *address, int port);
void nsq_publisher_set_loop(struct NSQPublisher *pub, struct ev_loop *loop);

void nsq_run(struct ev_loop *loop);



struct NSQDConnection {
    char *address;
    int port;
    struct BufferedSocket *bs;
    struct Buffer *command_buf;
    uint32_t current_msg_size;
    uint32_t current_frame_type;
    char *current_data;
    struct ev_loop *loop;
    ev_timer *reconnect_timer;
    void (*connect_callback)(struct NSQDConnection *conn, void *arg);
    void (*close_callback)(struct NSQDConnection *conn, void *arg);
    void (*msg_callback)(struct NSQDConnection *conn, struct NSQMessage *msg, void *arg);
    void *arg;
    struct NSQDConnection *next;
};

struct NSQDConnection *new_nsqd_connection(struct ev_loop *loop, const char *address, int port,
    void (*connect_callback)(struct NSQDConnection *conn, void *arg),
    void (*close_callback)(struct NSQDConnection *conn, void *arg),
    void (*msg_callback)(struct NSQDConnection *conn, struct NSQMessage *msg, void *arg),
    void *arg);

struct NSQDConnection *new_nsqd_pub_connection(struct ev_loop *loop, const char *address, int port,
    void (*connect_callback)(struct NSQDConnection *conn, void *arg),
    void (*close_callback)(struct NSQDConnection *conn, void *arg),
    void (*msg_callback)(struct NSQDConnection *conn, struct NSQMessage *msg, void *arg),
    struct NSQReaderCfg *cfg,
    void *arg);

void free_nsqd_connection(struct NSQDConnection *conn);
int nsqd_connection_connect(struct NSQDConnection *conn);
void nsqd_connection_disconnect(struct NSQDConnection *conn);

void nsqd_connection_init_timer(struct NSQDConnection *conn,
        void (*reconnect_callback)(EV_P_ ev_timer *w, int revents));
void nsqd_connection_stop_timer(struct NSQDConnection *conn);

void nsq_subscribe(struct Buffer *buf, const char *topic, const char *channel);
void nsq_ready(struct Buffer *buf, int count);
void nsq_finish(struct Buffer *buf, const char *id);
void nsq_requeue(struct Buffer *buf, const char *id, int timeout_ms);
void nsq_nop(struct Buffer *buf);
void nsq_pub(struct Buffer *buf, char *topic, int size, char *msg);

struct NSQMessage {
    int64_t timestamp;
    uint16_t attempts;
    char id[16+1];
    size_t body_length;
    char *body;
};

struct NSQMessage *nsq_decode_message(const char *data, size_t data_length);
void free_nsq_message(struct NSQMessage *msg);

struct NSQLookupdEndpoint {
    char *address;
    int port;
    struct NSQLookupdEndpoint *next;
};

struct NSQLookupdEndpoint *new_nsqlookupd_endpoint(const char *address, int port);
void free_nsqlookupd_endpoint(struct NSQLookupdEndpoint *nsqlookupd_endpoint);

#endif
