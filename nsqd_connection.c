#include "nsq.h"

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

static void nsqd_connection_read_size(struct BufferedSocket *buffsock, void *arg);
static void nsqd_connection_read_data(struct BufferedSocket *buffsock, void *arg);

/*
 * callback function called after tcp connection to nsqd is established
 */

static void nsqd_connection_connect_cb(struct BufferedSocket *buffsock, void *arg)
{
    struct NSQDConnection *conn = (struct NSQDConnection *)arg;

    _DEBUG("%s: %p\n", __FUNCTION__, arg);

    // send magic
    char buff[10] = "  V2";
    buffered_socket_write(conn->bs, (void *)buff, 4);

    struct NSQReader *rdr = conn->arg;

    if(rdr->conn_cfg){
        char send_buff[2048] = {'\0'};
        size_t n = snprintf(send_buff, 2048, "IDENTIFY\n");
        uint32_t ordered = htobe32(strlen(rdr->conn_cfg));
        memcpy(send_buff + n, &ordered, 4);
        n += snprintf(send_buff + n + 4 , 2044 - n, "%s", rdr->conn_cfg);
        buffered_socket_write(conn->bs, (void *)send_buff, n + 4);
    }

    if (conn->connect_callback) {
        conn->connect_callback(conn, conn->arg);
    }

    buffered_socket_read_bytes(buffsock, 4, nsqd_connection_read_size, conn);
}

/*
 * reads size field from nsq packet
 * nsq packet has following format
 *
 *  [x][x][x][x][x][x][x][x][x][x][x][x]...
 * |  (int32) ||  (int32) || (binary)
 * |  4-byte  ||  4-byte  || N-byte
 * ------------------------------------...
 *     size     frame type     data
 */

static void nsqd_connection_read_size(struct BufferedSocket *buffsock, void *arg)
{
    struct NSQDConnection *conn = (struct NSQDConnection *)arg;
    uint32_t *msg_size_be;

    _DEBUG("%s: %p\n", __FUNCTION__, arg);

    msg_size_be = (uint32_t *)buffsock->read_buf->data;
    buffer_drain(buffsock->read_buf, 4);

    // convert message length header from big-endian
    conn->current_msg_size = ntohl(*msg_size_be);

    _DEBUG("%s: msg_size = %d bytes %p\n", __FUNCTION__, conn->current_msg_size, buffsock->read_buf->data);

    buffered_socket_read_bytes(buffsock, conn->current_msg_size, nsqd_connection_read_data, conn);
}

/*
 * reads data part from nsq packet
 * nsq packet has following format
 *
 *  [x][x][x][x][x][x][x][x][x][x][x][x]...
 * |  (int32) ||  (int32) || (binary)
 * |  4-byte  ||  4-byte  || N-byte
 * ------------------------------------...
 *     size     frame type     data
 */

static void nsqd_connection_read_data(struct BufferedSocket *buffsock, void *arg)
{
    struct NSQDConnection *conn = (struct NSQDConnection *)arg;
    struct NSQMessage *msg;

    conn->current_frame_type = ntohl(*((uint32_t *)buffsock->read_buf->data));
    buffer_drain(buffsock->read_buf, 4);
    conn->current_msg_size -= 4;

    _DEBUG("%s: frame type %d, data: %.*s\n", __FUNCTION__, conn->current_frame_type,
        conn->current_msg_size, buffsock->read_buf->data);

    conn->current_data = buffsock->read_buf->data;
    switch (conn->current_frame_type) {
        case NSQ_FRAME_TYPE_RESPONSE:
            if (strncmp(conn->current_data, "_heartbeat_", 11) == 0) {
                buffer_reset(conn->command_buf);
                nsq_nop(conn->command_buf);
                buffered_socket_write_buffer(conn->bs, conn->command_buf);
            }else if (strncmp(conn->current_data, "OK", 2) == 0) {
                if(conn->success_callback){
                    conn->success_callback(conn, conn->arg);
                }
            }
            break;
        case NSQ_FRAME_TYPE_MESSAGE:
            msg = nsq_decode_message(conn->current_data, conn->current_msg_size);
            if (conn->msg_callback) {
                conn->msg_callback(conn, msg, conn->arg);
            }
            break;
        case NSQ_FRAME_TYPE_ERROR:
            msg = nsq_decode_message(conn->current_data, conn->current_msg_size);
            if (conn->msg_callback) {
                conn->msg_callback(conn, msg, conn->arg);
            }
            break;
    }

    buffer_drain(buffsock->read_buf, conn->current_msg_size);

    buffered_socket_read_bytes(buffsock, 4, nsqd_connection_read_size, conn);
}

/*
 * callback function called after tcp connection to nsqd is closed
 */

static void nsqd_connection_close_cb(struct BufferedSocket *buffsock, void *arg)
{
    struct NSQDConnection *conn = (struct NSQDConnection *)arg;

    _DEBUG("%s: %p\n", __FUNCTION__, arg);

    if (conn->close_callback) {
        conn->close_callback(conn, conn->arg);
    }
}

/*
 * callback function called if an error occurs during data transfer
 */

static void nsqd_connection_error_cb(struct BufferedSocket *buffsock, void *arg)
{
    struct NSQDConnection *conn = (struct NSQDConnection *)arg;
    if(conn->error_callback){
        conn->error_callback(conn, conn->arg);
    }

    _DEBUG("%s: conn %p\n", __FUNCTION__, conn);
}

/*
 * creates new nsq reader connection object and assigns user defined callback functions
 */

struct NSQDConnection *new_nsqd_connection(struct ev_loop *loop, const char *address, int port,
    void (*connect_callback)(struct NSQDConnection *conn, void *arg),
    void (*close_callback)(struct NSQDConnection *conn, void *arg),
    void (*msg_callback)(struct NSQDConnection *conn, struct NSQMessage *msg, void *arg),
    void *arg)
{
    struct NSQDConnection *conn;
    struct NSQReader *rdr = (struct NSQReader *)arg;

    conn = (struct NSQDConnection *)calloc(1, sizeof(struct NSQDConnection));
    conn->address = strdup(address);
    conn->port = port;
    conn->command_buf = new_buffer(rdr->cfg->command_buf_len, rdr->cfg->command_buf_capacity);
    conn->current_msg_size = 0;
    conn->connect_callback = connect_callback;
    conn->close_callback = close_callback;
    conn->msg_callback = msg_callback;
    conn->arg = arg;
    conn->loop = loop;
    conn->reconnect_timer = NULL;

    conn->bs = new_buffered_socket(loop, address, port,
        rdr->cfg->read_buf_len, rdr->cfg->read_buf_capacity,
        rdr->cfg->write_buf_len, rdr->cfg->write_buf_capacity,
        nsqd_connection_connect_cb, nsqd_connection_close_cb,
        NULL, NULL, NULL, nsqd_connection_error_cb,
        conn);

    return conn;
}

/*
 * creates new nsq publisher connection object and assigns user defined callback functions
 *
 * connect_callback - will be called when connection is established
 * close_callback - will be called when connection is closed
 * success_callback - will be called when msg is sent
 * error_callback - will be called for all errors
 * async_write_callback - will be called when async write event is triggered by user for sending data
 * msg_callback - will be called for all msg es recvd on connection
 */

struct NSQDConnection *new_nsqd_pub_connection(struct ev_loop *loop, const char *address, int port,
    void (*connect_callback)(struct NSQDConnection *conn, void *arg),
    void (*close_callback)(struct NSQDConnection *conn, void *arg),
    void (*success_callback)(struct NSQDConnection *conn, void *arg),
    void (*error_callback)(struct NSQDConnection *conn, void *arg),
    void (*async_write_callback)(struct BufferedSocket *buffsock, void *arg),
    void (*msg_callback)(struct NSQDConnection *conn, struct NSQMessage *msg, void *arg),
    struct NSQReaderCfg *cfg,
    void *arg)
{
    struct NSQDConnection *conn;

    conn = (struct NSQDConnection *)calloc(1, sizeof(struct NSQDConnection));
    conn->address = strdup(address);
    conn->port = port;
    if(cfg == NULL){
        conn->command_buf = new_buffer(DEFAULT_COMMAND_BUF_LEN, DEFAULT_COMMAND_BUF_CAPACITY);
    }else{
        conn->command_buf = new_buffer(cfg->command_buf_len, cfg->command_buf_capacity);
    }
    conn->current_msg_size = 0;
    conn->connect_callback = connect_callback;
    conn->close_callback = close_callback;
    conn->msg_callback = msg_callback;
    conn->success_callback = success_callback;
    conn->error_callback = error_callback;
    conn->arg = arg;
    conn->loop = loop;
    conn->reconnect_timer = NULL;

    if(cfg == NULL){
        conn->bs = new_buffered_socket(loop, address, port,
            DEFAULT_READ_BUF_LEN, DEFAULT_READ_BUF_CAPACITY,
            DEFAULT_WRITE_BUF_LEN, DEFAULT_WRITE_BUF_CAPACITY,
            nsqd_connection_connect_cb, nsqd_connection_close_cb,
            NULL, NULL, async_write_callback, nsqd_connection_error_cb,
            conn);
    }else{
        conn->bs = new_buffered_socket(loop, address, port,
            cfg->read_buf_len, cfg->read_buf_capacity,
            cfg->write_buf_len, cfg->write_buf_capacity,
            nsqd_connection_connect_cb, nsqd_connection_close_cb,
            NULL, NULL, async_write_callback, nsqd_connection_error_cb,
            conn);
    }

    return conn;
}

/*
 * free nsq connection
 */

void free_nsqd_connection(struct NSQDConnection *conn)
{
    if (conn) {
        nsqd_connection_stop_timer(conn);
        free(conn->address);
        free_buffer(conn->command_buf);
        free_buffered_socket(conn->bs);
        free(conn);
    }
}

/*
 * connect to nsq with buffered socket
 */

int nsqd_connection_connect(struct NSQDConnection *conn)
{
    return buffered_socket_connect(conn->bs);
}

void nsqd_connection_disconnect(struct NSQDConnection *conn)
{
    buffered_socket_close(conn->bs);
}

/*
 * start reconnect timer.
 */

void nsqd_connection_init_timer(struct NSQDConnection *conn,
        void (*reconnect_callback)(EV_P_ ev_timer *w, int revents))
{
    struct NSQReader *rdr = (struct NSQReader *)conn->arg;
    conn->reconnect_timer = (ev_timer *)malloc(sizeof(ev_timer));
    ev_timer_init(conn->reconnect_timer, reconnect_callback, rdr->cfg->lookupd_interval, rdr->cfg->lookupd_interval);
    conn->reconnect_timer->data = conn;
}

void nsqd_connection_stop_timer(struct NSQDConnection *conn)
{
    if (conn && conn->reconnect_timer) {
        ev_timer_stop(conn->loop, conn->reconnect_timer);
        free(conn->reconnect_timer);
    }
}

/*
 * runs main event loop.ev_loop() function blocks;
 */

void nsq_run(struct ev_loop *loop)
{
    srand(time(NULL));
    ev_loop(loop, 0);
}
