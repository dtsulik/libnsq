#include "nsq.h"
#include "utlist.h"
#include "http.h"

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

/*
 * callback function called when reader object finishes tcp connection
 */

static void nsq_reader_connect_cb(struct NSQDConnection *conn, void *arg)
{
    struct NSQReader *rdr = (struct NSQReader *)arg;

    _DEBUG("%s: %p\n", __FUNCTION__, rdr);

    if (rdr->connect_callback) {
        rdr->connect_callback(rdr, conn);
    }

    // subscribe
    buffer_reset(conn->command_buf);
    nsq_subscribe(conn->command_buf, rdr->topic, rdr->channel);
    buffered_socket_write_buffer(conn->bs, conn->command_buf);

    // send initial RDY
    buffer_reset(conn->command_buf);
    nsq_ready(conn->command_buf, rdr->max_in_flight);
    buffered_socket_write_buffer(conn->bs, conn->command_buf);
}

/*
 * callback function called when reader object receives message
 */

static void nsq_reader_msg_cb(struct NSQDConnection *conn, struct NSQMessage *msg, void *arg)
{
    struct NSQReader *rdr = (struct NSQReader *)arg;

    _DEBUG("%s: %p %p\n", __FUNCTION__, msg, rdr);

    if (rdr->msg_callback) {
        msg->id[sizeof(msg->id)-1] = '\0';
        rdr->msg_callback(rdr, conn, msg, rdr->ctx);
    }
}

/*
 * callback function called when reader object closes the connection
 */

static void nsq_reader_close_cb(struct NSQDConnection *conn, void *arg)
{
    struct NSQReader *rdr = (struct NSQReader *)arg;

    _DEBUG("%s: %p\n", __FUNCTION__, rdr);

    if (rdr->close_callback) {
        rdr->close_callback(rdr, conn);
    }

    if(rdr->conns){
        LL_DELETE(rdr->conns, conn);
    }

    // There is no lookupd, try to reconnect to nsqd directly
    if (rdr->lookupd == NULL) {
        ev_timer_again(conn->loop, conn->reconnect_timer);
    } else {
        free_nsqd_connection(conn);
    }
}

void nsq_lookupd_request_cb(struct HttpRequest *req, struct HttpResponse *resp, void *arg);

static void nsq_reader_reconnect_cb(EV_P_ struct ev_timer *w, int revents)
{
    struct NSQDConnection *conn = (struct NSQDConnection *)w->data;
    struct NSQReader *rdr = (struct NSQReader *)conn->arg;

    if (rdr->lookupd == NULL) {
        _DEBUG("%s: There is no lookupd, try to reconnect to nsqd directly\n", __FUNCTION__);
        nsq_reader_connect_to_nsqd(rdr, conn->address, conn->port);
    }

    free_nsqd_connection(conn);
}

/*
 * unused
 */

static void nsq_reader_lookupd_poll_cb(EV_P_ struct ev_timer *w, int revents)
{
    struct NSQReader *rdr = (struct NSQReader *)w->data;
    struct NSQLookupdEndpoint *nsqlookupd_endpoint;
    struct HttpRequest *req;
    int i, idx, count = 0;
    char buf[256];

    LL_FOREACH(rdr->lookupd, nsqlookupd_endpoint) {
        count++;
    }
    if(count == 0) {
        goto end;
    }
    idx = rand() % count;

    _DEBUG("%s: rdr %p (chose %d)\n", __FUNCTION__, rdr, idx);

    i = 0;
    LL_FOREACH(rdr->lookupd, nsqlookupd_endpoint) {
        if (i++ == idx) {
            sprintf(buf, "http://%s:%d/lookup?topic=%s", nsqlookupd_endpoint->address,
                nsqlookupd_endpoint->port, rdr->topic);
            req = new_http_request(buf, nsq_lookupd_request_cb, rdr);
            http_client_get((struct HttpClient *)rdr->httpc, req);
            break;
        }
    }

end:
    ev_timer_again(rdr->loop, &rdr->lookupd_poll_timer);
}

/*
 * break the ev loop stop the reader
 */

static void nsq_break_cb (EV_P_ ev_async *w, int revents){
    ev_break(loop, EVBREAK_ALL);
}

/*
 * create new nsq reader object and assign user defined callback functions
 *
 * connect_callback - will be called when connection is established
 * close_callback - will be called when connection is closed
 * msg_callback - will be called for all msg es recvd on connection
 */

struct NSQReader *new_nsq_reader(struct ev_loop *loop, const char *topic, const char *channel, void *ctx,
    struct NSQReaderCfg *cfg,
    void (*connect_callback)(struct NSQReader *rdr, struct NSQDConnection *conn),
    void (*close_callback)(struct NSQReader *rdr, struct NSQDConnection *conn),
    void (*msg_callback)(struct NSQReader *rdr, struct NSQDConnection *conn, struct NSQMessage *msg, void *ctx))
{
    struct NSQReader *rdr;

    if(loop == NULL || topic == NULL || channel == NULL){
        return NULL;
    }

    rdr = (struct NSQReader *)malloc(sizeof(struct NSQReader));
    rdr->cfg = (struct NSQReaderCfg *)malloc(sizeof(struct NSQReaderCfg));
    if (cfg == NULL) {
        rdr->cfg->lookupd_interval     = DEFAULT_LOOKUPD_INTERVAL;
        rdr->cfg->command_buf_len      = DEFAULT_COMMAND_BUF_LEN;
        rdr->cfg->command_buf_capacity = DEFAULT_COMMAND_BUF_CAPACITY;
        rdr->cfg->read_buf_len         = DEFAULT_READ_BUF_LEN;
        rdr->cfg->read_buf_capacity    = DEFAULT_READ_BUF_CAPACITY;
        rdr->cfg->write_buf_len        = DEFAULT_WRITE_BUF_LEN;
        rdr->cfg->write_buf_capacity   = DEFAULT_WRITE_BUF_CAPACITY;
    } else {
        rdr->cfg->lookupd_interval     = cfg->lookupd_interval     <= 0 ? DEFAULT_LOOKUPD_INTERVAL     : cfg->lookupd_interval;
        rdr->cfg->command_buf_len      = cfg->command_buf_len      <= 0 ? DEFAULT_COMMAND_BUF_LEN      : cfg->command_buf_len;
        rdr->cfg->command_buf_capacity = cfg->command_buf_capacity <= 0 ? DEFAULT_COMMAND_BUF_CAPACITY : cfg->command_buf_capacity;
        rdr->cfg->read_buf_len         = cfg->read_buf_len         <= 0 ? DEFAULT_READ_BUF_LEN         : cfg->read_buf_len;
        rdr->cfg->read_buf_capacity    = cfg->read_buf_capacity    <= 0 ? DEFAULT_READ_BUF_CAPACITY    : cfg->read_buf_capacity;
        rdr->cfg->write_buf_len        = cfg->write_buf_len        <= 0 ? DEFAULT_WRITE_BUF_LEN        : cfg->write_buf_len;
        rdr->cfg->write_buf_capacity   = cfg->write_buf_capacity   <= 0 ? DEFAULT_WRITE_BUF_CAPACITY   : cfg->write_buf_capacity;
    }
    rdr->topic = strdup(topic);
    rdr->channel = strdup(channel);
    rdr->max_in_flight = 1;
    rdr->connect_callback = connect_callback;
    rdr->close_callback = close_callback;
    rdr->msg_callback = msg_callback;
    rdr->ctx = ctx;
    rdr->conns = NULL;
    rdr->lookupd = NULL;
    rdr->loop = loop;

    ev_async_init(&rdr->breaker, nsq_break_cb);
    ev_async_start(loop, &rdr->breaker);

    rdr->httpc = new_http_client(rdr->loop);

    return rdr;
}

/*
 * close connection and free respective objects
 */

void free_nsq_reader(struct NSQReader *rdr)
{
    struct NSQDConnection *conn;
    struct NSQLookupdEndpoint *nsqlookupd_endpoint;

    if (rdr) {
        // TODO: this should probably trigger disconnections and then keep
        // trying to clean up until everything upstream is finished
        LL_FOREACH(rdr->conns, conn) {
            nsqd_connection_disconnect(conn);
        }
        LL_FOREACH(rdr->lookupd, nsqlookupd_endpoint) {
            free_nsqlookupd_endpoint(nsqlookupd_endpoint);
        }
        free(rdr->topic);
        free(rdr->channel);
        free(rdr->cfg);
        ev_async_send(rdr->loop, &rdr->breaker);
        free(rdr);
    }
}

/*
 * unused
 */

int nsq_reader_add_nsqlookupd_endpoint(struct NSQReader *rdr, const char *address, int port)
{
    struct NSQLookupdEndpoint *nsqlookupd_endpoint;
    struct NSQDConnection *conn;

    if (rdr->lookupd == NULL) {
        // Stop reconnect timers, use lookupd timer instead
        LL_FOREACH(rdr->conns, conn) {
            nsqd_connection_stop_timer(conn);
        }

        ev_timer_init(&rdr->lookupd_poll_timer, nsq_reader_lookupd_poll_cb, 0., rdr->cfg->lookupd_interval);
        rdr->lookupd_poll_timer.data = rdr;
        ev_timer_again(rdr->loop, &rdr->lookupd_poll_timer);
    }

    nsqlookupd_endpoint = new_nsqlookupd_endpoint(address, port);
    LL_APPEND(rdr->lookupd, nsqlookupd_endpoint);

    return 1;
}

/*
 * connect to nsqd with buffered socket
 */

int nsq_reader_connect_to_nsqd(struct NSQReader *rdr, const char *address, int port)
{
    struct NSQDConnection *conn = NULL;
    int rc = - 1;

    if(rdr == NULL || address == NULL){
        return rc;
    }

    conn = new_nsqd_connection(rdr->loop, address, port,
        nsq_reader_connect_cb, nsq_reader_close_cb, nsq_reader_msg_cb, rdr);

    rc = nsqd_connection_connect(conn);
    if (rc > 0) {
        LL_APPEND(rdr->conns, conn);
    }

    if (rdr->lookupd == NULL) {
        nsqd_connection_init_timer(conn, nsq_reader_reconnect_cb);
    }

    return rc;
}

