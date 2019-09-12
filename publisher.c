#include "nsq.h"
#include "utlist.h"
#include "http.h"

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

static void nsq_publisher_connect_cb(struct NSQDConnection *conn, void *arg)
{
    struct NSQPublisher *pub = (struct NSQPublisher *)arg;

    _DEBUG("%s: %p\n", __FUNCTION__, pub);

    if (pub->connect_callback) {
        pub->connect_callback(pub, conn);
    }
}

static void nsq_publisher_msg_cb(struct NSQDConnection *conn, struct NSQMessage *msg, void *arg)
{
    struct NSQPublisher *pub = (struct NSQPublisher *)arg;

    _DEBUG("%s: %p %p\n", __FUNCTION__, msg, pub);

    if (pub->msg_callback) {
        msg->id[sizeof(msg->id)-1] = '\0';
        pub->msg_callback(pub, conn, msg, pub->ctx);
    }
}

static void nsq_publisher_close_cb(struct NSQDConnection *conn, void *arg)
{
    struct NSQPublisher *pub = (struct NSQPublisher *)arg;

    _DEBUG("%s: %p\n", __FUNCTION__, pub);

    if (pub->close_callback) {
        pub->close_callback(pub, conn);
    }

    LL_DELETE(pub->conns, conn);

    // There is no lookupd, try to reconnect to nsqd directly
    if (pub->lookupd == NULL) {
        ev_timer_again(conn->loop, conn->reconnect_timer);
    } else {
        free_nsqd_connection(conn);
    }
}

void nsq_lookupd_request_cb(struct HttpRequest *req, struct HttpResponse *resp, void *arg);

static void nsq_publisher_reconnect_cb(EV_P_ struct ev_timer *w, int revents)
{
    struct NSQDConnection *conn = (struct NSQDConnection *)w->data;
    struct NSQPublisher *pub = (struct NSQPublisher *)conn->arg;

    if (pub->lookupd == NULL) {
        _DEBUG("%s: There is no lookupd, try to reconnect to nsqd directly\n", __FUNCTION__);
        nsq_publisher_connect_to_nsqd(pub, conn->address, conn->port);
    }

    free_nsqd_connection(conn);
}

static void nsq_publisher_lookupd_poll_cb(EV_P_ struct ev_timer *w, int revents)
{
    struct NSQPublisher *pub = (struct NSQPublisher *)w->data;
    struct NSQLookupdEndpoint *nsqlookupd_endpoint;
    struct HttpRequest *req;
    int i, idx, count = 0;
    char buf[256];

    LL_FOREACH(pub->lookupd, nsqlookupd_endpoint) {
        count++;
    }
    if(count == 0) {
        goto end;
    }
    idx = rand() % count;

    _DEBUG("%s: pub %p (chose %d)\n", __FUNCTION__, pub, idx);

    i = 0;
    LL_FOREACH(pub->lookupd, nsqlookupd_endpoint) {
        if (i++ == idx) {
            sprintf(buf, "http://%s:%d/lookup?topic=%s", nsqlookupd_endpoint->address,
                nsqlookupd_endpoint->port, pub->topic);
            req = new_http_request(buf, nsq_lookupd_request_cb, pub);
            http_client_get((struct HttpClient *)pub->httpc, req);
            break;
        }
    }

end:
    ev_timer_again(pub->loop, &pub->lookupd_poll_timer);
}

struct NSQPublisher *new_nsq_publisher(struct ev_loop *loop, const char *topic, const char *channel, void *ctx,
    struct NSQPublisherCfg *cfg,
    void (*connect_callback)(struct NSQPublisher *pub, struct NSQDConnection *conn),
    void (*close_callback)(struct NSQPublisher *pub, struct NSQDConnection *conn),
    void (*msg_callback)(struct NSQPublisher *pub, struct NSQDConnection *conn, struct NSQMessage *msg, void *ctx))
{
    struct NSQPublisher *pub;

    pub = (struct NSQPublisher *)malloc(sizeof(struct NSQPublisher));
    pub->cfg = (struct NSQPublisherCfg *)malloc(sizeof(struct NSQPublisherCfg));

    if (cfg == NULL) {
        pub->cfg->lookupd_interval     = DEFAULT_LOOKUPD_INTERVAL;
        pub->cfg->command_buf_len      = DEFAULT_COMMAND_BUF_LEN;
        pub->cfg->command_buf_capacity = DEFAULT_COMMAND_BUF_CAPACITY;
        pub->cfg->read_buf_len         = DEFAULT_READ_BUF_LEN;
        pub->cfg->read_buf_capacity    = DEFAULT_READ_BUF_CAPACITY;
        pub->cfg->write_buf_len        = DEFAULT_WRITE_BUF_LEN;
        pub->cfg->write_buf_capacity   = DEFAULT_WRITE_BUF_CAPACITY;
    } else {
        pub->cfg->lookupd_interval     = cfg->lookupd_interval     <= 0 ? DEFAULT_LOOKUPD_INTERVAL     : cfg->lookupd_interval;
        pub->cfg->command_buf_len      = cfg->command_buf_len      <= 0 ? DEFAULT_COMMAND_BUF_LEN      : cfg->command_buf_len;
        pub->cfg->command_buf_capacity = cfg->command_buf_capacity <= 0 ? DEFAULT_COMMAND_BUF_CAPACITY : cfg->command_buf_capacity;
        pub->cfg->read_buf_len         = cfg->read_buf_len         <= 0 ? DEFAULT_READ_BUF_LEN         : cfg->read_buf_len;
        pub->cfg->read_buf_capacity    = cfg->read_buf_capacity    <= 0 ? DEFAULT_READ_BUF_CAPACITY    : cfg->read_buf_capacity;
        pub->cfg->write_buf_len        = cfg->write_buf_len        <= 0 ? DEFAULT_WRITE_BUF_LEN        : cfg->write_buf_len;
        pub->cfg->write_buf_capacity   = cfg->write_buf_capacity   <= 0 ? DEFAULT_WRITE_BUF_CAPACITY   : cfg->write_buf_capacity;
    }
    pub->topic = strdup(topic);
    pub->channel = strdup(channel);
    pub->max_in_flight = 1;
    pub->connect_callback = connect_callback;
    pub->close_callback = close_callback;
    pub->msg_callback = msg_callback;
    pub->ctx = ctx;
    pub->conns = NULL;
    pub->lookupd = NULL;
    pub->loop = loop;

    pub->httpc = new_http_client(pub->loop);

    return pub;
}

void free_nsq_publisher(struct NSQPublisher *pub)
{
    struct NSQDConnection *conn;
    struct NSQLookupdEndpoint *nsqlookupd_endpoint;

    if (pub) {
        // TODO: this should probably trigger disconnections and then keep
        // trying to clean up until everything upstream is finished
        LL_FOREACH(pub->conns, conn) {
            nsqd_connection_disconnect(conn);
        }
        LL_FOREACH(pub->lookupd, nsqlookupd_endpoint) {
            free_nsqlookupd_endpoint(nsqlookupd_endpoint);
        }
        free(pub->topic);
        free(pub->channel);
        free(pub->cfg);
        free(pub);
    }
}

int nsq_publisher_add_nsqlookupd_endpoint(struct NSQPublisher *pub, const char *address, int port)
{
    struct NSQLookupdEndpoint *nsqlookupd_endpoint;
    struct NSQDConnection *conn;

    if (pub->lookupd == NULL) {
        // Stop reconnect timers, use lookupd timer instead
        LL_FOREACH(pub->conns, conn) {
            nsqd_connection_stop_timer(conn);
        }

        ev_timer_init(&pub->lookupd_poll_timer, nsq_publisher_lookupd_poll_cb, 0., pub->cfg->lookupd_interval);
        pub->lookupd_poll_timer.data = pub;
        ev_timer_again(pub->loop, &pub->lookupd_poll_timer);
    }

    nsqlookupd_endpoint = new_nsqlookupd_endpoint(address, port);
    LL_APPEND(pub->lookupd, nsqlookupd_endpoint);

    return 1;
}

int nsq_publisher_connect_to_nsqd(struct NSQPublisher *pub, const char *address, int port)
{
    struct NSQDConnection *conn = NULL;
    int rc;

    conn = new_nsqd_pub_connection(pub->loop, address, port,
        nsq_publisher_connect_cb, nsq_publisher_close_cb, nsq_publisher_msg_cb, NULL, pub);

    rc = nsqd_connection_connect(conn);
    if (rc > 0) {
        LL_APPEND(pub->conns, conn);
    }

    if (pub->lookupd == NULL) {
        nsqd_connection_init_timer(conn, nsq_publisher_reconnect_cb);
    }

    return rc;
}


void nsq_run(struct ev_loop *loop)
{
    srand(time(NULL));
    ev_loop(loop, 0);
}
