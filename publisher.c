#include "nsq.h"
#include "utlist.h"
#include "http.h"

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

#define DEFAULT_MAX_SEND_SIZE 5242880
#define STACK_BUFFER_SIZE 1024
#define SPINLOCKNS  10000000

/*
 * callback function called after tcp connection to nsqd is established
 */

static void nsq_publisher_connect_cb(struct NSQDConnection *conn, void *arg)
{
    struct NSQPublisher *pub = (struct NSQPublisher *)arg;

    _DEBUG("%s: %p\n", __FUNCTION__, pub);

    if (pub->connect_callback) {
        pub->connect_callback(pub, conn);
    }
}

/*
 * callback function called if an error occurs during data transfer
 */

static void nsq_publisher_error_cb(struct NSQDConnection *conn, void *arg)
{
    struct NSQPublisher *pub = (struct NSQPublisher *)arg;

    _DEBUG("%s: %p, errno %d\n", __FUNCTION__, pub, errno);

    if (pub->error_callback) {
        pub->error_callback(pub, conn, pub->error_callback_arg);
    }
}

/*
 * callback function called for each successfull msg sent
 */

static void nsq_publisher_success_cb(struct NSQDConnection *conn, void *arg)
{
    struct NSQPublisher *pub = (struct NSQPublisher *)arg;

    _DEBUG("%s: %p\n", __FUNCTION__, pub);

    if (pub->success_callback) {
        pub->success_callback(pub, conn, pub->success_callback_arg);
    }
}

/*
 * callback function called for each msg recvd
 */

static void nsq_publisher_msg_cb(struct NSQDConnection *conn, struct NSQMessage *msg, void *arg)
{
    struct NSQPublisher *pub = (struct NSQPublisher *)arg;

    _DEBUG("%s: %p %p\n", __FUNCTION__, msg, pub);

    if (pub->msg_callback) {
        msg->id[sizeof(msg->id)-1] = '\0';
        pub->msg_callback(pub, conn, msg, pub->ctx);
    }
}

/*
 * callback function called when connection is closed
 */

static void nsq_publisher_close_cb(struct NSQDConnection *conn, void *arg)
{
    struct NSQPublisher *pub = (struct NSQPublisher *)arg;
    if(!pub){
        return;
    }
    _DEBUG("%s: %p\n", __FUNCTION__, pub);
    pthread_mutex_lock(pub->lock);

    if (pub->close_callback) {
        pub->close_callback(pub, conn);
    }

    if(pub->conns){
        LL_DELETE(pub->conns, conn);
    }

    // There is no lookupd, try to reconnect to nsqd directly
    if (pub->lookupd == NULL) {
        ev_timer_again(conn->loop, conn->reconnect_timer);
    } else {
        free_nsqd_connection(conn);
    }
    pthread_mutex_unlock(pub->lock);
}

/*
 * unused
 */

void nsq_lookupd_request_cb(struct HttpRequest *req, struct HttpResponse *resp, void *arg);

/*
 * callback function called when connection is closed
 */

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

/*
 * callback function for curl
 */

size_t nsq_delete_topic_wr_cb(char *ptr, size_t size, size_t nmemb, void *arg)
{
    return size * nmemb;
}

/*
 * delete nsq topic
 */

int nsq_delete_topic(char *address, int port, char *topic)
{
    int rc = -1;
    char buf[256];
    CURLcode res;
    CURL *curl = NULL;
    curl = curl_easy_init();

    sprintf(buf, "http://%s:%d/topic/delete?topic=%s", address, port, topic);
    if(curl) {
        struct curl_slist *chunk = NULL;
        char hosts_str[64];
        snprintf(hosts_str, 64, "Host: %s:%d", address, port);
        chunk = curl_slist_append(chunk, "Accept: */*");
        chunk = curl_slist_append(chunk, hosts_str);

        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
        curl_easy_setopt(curl, CURLOPT_URL, buf);
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_VERBOSE, 0L);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, nsq_delete_topic_wr_cb);
        curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, 0L);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 2L);
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 2L);
        res = curl_easy_perform(curl);
        if(res != CURLE_OK){
            _DEBUG("curl_easy_perform() failed: %s\n",
            curl_easy_strerror(res));
        }
        rc = 0;
        curl_slist_free_all(chunk);
        curl_easy_cleanup(curl);
    }
    return rc;
}

/*
 * unused
 */

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

/*
 * creates nsq publisher connection object and assigns user defined callback functions
 *
 * connect_callback - will be called when connection is established
 * close_callback - will be called when connection is closed
 * success_callback - will be called when msg is sent
 * error_callback - will be called for all errors
 * async_write_callback - will be called when async write event is triggered by user for sending data
 * msg_callback - will be called for all msg es recvd on connection
 */

struct NSQPublisher *new_nsq_publisher(struct ev_loop *loop, const char *topic, const char *channel, void *ctx,
    struct NSQPublisherCfg *cfg,
    void (*connect_callback)(struct NSQPublisher *pub, struct NSQDConnection *conn),
    void (*close_callback)(struct NSQPublisher *pub, struct NSQDConnection *conn),
    void (*success_callback)(struct NSQPublisher *pub, struct NSQDConnection *conn, void *arg),
    void (*error_callback)(struct NSQPublisher *pub, struct NSQDConnection *conn, void *arg),
    void (*async_write_callback)(struct BufferedSocket *buffsock, void *arg),
    void (*msg_callback)(struct NSQPublisher *pub, struct NSQDConnection *conn, struct NSQMessage *msg, void *ctx))
{
    struct NSQPublisher *pub;

    if(loop == NULL || topic == NULL || channel == NULL){
        return NULL;
    }

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
    pub->lock = malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(pub->lock, NULL);
    pub->topic = strdup(topic);
    pub->channel = strdup(channel);
    pub->max_in_flight = 1;
    pub->connect_callback = connect_callback;
    pub->close_callback = close_callback;
    pub->msg_callback = msg_callback;
    pub->success_callback = success_callback;
    pub->error_callback = error_callback;
    pub->async_write_callback = async_write_callback;
    pub->ctx = ctx;
    pub->conns = NULL;
    pub->lookupd = NULL;
    pub->loop = loop;

    pub->httpc = new_http_client(pub->loop);

    return pub;
}

/*
 * free nsq publisher connection
 */

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

/*
 * unused
 */

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

/*
 * connect to nsqd using buffered socket
 */

int nsq_publisher_connect_to_nsqd(struct NSQPublisher *pub, const char *address, int port)
{
    struct NSQDConnection *conn = NULL;
    int rc = -1;

    if(pub == NULL || address == NULL){
        return rc;
    }

    conn = new_nsqd_pub_connection(pub->loop, address, port,
        nsq_publisher_connect_cb, nsq_publisher_close_cb, nsq_publisher_success_cb,
        nsq_publisher_error_cb, pub->async_write_callback, nsq_publisher_msg_cb, NULL, pub);

    rc = nsqd_connection_connect(conn);
    if (rc > 0) {
        LL_APPEND(pub->conns, conn);
    }

    if (pub->lookupd == NULL) {
        nsqd_connection_init_timer(conn, nsq_publisher_reconnect_cb);
    }

    return rc;
}

/*
 * publish any msg es in the connection buffer
 */

int nsq_publisher_pub(struct NSQPublisher *pub)
{
    int rc = -1;
    struct NSQDConnection *conn = NULL;
    if(pub){
        pthread_mutex_lock(pub->lock);
        if(pub->conns){
            LL_FOREACH(pub->conns, conn) {
                if(conn){
                    rc = buffered_socket_async_write(conn->bs);
                    if(rc == 0){
                        break;
                    }
                }
            }
        }
        pthread_mutex_unlock(pub->lock);
    }
    return rc;
}
