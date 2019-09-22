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

static void nsq_publisher_connect_cb(struct NSQDConnection *conn, void *arg)
{
    struct NSQPublisher *pub = (struct NSQPublisher *)arg;

    _DEBUG("%s: %p\n", __FUNCTION__, pub);

    if (pub->connect_callback) {
        pub->connect_callback(pub, conn);
    }
}

static void nsq_publisher_error_cb(struct NSQDConnection *conn, void *arg)
{
    struct NSQPublisher *pub = (struct NSQPublisher *)arg;

    _DEBUG("%s: %p\n", __FUNCTION__, pub);

    if (pub->error_callback) {
        pub->error_callback(pub, conn, pub->error_callback_arg);
    }
}

static void nsq_publisher_success_cb(struct NSQDConnection *conn, void *arg)
{
    struct NSQPublisher *pub = (struct NSQPublisher *)arg;

    _DEBUG("%s: %p\n", __FUNCTION__, pub);

    if (pub->success_callback) {
        pub->success_callback(pub, conn, pub->success_callback_arg);
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
        nsq_publisher_connect_to_nsqd(pub, conn->address, conn->port, NULL);
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
    void (*success_callback)(struct NSQPublisher *pub, struct NSQDConnection *conn, void *arg),
    void (*error_callback)(struct NSQPublisher *pub, struct NSQDConnection *conn, void *arg),
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
    pub->topic = strdup(topic);
    pub->channel = strdup(channel);
    pub->max_in_flight = 1;
    pub->connect_callback = connect_callback;
    pub->close_callback = close_callback;
    pub->msg_callback = msg_callback;
    pub->success_callback = success_callback;
    pub->error_callback = error_callback;
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

int nsq_publisher_connect_to_nsqd(struct NSQPublisher *pub, const char *address, int port, struct NSQDConnection **conn)
{
    struct NSQDConnection *conn_ptr = NULL;
    int rc = -1;

    if(pub == NULL || address == NULL){
        return rc;
    }

    conn_ptr = new_nsqd_pub_connection(pub->loop, address, port,
        nsq_publisher_connect_cb, nsq_publisher_close_cb, nsq_publisher_success_cb, nsq_publisher_error_cb, nsq_publisher_msg_cb, NULL, pub);

    rc = nsqd_connection_connect(conn_ptr);
    if (rc > 0) {
        LL_APPEND(pub->conns, conn_ptr);
    }

    if (pub->lookupd == NULL) {
        nsqd_connection_init_timer(conn_ptr, nsq_publisher_reconnect_cb);
    }

    printf("if %p\n\n\n\n", conn);
    if(conn != NULL){
        if(*conn == NULL){
            *conn = conn_ptr;
        }        
    }

    return rc;
}

void nsq_pub_unbuffered_read_cb(EV_P_ struct ev_io *w, int revents){
    struct NSQDUnbufferedCon *ucon = w->data;
    ucon->reading = 1;
    char b[STACK_BUFFER_SIZE];

    _DEBUG("%s: ucon %p\n", __FUNCTION__, ucon);

    int rc = read(w->fd, b, STACK_BUFFER_SIZE);
    if(rc <= 0 ){
        _DEBUG("%s: error %p\n", __FUNCTION__, ucon);
        close(w->fd);
    }

    int total_processed = 0;
    while(total_processed < rc){
        uint32_t current_msg_size = ntohl(*(uint32_t *)b);
        uint32_t current_frame_type = ntohl(*((uint32_t *)b + 4));

        char *data = b + 8;
        printf("frame type %x\n", current_frame_type);
        switch(current_frame_type){
            case NSQ_FRAME_TYPE_RESPONSE:
                if (strncmp(data, "_heartbeat_", 11) == 0) {
                    printf("heartttttttttttt\n");
                    size_t n;
                    n = sprintf(b, "NOP\n");

                    int total_sent = 0;
                    rc = 0;
                    while(total_sent < n){
                        rc = send(w->fd, b, n, 0);
                        if(rc < 0){
                            close(w->fd);
                            ucon->sock = -1;
                            return;
                        }else{
                            total_sent += rc;
                        }
                    }
                }else if (strncmp(data, "OK", 2) == 0) {
                    printf("data %s\n", data);
                    ucon->OK_recvd = 1;
                }
                break;
            case NSQ_FRAME_TYPE_ERROR:
                ucon->ERROR_recvd = 1;
                break;
        }
        total_processed += current_msg_size + 4;
    }

    ucon->reading = 0;
}

void *nsq_new_unbuffered_pub_thr(void *p){
    struct NSQDUnbufferedCon *ucon = (struct NSQDUnbufferedCon *)p;
    srand(time(NULL));
    ev_loop(ucon->loop, 0);
    return NULL;
}

void free_unbuffered_pub(struct NSQDUnbufferedCon *ucon){
    ev_break(ucon->loop, EVBREAK_ONE);
    ev_io_stop(ucon->loop, &ucon->read_ev);
    close(ucon->sock);
    ev_loop_destroy(ucon->loop);
    free(ucon);
}

struct NSQDUnbufferedCon *nsq_new_unbuffered_pub(const char *address, int port){
    struct NSQDUnbufferedCon *ucon = calloc(1, sizeof(struct NSQDUnbufferedCon));
    ucon->loop = ev_loop_new(0);

    int rc = nsq_pub_unbuffered_connect(ucon, address, port);

    if(rc <= 0){
        return NULL;
    }

    pthread_t t;
    pthread_attr_t t_attr;
    pthread_attr_init(&t_attr);
    pthread_attr_setdetachstate(&t_attr, PTHREAD_CREATE_DETACHED);
    pthread_create(&t, &t_attr, nsq_new_unbuffered_pub_thr, ucon);

    return ucon;
}

int nsq_pub_unbuffered_connect(struct NSQDUnbufferedCon *ucon, const char *address, int port){
    int sock = -1, ret;
    struct addrinfo hints, *p, *dstinfo;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    char port_str[6];
    int rc = snprintf(port_str, 6, "%d", port);
    if(rc < 0){
        return -3;
    }

    if((ret = getaddrinfo(address, port_str, &hints, &dstinfo)) != 0){
        return -2;
    }

    for(p = dstinfo; p != NULL; p = p->ai_next){
        if((sock = socket(p->ai_family, p->ai_socktype | SOCK_CLOEXEC, p->ai_protocol)) == -1){
            continue;
        }
        break;
    }

    if(connect(sock, dstinfo->ai_addr, dstinfo->ai_addrlen) == -1){
        close(sock);
        freeaddrinfo(dstinfo);
        return -3;
    }

    freeaddrinfo(dstinfo);

    char b[STACK_BUFFER_SIZE];
    size_t n;
    n = sprintf(b, "  V2");

    int total_sent = 0;
    rc = 0;
    while(total_sent < n){
        rc = send(sock, b, n, 0);
        if(rc < 0){
            close(sock);
            return -1;
        }else{
            total_sent += rc;
        }
    }

    ucon->read_ev.data = ucon;
    ev_io_init(&ucon->read_ev, nsq_pub_unbuffered_read_cb, sock, EV_READ);
    ev_io_start(ucon->loop, &ucon->read_ev);

    ucon->sock = sock;
    ucon->reading = 0;
    return sock;
}

int nsq_unbuffered_publish(struct NSQDUnbufferedCon *ucon, char *topic, char *msg, int size, int timeout_in_seconds){
    int rc = -1;

    _DEBUG("%s: topic %s msg %s size %d\n", __FUNCTION__, topic, msg, size);

    if(ucon->sock < 0 || topic == NULL || msg == NULL){
        if(size > (STACK_BUFFER_SIZE - sizeof(topic) - 4) || sizeof(topic) > (STACK_BUFFER_SIZE - 4)){
            return -1;
        }
        return -2;
    }

    char b[STACK_BUFFER_SIZE];
    size_t n;

    n = sprintf(b, "PUB %s\n", topic);
    uint32_t ordered = htobe32(size);
    int total_sent = 0;

    memcpy(b + n, &ordered, 4);
    n += 4;
    memcpy(b + n, msg, size);
    n += size;

    while(!ucon->reading){
        ucon->OK_recvd = 0;
        break;
    }

    while(total_sent < n){
        printf("%d\n", ucon->sock);
        rc = write(ucon->sock, b, n);
        if(rc < 0){
            printf("errno %d\n", errno);
            break;
        }else{
            total_sent += rc;
        }
    }

    struct timespec tv;
    tv.tv_sec = 0;
    tv.tv_nsec = SPINLOCKNS;

    time_t now = time(NULL);
    time_t expiry = now + timeout_in_seconds;

    while(!ucon->OK_recvd){
        now = time(NULL);
        if(now >= expiry){printf("time\n"); return -1; break;}
        nanosleep(&tv, NULL);
    }
    ucon->OK_recvd = 0;

    return total_sent;
}

int __nsq_unbuffered_publish(struct NSQDConnection *conn, char *topic, char *msg, int size, int flags){
    int rc = -1;

    _DEBUG("%s: topic %s msg %s size %d\n", __FUNCTION__, topic, msg, size);

    if(conn && size <= DEFAULT_MAX_SEND_SIZE){
        char b[STACK_BUFFER_SIZE];
        size_t n;

        n = sprintf(b, "PUB %s\n", topic);
        uint32_t ordered = htobe32(size);

        if(flags == COW_PUB || size <= STACK_BUFFER_SIZE){
            memcpy(b + n, &ordered, 4);
            n += 4;
            memcpy(b + n, msg, size);
            n += size;
            rc = send(conn->bs->fd, b, n, 0);
        }else{
            rc = send(conn->bs->fd, b, n, 0);
            if(rc == n){
                rc = send(conn->bs->fd, &ordered, sizeof(ordered), 0);
                if(rc == sizeof(ordered)){
                    rc = send(conn->bs->fd, msg, size, 0);
                }
            }
        }
    }

    return rc;
}
