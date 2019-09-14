#include "nsq.h"

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

#define BUFFER_HAS_DATA(b)  ((b)->offset)
#define BUFFER_USED(b)      ((b)->data - (b)->orig + (b)->offset)
#define BUFFER_AVAILABLE(b) ((b)->length - BUFFER_USED(b))

enum EvSocketStates {
    ES_INIT,
    ES_CONNECTING,
    ES_CONNECTED,
    ES_DISCONNECTED
};

struct EvSocket {
    char *address;
    int port;
    int fd;
    int state;
    struct ev_io read_ev;
    struct ev_io write_ev;
    struct Buffer *read_buf;
    struct Buffer *write_buf;
    struct ev_timer read_bytes_timer_ev;
    size_t read_bytes_n;
    void (*read_bytes_callback)(struct EvSocket *ev_sock, void *arg);
    void *read_bytes_arg;
    struct ev_loop *loop;
    void (*connect_callback)(struct EvSocket *ev_sock, void *arg);
    void (*close_callback)(struct EvSocket *ev_sock, void *arg);
    void (*read_callback)(struct EvSocket *ev_sock, struct Buffer *buf, void *arg);
    void (*write_callback)(struct EvSocket *ev_sock, void *arg);
    void (*error_callback)(struct EvSocket *ev_sock, void *arg);
    void *cbarg;
};

void ev_socket_connect_cb(int, void *);

int ev_socket_connect(struct EvSocket *ev_sock)
{
    struct addrinfo ai, *aitop;
    char strport[32];
    struct sockaddr *sa;
    int slen;
    long flags;

    if ((ev_sock->state == ES_CONNECTED) || (ev_sock->state == ES_CONNECTING)) {
        return 0;
    }

    memset(&ai, 0, sizeof(struct addrinfo));
    ai.ai_family = AF_INET;
    ai.ai_socktype = SOCK_STREAM;
    snprintf(strport, sizeof(strport), "%d", ev_sock->port);
    if (getaddrinfo(ev_sock->address, strport, &ai, &aitop) != 0) {
        _DEBUG("%s: getaddrinfo() failed\n", __FUNCTION__);
        return 0;
    }
    sa = aitop->ai_addr;
    slen = aitop->ai_addrlen;

    if ((ev_sock->fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        _DEBUG("%s: socket() failed\n", __FUNCTION__);
        return 0;
    }

    // set non-blocking
    if ((flags = fcntl(ev_sock->fd, F_GETFL, NULL)) < 0) {
        close(ev_sock->fd);
        _DEBUG("%s: fcntl(%d, F_GETFL) failed\n", __FUNCTION__, ev_sock->fd);
        return 0;
    }
    if (fcntl(ev_sock->fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        close(ev_sock->fd);
        _DEBUG("%s: fcntl(%d, F_SETFL) failed\n", __FUNCTION__, ev_sock->fd);
        return 0;
    }

    if (connect(ev_sock->fd, sa, slen) == -1) {
        if (errno != EINPROGRESS) {
            close(ev_sock->fd);
            _DEBUG("%s: connect() failed\n", __FUNCTION__);
            return 0;
        }
    }

    freeaddrinfo(aitop);

    ev_once(ev_sock->loop, ev_sock->fd, EV_WRITE, 2.0, ev_socket_connect_cb, ev_sock);

    ev_sock->state = ES_CONNECTING;

    return ev_sock->fd;
}

static void ev_socket_connect_cb(int revents, void *arg)
{
    struct EvSocket *ev_sock = (struct EvSocket *)arg;
    int error;
    socklen_t errsz = sizeof(error);

    if (revents & EV_TIMEOUT) {
        _DEBUG("%s: connection timeout for \"%s:%d\" on %d\n",
            __FUNCTION__, ev_sock->address, ev_sock->port, ev_sock->fd);

        buffered_socket_close(ev_sock);
        return;
    }

    if (getsockopt(ev_sock->fd, SOL_SOCKET, SO_ERROR, (void *)&error, &errsz) == -1) {
        _DEBUG("%s: getsockopt failed for \"%s:%d\" on %d\n",
               __FUNCTION__, ev_sock->address, ev_sock->port, ev_sock->fd);
        buffered_socket_close(ev_sock);
        return;
    }

    if (error) {
        _DEBUG("%s: \"%s\" for \"%s:%d\" on %d\n",
               __FUNCTION__, strerror(error), ev_sock->address, ev_sock->port, ev_sock->fd);
        buffered_socket_close(ev_sock);
        return;
    }

    _DEBUG("%s: connected to \"%s:%d\" on %d\n",
           __FUNCTION__, ev_sock->address, ev_sock->port, ev_sock->fd);

    ev_sock->state = ES_CONNECTED;

    // setup the read io watcher
    ev_sock->read_ev.data = ev_sock;
    ev_init(&ev_sock->read_ev, ev_socket_read_cb);
    ev_io_set(&ev_sock->read_ev, ev_sock->fd, EV_READ);

    // setup the write io watcher
    ev_sock->write_ev.data = ev_sock;
    ev_init(&ev_sock->write_ev, ev_socket_write_cb);
    ev_io_set(&ev_sock->write_ev, ev_sock->fd, EV_WRITE);

    // kick off the read events
    ev_io_start(ev_sock->loop, &ev_sock->read_ev);

    if (ev_sock->connect_callback) {
        (*ev_sock->connect_callback)(ev_sock, ev_sock->cbarg);
    }
}

int buffer_write_fd(struct Buffer *buf, int fd)
{
    int n;
    n = send(fd, buf->data, buf->offset, 0);
    if (n > 0) {
        buffer_drain(buf, n);
    }
    return n;
}

int nsq_unbuffered_pub(NSQDConnection *conn, char *topic, char *msg, int size){
    char b[1024 * 1024 * 1024];
    size_t n;

    ev_io_stop(conn->bs->loop, &conn->bs->write_ev);
    int fd = conn->bs->fd;

    n = sprintf(b, "  V2");

    int rc = send(fd, b, n, 0);
    if(rc < 0 || rc != n){
        return rc;
    }

    n = sprintf(b, "PUB %s\n", topic);
    uint32_t ordered = htobe32(size);
    memcpy(b + n, &ordered, 4);
    n += 4;
    memcpy(b + n, msg, size);
    n += size;

    rc = send(fd, b, n, 0);
    return rc;
}
