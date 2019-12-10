#include "nsq.h"
#include "utlist.h"
#include "http.h"

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

void nsq_ucon_error(struct NSQDUnbufferedCon *ucon){
    _DEBUG("%s: %p errno %d\n", __FUNCTION__, ucon, errno);
    if(!ucon){
        return;
    }else{
        pthread_mutex_lock(&ucon->state_lock);
        if(ev_is_active(&ucon->read_ev)){
            ev_io_stop(ucon->loop, &ucon->read_ev);
        }

        close(ucon->sock);
        ucon->sock = -1;
        ucon->state = NSQ_DISCONNECTED;
        pthread_mutex_unlock(&ucon->state_lock);
    }

    if(ucon->error_callback){
        ucon->error_callback(ucon, ucon->cbarg);
    }
}

void nsq_ucon_reconnect(EV_P_ ev_timer *w, int revents){
    struct NSQDUnbufferedCon *ucon = (struct NSQDUnbufferedCon *)w->data;
    _DEBUG("%s:%d: %p - %d\n", __FUNCTION__, __LINE__, ucon, ucon->state);
    if(!ucon || !(ucon->state == NSQ_DISCONNECTED)){
        return;
    }

    pthread_mutex_lock(&ucon->state_lock);
    if(ucon->sock > 0){
        close(ucon->sock);
        ucon->sock = -1;
    }
    ucon->state = NSQ_CONNECTING;
    int rc = tcp_connect(ucon->address, ucon->port, ucon);

    if(rc <= 0){
        _DEBUG("%s:%d: %p - errno %d state %d\n", __FUNCTION__, __LINE__, ucon, errno, ucon->state);
        ucon->state = NSQ_DISCONNECTED;
    }else{
        _DEBUG("%s:%d: set connected %d rc %d errno\n", __FUNCTION__, __LINE__, rc, errno);
        ucon->state = NSQ_CONNECTED;
        ucon->sock = rc;
        ucon->read_ev.data = ucon;
        ev_io_init(&ucon->read_ev, nsq_pub_unbuffered_read_cb, ucon->sock, EV_READ);
        ev_io_start(ucon->loop, &ucon->read_ev);

        // send magic
        char b[STACK_BUFFER_SIZE];
        size_t n;
        n = sprintf(b, "  V2");

        int total_sent = 0;
        rc = 0;
        while(total_sent < n){
            rc = send(ucon->sock, b, n, MSG_NOSIGNAL);
            if(rc <= 0){
                if(errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR ||
                    errno == EINPROGRESS || errno == EALREADY){
                    continue;
                }
                nsq_ucon_error(ucon);
                break;
            }else{
                total_sent += rc;
            }
        }

        if(ucon->connect_callback){
            ucon->connect_callback(ucon, ucon->cbarg);
        }
    }
    pthread_mutex_unlock(&ucon->state_lock);
}

void nsq_pub_unbuffered_read_cb(EV_P_ struct ev_io *w, int revents){
    struct NSQDUnbufferedCon *ucon = w->data;
    if(ucon == NULL){
        return;
    }

    char b[STACK_BUFFER_SIZE];

    _DEBUG("%s: ucon %p\n", __FUNCTION__, ucon);

    int rc = read(w->fd, b, STACK_BUFFER_SIZE);
    if(rc <= 0) {
        if(errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR ||
            errno == EINPROGRESS || errno == EALREADY){
            return;
        }
        goto error;
    }

    int total_processed = 0;
    while(total_processed < rc){
        uint32_t current_msg_size = ntohl(*(uint32_t *)b);
        uint32_t current_frame_type = ntohl(*(uint32_t *)(b+4));

        char *data = b + 8;
        switch(current_frame_type){
            case NSQ_FRAME_TYPE_RESPONSE : {
                if(rc < 11){break;}
                if (strncmp(data, "_heartbeat_", 11) == 0) {
                    size_t n;
                    n = sprintf(b, "NOP\n");

                    int total_sent = 0;
                    rc = 0;
                    while(total_sent < n){
                        rc = send(w->fd, b, n, MSG_NOSIGNAL);
                        if(rc <= 0) {
                            if(errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR ||
                                errno == EINPROGRESS || errno == EALREADY){
                                return;
                            }
                            goto error;
                        }else{
                            total_sent += rc;
                        }
                    }
                }
            }break;
            case NSQ_FRAME_TYPE_ERROR:{
                goto error;
            }break;
        }
        total_processed += current_msg_size + 4;
    }
    return;
error:
    nsq_ucon_error(ucon);
}

void *nsq_new_unbuffered_pub_thr(void *p){
    struct NSQDUnbufferedCon *ucon = (struct NSQDUnbufferedCon *)p;
    ucon->loop = ev_loop_new(0);

    ucon->read_ev.data = ucon;
    if(ucon->sock > 0 && ucon->state == NSQ_CONNECTED){        
        ev_io_init(&ucon->read_ev, nsq_pub_unbuffered_read_cb, ucon->sock, EV_READ);
        ev_io_start(ucon->loop, &ucon->read_ev);
    }

    ucon->reconnect_timer.data = ucon;
    ev_timer_init(&ucon->reconnect_timer, nsq_ucon_reconnect, ucon->reconnect_interval, ucon->reconnect_interval);
    ev_timer_start(ucon->loop, &ucon->reconnect_timer);

    srand(time(NULL));
    _DEBUG("%s: starting ev loop: %p\n", __FUNCTION__, p);
    ev_loop(ucon->loop, 0);
    _DEBUG("%s: exiting ev loop: %p\n", __FUNCTION__, p);
    return NULL;
}

void free_unbuffered_pub(struct NSQDUnbufferedCon *ucon){
    ev_timer_stop(ucon->loop, &ucon->reconnect_timer);
    ev_io_stop(ucon->loop, &ucon->read_ev);
    ev_break(ucon->loop, EVBREAK_ALL);
    close(ucon->sock);
    sleep(2);
    free(ucon);
}

struct NSQDUnbufferedCon *nsq_new_unbuffered_pub(const char *address, int port,
    void (*connect_callback)(struct NSQDUnbufferedCon *ucon, void *cbarg),
    void (*error_callback)(struct NSQDUnbufferedCon *ucon, void *cbarg), void *cbarg,
    double reconnect_interval){

    struct NSQDUnbufferedCon *ucon = calloc(1, sizeof(struct NSQDUnbufferedCon));
    ucon->connect_callback = connect_callback;
    ucon->error_callback = error_callback;
    ucon->cbarg = cbarg;
    snprintf(ucon->address, 127, "%s", address);
    ucon->port = port;
    ucon->reconnect_interval = reconnect_interval;

    pthread_mutex_init(&ucon->state_lock, NULL);
    ucon->state = NSQ_CONNECTING;

    int rc = tcp_connect(address, port, ucon);

    if(rc <= 0){
        nsq_ucon_error(ucon);
    }else{
        int optval = 0;
        size_t optsz = sizeof(optval);
        int erc = getsockopt(rc, SOL_SOCKET, SO_ERROR, &optval, &optsz);
        if(erc < 0){
            free(ucon);
            return NULL;
        }
        _DEBUG("%s: sock error %d rc %d errno\n", __FUNCTION__, erc, errno);
        if(optval != EINPROGRESS && optval != EALREADY){
            _DEBUG("%s: set connected %d rc %d errno\n", __FUNCTION__, rc, errno);
            ucon->state = NSQ_CONNECTED;
            ucon->sock = rc;
            if(ucon->connect_callback){
                ucon->connect_callback(ucon, ucon->cbarg);
            }
        }
    }

    if(ucon->state == NSQ_CONNECTED){
        // send magic
        char b[STACK_BUFFER_SIZE];
        size_t n;
        n = sprintf(b, "  V2");

        int total_sent = 0;
        rc = 0;
        while(total_sent < n){
            rc = send(ucon->sock, b, n, MSG_NOSIGNAL);
            if(rc <= 0){
                if(errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR ||
                    errno == EINPROGRESS || errno == EALREADY){
                    continue;
                }
                nsq_ucon_error(ucon);
                break;
            }else{
                total_sent += rc;
            }
        }
    }

    pthread_t t;
    pthread_attr_t t_attr;
    pthread_attr_init(&t_attr);
    pthread_attr_setdetachstate(&t_attr, PTHREAD_CREATE_DETACHED);
    int trc = pthread_create(&t, &t_attr, nsq_new_unbuffered_pub_thr, ucon);
    pthread_setname_np(t, "NSQ Publisher");

    if(trc == 0){
        return ucon;
    }else{
        free(ucon);
        return NULL;
    }
}

int tcp_connect(const char *address, int port, struct NSQDUnbufferedCon *ucon){
    int sock = -1, ret;
    struct addrinfo hints, *p, *dstinfo;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    char port_str[6];
    int rc = snprintf(port_str, 6, "%d", port);
    if(rc < 0){
        return -1;
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

    int flags = 0;
    if ((flags = fcntl(sock, F_GETFL, NULL)) < 0) {
        close(sock);
        return 0;
    }
    if (fcntl(sock, F_SETFL, flags | O_NONBLOCK) == -1) {
        close(sock);
        return 0;
    }

    time_t start = time(NULL);
retry:
    if((time(NULL) - start) > 2){
        close(sock);
        freeaddrinfo(dstinfo);
        return -9;
    }
    rc = connect(sock, dstinfo->ai_addr, dstinfo->ai_addrlen);
    _DEBUG("%s: %p set connect rc:%d sock:%d errno:%d start %ld now %ld\n", __FUNCTION__, ucon, rc, sock, errno, start, time(NULL));
    if(rc == -1){
        if(errno == 115 || errno == 114){
            usleep(200000);
            goto retry;
        }else{
            close(sock);
            freeaddrinfo(dstinfo);
            return -3;
        }
    }

    freeaddrinfo(dstinfo);
    return sock;
}

int nsq_upub(struct NSQDUnbufferedCon *primary, struct NSQDUnbufferedCon *secondary, char *topic, char *msg, int size){
    int rc = -1;
    _DEBUG("%s: topic: %s msg: %s size: %d\n", __FUNCTION__, topic, msg, size);
    if(!primary && !secondary){
        return 0;
    }
    if(!secondary){
        if(primary->state == NSQ_CONNECTED){
            rc = nsq_unbuffered_publish(primary->sock, topic, msg, size);
            if(rc < 0){
                nsq_ucon_error(primary);
            }
        }else{return -1;}
    }else{
        if(primary){
            if(primary->state == NSQ_CONNECTED){
                rc = nsq_unbuffered_publish(primary->sock, topic, msg, size);
                if(rc < 0){
                    nsq_ucon_error(primary);
                }
                return rc;
            }else{return -1;}
        }
        if(rc < 0){
            if(secondary->state == NSQ_CONNECTED){
                rc = nsq_unbuffered_publish(secondary->sock, topic, msg, size);
                if(rc < 0){
                    nsq_ucon_error(primary);
                }                
            }else{return -2;}
        }
    }
    return rc;
}

int nsq_unbuffered_publish(int sock, char *topic, char *msg, int size){
    int rc = -1;

    _DEBUG("%s: topic: %s msg: %s size: %d\n", __FUNCTION__, topic, msg, size);

    if(sock < 0 || topic == NULL || msg == NULL){
        if(size > (STACK_BUFFER_SIZE - strlen(topic) - 4) || strlen(topic) > (STACK_BUFFER_SIZE - 4)){
            return -2;
        }
        return -3;
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

    while(total_sent < n){
        rc = send(sock, b, n, MSG_NOSIGNAL);
        if(rc <= 0){
            if(errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR ||
                errno == EINPROGRESS || errno == EALREADY){
                continue;
            }
            _DEBUG("%s: error: %d\n", __FUNCTION__, errno);
            return -3;
        }else{
            total_sent += rc;
        }
    }
    return total_sent;
}
