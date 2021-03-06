PREFIX=/usr/local
DESTDIR=
LIBDIR=${PREFIX}/lib
INCDIR=${PREFIX}/include

# CFLAGS+=-g -Wall -O0 -DDEBUG -fPIC -pthread -I../INC -fno-omit-frame-pointer
CFLAGS+=-g -Wall -O2 -fPIC -pthread -I../INC -fno-omit-frame-pointer
LIBS=-L../lib -lev -levbuffsock -lcurl -lpthread
AR=ar
AR_FLAGS=rc
RANLIB=ranlib

ifeq (1, $(WITH_JANSSON))
LIBS+=-ljansson
CFLAGS+=-DWITH_JANSSON
else
LIBS+=-ljson-c
endif

all: clean libnsq test

libnsq: libnsq.a

%.o: %.c
	$(CC) -o $@ -c $< $(CFLAGS)

libnsq.a: command.o publisher.o reader.o nsqd_connection.o http.o message.o nsqlookupd.o json.o upublisher.o
	$(AR) $(AR_FLAGS) $@ $^
	$(RANLIB) $@

test: test-nsqd

test-nsqd.o: test.c
	$(CC) -o $@ -c $< $(CFLAGS) -DNSQD_STANDALONE

test-nsqd: test-nsqd.o libnsq.a
	$(CC) -o $@ $^ $(LIBS)

test-lookupd: test.o libnsq.a
	$(CC) -o $@ $^ $(LIBS)

clean:
	rm -rf libnsq.a test-nsqd test-lookupd test.dSYM *.o core*

.PHONY: install clean all test libnsq libevbuffsock

libevbuffsock:
	cd ../ && git clone https://github.com/dtsulik/libevbuffsock.git && $(MAKE) && $(MAKE) install

install:
	install -m 755 -d ${DESTDIR}${INCDIR}
	install -m 755 -d ${DESTDIR}${LIBDIR}
	install -m 755 libnsq.a ${DESTDIR}${LIBDIR}/libnsq.a
	install -m 755 nsq.h ${DESTDIR}${INCDIR}/nsq.h
