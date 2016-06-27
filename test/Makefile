
CFLAGSUV += -I../deps/libuv/include  -pthread -g -O2



OBJPROCESS = ./process.c ../deps/libuv/.libs/libuv.a

OBJTCP = ./tcp.c ../deps/libuv/.libs/libuv.a


TESTCODE = process tcp

all:$(TESTCODE)


.PHONY: process
process: $(OBJPROCESS)
	$(CC) $(OBJPROCESS) $(CFLAGSUV) -fPIC  -o process

.PHONY: tcp
tcp: $(OBJTCP)
	$(CC) $(OBJTCP) $(CFLAGSUV) -fPIC  -o tcp

clean:
	for name in `echo $(TESTCODE)`; \
    do \
        rm -f "$$name"; \
    done
