
CFLAGSUV += -I./deps/libuv/include -I./src/  -pthread -g -O2
CFLAGSDB +=  -I./deps/leveldb/include  -lstdc++ -pthread -lrt -lm  -ldl  -g -O2


OBJECTSUV = ./src/octel.c ./src/protocol.c ./deps/libuv/.libs/libuv.a
OBJECTSDB = ./src/leveldb.c deps/leveldb/out-static/libleveldb.a

all: db uv

.PHONY: db
db: $(OBJECTSDB)
	$(CC) $(OBJECTSDB) $(CFLAGSDB) -fPIC  -o leveldb

.PHONY: uv
uv: $(OBJECTSUV)
	$(CC) $(OBJECTSUV) $(CFLAGSUV) -fPIC  -o uv

clean:
	rm -f ./octel
