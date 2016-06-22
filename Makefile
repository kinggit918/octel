
CFLAGS += -I./deps/libuv/include -pthread -g -O2 -fPIC


OBJECTS = ./src/main.c ./deps/libuv/.libs/libuv.a

all: octel

.PHONY: octel
octel: $(OBJECTS)
	$(CC) $(OBJECTS) $(CFLAGS) -fPIC  -o octel

clean:
	rm -f ./octel
