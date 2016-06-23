cmake_minimum_required(VERSION 3.5)
project(octel C)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gun99")

INCLUDE_DIRECTORIES(
        deps/raft/
        deps/libuv/include/
        deps/leveldb/include/
)

find_package (Threads)

set(SOURCE_FILES_UV src/main.c deps/libuv/.libs/libuv.a)

set(SOURCE_FILES_DB src/leveldb.c  deps/leveldb/out-static/libleveldb.a)

add_executable(uv ${SOURCE_FILES_UV})


add_executable(leveldb ${SOURCE_FILES_DB})

target_link_libraries(uv  ${CMAKE_THREAD_LIBS_INIT})

