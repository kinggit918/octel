cmake_minimum_required(VERSION 3.5)
project(octel C)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gun99")

INCLUDE_DIRECTORIES(
        deps/raft/
        deps/libuv/include/
)
find_package (Threads)

set(SOURCE_FILES src/main.c deps/libuv/.libs/libuv.a)
add_executable(octel ${SOURCE_FILES})


target_link_libraries(octel  ${CMAKE_THREAD_LIBS_INIT})

