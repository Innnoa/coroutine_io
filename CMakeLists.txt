cmake_minimum_required(VERSION 3.22.1)
project(coroutine_io)

set(CMAKE_CXX_STANDARD 20)

add_executable(coroutine_io main.cpp)

add_executable(coroutine_io_test test_coroutine_io.cpp)
