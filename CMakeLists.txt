cmake_minimum_required(VERSION 3.20)
project(concurrent_memory_pool CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(memory_pool_test main.cpp)
