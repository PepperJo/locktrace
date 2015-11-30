cmake_minimum_required(VERSION 2.8.0 FATAL_ERROR)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(locktrace)

set(PSL_DIR ${CMAKE_SOURCE_DIR}/../../IBM/psl)
include_directories(${CMAKE_SOURCE_DIR} ${PSL_DIR}/include)
link_directories(${PSL_DIR}/build/lib)

add_compile_options(-std=c++14)
add_compile_options(-Wall -Werror)
add_compile_options(-march=native)

find_package(Boost COMPONENTS program_options system chrono regex REQUIRED)
include_directories(${Boost_INCLUDE_DIR})

include_directories(.)

set(RDMA_LIBS rdmacm ibverbs)

add_executable(lt main.cpp)
target_link_libraries(lt psl)
target_link_libraries(lt ${Boost_LIBRARIES})
