cmake_minimum_required (VERSION 3.25)


project(1brc LANGUAGES CXX
             DESCRIPTION "1brc")

include(cmake/CPM.cmake)
CPMAddPackage("gh:iv-project/IVio@0.3.1")
CPMAddPackage("gh:martinus/unordered_dense@4.4.0")
add_executable(1brc
    main.cpp
)
target_link_libraries(1brc PUBLIC ivio::ivio
unordered_dense::unordered_dense
)
