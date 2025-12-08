cmake_minimum_required(VERSION 3.30)
project(monodb_common
    VERSION 0.1.0
    DESCRIPTION "C API for monodb common"
    LANGUAGES C
)

set(CMAKE_C_STANDARD 23)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

set(COMMON_HEADERS
    include/value.h
)

set(COMMON_SOURCES
    src/value.c
)

option(VALUE_BUILD_SHARED "Build shared library" ON)

if (VALUE_BUILD_SHARED)
    add_library(monodb_common SHARED ${COMMON_SOURCES} ${COMMON_HEADERS})
else()
    add_library(monodb_common STATIC ${COMMON_SOURCES} ${COMMON_HEADERS})
endif()

target_include_directories(monodb_common
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

if (MSVC)
    target_compile_options(monodb_common PRIVATE /W4)
else()
    target_compile_options(monodb_common PRIVATE -Wall -Wextra -Wpedantic -Wconversion)
endif()

set_target_properties(monodb_common PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR}
)

option(VALUE_BUILD_TESTS "Build and run tests" ON)

if (VALUE_BUILD_TESTS)
    enable_testing()

    add_executable(value_test tests/test.c)

    target_link_libraries(value_test
        PRIVATE
            monodb_common
    )

    target_include_directories(value_test
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/include
    )

    add_test(
        NAME value_test
        COMMAND value_test
    )
endif()

include(GNUInstallDirs)

install(
    TARGETS monodb_common
    EXPORT monodb_common-targets
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

install(FILES ${VALUE_HEADERS} DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(
    EXPORT monodb_common-targets
    FILE monodb_common-config.cmake
    NAMESPACE monodb_common::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/value
)
