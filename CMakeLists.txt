cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME redis)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

# Find Boost headers from vcpkg
find_package(Boost REQUIRED)
include_directories(
    src/include
    ${Boost_INCLUDE_DIRS}
)

# Add redis_secret.cpp to the sources
set(EXTENSION_SOURCES
    src/redis_extension.cpp
    src/redis_secret.cpp
    src/query_farm_telemetry.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Add Boost include directories to targets
target_include_directories(${EXTENSION_NAME} PRIVATE ${Boost_INCLUDE_DIRS})
target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE ${Boost_INCLUDE_DIRS})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
