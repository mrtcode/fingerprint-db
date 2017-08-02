cmake_minimum_required(VERSION 3.8)
project(fingerprint-db)

set(CMAKE_C_STANDARD 99)

set(SOURCE_FILES main.c hashtable.c hashtable.h db.c db.h xxhash.c)
add_executable(fingerprint-db ${SOURCE_FILES})

target_link_libraries(fingerprint-db icui18n icuuc icudata z onion_static sqlite3 jansson.a pam pthread)