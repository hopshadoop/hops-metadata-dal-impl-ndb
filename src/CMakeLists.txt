cmake_minimum_required(VERSION 2.6 FATAL_ERROR)

set(CMAKE_BUILD_TYPE, Release)

include(JNIFlags.cmake NO_POLICY_SCOPE)

find_package(JNI REQUIRED)
INCLUDE_DIRECTORIES(${JNI_INCLUDE_DIRS})

if (DEFINED ENV{LIBNDBPATH})
  message("LIBNDBPATH is defined as $ENV{LIBNDBPATH}")
else()
  message("LIBNDBPATH is NOT defined setting it to /usr/lib")
  set(ENV{LIBNDBPATH} "/usr/lib")
endif()
if(EXISTS "$ENV{LIBNDBPATH}/libndbclient.so")
  set ( PROJECT_LINK_LIBS libndbclient.so )
  link_directories($ENV{LIBNDBPATH})
else()
  message(FATAL_ERROR "libndbclient.so should be in /usr/lib or its path should be set in the environment variable LIBNDBPATH")

endif()




MACRO(INCLUDE_HEADERS)
    FILE(GLOB_RECURSE new_list main/native/streaming/*.h)
    FOREACH(file_path ${new_list})
      GET_FILENAME_COMPONENT(dir_path ${file_path} PATH)
      INCLUDE_DIRECTORIES(${dir_path})
    ENDFOREACH()
ENDMACRO()

INCLUDE_HEADERS()

file(GLOB SOURCES "main/native/streaming/src/*.cpp")

# Compile a library with both shared and static variants
function(add_dual_library LIBNAME)
    add_library(${LIBNAME} SHARED ${ARGN})
    add_library(${LIBNAME}_static STATIC ${ARGN})
    set_target_properties(${LIBNAME}_static PROPERTIES OUTPUT_NAME ${LIBNAME})
endfunction(add_dual_library)

# Link both a static and a dynamic target against some libraries
function(target_link_dual_libraries LIBNAME)
    target_link_libraries(${LIBNAME} ${ARGN})
    target_link_libraries(${LIBNAME}_static ${ARGN})
endfunction(target_link_dual_libraries)

function(output_directory TGT DIR)
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
endfunction(output_directory TGT DIR)

function(dual_output_directory TGT DIR)
    output_directory(${TGT} "${DIR}")
    output_directory(${TGT}_static "${DIR}")
endfunction(dual_output_directory TGT DIR)


add_dual_library (hopsyarn SHARED ${SOURCES})
target_link_dual_libraries(hopsyarn ${PROJECT_LINK_LIBS} )
dual_output_directory(hopsyarn ../classes)