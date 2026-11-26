# CMakeLists.txt: builds the C++ unit tests (lob_tests) and the Python module (lobcpp).
#
# Windows PowerShell, from the repo root, with the venv active (pip install pybind11 cmake):
#   $py = (Get-Command python).Source -replace '\\', '/'
#   $pb = (python -c "import pybind11; print(pybind11.get_cmake_dir())") -replace '\\', '/'
#   cmake -S cpp -B build -G "Visual Studio 17 2022" -A x64 -DPython_EXECUTABLE="$py" -Dpybind11_DIR="$pb"
#   cmake --build build --config Release
#   .\build\Release\lob_tests.exe
# -Dpybind11_DIR is optional: without it, CMake asks the Python interpreter where pybind11 is.
# The module (lobcpp.<python tag>.pyd) is written to the repo root, next to book.py, so that
# 'import lobcpp' works from there and from pytest.
cmake_minimum_required(VERSION 3.18)
project(lobcpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)       # plain C++17, no compiler-specific dialect

if(MSVC)
  add_compile_options(/W4 /permissive- /utf-8)
else()
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# Header-only library: just an include directory that the other targets use.
add_library(lob INTERFACE)
target_include_directories(lob INTERFACE include)

# C++ unit tests.
add_executable(lob_tests tests/test_book.cpp)
target_link_libraries(lob_tests PRIVATE lob)
enable_testing()
add_test(NAME lob_tests COMMAND lob_tests)

# Python module. Find the interpreter first so that pybind11 uses the same one (Python_EXECUTABLE).
find_package(Python 3.9 REQUIRED COMPONENTS Interpreter Development.Module)
# pybind11_DIR = the folder holding pybind11Config.cmake. Given with -Dpybind11_DIR=..., or else asked
# from the interpreter. "if(NOT ...)" is also true for the value "pybind11_DIR-NOTFOUND" that a failed
# earlier run leaves in the cache.
# pybind11.get_cmake_dir() returns the plain path. 'python -m pybind11 --cmakedir' is NOT used: it
# quotes a path that contains a space (with "..." on Windows), and CMake would keep the quotes.
if(NOT pybind11_DIR)
  execute_process(COMMAND "${Python_EXECUTABLE}" -c "import pybind11; print(pybind11.get_cmake_dir())"
                  RESULT_VARIABLE _rc OUTPUT_VARIABLE _dir ERROR_VARIABLE _err
                  OUTPUT_STRIP_TRAILING_WHITESPACE)
  if(NOT _rc EQUAL 0 OR _dir STREQUAL "")
    message(FATAL_ERROR "asking ${Python_EXECUTABLE} for pybind11.get_cmake_dir() failed (exit code ${_rc}): ${_err}\n"
                        "Install pybind11 in this Python (pip install pybind11) or pass -Dpybind11_DIR=<folder>.")
  endif()
  file(TO_CMAKE_PATH "${_dir}" _dir)   # backslashes -> forward slashes
  set(pybind11_DIR "${_dir}" CACHE PATH "Folder containing pybind11Config.cmake" FORCE)
endif()
message(STATUS "pybind11_DIR: ${pybind11_DIR}")
if(NOT EXISTS "${pybind11_DIR}/pybind11Config.cmake")
  message(FATAL_ERROR "${pybind11_DIR}/pybind11Config.cmake does not exist. pybind11_DIR must be the folder "
                      "printed by: python -c \"import pybind11; print(pybind11.get_cmake_dir())\" (without quotes)")
endif()
find_package(pybind11 CONFIG REQUIRED)
pybind11_add_module(lobcpp src/bindings.cpp)
target_link_libraries(lobcpp PRIVATE lob)
# "$<1:...>" is a generator expression; it stops Visual Studio from adding a Release/ subfolder.
set_target_properties(lobcpp PROPERTIES LIBRARY_OUTPUT_DIRECTORY "$<1:${PROJECT_SOURCE_DIR}/..>")