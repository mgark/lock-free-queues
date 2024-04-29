# puts all .cpp files inside current directory to the SOURCES variable
file(GLOB SOURCES *.cpp)

# compiles the files defined by SOURCES to generante the executable defined by EXEC
foreach( testsourcefile ${SOURCES} )

    message("processing ${testsourcefile}")
    set(SOURCE_TEST_FILES ${testsourcefile})
    string( REPLACE ".cpp" "" testname ${testsourcefile} )
    get_filename_component(exe_name ${testname} NAME)
    set(TARGET_TEST_NAME ${exe_name})

    #include parent header files
    get_filename_component(PARENT_DIR ../include ABSOLUTE)
    include_directories(${PARENT_DIR})
        
    add_executable(${TARGET_TEST_NAME})
    target_sources(${TARGET_TEST_NAME} PRIVATE ${SOURCE_TEST_FILES})
    target_compile_options(${TARGET_TEST_NAME} PRIVATE ${COMPILE_FLAGS})
    target_link_libraries(${TARGET_TEST_NAME} PRIVATE Catch2::Catch2) 
    catch_discover_tests(${TARGET_TEST_NAME})

endforeach( testsourcefile ${SOURCES} )

