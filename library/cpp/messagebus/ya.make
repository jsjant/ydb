LIBRARY()

OWNER(g:messagebus)

IF (SANITIZER_TYPE == "undefined")
    NO_SANITIZE()
ENDIF()

SRCS(
    acceptor.cpp
    acceptor_status.cpp
    connection.cpp
    coreconn.cpp
    duration_histogram.cpp
    event_loop.cpp
    futex_like.cpp
    handler.cpp
    key_value_printer.cpp
    local_flags.cpp
    locator.cpp
    mb_lwtrace.cpp
    message.cpp
    message_counter.cpp
    message_status.cpp
    message_status_counter.cpp
    messqueue.cpp
    misc/atomic_box.h
    misc/granup.h
    misc/test_sync.h
    misc/tokenquota.h
    misc/weak_ptr.h
    network.cpp
    queue_config.cpp
    remote_client_connection.cpp
    remote_client_session.cpp
    remote_client_session_semaphore.cpp
    remote_connection.cpp
    remote_connection_status.cpp
    remote_server_connection.cpp
    remote_server_session.cpp
    remote_server_session_semaphore.cpp
    session.cpp
    session_impl.cpp
    session_job_count.cpp
    shutdown_state.cpp
    socket_addr.cpp
    storage.cpp
    synchandler.cpp
    use_after_free_checker.cpp
    use_count_checker.cpp
    ybus.h
)

PEERDIR(
    contrib/libs/sparsehash
    library/cpp/codecs 
    library/cpp/deprecated/enum_codegen 
    library/cpp/getopt/small
    library/cpp/lwtrace 
    library/cpp/messagebus/actor 
    library/cpp/messagebus/config 
    library/cpp/messagebus/monitoring 
    library/cpp/messagebus/scheduler 
    library/cpp/string_utils/indent_text
    library/cpp/threading/future
)

END()
