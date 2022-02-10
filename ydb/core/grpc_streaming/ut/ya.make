UNITTEST_FOR(ydb/core/grpc_streaming)

OWNER(g:kikimr)

FORK_SUBTESTS()

TIMEOUT(300)

SIZE(MEDIUM)

SRCS(
    grpc_streaming_ut.cpp
)

PEERDIR(
    library/cpp/grpc/client 
    ydb/core/grpc_streaming/ut/grpc
    ydb/core/testlib
)

YQL_LAST_ABI_VERSION()

END()
