UNITTEST_FOR(ydb/core/tx/tx_allocator)

OWNER(
    svc
    g:kikimr
)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/mind
    ydb/core/testlib
    ydb/core/tx
    ydb/core/tx/tx_allocator
)

YQL_LAST_ABI_VERSION()

SRCS(
    txallocator_ut.cpp
    txallocator_ut_helpers.cpp
)

END()
