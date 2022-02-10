LIBRARY()

OWNER(
    monster
    g:kikimr
)

SRCS(
    partition_stats.h
    partition_stats.cpp
)

PEERDIR(
    library/cpp/actors/core 
    ydb/core/base
    ydb/core/kqp/runtime
    ydb/core/sys_view/common
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
