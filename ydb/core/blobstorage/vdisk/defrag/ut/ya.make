UNITTEST_FOR(ydb/core/blobstorage/vdisk/defrag)

OWNER(g:kikimr)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/blobstorage/vdisk/defrag
    ydb/core/blobstorage/vdisk/hulldb
)

SRCS(
    defrag_actor_ut.cpp
)

END()
