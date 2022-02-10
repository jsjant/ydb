#pragma once

#include "blob.h"

#include <ydb/core/tx/columnshard/inflight_request_tracker.h>
#include <ydb/core/tablet_flat/flat_executor.h>
#include <ydb/core/blobstorage/dsproxy/blobstorage_backoff.h>

#include <util/generic/string.h>

namespace NKikimr::NColumnShard {

using NOlap::TUnifiedBlobId;
using NOlap::TBlobRange;


// A batch of blobs that are written by a single task.
// The batch is later saved or discarded as a whole.
class TBlobBatch : public TMoveOnly {
    friend class TBlobManager;

    struct TBatchInfo;

    std::unique_ptr<TBatchInfo> BatchInfo;

private:
    explicit TBlobBatch(std::unique_ptr<TBatchInfo> batchInfo);

    void SendWriteRequest(const TActorContext& ctx, ui32 groupId, const TLogoBlobID& logoBlobId,
        const TString& data, ui64 cookie, TInstant deadline);

public:
    TBlobBatch();
    TBlobBatch(TBlobBatch&& other);
    TBlobBatch& operator = (TBlobBatch&& other);
    ~TBlobBatch();

    // Write new blob as a part of this batch
    TUnifiedBlobId SendWriteBlobRequest(const TString& blobData, TInstant deadline, const TActorContext& ctx);

    // Called with the result of WriteBlob request
    void OnBlobWriteResult(TEvBlobStorage::TEvPutResult::TPtr& ev);

    // Tells if all WriteBlob requests got corresponding results
    bool AllBlobWritesCompleted() const;

    // Number of blobs in the batch
    ui64 GetBlobCount() const;

    // Size of all blobs in the batch
    ui64 GetTotalSize() const;

    // Small blobs will be saved as rows in SmallBlobs local table when the batch gets saved
    TUnifiedBlobId AddSmallBlob(const TString& data);
};

class IBlobManagerDb;

// An interface for writing and deleting blobs for the ColumnShard index management.
// All garbage collection related logic is hidden inside the implementation.
class IBlobManager {
protected:
    static constexpr ui32 BLOB_CHANNEL = 2;

public:
    virtual ~IBlobManager() = default;

    // Allocates a temporary blob batch with the BlobManager. If the tablet crashes or if
    // this object is destroyed without doing SaveBlobBatch then all blobs in this batch
    // will get garbage-collected.
    virtual TBlobBatch StartBlobBatch(ui32 channel = BLOB_CHANNEL) = 0;

    // This method is called in the same transaction in which the user saves references to blobs
    // in some LocalDB table. It tells the BlobManager that the blobs are becoming permanently saved.
    // NOTE: At this point all blob writes must be already acknowleged.
    virtual void SaveBlobBatch(TBlobBatch&& blobBatch, IBlobManagerDb& db) = 0;

    // Deletes the blob that was previously permanently saved
    virtual void DeleteBlob(const TUnifiedBlobId& blobId, IBlobManagerDb& db) = 0;
};

// Garbage Collection generation and step
using TGenStep = std::tuple<ui32, ui32>;

// A ref-counted object to keep track when GC barrier can be moved to some step.
// This means that all needed blobs below this step have been KeepFlag-ed and Ack-ed
struct TAllocatedGenStep : public TThrRefBase {
    const TGenStep GenStep;

    explicit TAllocatedGenStep(const TGenStep& genStep)
        : GenStep(genStep)
    {}

    bool Finished() const {
        return RefCount() == 1;
    }
};

using TAllocatedGenStepConstPtr = TIntrusiveConstPtr<TAllocatedGenStep>;

struct TBlobManagerCounters {
    ui64 BatchesStarted = 0;
    ui64 BatchesCommitted = 0;
    // TODO: ui64 BatchesDiscarded = 0; // Can we count them?
    ui64 BlobsWritten = 0;
    ui64 BlobsDeleted = 0;
    ui64 BlobKeepEntries = 0;
    ui64 BlobDontKeepEntries = 0;
    ui64 BlobSkippedEntries = 0;
    ui64 GcRequestsSent = 0;
    ui64 SmallBlobsWritten = 0;
    ui64 SmallBlobsBytesWritten = 0;
    ui64 SmallBlobsDeleted = 0;
    ui64 SmallBlobsBytesDeleted = 0;
};

// The implementation of BlobManager that hides all GC-related details
class TBlobManager : public IBlobManager, public IBlobInUseTracker {
private:
    static constexpr size_t BLOB_COUNT_TO_TRIGGER_GC_DEFAULT = 1000;
    static constexpr ui64 GC_INTERVAL_SECONDS_DEFAULT = 60;

private:
    TIntrusivePtr<TTabletStorageInfo> TabletInfo;
    const ui32 CurrentGen;
    ui32 CurrentStep;
    TControlWrapper BlobCountToTriggerGC;
    TControlWrapper GCIntervalSeconds;

    // Lists of blobs that need Keep flag to be set
    TSet<TLogoBlobID> BlobsToKeep;
    // Lists of blobs that need DoNotKeep flag to be set
    TSet<TLogoBlobID> BlobsToDelete;

    // List of blobs that are marked for deletion but are still used by in-flight requests
    TSet<TLogoBlobID> BlobsToDeleteDelayed;

    // List of small blobs that are marked for deletion but are still used by in-flight requests
    THashSet<TUnifiedBlobId> SmallBlobsToDeleteDelayed;

    // List of small blobs that that were in-use when DeleteBlob was called and are no longer in-use
    // Now they can now be deleted
    THashSet<TUnifiedBlobId> SmallBlobsToDelete;

    // List of blobs that are used by in-flight requests
    THashMap<TUnifiedBlobId, i64> BlobsUseCount;

    // Sorted queue of GenSteps that have in-flight BlobBatches
    TDeque<TAllocatedGenStepConstPtr> AllocatedGenSteps;

    // The Gen:Step that has been acknowledged by the Distributed Storage
    TGenStep LastCollectedGenStep = {0, 0};

    // The Gen:Step where GC barrier can be moved
    TGenStep NewCollectGenStep = {0, 0};

    // Distributed Storage requires a monotonically increasing counter for GC requests
    ui64 PerGenerationCounter = 1;

    // GC requests that are currently in-flight: they have been
    // sent to Distributed Storage and we are waiting for the results
    struct TGCLists {
        THashSet<TLogoBlobID> KeepList;
        THashSet<TLogoBlobID> DontKeepList;
        TVector<TLogoBlobID> KeepListSkipped;       // List of blobs excluded from Keep list for optimization
        TVector<TLogoBlobID> DontKeepListSkipped;   // List of blobs excluded from both Keep/DontKeep lists
                                                    // NOTE: skipped blobs still need to be removed from local db after GC request completes
    };
    THashMap<ui32, TGCLists> PerGroupGCListsInFlight;
    // Maps PerGenerationCounter value to the group in PerGroupGCListsInFlight
    THashMap<ui64, ui32> CounterToGroupInFlight;
    // The barrier in the current in-flight GC request(s)
    TGenStep CollectGenStepInFlight = {0, 0};

    // Stores counter updates since last call to GetCountersUpdate()
    // Then the counters are reset and start accumulating new delta
    TBlobManagerCounters CountersUpdate;

    TInstant PreviousGCTime; // Used for delaying next GC if there are too few blobs to collect

public:
    TBlobManager(TIntrusivePtr<TTabletStorageInfo> tabletInfo, ui32 gen);

    void RegisterControls(NKikimr::TControlBoard& icb);

    // Loads the state at startup
    bool LoadState(IBlobManagerDb& db);

    // Checks if GC barrier can be moved. Updates NewCollectGenStep if possible.
    bool TryMoveGCBarrier();

    // Prepares Keep/DontKeep lists and GC barrier
    THashMap<ui32, std::unique_ptr<TEvBlobStorage::TEvCollectGarbage>> PreparePerGroupGCRequests();

    // Called with GC result received from Distributed Storage
    void OnGCResult(TEvBlobStorage::TEvCollectGarbageResult::TPtr ev, IBlobManagerDb& db);

    TBlobManagerCounters GetCountersUpdate() {
        TBlobManagerCounters res = CountersUpdate;
        CountersUpdate = TBlobManagerCounters();
        return res;
    }

    // Implementation of IBlobManager interface
    TBlobBatch StartBlobBatch(ui32 channel = BLOB_CHANNEL) override;
    void SaveBlobBatch(TBlobBatch&& blobBatch, IBlobManagerDb& db) override;
    void DeleteBlob(const TUnifiedBlobId& blobId, IBlobManagerDb& db) override;

    // Implementation of IBlobInUseTracker
    void SetBlobInUse(const TUnifiedBlobId& blobId, bool inUse) override;

private:
    void DeleteSmallBlob(const TUnifiedBlobId& blobId, IBlobManagerDb& db);

    // Delete small blobs that were previously in use and could not be deleted
    void PerformDelayedDeletes(IBlobManagerDb& db);
};

}
