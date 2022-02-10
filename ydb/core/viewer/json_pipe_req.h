#pragma once

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include "viewer.h"

namespace NKikimr {
namespace NViewer {

using namespace NKikimr;
using namespace NSchemeCache;

template <typename TDerived>
class TViewerPipeClient : public TActorBootstrapped<TDerived> {
protected:
    using TBase = TActorBootstrapped<TDerived>;
    bool Followers = true;
    bool Metrics = true;
    bool WithRetry = true;
    ui32 Requests = 0;
    static constexpr ui32 MaxRequestsInFlight = 50;

    struct TPipeInfo {
        TActorId PipeClient;
        ui32 Requests = 0;
    };

    std::unordered_map<TTabletId, TPipeInfo> PipeInfo;

    struct TDelayedRequest {
        std::unique_ptr<IEventHandle> Event;
    };

    std::deque<TDelayedRequest> DelayedRequests;

    NTabletPipe::TClientConfig GetPipeClientConfig() {
        NTabletPipe::TClientConfig clientConfig;
        if (WithRetry) {
            clientConfig.RetryPolicy = {.RetryLimitCount = 3};
        }
        return clientConfig;
    }

    TActorId ConnectTabletPipe(TTabletId tabletId) {
        TPipeInfo& pipeInfo = PipeInfo[tabletId];
        if (!pipeInfo.PipeClient) {
            auto pipe = NTabletPipe::CreateClient(TBase::SelfId(), tabletId, GetPipeClientConfig());
            pipeInfo.PipeClient = TBase::RegisterWithSameMailbox(pipe);
        }
        pipeInfo.Requests++;
        return pipeInfo.PipeClient;
    }

    void SendEvent(std::unique_ptr<IEventHandle> event) {
        if (DelayedRequests.empty() && Requests < MaxRequestsInFlight) {
            TActivationContext::Send(event.release());
            ++Requests;
        } else {
            DelayedRequests.push_back({
                .Event = std::move(event),
            });
        }
    }

    void SendRequest(const TActorId& recipient, IEventBase* ev, ui32 flags = 0, ui64 cookie = 0) {
        SendEvent(std::make_unique<IEventHandle>(recipient, TBase::SelfId(), ev, flags, cookie));
    }

    void SendRequestToPipe(const TActorId& pipe, IEventBase* ev, ui64 cookie = 0) {
        std::unique_ptr<IEventHandle> event = std::make_unique<IEventHandle>(pipe, TBase::SelfId(), ev, 0/*flags*/, cookie);
        event->Rewrite(TEvTabletPipe::EvSend, pipe);
        SendEvent(std::move(event));
    }

    void SendDelayedRequests() {
        while (!DelayedRequests.empty() && Requests < MaxRequestsInFlight) {
            auto& request(DelayedRequests.front());
            TActivationContext::Send(request.Event.release());
            ++Requests;
            DelayedRequests.pop_front();
        }
    }

    void RequestHiveDomainStats(TTabletId hiveId) {
        TActorId pipeClient = ConnectTabletPipe(hiveId);
        THolder<TEvHive::TEvRequestHiveDomainStats> request = MakeHolder<TEvHive::TEvRequestHiveDomainStats>();
        request->Record.SetReturnFollowers(Followers);
        request->Record.SetReturnMetrics(Metrics);
        SendRequestToPipe(pipeClient, request.Release(), hiveId);
    }

    void RequestHiveNodeStats(TTabletId hiveId) {
        TActorId pipeClient = ConnectTabletPipe(hiveId);
        THolder<TEvHive::TEvRequestHiveNodeStats> request = MakeHolder<TEvHive::TEvRequestHiveNodeStats>();
        request->Record.SetReturnMetrics(Metrics);
        SendRequestToPipe(pipeClient, request.Release(), hiveId);
    }

    void RequestHiveStorageStats(TTabletId hiveId) {
        TActorId pipeClient = ConnectTabletPipe(hiveId);
        THolder<TEvHive::TEvRequestHiveStorageStats> request = MakeHolder<TEvHive::TEvRequestHiveStorageStats>();
        SendRequestToPipe(pipeClient, request.Release(), hiveId);
    }

    TTabletId GetConsoleId() {
        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
        TIntrusivePtr<TDomainsInfo::TDomain> domain = domains->Domains.begin()->second;
        auto group = domains->GetDefaultStateStorageGroup(domain->DomainUid);
        return MakeConsoleID(group);
    }

    void RequestConsoleListTenants() {
        TActorId pipeClient = ConnectTabletPipe(GetConsoleId());
        THolder<NConsole::TEvConsole::TEvListTenantsRequest> request = MakeHolder<NConsole::TEvConsole::TEvListTenantsRequest>();
        SendRequestToPipe(pipeClient, request.Release());
    }

    void RequestConsoleGetTenantStatus(const TString& path) {
        TActorId pipeClient = ConnectTabletPipe(GetConsoleId());
        THolder<NConsole::TEvConsole::TEvGetTenantStatusRequest> request = MakeHolder<NConsole::TEvConsole::TEvGetTenantStatusRequest>();
        request->Record.MutableRequest()->set_path(path);
        SendRequestToPipe(pipeClient, request.Release());
    }

    TTabletId GetBSControllerId() {
        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
        TIntrusivePtr<TDomainsInfo::TDomain> domain = domains->Domains.begin()->second;
        ui32 stateStorageGroup = domains->GetDefaultStateStorageGroup(domain->DomainUid);
        return MakeBSControllerID(stateStorageGroup);
    }

    void RequestBSControllerConfig() {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        request->Record.MutableRequest()->AddCommand()->MutableQueryBaseConfig();
        SendRequestToPipe(pipeClient, request.Release());
    }

    void RequestBSControllerInfo() {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        THolder<TEvBlobStorage::TEvRequestControllerInfo> request = MakeHolder<TEvBlobStorage::TEvRequestControllerInfo>();
        SendRequestToPipe(pipeClient, request.Release());
    }

    void RequestBSControllerSelectGroups(THolder<TEvBlobStorage::TEvControllerSelectGroups> request) {
        TActorId pipeClient = ConnectTabletPipe(GetBSControllerId());
        SendRequestToPipe(pipeClient, request.Release());
    }

    void RequestSchemeCacheNavigate(const TString& path) {
        THolder<NSchemeCache::TSchemeCacheNavigate> request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = SplitPath(path);
        entry.RedirectRequired = false;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
        request->ResultSet.emplace_back(entry);
        SendRequest(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    void RequestSchemeCacheNavigate(const TPathId& pathId) {
        THolder<NSchemeCache::TSchemeCacheNavigate> request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.TableId.PathId = pathId;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
        entry.RedirectRequired = false;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
        request->ResultSet.emplace_back(entry);
        SendRequest(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    void InitConfig(const TCgiParameters& params) {
        Followers = FromStringWithDefault(params.Get("followers"), Followers);
        Metrics = FromStringWithDefault(params.Get("metrics"), Metrics);
        WithRetry = FromStringWithDefault(params.Get("with_retry"), WithRetry);
    }

    void ClosePipes() {
        for (const auto& [tabletId, pipeInfo] : PipeInfo) {
            if (pipeInfo.PipeClient) {
                NTabletPipe::CloseClient(TBase::SelfId(), pipeInfo.PipeClient);
            }
        }
        PipeInfo.clear();
    }

    ui32 FailPipeConnect(TTabletId tabletId) {
        auto itPipeInfo = PipeInfo.find(tabletId);
        if (itPipeInfo != PipeInfo.end()) {
            ui32 requests = itPipeInfo->second.Requests;
            NTabletPipe::CloseClient(TBase::SelfId(), itPipeInfo->second.PipeClient);
            PipeInfo.erase(itPipeInfo);
            return requests;
        }
        return 0;
    }

    void RequestDone(ui32 requests = 1) {
        Requests -= requests;
        if (!DelayedRequests.empty()) {
            SendDelayedRequests();
        }
        if (Requests == 0) {
            static_cast<TDerived*>(this)->ReplyAndPassAway();
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            ui32 requests = FailPipeConnect(ev->Get()->TabletId);
            RequestDone(requests);
        }
    }

    void PassAway() override {
        ClosePipes();
        TBase::PassAway();
    }
};

}
}
