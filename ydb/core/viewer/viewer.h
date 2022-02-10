#pragma once

#include <ydb/core/viewer/json/json.h>

#include <ydb/core/tablet/defs.h>
#include <library/cpp/actors/core/defs.h> 
#include <library/cpp/actors/core/actor.h> 
#include <library/cpp/actors/core/event.h> 
#include <ydb/core/driver_lib/run/config.h>
#include <ydb/core/viewer/protos/viewer.pb.h>
#include <util/system/hostname.h>

namespace NKikimr {
namespace NViewer {

inline TActorId MakeViewerID(ui32 node = 0) {
    char x[12] = {'v','i','e','w','e','r'};
    return TActorId(node, TStringBuf(x, 12));
}

IActor* CreateViewer(const TKikimrRunConfig &kikimrRunConfig);

class IViewer {
public:
    struct TBrowseContext {
        struct TPathEntry {
            NKikimrViewer::EObjectType Type;
            TString Name;
        };

        TString Path;
        TVector<TPathEntry> Paths;
        TActorId Owner;
        TString UserToken;

        TBrowseContext(const TActorId owner, const TString& userToken)
            : Owner(owner)
            , UserToken(userToken)
        {}

        const TString& GetMyName() const {
            static TString emptyName;
            if (Paths.empty()) {
                return emptyName;
            }
            return Paths.back().Name;
        }

        NKikimrViewer::EObjectType GetMyType() const {
            if (Paths.empty()) {
                return NKikimrViewer::EObjectType::Unknown;
            }
            return Paths.back().Type;
        }

        const TString& GetParentName() const {
            static TString emptyName;
            if (Paths.size() < 2) {
                return emptyName;
            }
            return (Paths.rbegin() + 1)->Name;
        }

        NKikimrViewer::EObjectType GetParentType() const {
            if (Paths.size() < 2) {
                return NKikimrViewer::EObjectType::Unknown;
            }
            return (Paths.rbegin() + 1)->Type;
        }
    };

    using TVirtualHandlerType = std::function<IActor*(const TActorId& owner, const TBrowseContext& context)>;

    struct TVirtualHandler {
        IViewer::TVirtualHandlerType BrowseHandler = nullptr;

        TVirtualHandler(IViewer::TVirtualHandlerType browseHandler)
            : BrowseHandler(browseHandler)
        {}
    };

    struct TContentRequestContext {
        // request settings
        TJsonSettings JsonSettings;
        TDuration Timeout = TDuration::MilliSeconds(10000);
        TString UserToken;

        // filter
        ui32 Limit = 50;
        ui32 Offset = 0;
        TString Key;

        // path to object
        TString Path;

        // object type and location
        NKikimrViewer::EObjectType Type = NKikimrViewer::Unknown;
        TString ObjectName;

        TString Dump() const;
    };

    using TContentHandler = std::function<IActor*(const TActorId&, const TContentRequestContext&)>;

    virtual const TKikimrRunConfig& GetKikimrRunConfig() const = 0;
    virtual TVector<const TVirtualHandler*> GetVirtualHandlers(NKikimrViewer::EObjectType type, const TString& path) const = 0;
    virtual void RegisterVirtualHandler(NKikimrViewer::EObjectType parentObjectType, TVirtualHandlerType handler) = 0;

    // returns nullptr if no such handler
    virtual TContentHandler GetContentHandler(NKikimrViewer::EObjectType objectType) const = 0;

    virtual void RegisterContentHandler(
        NKikimrViewer::EObjectType objectType,
        const TContentHandler& handler) = 0;

    TString GetHTTPOKJSON();
    TString GetHTTPGATEWAYTIMEOUT();
};

void SetupPQVirtualHandlers(IViewer* viewer);
void SetupDBVirtualHandlers(IViewer* viewer);
void SetupKqpContentHandler(IViewer* viewer);

template <typename RequestType>
struct TJsonRequestSchema {
    static TString GetSchema() { return TString(); }
};

template <typename RequestType>
struct TJsonRequestSummary {
    static TString GetSummary() { return TString(); }
};

template <typename RequestType>
struct TJsonRequestDescription {
    static TString GetDescription() { return TString(); }
};

template <typename RequestType>
struct TJsonRequestParameters {
    static TString GetParameters() { return TString(); }
};

static const char HTTPOKJSON[] = "HTTP/1.1 200 Ok\r\nAccess-Control-Allow-Origin: *\r\nContent-Type: application/json; charset=utf-8\r\nConnection: Close\r\n\r\n";
static const char HTTPOKTEXT[] = "HTTP/1.1 200 Ok\r\nAccess-Control-Allow-Origin: *\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: Close\r\n\r\n";
static const char HTTPFORBIDDENJSON[] = "HTTP/1.1 403 Ok\r\nAccess-Control-Allow-Origin: *\r\nContent-Type: application/json; charset=utf-8\r\nConnection: Close\r\n\r\n";
static const char HTTPGATEWAYTIMEOUT[] = "HTTP/1.1 504 Gateway Time-out\r\nConnection: Close\r\n\r\nGateway Time-out\r\n";
static const char HTTPBADREQUEST[] = "HTTP/1.1 400 Bad Request\r\nConnection: Close\r\n\r\nBad Request\r\n";
static const char HTTPBADREQUEST_HEADERS[] = "HTTP/1.1 400 Bad Request\r\nConnection: Close\r\n\r\n";

template <typename ValueType>
void SplitIds(TStringBuf source, char delim, TVector<ValueType>& values) {
    for (TStringBuf value = source.NextTok(delim); !value.empty(); value = source.NextTok(delim)) {
        if (value == ".") {
            values.emplace_back(ValueType());
        } else {
            values.emplace_back(FromStringWithDefault<ValueType>(value, ValueType()));
        }
    }
}

template <typename ValueType>
void SplitIds(TStringBuf source, char delim, std::vector<ValueType>& values) {
    for (TStringBuf value = source.NextTok(delim); !value.empty(); value = source.NextTok(delim)) {
        if (value == ".") {
            values.emplace_back(ValueType());
        } else {
            values.emplace_back(FromStringWithDefault<ValueType>(value, ValueType()));
        }
    }
}

TString GetHTTPOKJSON();
TString GetHTTPGATEWAYTIMEOUT();
NKikimrViewer::EFlag GetFlagFromTabletState(NKikimrWhiteboard::TTabletStateInfo::ETabletState state);
NKikimrViewer::EFlag GetPDiskStateFlag(const NKikimrWhiteboard::TPDiskStateInfo& info);
NKikimrViewer::EFlag GetPDiskOverallFlag(const NKikimrWhiteboard::TPDiskStateInfo& info);
NKikimrViewer::EFlag GetVDiskOverallFlag(const NKikimrWhiteboard::TVDiskStateInfo& info);

struct TBSGroupState {
    NKikimrViewer::EFlag Overall;
    ui32 MissingDisks = 0;
    ui32 SpaceProblems = 0;
};

NKikimrViewer::EFlag GetBSGroupOverallFlagWithoutLatency(
        const NKikimrWhiteboard::TBSGroupStateInfo& info,
        const TMap<NKikimrBlobStorage::TVDiskID, const NKikimrWhiteboard::TVDiskStateInfo&>& vDisksIndex,
        const TMap<std::pair<ui32, ui32>, const NKikimrWhiteboard::TPDiskStateInfo&>& pDisksIndex);
TBSGroupState GetBSGroupOverallStateWithoutLatency(
        const NKikimrWhiteboard::TBSGroupStateInfo& info,
        const TMap<NKikimrBlobStorage::TVDiskID, const NKikimrWhiteboard::TVDiskStateInfo&>& vDisksIndex,
        const TMap<std::pair<ui32, ui32>, const NKikimrWhiteboard::TPDiskStateInfo&>& pDisksIndex);
NKikimrViewer::EFlag GetBSGroupOverallFlag(
        const NKikimrWhiteboard::TBSGroupStateInfo& info,
        const TMap<NKikimrBlobStorage::TVDiskID, const NKikimrWhiteboard::TVDiskStateInfo&>& vDisksIndex,
        const TMap<std::pair<ui32, ui32>, const NKikimrWhiteboard::TPDiskStateInfo&>& pDisksIndex);
TBSGroupState GetBSGroupOverallState(
        const NKikimrWhiteboard::TBSGroupStateInfo& info,
        const TMap<NKikimrBlobStorage::TVDiskID, const NKikimrWhiteboard::TVDiskStateInfo&>& vDisksIndex,
        const TMap<std::pair<ui32, ui32>, const NKikimrWhiteboard::TPDiskStateInfo&>& pDisksIndex);

NKikimrViewer::EFlag GetFlagFromUsage(double usage);

NKikimrWhiteboard::EFlag GetWhiteboardFlag(NKikimrViewer::EFlag flag);
NKikimrViewer::EFlag GetViewerFlag(NKikimrWhiteboard::EFlag flag);

} // NViewer
} // NKikimr
