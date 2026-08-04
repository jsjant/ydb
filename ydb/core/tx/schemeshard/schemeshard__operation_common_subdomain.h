#pragma once

#include "schemeshard__operation_part.h"

namespace NKikimr::NSchemeShard {

inline bool CheckStoragePoolsInQuotas(
    const Ydb::Cms::DatabaseQuotas& quotas,
    const TVector<TStoragePool>& pools,
    const TString& path,
    TString& error
) {
    TVector<TString> quotedKinds;
    quotedKinds.reserve(quotas.storage_quotas_size());
    for (const auto& storageQuota : quotas.storage_quotas()) {
        quotedKinds.emplace_back(storageQuota.unit_kind());
    }
    Sort(quotedKinds);
    if (const auto equalKinds = AdjacentFind(quotedKinds);
        equalKinds != quotedKinds.end()
    ) {
        error = TStringBuilder()
            << "Malformed subdomain request: storage kinds in DatabaseQuotas must be unique, but "
            << *equalKinds << " appears twice in the quotas definition of the " << path << " subdomain.";
        return false;
    }

    TVector<TString> existingKinds;
    existingKinds.reserve(pools.size());
    for (const auto& pool : pools) {
        existingKinds.emplace_back(pool.GetKind());
    }
    Sort(existingKinds);
    TVector<TString> unknownKinds;
    SetDifference(quotedKinds.begin(), quotedKinds.end(),
                  existingKinds.begin(), existingKinds.end(),
                  std::back_inserter(unknownKinds)
    );
    if (!unknownKinds.empty()) {
        error = TStringBuilder()
            << "Malformed subdomain request: cannot set storage quotas of the following kinds: " << JoinSeq(", ", unknownKinds)
            << ", because no storage pool in the subdomain " << path << " has the specified kinds. "
            << "Existing storage kinds are: " << JoinSeq(", ", existingKinds);
        return false;
    }
    return true;
}

inline bool CheckDetailedMetricsSettingsAllowedOnDatabase(bool isRootDomain, TStringBuf setting, TString& error) {
    if (isRootDomain) {
        error = TStringBuilder() << setting << " cannot be set on the root database";
        return false;
    }
    return true;
}

// Validates the database-wide default detailed metrics level
// (TABLES_METRICS_LEVEL) coming in with a (ext)subdomain create/alter request.
// MetricsLevelUnspecified is accepted: at the database level it means "no
// default", so it is also how an existing default is cleared.
inline bool CheckTablesMetricsLevel(ETablesMetricsLevel level, bool isRootDomain, TString& error) {
    if (!AppData()->FeatureFlags.GetEnableDataShardDetailedMetrics()) {
        error = "Detailed metrics are disabled (EnableDataShardDetailedMetrics feature flag is off)";
        return false;
    }

    if (!CheckDetailedMetricsSettingsAllowedOnDatabase(isRootDomain, "TABLES_METRICS_LEVEL", error)) {
        return false;
    }

    switch (level) {
    case NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelUnspecified:
    case NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelDisabled:
    case NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable:
    case NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition:
        return true;
    default:
        error = TStringBuilder() << "Unknown TABLES_METRICS_LEVEL: " << static_cast<ui32>(level);
        return false;
    }
}

// Validates monitoring_project_id coming in with a (ext)subdomain
// create/alter request. It's a plain label value: the only requirement here
// is that an explicit request doesn't clear it to an empty string. Whether it
// must be set at all (before detailed metrics can be enabled) is enforced
// elsewhere, not here.
inline bool CheckMonitoringProjectId(const TString& monitoringProjectId, bool isRootDomain, TString& error) {
    if (monitoringProjectId.empty()) {
        error = "monitoring_project_id cannot be set to an empty string";
        return false;
    }
    return CheckDetailedMetricsSettingsAllowedOnDatabase(isRootDomain, "monitoring_project_id", error);
}

namespace NSubDomainState {

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "NSubDomainState::TConfigureParts"
                << " operationId# " << OperationId;
    }
public:
    TConfigureParts(TOperationId id);

    bool ProgressState(TOperationContext& context) override;
    bool HandleReply(TEvSchemeShard::TEvInitTenantSchemeShardResult__HandlePtr& ev, TOperationContext& context) override;
    bool HandleReply(TEvSubDomain::TEvConfigureStatus__HandlePtr& ev, TOperationContext& context) override;
};

class TPropose: public TSubOperationState {
private:
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "NSubDomainState::TPropose"
                << " operationId# " << OperationId;
    }

public:
    TPropose(TOperationId id);

    bool ProgressState(TOperationContext& context) override;
    bool HandleReply(TEvPrivate::TEvOperationPlan__HandlePtr& ev, TOperationContext& context) override;
};

} // namespace NSubDomainState

} // namespace NKikimr::NSchemeShard
