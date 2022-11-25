#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/services/metadata/abstract/decoder.h>
#include <ydb/services/metadata/service.h>
#include <ydb/services/metadata/manager/object.h>
#include <ydb/services/metadata/manager/preparation_controller.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NColumnShard::NTiers {

class TTieringInterval {
private:
    YDB_ACCESSOR_DEF(TString, TierName);
    YDB_ACCESSOR_DEF(TDuration, DurationForEvict);
public:
    TTieringInterval() = default;
    TTieringInterval(const TString& name, const TDuration d)
        : TierName(name)
        , DurationForEvict(d)
    {

    }

    bool operator<(const TTieringInterval& item) const {
        return DurationForEvict < item.DurationForEvict;
    }

    NJson::TJsonValue SerializeToJson() const {
        NJson::TJsonValue result;
        result.InsertValue("tierName", TierName);
        result.InsertValue("durationForEvict", DurationForEvict.ToString());
        return result;
    }

    bool DeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
        if (!jsonInfo["tierName"].GetString(&TierName)) {
            return false;
        }
        const TString dStr = jsonInfo["durationForEvict"].GetStringRobust();
        if (!TDuration::TryParse(dStr, DurationForEvict)) {
            return false;
        }
        return true;
    }
};

class TTieringRule: public NMetadataManager::TObject<TTieringRule> {
private:
    YDB_ACCESSOR_DEF(TString, TieringRuleId);
    YDB_ACCESSOR_DEF(TString, DefaultColumn);
    YDB_ACCESSOR_DEF(TVector<TTieringInterval>, Intervals);
protected:
    NJson::TJsonValue SerializeDescriptionToJson() const;
    bool DeserializeDescriptionFromJson(const NJson::TJsonValue& jsonInfo);
public:
    bool ContainsTier(const TString& tierName) const;

    void AddInterval(const TString& name, const TDuration evDuration) {
        Intervals.emplace_back(TTieringInterval(name, evDuration));
    }

    static TString GetTypeId() {
        return "TIERING_RULE";
    }

    static TString GetStorageTablePath();
    static void AlteringPreparation(std::vector<TTieringRule>&& objects,
        NMetadataManager::IAlterPreparationController<TTieringRule>::TPtr controller,
        const NMetadata::IOperationsManager::TModificationContext& context);

    NJson::TJsonValue GetDebugJson() const;

    class TDecoder: public NInternal::TDecoderBase {
    private:
        YDB_READONLY(i32, TieringRuleIdIdx, -1);
        YDB_READONLY(i32, DefaultColumnIdx, -1);
        YDB_READONLY(i32, DescriptionIdx, -1);
    public:
        static inline const TString TieringRuleId = "tieringRuleId";
        static inline const TString DefaultColumn = "defaultColumn";
        static inline const TString Description = "description";

        static std::vector<Ydb::Column> GetPKColumns();
        static std::vector<Ydb::Column> GetColumns();
        static std::vector<TString> GetPKColumnIds();

        TDecoder(const Ydb::ResultSet& rawData) {
            TieringRuleIdIdx = GetFieldIndex(rawData, TieringRuleId);
            DefaultColumnIdx = GetFieldIndex(rawData, DefaultColumn);
            DescriptionIdx = GetFieldIndex(rawData, Description);
        }
    };
    NKikimr::NMetadataManager::TTableRecord SerializeToRecord() const;
    bool DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& r);
    static NMetadata::TOperationParsingResult BuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
        const NMetadata::IOperationsManager::TModificationContext& context);
    NKikimr::NOlap::TTiersInfo BuildTiersInfo() const;
};

}
