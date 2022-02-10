#pragma once

#include "scheme_type_id.h"

#include <library/cpp/actors/core/actorid.h> 
#include <ydb/core/scheme_types/scheme_types_defs.h>

#include <util/stream/output.h>


#define KIKIMR_FOREACH_TYPE(xx, ...) \
    KIKIMR_FOREACH_MINIKQL_TYPE(xx, __VA_ARGS__) \
    xx(ActorId, TActorId, __VA_ARGS__) \
    xx(StepOrderId, TStepOrderId, __VA_ARGS__) \
    /**/

namespace NKikimr {
namespace NScheme {

////////////////////////////////////////////////////////
/// other internal types
/// 0x2001 - 0x8000
/// DO NOT FORGET TO REGISTER THE TYPES in Library::OpenLibrary() / file tablet_library.h
namespace NNames {
    extern const char ActorID[8];
}

class TActorId : public TTypedType<NActors::TActorId, TActorId, NTypeIds::ActorId, NNames::ActorID>
{
public:
};

namespace NNames {
    extern const char StepOrderId[12];
}

class TStepOrderId : public IIntegerPair<ui64, ui64, NTypeIds::StepOrderId, NNames::StepOrderId> {};

////////////////////////////////////////////////////////
/// user types
/// 0x8001 - 0xFFFF
/// DO NOT FORGET TO REGISTER THE TYPES in Library::OpenLibrary() / file tablet_library.h

// todo: range enum

////////////////////////////////////////////////////////
/// 0x10000 - 0xFFFFFFFF reserved
/// DO NOT FORGET TO REGISTER THE TYPES in Library::OpenLibrary() / file tablet_library.h


////////////////////////////////////////////////////////

inline ui32 GetFixedSize(NKikimr::NScheme::TTypeId typeId) {
    switch (typeId) {
#define KIKIMR_TYPE_MACRO(typeEnum, typeType, ...) case NTypeIds::typeEnum: return typeType::GetFixedSize();
    KIKIMR_FOREACH_TYPE(KIKIMR_TYPE_MACRO)
#undef KIKIMR_TYPE_MACRO
    default:
        return 0;
    }
}

} // namespace NScheme
} // namespace NKikimr
