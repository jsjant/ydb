#include "tablet_counters_aggregator.h"
#include "private/labeled_db_counters.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/path.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/interconnect.h>

#include <util/generic/array_size.h>
#include <util/string/cast.h>

namespace NKikimr {

using namespace NActors;

void TestHeavy(const ui32 v, ui32 numWorkers) {

    TInstant t(Now());

    TVector<TActorId> cc;
    TActorId aggregatorId;
    TTestBasicRuntime runtime(1);
    constexpr int NODES = 10;
    constexpr int GROUPS = 1000;
    constexpr int VALUES = 20;

    runtime.Initialize(TAppPrepare().Unwrap());
    TActorId edge = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TABLET_AGGREGATOR, NActors::NLog::PRI_DEBUG);

    runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev){
        if (ev->GetTypeRewrite() == TEvInterconnect::EvNodesInfo && ev->Sender != edge) {
            return TTestActorRuntime::EEventAction::DROP;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    });

    IActor* aggregator = CreateClusterLabeledCountersAggregatorActor(edge, TTabletTypes::PersQueue, v, TString(), numWorkers);
    aggregatorId = runtime.Register(aggregator);

    if (numWorkers == 0) {
        cc.push_back(aggregatorId);
        ++numWorkers;
    }

    runtime.SetRegistrationObserverFunc([&cc, &aggregatorId](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId) {
                TTestActorRuntime::DefaultRegistrationObserver(runtime, parentId, actorId);
                if (parentId == aggregatorId) {
                    cc.push_back(actorId);
                }
            });

    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, numWorkers);
    runtime.DispatchEvents(options);
    for (const auto& a : cc) {
        auto nodes = MakeIntrusive<TIntrusiveVector<TEvInterconnect::TNodeInfo>>();
        for (auto i = 1; i <= NODES; ++i) {
            nodes->emplace_back(TEvInterconnect::TNodeInfo(i, "::", "localhost", "localhost", 1234, TNodeLocation()));
        }
        THolder<TEvInterconnect::TEvNodesInfo> nodesInfo = MakeHolder<TEvInterconnect::TEvNodesInfo>(nodes);
        runtime.Send(new NActors::IEventHandle(a, edge, nodesInfo.Release()), 0, true);
    }

    for (auto i = 1; i <= NODES; ++i) {
        THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse> response = MakeHolder<TEvTabletCounters::TEvTabletLabeledCountersResponse>();
        for (auto k = 0; k < GROUPS; ++k) {
            char delim = (k % 2 == 0) ? '/' : '|';
            auto& group1 = *response->Record.AddLabeledCountersByGroup();
            group1.SetGroup(Sprintf("group%d%c%d", i, delim, k));
            group1.SetGroupNames(Sprintf("A%cB", delim));
            if (k % 4 != 0)
                group1.SetDelimiter(TStringBuilder() << delim);
            for (auto j = 0; j < VALUES; ++j) {
                auto& counter1 = *group1.AddLabeledCounter();
                counter1.SetName(Sprintf("value%d", j));
                counter1.SetValue(13);
                counter1.SetType(TLabeledCounterOptions::CT_SIMPLE);
                counter1.SetAggregateFunc(TLabeledCounterOptions::EAF_SUM);
            }
        }
        Cerr << "Sending message to " << cc[i % numWorkers] << " from " << aggregatorId <<  " id " << i << "\n";
        runtime.Send(new NActors::IEventHandle(cc[i % numWorkers], aggregatorId, response.Release(), 0, i), 0, true);
    }
    {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvInterconnect::EvNodesInfo, numWorkers);
        runtime.DispatchEvents(options, TDuration::Seconds(1));
    }

    THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse> response = runtime.GrabEdgeEvent<TEvTabletCounters::TEvTabletLabeledCountersResponse>();

    UNIT_ASSERT(response != nullptr);
    UNIT_ASSERT_VALUES_EQUAL(response->Record.LabeledCountersByGroupSize(), NODES * GROUPS);

    Cerr << "TEST " << v << " " << numWorkers << " duration " << TInstant::Now() - t << "\n";
}

Y_UNIT_TEST_SUITE(TTabletCountersAggregator) {

    struct TTabletWithHist {
        TTabletWithHist(ui64 tabletId, const TTabletTypes::EType tabletType)
            : TabletId(tabletId)
            , TenantPathId(1113, 1001)
            , CounterEventsInFlight(new TEvTabletCounters::TInFlightCookie)
            , TabletType(tabletType)
            , ExecutorCounters(new TTabletCountersBase)
        {
            auto simpleCount = sizeof(SimpleCountersMetaInfo) / sizeof(SimpleCountersMetaInfo[0]);
            auto percentileCount = sizeof(PercentileCountersMetaInfo) / sizeof(PercentileCountersMetaInfo[0]);
            AppCounters.reset(new TTabletCountersBase(
                simpleCount,
                0, // cumulativeCnt
                percentileCount,
                SimpleCountersMetaInfo,
                nullptr, // cumulative meta
                PercentileCountersMetaInfo));

            for (auto i: xrange(percentileCount))
                AppCounters->Percentile()[i].Initialize(RangeDefs[i].first, RangeDefs[i].second, true);

            AppCountersBaseline.reset(new TTabletCountersBase());
            AppCounters->RememberCurrentStateAsBaseline(*AppCountersBaseline);

            ExecutorCountersBaseline.reset(new TTabletCountersBase());
            ExecutorCounters->RememberCurrentStateAsBaseline(*ExecutorCountersBaseline);
        }

        void SendUpdate(TTestBasicRuntime& runtime, const TActorId& aggregatorId, const TActorId& sender) {
            auto executorCounters = ExecutorCounters->MakeDiffForAggr(*ExecutorCountersBaseline);
            ExecutorCounters->RememberCurrentStateAsBaseline(*ExecutorCountersBaseline);

            auto appCounters = AppCounters->MakeDiffForAggr(*AppCountersBaseline);
            AppCounters->RememberCurrentStateAsBaseline(*AppCountersBaseline);

            runtime.Send(new IEventHandle(aggregatorId, sender, new TEvTabletCounters::TEvTabletAddCounters(
                CounterEventsInFlight, TabletId, TabletType, TenantPathId, executorCounters, appCounters)));

            // force recalc
            runtime.Send(new IEventHandle(aggregatorId, sender, new NActors::TEvents::TEvWakeup()));
        }

        void ForgetTablet(TTestBasicRuntime& runtime, const TActorId& aggregatorId, const TActorId& sender) {
            runtime.Send(new IEventHandle(
                aggregatorId,
                sender,
                new TEvTabletCounters::TEvTabletCountersForgetTablet(TabletId, TabletType, TenantPathId)));

            // force recalc
            runtime.Send(new IEventHandle(aggregatorId, sender, new NActors::TEvents::TEvWakeup()));
        }

        void SetSimpleCount(const char* name, ui64 count) {
            size_t index = SimpleNameToIndex(name);
            AppCounters->Simple()[index].Set(count);
        }

        void UpdatePercentile(const char* name, ui64 what) {
            size_t index = PercentileNameToIndex(name);
            AppCounters->Percentile()[index].IncrementFor(what);
        }

        void UpdatePercentile(const char* name, ui64 what, ui64 value) {
            size_t index = PercentileNameToIndex(name);
            AppCounters->Percentile()[index].AddFor(what, value);
        }

    public:
        static ::NMonitoring::TDynamicCounterPtr GetAppCounters(TTestBasicRuntime& runtime, const TTabletTypes::EType tabletType) {
            ::NMonitoring::TDynamicCounterPtr counters = runtime.GetAppData(0).Counters;
            UNIT_ASSERT(counters);

            TString tabletTypeStr = TTabletTypes::TypeToStr(tabletType);
            auto dsCounters = counters->GetSubgroup("counters", "tablets")->GetSubgroup("type", tabletTypeStr);
            return dsCounters->GetSubgroup("category", "app");
        }

        template <typename TArray>
        static size_t StringToIndex(const char* name, const TArray& array) {
            size_t i = 0;
            for (const auto& s: array) {
                if (TStringBuf(name) == TStringBuf(s))
                    return i;
                ++i;
            }
            return i;
        }

        static size_t SimpleNameToIndex(const char* name) {
            return StringToIndex(name, SimpleCountersMetaInfo);
        }

        static size_t PercentileNameToIndex(const char* name) {
            return StringToIndex(name, PercentileCountersMetaInfo);
        }

        static NMonitoring::THistogramPtr GetHistogram(TTestBasicRuntime& runtime, const char* name, const TTabletTypes::EType tabletType) {
            size_t index = PercentileNameToIndex(name);
           return GetAppCounters(runtime, tabletType)->FindHistogram(PercentileCountersMetaInfo[index]);
        }

        static void CheckHistogram(
            TTestBasicRuntime& runtime,
            const char* name,
            const std::vector<ui64>& goldValues,
            const TTabletTypes::EType tabletType
        )
        {
            auto histogram = TTabletWithHist::GetHistogram(runtime, name, tabletType);
            UNIT_ASSERT(histogram);
            auto snapshot = histogram->Snapshot();
            UNIT_ASSERT(snapshot);

            UNIT_ASSERT_VALUES_EQUAL(snapshot->Count(), goldValues.size());
            {
                // for pretty printing the diff
                std::vector<ui64> values;
                values.reserve(goldValues.size());
                for (auto i: xrange(goldValues.size()))
                    values.push_back(snapshot->Value(i));
                UNIT_ASSERT_VALUES_EQUAL(values, goldValues);
            }
        }

    public:
        ui64 TabletId;
        TPathId TenantPathId;
        TIntrusivePtr<TEvTabletCounters::TInFlightCookie> CounterEventsInFlight;
        const TTabletTypes::EType TabletType;

        std::unique_ptr<TTabletCountersBase> ExecutorCounters;
        std::unique_ptr<TTabletCountersBase> ExecutorCountersBaseline;

        std::unique_ptr<TTabletCountersBase> AppCounters;
        std::unique_ptr<TTabletCountersBase> AppCountersBaseline;

    public:
        static constexpr TTabletPercentileCounter::TRangeDef RangeDefs1[] = {
            {0,   "0"}
        };

        static constexpr TTabletPercentileCounter::TRangeDef RangeDefs4[] = {
            {0,   "0"},
            {1,   "1"},
            {13,  "13"},
            {29,  "29"}
        };

        static constexpr std::pair<const TTabletPercentileCounter::TRangeDef*, size_t> RangeDefs[] = {
            {RangeDefs1, 1},
            {RangeDefs4, 4},
            {RangeDefs1, 1},
            {RangeDefs4, 4},
        };

        static constexpr const char* PercentileCountersMetaInfo[] = {
            "MyHistSingleBucket",
            "HIST(Count)",
            "HIST(CountSingleBucket)",
            "MyHist",
        };

        static constexpr const char* SimpleCountersMetaInfo[] = {
            "JustCount1",
            "Count",
            "CountSingleBucket",
            "JustCount2",
        };
    };

    Y_UNIT_TEST(IntegralPercentileAggregationHistNamedSingleBucket) {
        // test case when only 1 range in hist
        // histogram with name "HIST(CountSingleBucket)" and
        // associated corresponding simple counter "CountSingleBucket"
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        TActorId edge = runtime.AllocateEdgeActor();

        auto aggregator = CreateTabletCountersAggregator(false);
        auto aggregatorId = runtime.Register(aggregator);
        runtime.EnableScheduleForActor(aggregatorId);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);

        TTabletWithHist tablet1(1, TTabletTypes::Dummy);

        tablet1.SetSimpleCount("CountSingleBucket", 1);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist tablet2(2, TTabletTypes::Dummy);
        tablet2.SetSimpleCount("CountSingleBucket", 13);
        tablet2.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(CountSingleBucket)",
            {0, 2},
            TTabletTypes::Dummy
        );

        // sanity check we didn't mess other histograms

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 0, 0, 0, 0},
            TTabletTypes::Dummy
        );

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {2, 0, 0, 0, 0},
            TTabletTypes::Dummy
        );

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHistSingleBucket",
            {0, 0},
            TTabletTypes::Dummy
        );
    }

    Y_UNIT_TEST(IntegralPercentileAggregationHistNamed) {
        // test special histogram with name "HIST(Count)" and
        // associated corresponding simple counter "Count"
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        TActorId edge = runtime.AllocateEdgeActor();

        auto aggregator = CreateTabletCountersAggregator(false);
        auto aggregatorId = runtime.Register(aggregator);
        runtime.EnableScheduleForActor(aggregatorId);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);

        TTabletWithHist tablet1(1, TTabletTypes::Dummy);

        tablet1.SetSimpleCount("Count", 1);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {0, 1, 0, 0, 0},
            TTabletTypes::Dummy
        );

        TTabletWithHist tablet2(2, TTabletTypes::Dummy);
        tablet2.SetSimpleCount("Count", 13);
        tablet2.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {0, 1, 1, 0, 0},
            TTabletTypes::Dummy
        );

        TTabletWithHist tablet3(3, TTabletTypes::Dummy);
        tablet3.SetSimpleCount("Count", 1);
        tablet3.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {0, 2, 1, 0, 0},
            TTabletTypes::Dummy
        );

        tablet3.SetSimpleCount("Count", 13);
        tablet3.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {0, 1, 2, 0, 0},
            TTabletTypes::Dummy
        );

        tablet3.ForgetTablet(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {0, 1, 1, 0, 0},
            TTabletTypes::Dummy
        );

        // sanity check we didn't mess other histograms

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 0, 0, 0, 0},
            TTabletTypes::Dummy
        );

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(CountSingleBucket)",
            {2, 0},
            TTabletTypes::Dummy
        );

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHistSingleBucket",
            {0, 0},
            TTabletTypes::Dummy
        );
    }

    Y_UNIT_TEST(IntegralPercentileAggregationHistNamedNoOverflowCheck) {
        // test special histogram with name "HIST(Count)" and
        // associated corresponding simple counter "Count"
        //
        // test just for extra sanity, because for Max<ui32> in bucket we
        // will need Max<ui32> tablets. So just check simple count behaviour
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        TActorId edge = runtime.AllocateEdgeActor();

        auto aggregator = CreateTabletCountersAggregator(false);
        auto aggregatorId = runtime.Register(aggregator);
        runtime.EnableScheduleForActor(aggregatorId);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);

        TTabletWithHist tablet1(1, TTabletTypes::Dummy);

        tablet1.SetSimpleCount("Count", Max<i64>() - 100UL);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {0, 0, 0, 0, 1},
            TTabletTypes::Dummy
        );

        TTabletWithHist tablet2(2, TTabletTypes::Dummy);
        tablet2.SetSimpleCount("Count", 100);
        tablet2.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {0, 0, 0, 0, 2},
            TTabletTypes::Dummy
        );
    }

    Y_UNIT_TEST(IntegralPercentileAggregationRegularCheckSingleTablet) {
        // test regular histogram, i.e. not named "HIST"
        // check that when single tablet sends multiple count updates,
        // the aggregated value is correct
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        TActorId edge = runtime.AllocateEdgeActor();

        auto aggregator = CreateTabletCountersAggregator(false);
        auto aggregatorId = runtime.Register(aggregator);
        runtime.EnableScheduleForActor(aggregatorId);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);

        TTabletWithHist tablet1(1, TTabletTypes::Dummy);
        tablet1.UpdatePercentile("MyHist", 1);
        tablet1.SendUpdate(runtime, aggregatorId, edge);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 1, 0, 0, 0},
            TTabletTypes::Dummy
        );

        tablet1.UpdatePercentile("MyHist", 13);
        tablet1.SendUpdate(runtime, aggregatorId, edge);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 1, 1, 0, 0},
            TTabletTypes::Dummy
        );

        tablet1.UpdatePercentile("MyHist", 1);
        tablet1.UpdatePercentile("MyHist", 1);
        tablet1.UpdatePercentile("MyHist", 100);
        tablet1.SendUpdate(runtime, aggregatorId, edge);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 3, 1, 0, 1},
            TTabletTypes::Dummy
        );
    }

    // Regression test for KIKIMR-13457
    Y_UNIT_TEST(IntegralPercentileAggregationRegular) {
        // test regular histogram, i.e. not named "HIST"
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        TActorId edge = runtime.AllocateEdgeActor();

        auto aggregator = CreateTabletCountersAggregator(false);
        auto aggregatorId = runtime.Register(aggregator);
        runtime.EnableScheduleForActor(aggregatorId);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);

        TTabletWithHist tablet1(1, TTabletTypes::Dummy);
        tablet1.UpdatePercentile("MyHist", 1);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist tablet2(2, TTabletTypes::Dummy);
        tablet2.UpdatePercentile("MyHist", 1);
        tablet2.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist tablet3(3, TTabletTypes::Dummy);
        tablet3.UpdatePercentile("MyHist", 1);
        tablet3.UpdatePercentile("MyHist", 13);
        tablet3.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 3, 1, 0, 0},
            TTabletTypes::Dummy
        );

        tablet3.ForgetTablet(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 2, 0, 0, 0},
            TTabletTypes::Dummy
        );

        // sanity check we didn't mess other histograms

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {2, 0, 0, 0, 0},
            TTabletTypes::Dummy
        );

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHistSingleBucket",
            {0, 0},
            TTabletTypes::Dummy
        );

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(CountSingleBucket)",
            {2, 0},
            TTabletTypes::Dummy
        );
    }

    Y_UNIT_TEST(IntegralPercentileAggregationRegularNoOverflowCheck) {
        // test regular histogram, i.e. not named "HIST"
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        TActorId edge = runtime.AllocateEdgeActor();

        auto aggregator = CreateTabletCountersAggregator(false);
        auto aggregatorId = runtime.Register(aggregator);
        runtime.EnableScheduleForActor(aggregatorId);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);

        TTabletWithHist tablet1(1, TTabletTypes::Dummy);
        tablet1.UpdatePercentile("MyHist", 10, Max<i64>() - 100);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist tablet2(2, TTabletTypes::Dummy);
        tablet2.UpdatePercentile("MyHist", 10, 25);
        tablet2.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist tablet3(3, TTabletTypes::Dummy);
        tablet3.UpdatePercentile("MyHist", 10, 5);
        tablet3.SendUpdate(runtime, aggregatorId, edge);

        ui64 v = Max<i64>() - 70;
        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 0, v, 0, 0},
            TTabletTypes::Dummy
        );

        tablet1.ForgetTablet(runtime, aggregatorId, edge);
        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 0, 30, 0, 0},
            TTabletTypes::Dummy
        );
    }

    Y_UNIT_TEST(ColumnShardCounters) {
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        TActorId edge = runtime.AllocateEdgeActor();

        auto aggregator = CreateTabletCountersAggregator(false);
        auto aggregatorId = runtime.Register(aggregator);
        runtime.EnableScheduleForActor(aggregatorId);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);

        TTabletWithHist tablet1(1, TTabletTypes::ColumnShard);

        tablet1.SetSimpleCount("Count", 1);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {0, 1, 0, 0, 0},
            tablet1.TabletType
        );
    }
}

Y_UNIT_TEST_SUITE(TTabletLabeledCountersAggregator) {
    Y_UNIT_TEST(SimpleAggregation) {
        TVector<TActorId> cc;
        TActorId aggregatorId;

        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        TActorId edge = runtime.AllocateEdgeActor();

        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev){
            if (ev->GetTypeRewrite() == TEvInterconnect::EvNodesInfo && ev->Sender != edge) {
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        IActor* aggregator = CreateClusterLabeledCountersAggregatorActor(edge, TTabletTypes::PersQueue, 2, TString(), 3);
        aggregatorId = runtime.Register(aggregator);

        runtime.SetRegistrationObserverFunc([&cc, &aggregatorId](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId) {
                TTestActorRuntime::DefaultRegistrationObserver(runtime, parentId, actorId);
                    if (parentId == aggregatorId) {
                        cc.push_back(actorId);
                    }
                });

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);
        for (const auto& a : cc) {
            auto nodes = MakeIntrusive<TIntrusiveVector<TEvInterconnect::TNodeInfo>>();
            nodes->emplace_back(TEvInterconnect::TNodeInfo(1, "::", "localhost", "localhost", 1234, TNodeLocation()));
            nodes->emplace_back(TEvInterconnect::TNodeInfo(2, "::", "localhost", "localhost", 1234, TNodeLocation()));
            nodes->emplace_back(TEvInterconnect::TNodeInfo(3, "::", "localhost", "localhost", 1234, TNodeLocation()));
            THolder<TEvInterconnect::TEvNodesInfo> nodesInfo = MakeHolder<TEvInterconnect::TEvNodesInfo>(nodes);
            runtime.Send(new NActors::IEventHandle(a, edge, nodesInfo.Release()), 0, true);
        }

        {
            THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse> response = MakeHolder<TEvTabletCounters::TEvTabletLabeledCountersResponse>();
            auto& group1 = *response->Record.AddLabeledCountersByGroup();
            group1.SetGroup("group1|group2");
            group1.SetGroupNames("AAA|BBB");
            group1.SetDelimiter("|");
            auto& counter1 = *group1.AddLabeledCounter();
            counter1.SetName("value1");
            counter1.SetValue(13);
            counter1.SetType(TLabeledCounterOptions::CT_SIMPLE);
            counter1.SetAggregateFunc(TLabeledCounterOptions::EAF_SUM);
            runtime.Send(new NActors::IEventHandle(cc[0], edge, response.Release(), 0, 1), 0, true);
        }

        {
            THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse> response = MakeHolder<TEvTabletCounters::TEvTabletLabeledCountersResponse>();
            response->Record.AddCounterNames("value1");
            auto& group1 = *response->Record.AddLabeledCountersByGroup();
            group1.SetGroup("group1|group2");
            group1.SetGroupNames("AAA|BBB");
            group1.SetDelimiter("|");
            auto& counter1 = *group1.AddLabeledCounter();
            counter1.SetNameId(0);
            counter1.SetValue(13);
            counter1.SetType(TLabeledCounterOptions::CT_SIMPLE);
            counter1.SetAggregateFunc(TLabeledCounterOptions::EAF_SUM);
            runtime.Send(new NActors::IEventHandle(cc[1], edge, response.Release(), 0, 2), 0, true);
        }

        {
            THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse> response = MakeHolder<TEvTabletCounters::TEvTabletLabeledCountersResponse>();
            response->Record.AddCounterNames("value1");
            auto& group1 = *response->Record.AddLabeledCountersByGroup();
            group1.SetGroup("group1|group2");
            group1.SetGroupNames("AAA|BBB");
            group1.SetDelimiter("|");
            auto& counter1 = *group1.AddLabeledCounter();
            counter1.SetNameId(0);
            counter1.SetValue(13);
            counter1.SetType(TLabeledCounterOptions::CT_SIMPLE);
            counter1.SetAggregateFunc(TLabeledCounterOptions::EAF_SUM);
            runtime.Send(new NActors::IEventHandle(cc[2], edge, response.Release(), 0, 3), 0, true);
        }

        runtime.DispatchEvents();
        THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse> response = runtime.GrabEdgeEvent<TEvTabletCounters::TEvTabletLabeledCountersResponse>();
#ifndef NDEBUG
        Cerr << response->Record.DebugString() << Endl;
#endif
        UNIT_ASSERT(response != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(response->Record.LabeledCountersByGroupSize(), 1);
        const auto& group1 = response->Record.GetLabeledCountersByGroup(0);
        UNIT_ASSERT_VALUES_EQUAL(group1.GetGroup(), "group1/group2");
        UNIT_ASSERT_VALUES_EQUAL(group1.LabeledCounterSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(group1.LabeledCounterSize(), 1);
        const auto& counter1 = group1.GetLabeledCounter(0);
        UNIT_ASSERT_VALUES_EQUAL(counter1.GetNameId(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counter1.GetValue(), 39);
    }

    Y_UNIT_TEST(HeavyAggregation) {
        TestHeavy(2, 10);
        TestHeavy(2, 20);
        TestHeavy(2, 1);
        TestHeavy(2, 0);
    }

    Y_UNIT_TEST(Version3Aggregation) {
        TVector<TActorId> cc;
        TActorId aggregatorId;

        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        // NOTE(shmel1k@): KIKIMR-14221
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);

        TActorId edge = runtime.AllocateEdgeActor();

        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev){
            if (ev->GetTypeRewrite() == TEvInterconnect::EvNodesInfo && ev->Sender != edge) {
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        IActor* aggregator = CreateClusterLabeledCountersAggregatorActor(edge, TTabletTypes::PersQueue, 3, "rt3.*--*,cons*/*/rt.*--*", 3);
        aggregatorId = runtime.Register(aggregator);

        runtime.SetRegistrationObserverFunc([&cc, &aggregatorId](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId) {
                TTestActorRuntime::DefaultRegistrationObserver(runtime, parentId, actorId);
                    if (parentId == aggregatorId) {
                        cc.push_back(actorId);
                    }
                });

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);
        for (const auto& a : cc) {
            auto nodes = MakeIntrusive<TIntrusiveVector<TEvInterconnect::TNodeInfo>>();
            nodes->emplace_back(TEvInterconnect::TNodeInfo(1, "::", "localhost", "localhost", 1234, TNodeLocation()));
            nodes->emplace_back(TEvInterconnect::TNodeInfo(2, "::", "localhost", "localhost", 1234, TNodeLocation()));
            nodes->emplace_back(TEvInterconnect::TNodeInfo(3, "::", "localhost", "localhost", 1234, TNodeLocation()));
            THolder<TEvInterconnect::TEvNodesInfo> nodesInfo = MakeHolder<TEvInterconnect::TEvNodesInfo>(nodes);
            runtime.Send(new NActors::IEventHandle(a, edge, nodesInfo.Release()), 0, true);
        }

        {
            THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse> response = MakeHolder<TEvTabletCounters::TEvTabletLabeledCountersResponse>();
            auto& group1 = *response->Record.AddLabeledCountersByGroup();
            group1.SetGroup("rt3.man--aba@caba--daba");
            group1.SetGroupNames("topic");
            group1.SetDelimiter("/");
            auto& counter1 = *group1.AddLabeledCounter();
            counter1.SetName("value1");
            counter1.SetValue(13);
            counter1.SetType(TLabeledCounterOptions::CT_SIMPLE);
            counter1.SetAggregateFunc(TLabeledCounterOptions::EAF_SUM);
            runtime.Send(new NActors::IEventHandle(cc[0], edge, response.Release(), 0, 1), 0, true);
        }

        {
            THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse> response = MakeHolder<TEvTabletCounters::TEvTabletLabeledCountersResponse>();
            response->Record.AddCounterNames("value1");
            auto& group1 = *response->Record.AddLabeledCountersByGroup();
            group1.SetGroup("cons@aaa/1/rt3.man--aba@caba--daba");
            group1.SetGroupNames("consumer/important/topic");
            group1.SetDelimiter("/");
            auto& counter1 = *group1.AddLabeledCounter();
            counter1.SetNameId(0);
            counter1.SetValue(13);
            counter1.SetType(TLabeledCounterOptions::CT_SIMPLE);
            counter1.SetAggregateFunc(TLabeledCounterOptions::EAF_SUM);
            runtime.Send(new NActors::IEventHandle(cc[1], edge, response.Release(), 0, 2), 0, true);
        }

        runtime.DispatchEvents();
        THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse> response = runtime.GrabEdgeEvent<TEvTabletCounters::TEvTabletLabeledCountersResponse>();
#ifndef NDEBUG
        Cerr << response->Record.DebugString() << Endl;
#endif
        UNIT_ASSERT(response != nullptr);
        Cerr << response->Record;
        UNIT_ASSERT_VALUES_EQUAL(response->Record.LabeledCountersByGroupSize(), 2);
        const auto& group1 = response->Record.GetLabeledCountersByGroup(1);
        const auto& group2 = response->Record.GetLabeledCountersByGroup(0);
        TVector<TString> res = {group1.GetGroup(), group2.GetGroup()};
        std::sort(res.begin(), res.end());

        UNIT_ASSERT_VALUES_EQUAL(res[0], "aba/caba/daba|man");
        UNIT_ASSERT_VALUES_EQUAL(res[1], "cons/aaa|1|aba/caba/daba|man");
    }

    Y_UNIT_TEST(DbAggregation) {
        TVector<TActorId> cc;
        TActorId aggregatorId;

        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);

        TActorId edge = runtime.AllocateEdgeActor();

        runtime.SetRegistrationObserverFunc([&cc, &aggregatorId]
            (TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId) {
                TTestActorRuntime::DefaultRegistrationObserver(runtime, parentId, actorId);
                    if (parentId == aggregatorId) {
                        cc.push_back(actorId);
                    }
                });

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);
        for (const auto& a : cc) {
            auto nodes = MakeIntrusive<TIntrusiveVector<TEvInterconnect::TNodeInfo>>();
            nodes->emplace_back(TEvInterconnect::TNodeInfo(1, "::", "localhost", "localhost", 1234, TNodeLocation()));
            nodes->emplace_back(TEvInterconnect::TNodeInfo(2, "::", "localhost", "localhost", 1234, TNodeLocation()));
            nodes->emplace_back(TEvInterconnect::TNodeInfo(3, "::", "localhost", "localhost", 1234, TNodeLocation()));
            THolder<TEvInterconnect::TEvNodesInfo> nodesInfo = MakeHolder<TEvInterconnect::TEvNodesInfo>(nodes);
            runtime.Send(new NActors::IEventHandle(a, edge, nodesInfo.Release()), 0, true);
        }

        NPrivate::TDbLabeledCounters PQCounters;

        const size_t namesN{5};
        std::array<const char *, namesN> names;
        names.fill("");
        names[0] = "whatever";
        names[1] = "whenever";
        std::array<const char *, namesN> groupNames;
        groupNames.fill("topic");
        groupNames[1] = "user||topic";
        std::array<ui8, namesN> types;
        types.fill(static_cast<ui8>(TLabeledCounterOptions::CT_SIMPLE));

        std::array<ui8, namesN> functions;
        functions.fill(static_cast<ui8>(TLabeledCounterOptions::EAF_SUM));
        functions[1] = static_cast<ui8>(TLabeledCounterOptions::EAF_MAX);

        {
            NKikimr::TTabletLabeledCountersBase labeledCounters(namesN, &names[0], &types[0], &functions[0],
                                                                "some_stream", &groupNames[0], 1, "/Root/PQ1");
            labeledCounters.GetCounters()[0].Set(10);
            labeledCounters.GetCounters()[1].Set(10);
            PQCounters.Apply(0, &labeledCounters);
            labeledCounters.GetCounters()[0].Set(11);
            labeledCounters.GetCounters()[1].Set(100);
            PQCounters.Apply(1, &labeledCounters);
            labeledCounters.GetCounters()[0].Set(12);
            labeledCounters.GetCounters()[1].Set(10);
            PQCounters.Apply(2, &labeledCounters);
            // SUM 33
            // MAX 100
        }

        {
            NKikimr::TTabletLabeledCountersBase labeledCounters(namesN, &names[0], &types[0], &functions[0],
                                                                "some_stream", &groupNames[0], 1, "/Root/PQ2");
            labeledCounters.GetCounters()[0].Set(20);
            labeledCounters.GetCounters()[1].Set(1);
            PQCounters.Apply(0, &labeledCounters);
            labeledCounters.GetCounters()[0].Set(21);
            labeledCounters.GetCounters()[1].Set(11);
            PQCounters.Apply(1, &labeledCounters);
            labeledCounters.GetCounters()[0].Set(22);
            labeledCounters.GetCounters()[1].Set(10);
            PQCounters.Apply(2, &labeledCounters);
            // SUM 63
            // MAX 11
        }

        NKikimr::NSysView::TDbServiceCounters counters;

        // Here we check that consequent calls do not interfere
        for (int i = 10; i >= 0; --i) {
            PQCounters.ToProto(counters);

            auto pqCounters = counters.FindOrAddLabeledCounters("some_stream");
            UNIT_ASSERT_VALUES_EQUAL(pqCounters->GetAggregatedPerTablets().group(), "some_stream");
            UNIT_ASSERT_VALUES_EQUAL(pqCounters->GetAggregatedPerTablets().delimiter(), "|");
            UNIT_ASSERT_VALUES_EQUAL(pqCounters->GetAggregatedPerTablets().GetLabeledCounter().size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(pqCounters->GetAggregatedPerTablets().GetLabeledCounter(0).value(), 63);
            UNIT_ASSERT_VALUES_EQUAL(pqCounters->GetAggregatedPerTablets().GetLabeledCounter(1).value(), 11);

            auto additional = pqCounters->MutableAggregatedPerTablets()->AddLabeledCounter();
            additional->SetNameId(1000);
            additional->SetValue(13);
            additional->SetType(TLabeledCounterOptions::CT_SIMPLE);
            additional->SetAggregateFunc(TLabeledCounterOptions::EAF_SUM);

            PQCounters.FromProto(counters);
        }
    }
}

Y_UNIT_TEST_SUITE(TEvTabletAddCountersDetailedMetricsFields) {
    Y_UNIT_TEST(DefaultsToLeaderWithNoTableInfo) {
        TEvTabletCounters::TEvTabletAddCounters ev(
            new TEvTabletCounters::TInFlightCookie, 1, TTabletTypes::DataShard, TPathId(1113, 1001),
            new TTabletCountersBase, new TTabletCountersBase);

        UNIT_ASSERT_VALUES_EQUAL(ev.FollowerId, 0u);
        UNIT_ASSERT(!ev.TableInfo.has_value());
    }

    Y_UNIT_TEST(StampsFollowerIdAndTableInfoWhenProvided) {
        TEvTabletCounters::TEvTabletAddCounters::TTableInfo tableInfo{
            .TableId = TPathId(1113, 42),
            .TablePath = "/Root/table",
            .SchemaVersion = 3,
            .MetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition,
            .MonitoringProjectId = "my-project",
        };

        TEvTabletCounters::TEvTabletAddCounters ev(
            new TEvTabletCounters::TInFlightCookie, 1, TTabletTypes::DataShard, TPathId(1113, 1001),
            new TTabletCountersBase, new TTabletCountersBase,
            7, tableInfo);

        UNIT_ASSERT_VALUES_EQUAL(ev.FollowerId, 7u);
        UNIT_ASSERT(ev.TableInfo.has_value());
        UNIT_ASSERT_VALUES_EQUAL(ev.TableInfo->TableId, tableInfo.TableId);
        UNIT_ASSERT_VALUES_EQUAL(ev.TableInfo->TablePath, tableInfo.TablePath);
        UNIT_ASSERT_VALUES_EQUAL(ev.TableInfo->SchemaVersion, tableInfo.SchemaVersion);
        UNIT_ASSERT_VALUES_EQUAL(ev.TableInfo->MetricsLevel, tableInfo.MetricsLevel);
        UNIT_ASSERT_VALUES_EQUAL(ev.TableInfo->MonitoringProjectId, tableInfo.MonitoringProjectId);
    }
}

/**
 * Tests for the detailed metrics, which the two aggregator actors of a node build
 * within the private "ydb_detailed_raw" counter group.
 */
Y_UNIT_TEST_SUITE(TTabletCountersAggregatorDetailedMetrics) {

    const TString DETAILED_RAW_GROUP = "ydb_detailed_raw";

    const TString DATABASE_PATH = "/Root/db";
    const TString MONITORING_PROJECT_ID = "my-project";

    const TString TABLE_PATH = "/Root/db/dir/table";
    const TString RELATIVE_TABLE_PATH = "dir/table";

    const TPathId TENANT_PATH_ID(1113, 1001);
    const TPathId TABLE_ID(1113, 42);

    constexpr TTabletTypes::EType TABLET_TYPE = TTabletTypes::DataShard;

    constexpr ui32 LEVEL_TABLE = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable;
    constexpr ui32 LEVEL_PARTITION = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition;

    constexpr const char* SIMPLE_COUNTER_NAMES[] = {
        "UniqueRows",
    };

    ////////////////////////////////////////////

    /**
     * A stand-in for the scheme cache: it resolves the path id of the database to
     * its path and nothing else.
     */
    class TFakeSchemeCache : public TActor<TFakeSchemeCache> {
    public:
        explicit TFakeSchemeCache(ui32* requestCounter)
            : TActor(&TThis::StateWork)
            , RequestCounter(requestCounter)
        {}

        STATEFN(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySet, Handle);
                default:
                    break;
            }
        }

    private:
        void Handle(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& ev) {
            ++*RequestCounter;

            TAutoPtr<NSchemeCache::TSchemeCacheNavigate> navigate = ev->Get()->Request.Release();

            for (auto& entry : navigate->ResultSet) {
                entry.Status = NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
                entry.Path = SplitPath(DATABASE_PATH);
            }

            Send(ev->Sender, new TEvTxProxySchemeCache::TEvNavigateKeySetResult(navigate));
        }

    private:
        ui32* const RequestCounter;
    };

    ////////////////////////////////////////////

    /**
     * The two aggregator actors of a single node and a scheme cache, which resolves
     * the path of the database.
     */
    struct TEnv {
        explicit TEnv(bool detailedMetricsEnabled)
            : Runtime(1)
        {
            TAppPrepare app;
            app.SetEnableDataShardDetailedMetrics(detailedMetricsEnabled);
            Runtime.Initialize(app.Unwrap());

            Edge = Runtime.AllocateEdgeActor();

            Runtime.RegisterService(
                MakeSchemeCacheID(),
                Runtime.Register(new TFakeSchemeCache(&NavigateRequests))
            );

            LeaderAggregatorId = Runtime.Register(CreateTabletCountersAggregator(false));
            FollowerAggregatorId = Runtime.Register(CreateTabletCountersAggregator(true));

            Runtime.EnableScheduleForActor(LeaderAggregatorId);
            Runtime.EnableScheduleForActor(FollowerAggregatorId);

            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 2);
            Runtime.DispatchEvents(options);
        }

        TActorId GetAggregatorId(ui32 followerId) const {
            return followerId == 0 ? LeaderAggregatorId : FollowerAggregatorId;
        }

        ::NMonitoring::TDynamicCounterPtr GetCountersRoot() {
            ::NMonitoring::TDynamicCounterPtr counters = Runtime.GetAppData(0).Counters;
            UNIT_ASSERT(counters);
            return counters;
        }

        TTestBasicRuntime Runtime;
        TActorId Edge;
        TActorId LeaderAggregatorId;
        TActorId FollowerAggregatorId;

        ui32 NavigateRequests = 0;
    };

    ////////////////////////////////////////////

    /**
     * A single tablet of the table, which reports its low level counters to
     * the aggregator actor of its role.
     */
    struct TFakeTablet {
        TFakeTablet(ui64 tabletId, ui32 followerId, ui32 metricsLevel)
            : TabletId(tabletId)
            , FollowerId(followerId)
            , MetricsLevel(metricsLevel)
            , CounterEventsInFlight(new TEvTabletCounters::TInFlightCookie)
            , ExecutorCounters(new TTabletCountersBase)
            , ExecutorCountersBaseline(new TTabletCountersBase)
            , AppCounters(new TTabletCountersBase(
                Y_ARRAY_SIZE(SIMPLE_COUNTER_NAMES),
                0, // cumulativeCnt
                0, // percentileCnt
                SIMPLE_COUNTER_NAMES,
                nullptr,
                nullptr))
            , AppCountersBaseline(new TTabletCountersBase)
        {
            ExecutorCounters->RememberCurrentStateAsBaseline(*ExecutorCountersBaseline);
            AppCounters->RememberCurrentStateAsBaseline(*AppCountersBaseline);
        }

        TFakeTablet& SetUniqueRows(ui64 value) {
            AppCounters->Simple()[0].Set(value);
            return *this;
        }

        void SendUpdate(TEnv& env) {
            auto executorCounters = ExecutorCounters->MakeDiffForAggr(*ExecutorCountersBaseline);
            ExecutorCounters->RememberCurrentStateAsBaseline(*ExecutorCountersBaseline);

            auto appCounters = AppCounters->MakeDiffForAggr(*AppCountersBaseline);
            AppCounters->RememberCurrentStateAsBaseline(*AppCountersBaseline);

            TEvTabletCounters::TEvTabletAddCounters::TTableInfo tableInfo;
            tableInfo.TableId = TABLE_ID;
            tableInfo.TablePath = TABLE_PATH;
            tableInfo.SchemaVersion = 1;
            tableInfo.MetricsLevel = MetricsLevel;
            tableInfo.MonitoringProjectId = MONITORING_PROJECT_ID;

            const TActorId aggregatorId = env.GetAggregatorId(FollowerId);

            env.Runtime.Send(new IEventHandle(aggregatorId, env.Edge,
                new TEvTabletCounters::TEvTabletAddCounters(
                    CounterEventsInFlight, TabletId, TABLET_TYPE, TENANT_PATH_ID,
                    executorCounters, appCounters, FollowerId, tableInfo)));

            // force recalc
            env.Runtime.Send(new IEventHandle(aggregatorId, env.Edge, new TEvents::TEvWakeup()));
        }

        void SendForget(TEnv& env) {
            const TActorId aggregatorId = env.GetAggregatorId(FollowerId);

            env.Runtime.Send(new IEventHandle(aggregatorId, env.Edge,
                new TEvTabletCounters::TEvTabletCountersForgetTablet(
                    TabletId, TABLET_TYPE, TENANT_PATH_ID)));

            // force recalc
            env.Runtime.Send(new IEventHandle(aggregatorId, env.Edge, new TEvents::TEvWakeup()));
        }

        const ui64 TabletId;
        const ui32 FollowerId;
        const ui32 MetricsLevel;

        TIntrusivePtr<TEvTabletCounters::TInFlightCookie> CounterEventsInFlight;

        std::unique_ptr<TTabletCountersBase> ExecutorCounters;
        std::unique_ptr<TTabletCountersBase> ExecutorCountersBaseline;

        std::unique_ptr<TTabletCountersBase> AppCounters;
        std::unique_ptr<TTabletCountersBase> AppCountersBaseline;
    };

    /**
     * Report the counters of the tablets, giving the aggregator actors the round, which
     * they spend on resolving the path of the database.
     */
    void ReportCounters(TEnv& env, const TVector<TFakeTablet*>& tablets) {
        for (ui32 round = 0; round < 2; ++round) {
            for (auto* tablet : tablets) {
                tablet->SendUpdate(env);
            }
            env.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        }
    }

    ////////////////////////////////////////////

    ::NMonitoring::TDynamicCounterPtr FindRawGroup(TEnv& env) {
        return env.GetCountersRoot()->FindSubgroup("counters", DETAILED_RAW_GROUP);
    }

    ::NMonitoring::TDynamicCounterPtr FindTableGroup(TEnv& env) {
        auto rawGroup = FindRawGroup(env);
        if (!rawGroup) {
            return nullptr;
        }

        auto databaseGroup = rawGroup->FindSubgroup("database", DATABASE_PATH);
        if (!databaseGroup) {
            return nullptr;
        }

        auto projectGroup = databaseGroup->FindSubgroup("monitoring_project_id", MONITORING_PROJECT_ID);
        if (!projectGroup) {
            return nullptr;
        }

        return projectGroup->FindSubgroup("table", RELATIVE_TABLE_PATH);
    }

    ::NMonitoring::TDynamicCounterPtr FindAppCounters(::NMonitoring::TDynamicCounterPtr bucketGroup) {
        if (!bucketGroup) {
            return nullptr;
        }

        auto typeGroup = bucketGroup->FindSubgroup("type", TString(TTabletTypes::TypeToStr(TABLET_TYPE)));
        if (!typeGroup) {
            return nullptr;
        }

        return typeGroup->FindSubgroup("category", "app");
    }

    ::NMonitoring::TDynamicCounterPtr FindRoleBucketCounters(TEnv& env, ui32 followerId) {
        auto tableGroup = FindTableGroup(env);
        if (!tableGroup) {
            return nullptr;
        }

        return FindAppCounters(
            tableGroup->FindSubgroup("role", followerId == 0 ? "leader" : "follower")
        );
    }

    ::NMonitoring::TDynamicCounterPtr FindLeafCounters(TEnv& env, ui64 tabletId, ui32 followerId) {
        auto tableGroup = FindTableGroup(env);
        if (!tableGroup) {
            return nullptr;
        }

        auto perPartitionGroup = tableGroup->FindSubgroup("detailed_metrics", "per_partition");
        if (!perPartitionGroup) {
            return nullptr;
        }

        auto tabletGroup = perPartitionGroup->FindSubgroup("tablet_id", ToString(tabletId));
        if (!tabletGroup) {
            return nullptr;
        }

        return FindAppCounters(tabletGroup->FindSubgroup("follower_id", ToString(followerId)));
    }

    ui64 GetUniqueRows(::NMonitoring::TDynamicCounterPtr countersGroup, const TString& aggregate) {
        UNIT_ASSERT_C(countersGroup, "no counter group for " << aggregate << "(UniqueRows)");

        auto counter = countersGroup->FindNamedCounter("sensor", aggregate + "(UniqueRows)");
        UNIT_ASSERT_C(counter, "no counter " << aggregate << "(UniqueRows)");

        return counter->Val();
    }

    ////////////////////////////////////////////

    /**
     * Verify that nothing at all is created while the feature flag is off.
     */
    Y_UNIT_TEST(NoCountersWhenDisabled) {
        TEnv env(false /* detailedMetricsEnabled */);

        TFakeTablet leader(1000, 0, LEVEL_PARTITION);
        TFakeTablet follower(1000, 1, LEVEL_PARTITION);

        leader.SetUniqueRows(1);
        follower.SetUniqueRows(2);

        ReportCounters(env, {&leader, &follower});

        UNIT_ASSERT(!FindRawGroup(env));
        UNIT_ASSERT_VALUES_EQUAL(env.NavigateRequests, 0u);
    }

    /**
     * Verify that at the partition level both aggregator actors of the node fill their
     * own leaves of one and the same counter tree.
     */
    Y_UNIT_TEST(PartitionLevelLeavesOfBothRoles) {
        TEnv env(true /* detailedMetricsEnabled */);

        TFakeTablet leader(1000, 0, LEVEL_PARTITION);
        TFakeTablet follower(1000, 1, LEVEL_PARTITION);

        leader.SetUniqueRows(1);
        follower.SetUniqueRows(2);

        ReportCounters(env, {&leader, &follower});

        UNIT_ASSERT_VALUES_EQUAL(GetUniqueRows(FindLeafCounters(env, 1000, 0), "SUM"), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetUniqueRows(FindLeafCounters(env, 1000, 1), "SUM"), 2);

        // No on-node rollup of any kind at the partition level
        auto tableGroup = FindTableGroup(env);
        UNIT_ASSERT(tableGroup);
        UNIT_ASSERT(!tableGroup->FindSubgroup("role", "leader"));
        UNIT_ASSERT(!tableGroup->FindSubgroup("role", "follower"));
    }

    /**
     * Verify that at the table level the partitions of the table are collapsed into
     * the two role buckets and no per-partition counters are created.
     */
    Y_UNIT_TEST(TableLevelCollapsesPartitions) {
        TEnv env(true /* detailedMetricsEnabled */);

        TFakeTablet leader1(1000, 0, LEVEL_TABLE);
        TFakeTablet leader2(2000, 0, LEVEL_TABLE);
        TFakeTablet follower(1000, 1, LEVEL_TABLE);

        leader1.SetUniqueRows(1);
        leader2.SetUniqueRows(2);
        follower.SetUniqueRows(8);

        ReportCounters(env, {&leader1, &leader2, &follower});

        UNIT_ASSERT_VALUES_EQUAL(GetUniqueRows(FindRoleBucketCounters(env, 0), "SUM"), 1 + 2);
        UNIT_ASSERT_VALUES_EQUAL(GetUniqueRows(FindRoleBucketCounters(env, 0), "MAX"), 2);
        UNIT_ASSERT_VALUES_EQUAL(GetUniqueRows(FindRoleBucketCounters(env, 1), "SUM"), 8);

        auto tableGroup = FindTableGroup(env);
        UNIT_ASSERT(tableGroup);
        UNIT_ASSERT(!tableGroup->FindSubgroup("detailed_metrics", "per_partition"));
    }

    /**
     * Verify that forgetting a tablet drops its leaf and leaves the tablets of
     * the other role alone.
     */
    Y_UNIT_TEST(ForgetTabletDropsItsLeavesOnly) {
        TEnv env(true /* detailedMetricsEnabled */);

        TFakeTablet leader1(1000, 0, LEVEL_PARTITION);
        TFakeTablet leader2(2000, 0, LEVEL_PARTITION);
        TFakeTablet follower(1000, 1, LEVEL_PARTITION);

        leader1.SetUniqueRows(1);
        leader2.SetUniqueRows(2);
        follower.SetUniqueRows(8);

        ReportCounters(env, {&leader1, &leader2, &follower});

        UNIT_ASSERT(FindLeafCounters(env, 1000, 0));
        UNIT_ASSERT(FindLeafCounters(env, 2000, 0));
        UNIT_ASSERT(FindLeafCounters(env, 1000, 1));

        // TEST 1: The leaf of the leader is gone, the one of its follower survives
        leader1.SendForget(env);
        env.Runtime.SimulateSleep(TDuration::MilliSeconds(1));

        UNIT_ASSERT(!FindLeafCounters(env, 1000, 0));
        UNIT_ASSERT(FindLeafCounters(env, 1000, 1));
        UNIT_ASSERT(FindLeafCounters(env, 2000, 0));

        // TEST 2: The last leaf of the aggregator of the leaders removes the table of
        //         that aggregator, while the follower keeps reporting into its own
        leader2.SendForget(env);
        env.Runtime.SimulateSleep(TDuration::MilliSeconds(1));

        UNIT_ASSERT(!FindLeafCounters(env, 2000, 0));

        follower.SetUniqueRows(16);
        ReportCounters(env, {&follower});

        UNIT_ASSERT_VALUES_EQUAL(GetUniqueRows(FindLeafCounters(env, 1000, 1), "SUM"), 16);

        // TEST 3: Forgetting the last tablet removes the table
        follower.SendForget(env);
        env.Runtime.SimulateSleep(TDuration::MilliSeconds(1));

        UNIT_ASSERT(!FindTableGroup(env));
    }
}

}
