/*
 * Copyright 2015 data Artisans GmbH, 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.exercises.basics;

import com.ververica.flinktraining.exercises.datatypes.*;
import com.ververica.flinktraining.exercises.functions.*;
import com.ververica.flinktraining.exercises.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.utils.ExerciseBase;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

/**
 * The "Ride Cleansing" exercise from the Flink training
 * (http://training.ververica.com).
 * The task of the exercise is to filter a data stream of taxi ride records to keep only rides that
 * start and end within New York City. The resulting stream should be printed.
 * <p>
 * Parameters:
 * -input path-to-input-file
 */
public class StreamingTaxi extends ExerciseBase {
    public static class QueryStreamAssigner implements AssignerWithPeriodicWatermarks<String> {
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return Watermark.MAX_WATERMARK;
        }

        @Override
        public long extractTimestamp(String element, long previousElementTimestamp) {
            return 0;
        }
    }


    final static MapStateDescriptor<Long, Query> queryDescriptor = new MapStateDescriptor<Long, Query>(
            "queries",
            BasicTypeInfo.LONG_TYPE_INFO,
            TypeInformation.of(Query.class ));


    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", ExerciseBase.pathToRideData);
        final int maxEventDelay = 60;       // events are out of order by max 60 seconds
        final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second
        SourceFunction<TaxiRide> trSource = rideSourceOrTest(new TaxiRideSource( ExerciseBase.pathToRideData, maxEventDelay, servingSpeedFactor));
        SourceFunction<TaxiFare> tfSource = fareSourceOrTest(new TaxiFareSource( ExerciseBase.pathToFareData, maxEventDelay, servingSpeedFactor));
        SourceFunction<String> querySource = stringSourceOrTest(
                new SocketTextStreamFunction( "localhost", 9999, "\n", -1));

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);



        // set up streaming sources
        DataStream<TaxiRide> rides = env.
                addSource( trSource, "TaxiRideSource");
        DataStream<TaxiFare> fares = env.
                addSource( tfSource, "TaxiFareSource");
        BroadcastStream<String> queryEntryPoint = env.
                addSource( querySource, "QuerySource").
                assignTimestampsAndWatermarks( new QueryStreamAssigner()).
                broadcast(queryDescriptor);





        DataStream<TaxiRide> keyedRides = rides.keyBy(ride -> ride.driverId);
        DataStream<TaxiFare> keyedFares = fares.keyBy(fare -> fare.driverId);
        DataStream<EnrichedRecord> records = keyedRides.
                connect(keyedFares).
                process(new JoinRecords());


        SingleOutputStreamOperator<EnrichedRecord> filteredRecords = records.
                filter(record -> !record.ride.isStart);
        DataStream<IncomeRecord> incomePerDriver = filteredRecords.
                keyBy(record -> record.key).
                timeWindow(Time.hours(1)).
                aggregate(new IncomeAggregator());
        DataStream<IncomeRecord> globalIncome = incomePerDriver.
                timeWindowAll(Time.hours(3)).
                reduce((i1, i2) -> {
                    i1.addIncomeRecord(i2);
                    i1.id = -1L;
                    return i1;
                });

        OutputTag<EnrichedRecord> incompleteRecordsTag =
                new OutputTag<EnrichedRecord>("onTripRecords"){};
        DataStream<Integer> passenger = filteredRecords.
                getSideOutput(incompleteRecordsTag).
                keyBy(record -> record.key).
                timeWindow(Time.minutes(15), Time.minutes(5)).
                aggregate( new PassengerCount()).
                timeWindowAll(Time.minutes(15), Time.minutes(5)).
                sum(0);


        DataStream<QueryResponse> queryableRecords = records.
                keyBy(record -> record.key).
                connect(queryEntryPoint).
                process(new QueryProcessor(queryDescriptor)).
                keyBy(queryResponse -> queryResponse.key).
                process(new QueryMaintainer());




        System.out.println(env.getExecutionPlan());
        env.execute("Taxi Ride Cleansing");
    }






}
