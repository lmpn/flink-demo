package com.ververica.flinktraining.exercises.functions;

import com.ververica.flinktraining.exercises.datatypes.EnrichedRecord;
import com.ververica.flinktraining.exercises.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datatypes.TaxiRide;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JoinRecords extends KeyedCoProcessFunction<Long, TaxiRide, TaxiFare, EnrichedRecord> {

    MapState<Long,TaxiRide> ride;
    MapState<Long,TaxiFare> fare;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<Long, TaxiRide> rideDescriptor =
                new MapStateDescriptor<>(
                        "ride",
                        BasicTypeInfo.LONG_TYPE_INFO,
                        TypeInformation.of(TaxiRide.class));
        MapStateDescriptor<Long,TaxiFare> fareDescriptor =
                new MapStateDescriptor<>(
                        "fare",
                        BasicTypeInfo.LONG_TYPE_INFO,
                        TypeInformation.of(TaxiFare.class));
        ride = getRuntimeContext().getMapState(rideDescriptor);
        fare = getRuntimeContext().getMapState(fareDescriptor);
    }

    @Override
    public void processElement1(TaxiRide inRide, Context ctx, Collector<EnrichedRecord> out) throws Exception {
        Long key = inRide.rideId;
        if (fare.contains(key)){
            TaxiFare item = fare.get(key);
            ctx.timerService().deleteEventTimeTimer(item.getEventTime());
            fare.remove(key);
            EnrichedRecord record = new EnrichedRecord(inRide, item);
            out.collect(record);
        }else{
            ride.put(key, inRide);
            ctx.timerService().registerEventTimeTimer(inRide.getEventTime());
        }
    }

    @Override
    public void processElement2(TaxiFare inFare, Context ctx, Collector<EnrichedRecord> out) throws Exception {
        Long key = inFare.rideId;
        if (ride.contains(key)){
            TaxiRide item = ride.get(key);
            ctx.timerService().deleteEventTimeTimer(item.getEventTime());
            ride.remove(key);
            EnrichedRecord record = new EnrichedRecord(item, inFare);
            out.collect(record);
        }else{
            fare.put(key, inFare);
            ctx.timerService().registerEventTimeTimer(inFare.getEventTime());
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<EnrichedRecord> out) throws Exception {
        Iterator collection = ride.iterator();
        List<Long> removeItem = new ArrayList<>();
        for (Iterator it = collection; it.hasNext(); ) {
            Object item = it.next();
            TaxiRide outRide = ((Map.Entry<Long,TaxiRide>) item).getValue();
            if (outRide.getEventTime() < timestamp){
                removeItem.add(outRide.rideId);
            }
        }


        removeItem.forEach( item -> {
            try {
                ride.remove(item);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });


        collection = fare.iterator();
        removeItem.clear();
        for (Iterator it = collection; it.hasNext(); ) {
            Object item = it.next();
            TaxiFare outFare = ((Map.Entry<Long,TaxiFare>) item).getValue();
            if (outFare.getEventTime() < timestamp){
                removeItem.add(outFare.rideId);
            }
        }

        removeItem.forEach( item -> {
            try {
                fare.remove(item);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

    }


}