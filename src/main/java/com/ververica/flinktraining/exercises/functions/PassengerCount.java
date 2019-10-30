package com.ververica.flinktraining.exercises.functions;


import com.ververica.flinktraining.exercises.datatypes.EnrichedRecord;
import org.apache.flink.api.common.functions.AggregateFunction;

public class PassengerCount implements AggregateFunction<EnrichedRecord,Integer,Integer> {
    @Override
    public Integer createAccumulator() {
        return new Integer(0);
    }

    @Override
    public Integer add(EnrichedRecord record, Integer integer) {
        if (record.ride.isStart)
            return integer + record.ride.passengerCnt;
        return integer;
    }

    @Override
    public Integer getResult(Integer integer) {
        return integer;
    }

    @Override
    public Integer merge(Integer integer, Integer acc1) {
        return acc1+integer;
    }
}
