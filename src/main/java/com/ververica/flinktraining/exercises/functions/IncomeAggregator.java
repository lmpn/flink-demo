package com.ververica.flinktraining.exercises.functions;

import com.ververica.flinktraining.exercises.datatypes.EnrichedRecord;
import com.ververica.flinktraining.exercises.datatypes.IncomeRecord;
import org.apache.flink.api.common.functions.AggregateFunction;

public class IncomeAggregator implements AggregateFunction<EnrichedRecord, IncomeRecord, IncomeRecord> {



    @Override
    public IncomeRecord createAccumulator() {
        return new IncomeRecord(-1L, 0f,0f,0f,0f);
    }

    @Override
    public IncomeRecord add(EnrichedRecord enrichedRecord, IncomeRecord incomeRecord) {
        incomeRecord.addEnrichedRecord(enrichedRecord);
        return incomeRecord;
    }

    @Override
    public IncomeRecord getResult(IncomeRecord incomeRecord) {
        return incomeRecord;
    }

    @Override
    public IncomeRecord merge(IncomeRecord incomeRecord, IncomeRecord acc1) {
        acc1.addIncomeRecord(incomeRecord);
        return acc1;
    }
}
