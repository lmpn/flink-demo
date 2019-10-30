package com.ververica.flinktraining.exercises.functions;

import com.ververica.flinktraining.exercises.datatypes.QueryResponse;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class QueryMaintainer extends KeyedProcessFunction<Long, QueryResponse, QueryResponse> {

    private ValueState<QueryResponse> currentQueryResponse;

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<QueryResponse> out) throws Exception {
        QueryResponse qr = currentQueryResponse.value();
        if (qr != null) {
            System.out.println("Cleaning query-response-" + qr.key + " " + qr.response);
            currentQueryResponse.clear();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<QueryResponse> descriptor =
                new ValueStateDescriptor<QueryResponse>("query-response",TypeInformation.of(QueryResponse.class));
        currentQueryResponse = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(QueryResponse value, Context ctx, Collector<QueryResponse> out) throws Exception {
        QueryResponse current = currentQueryResponse.value();
        if (current != null){
            if( current.wm < value.wm){
                currentQueryResponse.update(value);
                ctx.timerService().deleteEventTimeTimer(current.wm + 100*60*1000);
                ctx.timerService().registerEventTimeTimer(value.wm + 100*60*1000);
                out.collect(value);}
        }else{
            currentQueryResponse.update(value);
            ctx.timerService().registerEventTimeTimer(value.wm + 100*60*1000);
            out.collect(value);
        }
    }
}
