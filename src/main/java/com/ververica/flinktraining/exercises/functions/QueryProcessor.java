package com.ververica.flinktraining.exercises.functions;

import com.ververica.flinktraining.exercises.datatypes.EnrichedRecord;
import com.ververica.flinktraining.exercises.datatypes.Query;
import com.ververica.flinktraining.exercises.datatypes.QueryResponse;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;

public class QueryProcessor extends KeyedBroadcastProcessFunction<Long, EnrichedRecord, String, QueryResponse> {
    MapStateDescriptor<Long, Query> descriptor;
    ValueState<Long> cleanUpTimer;
    QueryResponse qr = new QueryResponse();

    public QueryProcessor(MapStateDescriptor<Long, Query> descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public void processElement(EnrichedRecord record, ReadOnlyContext readOnlyContext, Collector<QueryResponse> collector) throws Exception {
        Iterable<Map.Entry<Long, Query>> entries = readOnlyContext.getBroadcastState(descriptor).immutableEntries();
        entries.forEach(entry -> {
            Query query = entry.getValue();
            Long key = entry.getKey();
            String response = query.processRecord(record);
            qr.setQueryResponse(key,readOnlyContext.currentWatermark(),response);
            collector.collect(qr);
        });
    }

    @Override
    public void processBroadcastElement(String s, Context context, Collector<QueryResponse> collector) throws Exception {
        long wm = context.currentWatermark();
        Query query = Query.fromString(s,wm);
        System.out.println("Broadcast : " +wm);

        Iterator<Map.Entry<Long, Query>> queries = context.getBroadcastState(descriptor).iterator();
        queries.forEachRemaining(
                item -> {
                    final Query q = item.getValue();
                    if (wm - q.timestamp > query.RETENTION_TIME){
                        queries.remove();
                        System.out.println("Removed query: " + q);
                    }
                }
        );
        context.getBroadcastState(descriptor).put(query.key,query);
        System.out.println("New query: " + query.key);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        cleanUpTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", TypeInformation.of(Long.class)));
    }
}
