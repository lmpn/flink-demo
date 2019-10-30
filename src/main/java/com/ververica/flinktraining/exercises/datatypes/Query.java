package com.ververica.flinktraining.exercises.datatypes;

public abstract class Query {
    private static Long serialKey = Long.valueOf(0);
    public static final Long RETENTION_TIME = 10L * 60L * 1000L;
    public Long key;
    public long timestamp;

    public Query() {
        synchronized (serialKey){
            key = serialKey;
            serialKey++;
        }
    }

    public static Query fromString(String s, Long wm){
        if (s.toLowerCase().equals("active"))
            return new ActiveQuery();
        else if (s.toLowerCase().startsWith("dist: ")){
            String[] args = s.split(" ");
            float lon = Float.valueOf(args[1]);
            float lat = Float.valueOf(args[2]);
            return new ClosestTaxiQuery(lon, lat);
        }
        else
            return new UnactiveQuery();
    }

    public abstract String processRecord(EnrichedRecord record);




    static class ActiveQuery extends Query{

        @Override
        public String processRecord(EnrichedRecord record) {
            String response = "";
            response += "driver(" + record.ride.driverId +"): ";
            if (record.ride.isStart){
                response += "Active";
            }
            return response;
        }
        @Override
        public String toString() {
            return "ActiveQuery{" +
                    "ts: " +
                    super.timestamp+
                    "}";
        }
    }

    static class UnactiveQuery extends Query{

        @Override
        public String processRecord(EnrichedRecord record) {
            String response = "";
            response += "driver(" + record.ride.driverId +"): ";
            if (record.ride.isStart){
                response += "Inactive";
            }
            return response;
        }

        @Override
        public String toString() {
            return "UnactiveQuery{" +
                    "ts: " +
                    super.timestamp+
                    "}";
        }
    }

    static class ClosestTaxiQuery extends Query{
        private final float lat;
        private final float lon;

        public ClosestTaxiQuery(float lat, float lon) {
            this.lat = lat;
            this.lon = lon;
        }

        @Override
        public String processRecord(EnrichedRecord record) {
            String response = "";
            double distance = record.ride.getEuclideanDistance(lon,lat);
            response += "driver(" + record.ride.driverId +") is "+distance+" kms away";
            return response;
        }

        @Override
        public String toString() {
            return "ClosestTaxiQuery{" +
                    "lat=" + lat +
                    ", lon=" + lon +
                    " ts: " +
                    super.timestamp+
                    '}';
        }
    }


}
