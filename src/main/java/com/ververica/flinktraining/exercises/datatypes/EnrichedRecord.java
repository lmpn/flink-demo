package com.ververica.flinktraining.exercises.datatypes;

public class EnrichedRecord {
    public Long key;
    public TaxiRide ride;
    public TaxiFare fare;

    public EnrichedRecord() {
    }

    public EnrichedRecord(TaxiRide ride, TaxiFare fare) {
        this.ride = ride;
        this.fare = fare;
        this.key = ride.driverId;
    }

    @Override
    public String toString() {
        return "EnrichedRide{" +
                "\tride=" + "{"+ride.toString()+"}\n" +
                "\tfare=" + "{"+fare +"}"+
                '}';
    }
}
