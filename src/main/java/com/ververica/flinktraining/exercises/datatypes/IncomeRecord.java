package com.ververica.flinktraining.exercises.datatypes;


import com.ververica.flinktraining.exercises.utils.GeoUtils;

public class IncomeRecord{
    public long id;
    public float tolls;
    public float tips;
    public float total;
    public float gas;
    private final float gasPrice = 1.5f;

    public IncomeRecord(long id, float tolls, float tips, float total, float gas) {
        this.id = id;
        this.tolls = tolls;
        this.tips = tips;
        this.total = total;
        this.gas = gas;
    }


    public void addEnrichedRecord(EnrichedRecord record){
        if (record.ride.isStart) return;
        if (this.id == -1L) this.id = record.ride.driverId;
        this.tolls += record.fare.tolls;
        this.tips += record.fare.tip;
        this.total += record.fare.totalFare;
        double distance = GeoUtils.getEuclideanDistance(record.ride.startLon,record.ride.startLat, record.ride.endLon, record.ride.endLat);
        this.gas += distance*gasPrice;
    }

    public IncomeRecord() {
    }

    @Override
    public String toString() {
        return "IncomeRecord{" +
                "tolls=" + tolls +
                ", tips=" + tips +
                ", total=" + total +
                ", gas=" + gas +
                '}';
    }

    public void addIncomeRecord(IncomeRecord record) {
        this.tolls += record.tolls;
        this.tips += record.tips;
        this.total += record.total;
        this.gas += record.gas;

    }
}
