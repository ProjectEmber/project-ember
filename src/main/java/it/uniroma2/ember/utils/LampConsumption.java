package it.uniroma2.ember.utils;

/**
 * Implements a simple object to represent in a state list the lamp power consumption
 */
public class LampConsumption {

    private int id            = 0;
    private int count         = 0;
    private float consumption = 0;
    private float hourMean    = 0;
    private float dayMean     = 0;
    private float weekMean    = 0;
    private String address;


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getCount() {
        return count;
    }

    public float getConsumption() {
        return consumption;
    }

    public void setConsumption(float consumption) {
        this.consumption = consumption;
    }

    public void incrementCount() {
        count += 1;
    }

    public float getHourMean() {
        return hourMean;
    }

    public void setHourMean(float hourMean) {
        this.hourMean = hourMean;
    }

    public float getDayMean() {
        return dayMean;
    }

    public void setDayMean(float dayMean) {
        this.dayMean = dayMean;
    }

    public float getWeekMean() {
        return weekMean;
    }

    public void setWeekMean(float weekMean) {
        this.weekMean = weekMean;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public LampConsumption() { /* */ }
}
