package it.uniroma2.ember.utils;

/**
 * Implements a simple object to represent in a state list the lamp power consumption by street
 * (EMA version)
 */
public class LampEMAConsumptionStreet {

    private float consumption = 0;
    private String address;

    public float getConsumption() {
        return consumption;
    }

    public void setConsumption(float consumption) {
        this.consumption = consumption;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public LampEMAConsumptionStreet() { /* */ }
}
