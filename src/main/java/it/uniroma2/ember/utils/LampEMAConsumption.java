package it.uniroma2.ember.utils;

/**
 * Implements a simple object to represent in a state list the lamp power consumption (EMA version)
 */
public class LampEMAConsumption {

    private int id            = 0;
    private float consumption = 0;
    private String address;
    private long computed     = 0;


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

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

    public long getComputed() {
        return computed;
    }

    public void setComputed(long computed) {
        this.computed = computed;
    }

    public LampEMAConsumption() { /* */ }

    @Override
    public String toString() {
        return "LampEMAConsumption{" +
                "id=" + id +
                ", consumption=" + consumption +
                ", address='" + address + '\'' +
                '}';
    }
}
