package it.uniroma2.ember.utils;

/**
 * Created by federico on 18/03/17.
 */

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 *  The class TrafficData represents the actual traffic levels
 *  (it is provided by an external module as a JSON)
 */
public class TrafficData {

    private String address;
    private float intensity;
    private int retrieved;

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public float getIntensity() {
        return intensity;
    }

    public void setIntensity(float intensity) {
        this.intensity = intensity;
    }

    public int getRetrieved() {
        return retrieved;
    }

    public void setRetrieved(int retrieved) {
        this.retrieved = retrieved;
    }

    /**
     * @param address string representing the street
     * @param intensity the measured traffic intensity
     * @param retrieved timestamp of the measurement
     */
    public TrafficData(String address, float intensity, int retrieved) {
        this.address = address;
        this.intensity = intensity;
        this.retrieved = retrieved;
    }

    public TrafficData() { /* dummy constructor for jackson parsing */ }

    /**
     * Static method to parse the JSON string provided into a
     * TrafficData class object.
     *
     * @param json a string defining the JSON input
     * @return {@link TrafficData} or null in case of error
     */
    public static TrafficData parseTrafficData(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(json, TrafficData.class);

    }

}
