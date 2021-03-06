package it.uniroma2.ember.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import java.io.IOException;

/**
 *  The class LumenData is the OO representation for the JSON object
 *  provided by light sensors on lamps.
 */
public class LumenData {

    private int id;
    private String address;
    private float ambient;
    private long retrieved;

    public int getId() {
        return id;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public float getAmbient() {
        return ambient;
    }

    public void setAmbient(float ambient) {
        this.ambient = ambient;
    }

    public long getRetrieved() {
        return retrieved;
    }

    public void setRetrieved(long retrieved) {
        this.retrieved = retrieved;
    }

    /**
     * @param id unique identifier of light sensor (it is the same of the lamp)
     * @param address where the lamp is located
     * @param ambient luminosity level near the sensor
     */
    public LumenData(int id, String address, float ambient, long retrieved) {
        this.id = id;
        this.address   = address;
        this.ambient   = ambient;
        this.retrieved = retrieved;
    }

    public LumenData() { /* dummy constructor for jackson parsing */ }

    /**
     * Static method to parse the JSON string provided into a
     * LumenData class object.
     *
     * @param json a string defining the JSON input
     * @return {@link LumenData} or null in case of error
     */
    public static LumenData parseLumenData(String json) throws IOException {
        Gson gson = new Gson();
        return gson.fromJson(json, LumenData.class);

    }

}
