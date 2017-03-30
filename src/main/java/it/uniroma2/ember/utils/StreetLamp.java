package it.uniroma2.ember.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import java.io.IOException;

/**
 *  The class StreeLamp which is the OO representation for the JSON object
 *  provided by the actual street lamp (you know... light...? That sort of things)
 */
public class StreetLamp {

    private int id;
    private String address;
    private String model;
    private String control_unit;
    private int consumption;
    private boolean power_on;
    private float level;
    private long last_replacement;
    private long sent;

    public int getId() {
        return id;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getControl_unit() {
        return control_unit;
    }

    public void setControl_unit(String control_unit) {
        this.control_unit = control_unit;
    }

    public int getConsumption() {
        return consumption;
    }

    public void setConsumption(int consumption) {
        this.consumption = consumption;
    }

    public boolean isPower_on() {
        return power_on;
    }

    public void setPower_on(boolean power_on) {
        this.power_on = power_on;
    }

    public float getLevel() {
        return level;
    }

    public void setLevel(float level) {
        this.level = level;
    }

    public long getLast_replacement() {
        return last_replacement;
    }

    public void setLast_replacement(long last_replacement) {
        this.last_replacement = last_replacement;
    }

    public long getSent() {
        return sent;
    }

    public void setSent(long sent) {
        this.sent = sent;
    }

    /**
     * Default constructor for StreetLamp
     *
     * @param id unique identifier proper of the street lamp
     * @param address where the lamp is located
     * @param model model of the light bulb
     * @param control_unit the local control unit
     * @param consumption current consumption value (in Watt)
     * @param power_on boolean true if the lamp was on, false if it was off
     * @param level level of power of the light bulb
     * @param last_replacement timestamp of the last light bulb replacement
     * @param sent timestamp of the last update about the lamp
     */
    public StreetLamp(int id, String address, String model, String control_unit, int consumption, boolean power_on, float level, int last_replacement, int sent) {
        this.id = id;
        this.address = address;
        this.model = model;
        this.control_unit = control_unit;
        this.consumption = consumption;
        this.power_on = power_on;
        this.level = level;
        this.last_replacement = last_replacement;
        this.sent = last_replacement;
    }

    public StreetLamp() { /* dummy constructor for jackson parsing */ }

    /**
     * Static method to parse the JSON string provided into a
     * StreetLamp class object.
     *
     * @param json a string defining the JSON input
     * @return {@link StreetLamp} or null in case of error
     */
    public static StreetLamp parseStreetLamp(String json) throws IOException {
        Gson gson = new Gson();
        return gson.fromJson(json, StreetLamp.class);
    }

    @Override
    public String toString() {
        return "StreetLamp{" +
                "id=" + id +
                ", address='" + address + '\'' +
                ", model='" + model + '\'' +
                ", consumption=" + consumption +
                ", power_on=" + power_on +
                ", level=" + level +
                ", last_replacement=" + last_replacement +
                ", sent=" + sent +
                '}';
    }
}
