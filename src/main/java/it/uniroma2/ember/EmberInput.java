package it.uniroma2.ember;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.sql.Timestamp;

/**
 * This is a support class which offers the following methods
 *      - static parsing from JSON to StreetLamp
 *      - static parsing from JSON to TrafficData
 *      - static parsing from JSON to LumenData
 */

public class EmberInput {

    /**
     *  The class StreeLamp which is the OO representation for the JSON object
     *  provided by the actual street lamp (you know... light...? That sort of things)
     */
    public static class StreetLamp {

        private int id;
        private String address;
        private String model;
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
         * @param consumption current consumption value (in Watt)
         * @param power_on boolean true if the lamp was on, false if it was off
         * @param level level of power of the light bulb
         * @param last_replacement timestamp of the last light bulb replacement
         * @param sent timestamp of the last update about the lamp
         */
        public StreetLamp(int id, String address, String model, int consumption, boolean power_on, float level, int last_replacement, int sent) {
            this.id = id;
            this.address = address;
            this.model = model;
            this.consumption = consumption;
            this.power_on = power_on;
            this.level = level;
            this.last_replacement = last_replacement;
            this.sent = last_replacement;
        }

        public StreetLamp() { /* dummy constructor for jackson parsing */ }
    }

    /**
     * Static method to parse the JSON string provided into a
     * StreetLamp class object.
     *
     * @param json a string defining the JSON input
     * @return {@link StreetLamp} or null in case of error
     */
    public static StreetLamp parseStreetLamp(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(json, StreetLamp.class);

    }

    /**
     *  The class LumenData is the OO representation for the JSON object
     *  provided by light sensors on lamps.
     */
    public static class LumenData {

        private int id;
        private String address;
        private float ambient;

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

        /**
         * @param id unique identifier of light sensor (it is the same of the lamp)
         * @param address where the lamp is located
         * @param ambient luminosity level near the sensor
         */
        public LumenData(int id, String address, float ambient) {
            this.id = id;
            this.address = address;
            this.ambient = ambient;
        }

        public LumenData() { /* dummy constructor for jackson parsing */ }

    }

    /**
     * Static method to parse the JSON string provided into a
     * LumenData class object.
     *
     * @param json a string defining the JSON input
     * @return {@link LumenData} or null in case of error
     */
    public static LumenData parseLumenData(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(json, LumenData.class);

    }


    /**
     *  The class TrafficData represents the actual traffic levels
     *  (it is provided by an external module as a JSON)
     */
    public static class TrafficData {

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
    }

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
