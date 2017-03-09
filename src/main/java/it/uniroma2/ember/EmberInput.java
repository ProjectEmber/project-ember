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
        private int last_replacement;
        private int sent;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
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

        public int getLast_replacement() {
            return last_replacement;
        }

        public void setLast_replacement(int last_replacement) {
            this.last_replacement = last_replacement;
        }

        public int getSent() {
            return sent;
        }

        public void setSent(int sent) {
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
}
