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

    public class StreetLamp {

        private int id;
        private String address;
        private String model;
        private int consumption;
        private boolean power_on;
        private float level;
        private Timestamp last_replacement;
        private Timestamp sent;

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

        public Timestamp getLast_replacement() {
            return last_replacement;
        }

        public void setLast_replacement(Timestamp last_replacement) {
            this.last_replacement = last_replacement;
        }

        public Timestamp getSent() {
            return sent;
        }

        public void setSent(Timestamp sent) {
            this.sent = sent;
        }

        public StreetLamp(int id, String address, String model, int consumption, boolean power_on, float level, Timestamp last_replacement, Timestamp sent) {
            this.id = id;
            this.address = address;
            this.model = model;
            this.consumption = consumption;
            this.power_on = power_on;
            this.level = level;
            this.last_replacement = last_replacement;
            this.sent = sent;
        }
    }

    /**
     * Static method to parse the JSON string provided into a
     * StreetLamp class object.
     * @param json a string defining the JSON input
     * @return StreetLamp or null in case of error
     */
    public static StreetLamp parseStreetLamp(String json) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(json, StreetLamp.class);
        } catch (IOException e) {
            return null;
        }
    }
}
