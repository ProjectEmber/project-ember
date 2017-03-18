package it.uniroma2.ember;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.ember.utils.StreetLamp;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.Objects;


public class EmberControlFeedback {

    public static final float LUMEN_POWER_THRESHOLD     = 10f;
    public static final float LUMEN_CONTROL_THRESHOLD   = 0.2f;


    public static final float LAMP_SQUARE_AREA          = 30f;
    public static final float LAMP_SECURITY_LEVEL       =  5f;

    public static final float TRAFFIC_MAJOR_LEVEL       = 34f;
    public static final float TRAFFIC_COLLECTOR_LEVEL   = 24f;
    public static final float TRAFFIC_COLCTLOCAL_LEVEL  = 18f;
    public static final float TRAFFIC_LOCAL_LEVEL       = 15f;
    public static final float TRAFFIC_LOCALLOW_LEVEL    = 13f;



    /**
     * Implements a join function to set a proper level for the lamp
     * (this class actually implements the control feedback)
     */
    public static final class EmberControlRoom implements JoinFunction<StreetLamp, Tuple2<String, Tuple2<Float,Float>>,StreetLamp> {


        // auxiliary variables
        private static Float lumen;
        private static Float traffic;
        private static Float ctrLevel;

        /**
         * @param streetLamp a {@link it.uniroma2.ember.EmberInput.StreetLamp} instance
         * @param aggregatedSensorsData a <address, Tuple2<light_value,traffic_value>>
         * @return {@link it.uniroma2.ember.EmberInput.StreetLamp} instance with the correct level value
         * @throws Exception
         */
        @Override
        public StreetLamp join(StreetLamp streetLamp, Tuple2<String, Tuple2<Float, Float>> aggregatedSensorsData) throws Exception {
            if (!Objects.equals(aggregatedSensorsData.f0, "null")) {

                // retrieving sensor data
                lumen   = aggregatedSensorsData.f1.f0;
                traffic = aggregatedSensorsData.f1.f1;

                // computing an optimal value for lamp light level (expressed in 'lumen per lamp')
                // assuming traffic expressed in a 0 to 1 value and mapped in a lumen level,
                // the optimal value the lamp must assume is given by the formula:
                //
                // L = I * A = Illumination * Area
                // L = L + (T - L) = Level + (Traffic - Level)
                //
                // (with a minimum illumination of 10 to make the lamp be powered on)

                ctrLevel = streetLamp.getLevel();

                if (lumen <= LUMEN_POWER_THRESHOLD) {

                    // powering on the lamp
                    streetLamp.setPower_on(true);

                    if (lumen <= LUMEN_CONTROL_THRESHOLD) {

                        // mapping traffic
                        if (traffic > 0.85) traffic = TRAFFIC_MAJOR_LEVEL;
                        else if (traffic <= 0.85 && traffic > 0.60) traffic = TRAFFIC_COLLECTOR_LEVEL;
                        else if (traffic <= 0.60 && traffic > 0.40) traffic = TRAFFIC_COLCTLOCAL_LEVEL;
                        else if (traffic <= 0.40 && traffic > 0.20) traffic = TRAFFIC_LOCAL_LEVEL;
                        else if (traffic <= 0.20 && traffic > 0.10) traffic = TRAFFIC_LOCALLOW_LEVEL;

                        // calculating optimal light
                        ctrLevel = lumen * LAMP_SQUARE_AREA;

                        // calculating delta from traffic
                        ctrLevel = (traffic - ctrLevel > 0) ? traffic : ctrLevel;

                        // maintaining security levels
                        ctrLevel = (ctrLevel >= LAMP_SECURITY_LEVEL) ? ctrLevel : LAMP_SECURITY_LEVEL;
                    }

                } else {

                    // powering off the lamp
                    streetLamp.setPower_on(false);
                }

                // setting level, power on and timestamp
                streetLamp.setLevel(ctrLevel);
                streetLamp.setSent(Instant.now().getEpochSecond());
            }

            // if streetLamp is a valid object, the control is performed, else ...
            // if no active sensors data on the street, return the previous state unaltered
            return streetLamp;
        }
    }

    /**
     * Implements a simple FlatMapFunction to parse StreetLamp object into a JSON string
     */
    public static final class EmberSerializeLamp implements FlatMapFunction<StreetLamp, String> {

        /**
         * Override flatMap method from FlatMapFunction
         *
         * @param streetLamp, the {@link it.uniroma2.ember.EmberInput.StreetLamp} object to be parsed
         * @param collector the Collector<String> to handle the control stream handoff
         */
        @Override
        public void flatMap(StreetLamp streetLamp, Collector<String> collector) throws Exception {
            collector.collect(new ObjectMapper().writeValueAsString(streetLamp));
        }
    }
}
