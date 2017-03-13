package it.uniroma2.ember;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Objects;

/**
 * Created by odysseus on 09/03/17.
 */
public class EmberControlFeedback {

    /**
     * Implements a join function to set a proper level for the lamp
     * (this class actually implements the control feedback)
     */
    public static final class EmberControlRoom implements JoinFunction<EmberInput.StreetLamp, Tuple2<String, Tuple2<Float,Float>>,EmberInput.StreetLamp> {

        /**
         * @param streetLamp a {@link it.uniroma2.ember.EmberInput.StreetLamp} instance
         * @param aggregatedSensorsData a <address, Tuple2<light_value,traffic_value>>
         * @return {@link it.uniroma2.ember.EmberInput.StreetLamp} instance with the correct level value
         * @throws Exception
         */
        @Override
        public EmberInput.StreetLamp join(EmberInput.StreetLamp streetLamp, Tuple2<String, Tuple2<Float, Float>> aggregatedSensorsData) throws Exception {
            if (!Objects.equals(aggregatedSensorsData.f0, "null")) {
                streetLamp.setLevel(aggregatedSensorsData.f1.f0 - aggregatedSensorsData.f1.f1); //TODO
                return streetLamp;
            }
            return new EmberInput.StreetLamp();
        }
    }
}
