package it.uniroma2.ember;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

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

    /**
     * Implements a simple FlatMapFunction to parse StreetLamp object into a JSON string
     */
    public static final class EmberSerializeLamp implements FlatMapFunction<EmberInput.StreetLamp, String> {

        /**
         * Override flatMap method from FlatMapFunction
         *
         * @param streetLamp, the {@link it.uniroma2.ember.EmberInput.StreetLamp} object to be parsed
         * @param collector the Collector<String> to handle the control stream handoff
         */
        @Override
        public void flatMap(EmberInput.StreetLamp streetLamp, Collector<String> collector) throws Exception {
            collector.collect(new ObjectMapper().writeValueAsString(streetLamp));
        }
    }
}
