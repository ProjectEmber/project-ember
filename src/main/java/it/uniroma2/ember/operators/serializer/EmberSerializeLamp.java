package it.uniroma2.ember.operators.serializer;

import it.uniroma2.ember.utils.StreetLamp;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Implements a simple FlatMapFunction to parse StreetLamp object into a JSON string
 */

public final class EmberSerializeLamp implements FlatMapFunction<StreetLamp, String> {

    /**
     * Override flatMap method from FlatMapFunction
     *
     * @param streetLamp, the {@link StreetLamp} object to be parsed
     * @param collector the Collector<String> to handle the control stream handoff
     */
    @Override
    public void flatMap(StreetLamp streetLamp, Collector<String> collector) throws Exception {
        collector.collect(new ObjectMapper().writeValueAsString(streetLamp));
    }
}
