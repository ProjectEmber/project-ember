package it.uniroma2.ember.operators.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.ember.utils.LampConsumption;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Implements a simple FlatMapFunction to parse LampConsumption object into a JSON string
 */
public final class EmberSerializeConsumption implements FlatMapFunction<LampConsumption, String> {

    /**
     * Override flatMap method from FlatMapFunction
     *
     * @param lampConsumption, the {@link LampConsumption} object to be parsed
     * @param collector the Collector<String> to handle the control stream handoff
     */
    @Override
    public void flatMap(LampConsumption lampConsumption, Collector<String> collector) throws Exception {
        collector.collect(new ObjectMapper().writeValueAsString(lampConsumption));
    }
}