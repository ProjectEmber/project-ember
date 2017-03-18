package it.uniroma2.ember.stats;

/**
 * Created by federico on 18/03/17.
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Implements a simple FlatMapFunction to parse StreetLamp object into a JSON string
 */
public final class EmberSerializeRank implements FlatMapFunction<EmberLampLifeSpanRank, String> {

    /**
     * Override flatMap method from FlatMapFunction
     *
     * @param lifeSpanRank, the {@link it.uniroma2.ember.stats.EmberLampLifeSpanRank} object to be parsed
     * @param collector the Collector<String> to handle the control stream handoff
     */
    @Override
    public void flatMap(EmberLampLifeSpanRank lifeSpanRank, Collector<String> collector) throws Exception {
        collector.collect(new ObjectMapper().writeValueAsString(lifeSpanRank));
    }
}
