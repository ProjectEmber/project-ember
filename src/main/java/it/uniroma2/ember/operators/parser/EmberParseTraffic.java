package it.uniroma2.ember.operators.parser;

import it.uniroma2.ember.utils.TrafficData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Implements a simple FlatMapFunction to parse JSON raw string into a TrafficData object
 */
public final class EmberParseTraffic implements FlatMapFunction<String, TrafficData> {

    /**
     * Override flatMap method from FlatMapFunction
     *
     * @param s String, the in-line JSON to be parsed into a TrafficData object
     * @param collector the Collector<TrafficData> to handle the stream handoff
     */
    @Override
    public void flatMap(String s, Collector<TrafficData> collector) throws Exception {
        collector.collect(TrafficData.parseTrafficData(s));
    }
}
