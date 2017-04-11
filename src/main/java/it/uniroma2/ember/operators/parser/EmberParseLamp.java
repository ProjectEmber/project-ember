package it.uniroma2.ember.operators.parser;

import it.uniroma2.ember.utils.StreetLamp;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;

/**
 * Implements a simple FlatMapFunction to parse JSON raw string into a StreetLamp object
 */
public final class EmberParseLamp extends RichFlatMapFunction<String, StreetLamp> {

    private Counter counter;

    @Override
    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
        this.counter = getRuntimeContext()
                .getMetricGroup()
                .addGroup("myGroup")
                .counter("mycounter");
    }

    /**
     * Override flatMap method from FlatMapFunction
     *
     * @param s String, the in-line JSON to be parsed into a StreetLamp object
     * @param collector the Collector<StreetLamp> to handle the stream handoff
     */
    @Override
    public void flatMap(String s, Collector<StreetLamp> collector) throws Exception {
        this.counter.inc();
        collector.collect(StreetLamp.parseStreetLamp(s));
    }
}
