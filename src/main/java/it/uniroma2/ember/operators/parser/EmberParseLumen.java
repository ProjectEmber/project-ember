package it.uniroma2.ember.operators.parser;

import it.uniroma2.ember.utils.LumenData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Implements a simple FlatMapFunction to parse JSON raw string into a LumenData object
 */

public final class EmberParseLumen implements FlatMapFunction<String, LumenData> {

    /**
     * Override flatMap method from FlatMapFunction
     *
     * @param s String, the in-line JSON to be parsed into a LumenData object
     * @param collector the Collector<LumenData> to handle the stream handoff
     */
    @Override
    public void flatMap(String s, Collector<LumenData> collector) throws Exception {
        collector.collect(LumenData.parseLumenData(s));
    }
}
