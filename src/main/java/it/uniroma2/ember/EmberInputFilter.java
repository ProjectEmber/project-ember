package it.uniroma2.ember;

/**
 * This is a data stream processing class to filter out lamp stats necessary
 * for control feedback output from the lamp own data.
 */

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class EmberInputFilter {

    /**
     * Implements a simple FlatMapFunction to parse JSON raw string into a StreetLamp object
     */
    public static final class EmberParseLamp implements FlatMapFunction<String, EmberInput.StreetLamp> {

        /**
         * Override flatMap method from FlatMapFunction
         *
         * @param s String, the in-line JSON to be parsed into a StreetLamp object
         * @param collector the Collector<StreetLamp> to handle the stream handoff
         */
        @Override
        public void flatMap(String s, Collector<EmberInput.StreetLamp> collector) throws Exception {
            collector.collect(EmberInput.parseStreetLamp(s));
        }
    }

    /**
     * Implements the selector to distinguish (not)powered lamps
     */
    public static final class EmberPowerSelector implements OutputSelector<EmberInput.StreetLamp> {

        /**
         * Override select method from OutputSelector
         *
         * @param streetLamp the StreetLamp object analyzed
         * @return output as an iterable of string to split the streams
         */
        @Override
        public Iterable<String> select(EmberInput.StreetLamp streetLamp) {
            List<String> output = new ArrayList<String>();
            String tag = streetLamp.isPowerOn() ? "on" : "off";
            output.add(tag);
            return output;
        }
    }
}
