package it.uniroma2.ember;

/**
 * This is the routing topology for Apache Flink operators and transformations
 * Project Ember entrypoint
 */


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

public class CityOfLight
{
    public static void Main(int argc, char argv[]) throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();

        // get input data
        Properties properties = new Properties();

        // setting group id
        properties.setProperty("group.id", "thegrid");

        // STREETLAMPS DATA PROCESSING
        // setting topic and processing the stream from streetlamps
        KeyedStream<EmberInput.StreetLamp, String> lampSelector = env
                .addSource(new FlinkKafkaConsumer010<>("lamp", new SimpleStringSchema(), properties))
                // parsing into a StreetLamp object
                .flatMap(new EmberInputFilter.EmberParseLamp())
                // keying by address
                .keyBy(new EmberInputFilter.EmberLampAddressSelector());


        // LUMEN SENSORS DATA PROCESSING
        // setting topic and processing the stream from light sensors
        KeyedStream<EmberInput.LumenData, String> lumenStream = env
                .addSource(new FlinkKafkaConsumer010<>("lumen", new SimpleStringSchema(), properties))
                // parsing into LumenData object
                .flatMap(new EmberInputFilter.EmberParseLumen())
                // keying by address
                .keyBy(new EmberInputFilter.EmberLumenAddressSelector());

        // computing mean value for ambient per street by a minute interval
        DataStream<Tuple2<String, Float>> ambientMean = lumenStream
                .window(TumblingEventTimeWindows.of(Time.seconds(10*6)))
                .apply(new EmberStats.EmberAmbientMean());

        env.execute("EmberCityOfLight");
    }

}
