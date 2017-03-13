package it.uniroma2.ember;

/**
 * This is the routing topology for Apache Flink operators and transformations
 * Project Ember entrypoint
 */


import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;
import java.util.Properties;

public class CityOfLight {

    private final static int WINDOW_TIME_SEC  = 10;
    private final static int MONITOR_TIME_MINUTES_MIN = 1; // TODO by config
    private final static int MONITOR_TIME_MINUTES_MAX = 60; // TODO by config

    public static void main(String[] argv) throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();


        // get input data
        Properties properties = new Properties();

        // setting group id
        /* to be setted by config file eventually */
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // only required for Kafka 0.8
        properties.setProperty("zookeeper.connect", "localhost:2181");

        properties.setProperty("group.id", "thegrid");

        // STREETLAMPS DATA PROCESSING
        // setting topic and processing the stream from streetlamps
        DataStream<EmberInput.StreetLamp> lampStream = env
                .addSource(new FlinkKafkaConsumer010<>("lamp", new SimpleStringSchema(), properties))
                // parsing into a StreetLamp object
                .flatMap(new EmberInputFilter.EmberParseLamp());

        // LUMEN SENSORS DATA PROCESSING
        // setting topic and processing the stream from light sensors
        KeyedStream<EmberInput.LumenData, String> lumenStream = env
                .addSource(new FlinkKafkaConsumer010<>("lumen", new SimpleStringSchema(), properties))
                // parsing into LumenData object
                .flatMap(new EmberInputFilter.EmberParseLumen())
                // keying by address
                .keyBy(new EmberInputFilter.EmberLumenAddressSelector());


        // TRAFFIC DATA
        // setting topic and processing the stream from traffic data API
        KeyedStream<EmberInput.TrafficData, String> trafficStream = env
                .addSource(new FlinkKafkaConsumer010<>("traffic", new SimpleStringSchema(), properties))
                // parsing into TrafficData object
                .flatMap(new EmberInputFilter.EmberParseTraffic())
                // keying by address
                .keyBy(new EmberInputFilter.EmberTrafficAddressSelector());


        // AGGREGATION - LUMEN + TRAFFIC DATA
        // computing mean value for ambient per street by a minute interval
        DataStream<Tuple2<String, Float>> ambientMean = lumenStream
                .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_TIME_SEC)))
                .apply(new EmberStats.EmberAmbientMean());

        // computing mean value for traffic per street by a minute interval
        DataStream<Tuple2<String, Float>> trafficMean = trafficStream
                .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_TIME_SEC)))
                .apply(new EmberStats.EmberTrafficMean());

        // joining traffic and ambient streams in order to get the optimal light value
        DataStream<Tuple2<String,Tuple2<Float, Float>>> aggregatedSensorsStream = trafficMean
                .join(ambientMean)
                .where(new EmberSensorsAggregation.EmberTrafficMeanSelector())
                .equalTo(new EmberSensorsAggregation.EmberLumenMeanSelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_TIME_SEC)))
                .apply(new EmberSensorsAggregation.EmberAggregateSensors());


        // CONTROL
        // joining optimal light stream with lamp data
        DataStream<EmberInput.StreetLamp> controlStream = lampStream
                .join(aggregatedSensorsStream)
                .where(new EmberInputFilter.EmberLampAddressSelector())
                .equalTo(new EmberSensorsAggregation.EmberSensorsAddressSelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_TIME_SEC)))
                .apply(new EmberControlFeedback.EmberControlRoom());

        // serializing into a JSON
        DataStream<String> controlStreamSerialized = controlStream
                .flatMap(new EmberControlFeedback.EmberSerializeLamp());

        // using Apache Kafka as a sink for control output
        FlinkKafkaProducer010.FlinkKafkaProducer010Configuration controlKafkaConfig = FlinkKafkaProducer010.writeToKafkaWithTimestamps(
                controlStreamSerialized,
                "control",
                new SimpleStringSchema(),
                properties
        );
        // to guarantee an at-least-once delivery
        controlKafkaConfig.setLogFailuresOnly(false);
        controlKafkaConfig.setFlushOnCheckpoint(true);


        // MONITORING
        // to monitor Ember results we can rank the StreetLamps by:
        // - lamp failures
        // - remaining life-span
        // - mean power consumption

        // Lamp Failures
        // TODO

        // Life-Span
        DataStream<EmberStats.EmberLampLifeSpanRank> lifeSpanStream = lampStream
                .windowAll(SlidingEventTimeWindows.of(Time.minutes(MONITOR_TIME_MINUTES_MIN), Time.minutes(MONITOR_TIME_MINUTES_MAX)))
                .apply(new EmberStats.EmberLampLifeSpan());

        // Mean Power Consumption
        // TODO


        System.out.println(env.getExecutionPlan());

        env.execute("EmberCityOfLight");
    }

}
