package it.uniroma2.ember;

/**
 * This is the routing topology for Apache Flink operators and transformations
 * Project Ember entrypoint
 */


import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

public class CityOfLight {

    private final static long WINDOW_TIME_SEC  = 10;
    private final static long MONITOR_TIME_MINUTES_MIN = 1; // TODO by config
    private final static long MONITOR_TIME_MINUTES_MAX = 60; // TODO by config

    public static void main(String[] argv) throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


        // get input data
        Properties properties = new Properties();

        // setting group id
        /* to be setted by config file eventually */
        properties.setProperty("bootstrap.servers", "localhost:9092");
//        // only required for Kafka 0.8
//        properties.setProperty("zookeeper.connect", "localhost:2181");

        properties.setProperty("group.id", "thegrid");

        // STREETLAMPS DATA PROCESSING
        // setting topic and processing the stream from streetlamps
        DataStream<EmberInput.StreetLamp> lampStream = env
                .addSource(new FlinkKafkaConsumer010<>("lamp", new SimpleStringSchema(), properties))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
                    @Override
                    public long extractAscendingTimestamp(String s) {
                        return System.currentTimeMillis();
                    }
                })
                // parsing into a StreetLamp object
                .flatMap(new EmberInputFilter.EmberParseLamp());

        // LUMEN SENSORS DATA PROCESSING
        // setting topic and processing the stream from light sensors
        KeyedStream<EmberInput.LumenData, String> lumenStream = env
                .addSource(new FlinkKafkaConsumer010<>("lumen", new SimpleStringSchema(), properties))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
                    @Override
                    public long extractAscendingTimestamp(String s) {
                        return System.currentTimeMillis();
                    }
                })
                // parsing into LumenData object
                .flatMap(new EmberInputFilter.EmberParseLumen())
                // keying by address
                .keyBy(new EmberInputFilter.EmberLumenAddressSelector());


        // TRAFFIC DATA
        // setting topic and processing the stream from traffic data API
        KeyedStream<EmberInput.TrafficData, String> trafficStream = env
                .addSource(new FlinkKafkaConsumer010<>("traffic", new SimpleStringSchema(), properties))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
                    @Override
                    public long extractAscendingTimestamp(String s) {
                        return System.currentTimeMillis();
                    }
                })
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
        FlinkKafkaProducer010.FlinkKafkaProducer010Configuration kafkaConfigControl = FlinkKafkaProducer010.writeToKafkaWithTimestamps(
                controlStreamSerialized,
                "control",
                new SimpleStringSchema(),
                properties
        );
        // to guarantee an at-least-once delivery
        kafkaConfigControl.setLogFailuresOnly(false);
        kafkaConfigControl.setFlushOnCheckpoint(true);



        // MONITORING
        // to monitor Ember results we can rank the StreetLamps by:
        // 1. Life-Span
        DataStream<EmberStats.EmberLampLifeSpanRank> lifeSpanStream = lampStream
                .windowAll(SlidingEventTimeWindows.of(Time.minutes(MONITOR_TIME_MINUTES_MIN), Time.minutes(MONITOR_TIME_MINUTES_MAX)))
                .apply(new EmberStats.EmberLampLifeSpan());
        // serializing into a JSON
        DataStream<String> lifeSpanStreamSerialized = lifeSpanStream
                .flatMap(new EmberStats.EmberSerializeRank());

        // using Apache Kafka as a sink for ranking output
        FlinkKafkaProducer010.FlinkKafkaProducer010Configuration kafkaConfigRank = FlinkKafkaProducer010.writeToKafkaWithTimestamps(
                lifeSpanStreamSerialized,
                "rank",
                new SimpleStringSchema(),
                properties
        );
        kafkaConfigRank.setLogFailuresOnly(false);
        kafkaConfigRank.setFlushOnCheckpoint(true);


        // 2. Mean Power Consumption
        DataStream<EmberStats.LampConsumption> consumptionStream = lampStream
                .keyBy(new EmberInputFilter.EmberLampIdSelector())
                .flatMap(new EmberStats.EmberConsumptionMean());
        // state is queryable!
        // serializing into a JSON
        DataStream<String> consumptionStreamSerialized = consumptionStream
                .flatMap(new EmberStats.EmberSerializeConsumption());

        // using Apache Kafka as a sink for consumption output
        FlinkKafkaProducer010.FlinkKafkaProducer010Configuration kafkaConfigConsump = FlinkKafkaProducer010.writeToKafkaWithTimestamps(
                consumptionStreamSerialized,
                "consumption",
                new SimpleStringSchema(),
                properties
        );
        kafkaConfigConsump.setLogFailuresOnly(false);
        kafkaConfigConsump.setFlushOnCheckpoint(true);



        // ALERT
        // retrieving and serializing alert info
        DataStream<String> alertStream = env
                .addSource(new EmberAlert.EmberInfluxSource())
                .flatMap(new EmberAlert.EmberSerializeAlert());

        // using Apache Kafka as a sink for alert output
        FlinkKafkaProducer010.FlinkKafkaProducer010Configuration kafkaConfigAlert = FlinkKafkaProducer010.writeToKafkaWithTimestamps(
                alertStream,
                "alert",
                new SimpleStringSchema(),
                properties
        );
        kafkaConfigAlert.setLogFailuresOnly(false);
        kafkaConfigAlert.setFlushOnCheckpoint(true);



        Map<String, String> config = new HashMap<>();
        // This instructs the sink to emit after every element, otherwise they would be buffered
        config.put("bulk.flush.max.actions", "1");
        config.put("cluster.name", "elasticsearch");

        List<InetSocketAddress> transports = new ArrayList<>();
        transports.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));



        // DASHBOARD
        // storing for visualization and triggers in persistence level
        lampStream.addSink(new ElasticsearchSink(config, transports, new ElasticsearchSinkFunction<EmberInput.StreetLamp>() {
            public IndexRequest createIndexRequest(EmberInput.StreetLamp element) {
                Map<String, EmberInput.StreetLamp> json = new HashMap<>();
                json.put("street_lamp", element);

                return Requests.indexRequest()
                        .index("lamps")
                        .type("data")
                        .id(String.valueOf(element.getId()))
                        .source(json);
            }

            @Override
            public void process(EmberInput.StreetLamp element, RuntimeContext ctx, RequestIndexer indexer) {
                indexer.add(createIndexRequest(element));
            }
        }));
        controlStream.addSink(new EmberAlert.EmberControlSink());

        System.out.println(env.getExecutionPlan());

        env.execute("EmberCityOfLight");
    }

}
