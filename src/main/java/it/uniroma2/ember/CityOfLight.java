package it.uniroma2.ember;

/**
 * This is the routing topology for Apache Flink operators and transformations
 * Project Ember entrypoint
 */


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.ember.kafka.EmberKafkaProducer;
import it.uniroma2.ember.operators.join.EmberAggregateSensors;
import it.uniroma2.ember.operators.join.EmberControlRoom;
import it.uniroma2.ember.operators.parser.EmberParseLamp;
import it.uniroma2.ember.operators.parser.EmberParseLumen;
import it.uniroma2.ember.operators.parser.EmberParseTraffic;
import it.uniroma2.ember.operators.selector.*;
import it.uniroma2.ember.operators.serializer.EmberSerializeAlert;
import it.uniroma2.ember.operators.serializer.EmberSerializeLamp;
import it.uniroma2.ember.stats.*;
import it.uniroma2.ember.utils.LumenData;
import it.uniroma2.ember.utils.StreetLamp;
import it.uniroma2.ember.utils.TrafficData;
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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
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
        DataStream<StreetLamp> lampStream = env
                .addSource(new FlinkKafkaConsumer010<>("lamp", new SimpleStringSchema(), properties))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
                    @Override
                    public long extractAscendingTimestamp(String s) {
                        return System.currentTimeMillis();
                    }
                })
                // parsing into a StreetLamp object
                .flatMap(new EmberParseLamp());

        // LUMEN SENSORS DATA PROCESSING
        // setting topic and processing the stream from light sensors
        KeyedStream<LumenData, String> lumenStream = env
                .addSource(new FlinkKafkaConsumer010<>("lumen", new SimpleStringSchema(), properties))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
                    @Override
                    public long extractAscendingTimestamp(String s) {
                        return System.currentTimeMillis();
                    }
                })
                // parsing into LumenData object
                .flatMap(new EmberParseLumen())
                // keying by address
                .keyBy(new EmberLumenAddressSelector());


        // TRAFFIC DATA
        // setting topic and processing the stream from traffic data API
        KeyedStream<TrafficData, String> trafficStream = env
                .addSource(new FlinkKafkaConsumer010<>("traffic", new SimpleStringSchema(), properties))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
                    @Override
                    public long extractAscendingTimestamp(String s) {
                        return System.currentTimeMillis();
                    }
                })
                // parsing into TrafficData object
                .flatMap(new EmberParseTraffic())
                // keying by address
                .keyBy(new EmberTrafficAddressSelector());




        // AGGREGATION - LUMEN + TRAFFIC DATA
        // computing mean value for ambient per street by a minute interval
        DataStream<Tuple2<String, Float>> ambientMean = lumenStream
                .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_TIME_SEC)))
                .apply(new EmberAmbientMean());

        // computing mean value for traffic per street by a minute interval
        DataStream<Tuple2<String, Float>> trafficMean = trafficStream
                .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_TIME_SEC)))
                .apply(new EmberTrafficMean());

        // joining traffic and ambient streams in order to get the optimal light value
        DataStream<Tuple2<String,Tuple2<Float, Float>>> aggregatedSensorsStream = trafficMean
                .join(ambientMean)
                .where(new EmberTrafficMeanSelector())
                .equalTo(new EmberLumenMeanSelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_TIME_SEC)))
                .apply(new EmberAggregateSensors());



        // CONTROL
        // joining optimal light stream with lamp data
        DataStream<StreetLamp> controlStream = lampStream
                .join(aggregatedSensorsStream)
                .where(new EmberLampAddressSelector())
                .equalTo(new EmberSensorsAddressSelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_TIME_SEC)))
                .apply(new EmberControlRoom());
        // serializing into a JSON
        DataStream<String> controlStreamSerialized = controlStream
                .flatMap(new EmberSerializeLamp());

        // using Apache Kafka as a sink for control output
        EmberKafkaProducer.configuration(controlStreamSerialized, "control", properties);



        // MONITORING
        // to monitor Ember results we can rank the StreetLamps by:
        // 1. Life-Span
        DataStream<EmberLampLifeSpanRank> lifeSpanStream = lampStream
                .windowAll(SlidingEventTimeWindows.of(Time.minutes(MONITOR_TIME_MINUTES_MIN), Time.minutes(MONITOR_TIME_MINUTES_MAX)))
                .apply(new EmberLampLifeSpan());
        // serializing into a JSON
        DataStream<String> lifeSpanStreamSerialized = lifeSpanStream
                .flatMap(new EmberSerializeRank());

        // using Apache Kafka as a sink for ranking output
        EmberKafkaProducer.configuration(lifeSpanStreamSerialized, "rank", properties);


        // 2. Mean Power Consumption
        DataStream<LampConsumption> consumptionStream = lampStream
                .keyBy(new EmberLampIdSelector())
                .flatMap(new EmberConsumptionMean());
        // state is queryable!
        // serializing into a JSON
        DataStream<String> consumptionStreamSerialized = consumptionStream
                .flatMap(new EmberSerializeConsumption());

        // using Apache Kafka as a sink for consumption output
        EmberKafkaProducer.configuration(consumptionStreamSerialized, "consumption", properties);



        // ALERT
        // retrieving and serializing alert info
        DataStream<String> alertStream = env
                .addSource()
                .flatMap(new EmberSerializeAlert());

        // using Apache Kafka as a sink for alert output
        EmberKafkaProducer.configuration(alertStream, "alert", properties);



        Map<String, String> config = new HashMap<>();
        // This instructs the sink to emit after every element, otherwise they would be buffered
        config.put("bulk.flush.max.actions", "1");
        config.put("cluster.name", "embercluster");

        List<InetSocketAddress> transports = new ArrayList<>();
        transports.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));



        // DASHBOARD
        // storing for visualization and triggers in persistence level
        lampStream.addSink(new ElasticsearchSink(config, transports, new ElasticsearchSinkFunction<StreetLamp>() {
            public IndexRequest createIndexRequest(StreetLamp element) {
                ObjectMapper mapper = new ObjectMapper();
                byte[] json = new byte[0];
                try {
                    json = mapper.writeValueAsBytes(element);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }

                Settings settings = Settings.settingsBuilder()
                        .put("cluster.name","embercluster")
                        .build();

                TransportClient transportClient = null;
                try {
                    transportClient = TransportClient.builder().settings(settings).build()
                            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                }

                SearchResponse response = transportClient.prepareSearch("ember")
                        .setTypes("lamp")
                        .setQuery(QueryBuilders.termQuery("_id",String.valueOf(element.getId())))
                        .execute()
                        .actionGet();

                System.out.println(response.toString());

                return Requests.indexRequest()
                        .index("ember")
                        .type("lamp")
                        .id(String.valueOf(element.getId()))
                        .source(json);
            }

            @Override
            public void process(StreetLamp element, RuntimeContext ctx, RequestIndexer indexer) {
                indexer.add(createIndexRequest(element));
            }
        }));
        controlStream.addSink();

        System.out.println(env.getExecutionPlan());

        env.execute("EmberCityOfLight");
    }

}
