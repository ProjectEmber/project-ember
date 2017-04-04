package it.uniroma2.ember;

/**
 * This is the routing topology for Apache Flink operators and transformations
 * Project Ember entrypoint
 */


import it.uniroma2.ember.elasticsearch.EmberElasticsearchAlertSource;
import it.uniroma2.ember.elasticsearch.EmberElasticsearchRankSinkFunction;
import it.uniroma2.ember.elasticsearch.EmberElasticsearchSinkFunction;
import it.uniroma2.ember.kafka.EmberKafkaControlSink;
import it.uniroma2.ember.kafka.EmberKafkaProducer;
import it.uniroma2.ember.operators.join.EmberAggregateSensors;
import it.uniroma2.ember.operators.join.EmberControlRoom;
import it.uniroma2.ember.operators.parser.EmberParseLamp;
import it.uniroma2.ember.operators.parser.EmberParseLumen;
import it.uniroma2.ember.operators.parser.EmberParseTraffic;
import it.uniroma2.ember.operators.selector.*;
import it.uniroma2.ember.operators.serializer.*;
import it.uniroma2.ember.stats.*;
import it.uniroma2.ember.utils.*;
import org.apache.flink.api.java.functions.KeySelector;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

public class CityOfLight {

    private final static long WINDOW_TIME_SEC  = 10;

    private final static long WINDOW_CONSUMPTION_HOUR_MINUTES = 60;
    private final static long WINDOW_CONSUMPTION_DAY_HOURS    = 24;
    private final static long WINDOW_CONSUMPTION_WEEK_DAYS    = 7;

    private final static long MONITOR_TIME_MINUTES_MIN = 1; // TODO by config
    private final static long MONITOR_TIME_MINUTES_MAX = 60; // TODO by config

    private final static String CLUSTER_NAME = "embercluster"; // TODO by config
    private final static String CLUSTER_ADDRESS = "db.project-ember.city"; // TODO by config

    private final static int CLUSTER_PORT = 9300; // TODO by config

    @SuppressWarnings("unchecked")
    public static void main(String[] argv) throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // get input data
        Properties properties = new Properties();

        // setting group id
        /* to be setted by config file eventually */
        properties.setProperty("bootstrap.servers", "kafka.project-ember.city:9092");
        properties.setProperty("group.id", "thegrid");

        // preparing elasticsearch config for Elasticsearch API only
        Map<String, Object> elasticConfig = new HashMap<>();
        elasticConfig.put("cluster.address", CLUSTER_ADDRESS);
        elasticConfig.put("cluster.port", CLUSTER_PORT);
        elasticConfig.put("cluster.name", CLUSTER_NAME);

        // preparing elasticsearch for Elasticsearch Connector
        Map<String,String> config = new HashMap<>();
        config.put("bulk.flush.max.actions","1");
        config.put("cluster.name", CLUSTER_NAME);

        List<InetSocketAddress> transports = new ArrayList<>();
        transports.add(new InetSocketAddress(InetAddress.getByName(CLUSTER_ADDRESS), CLUSTER_PORT));

        // ready other properties from configuration TODO by config
        boolean streetAggregation = true;

        // STREETLAMPS DATA PROCESSING
        // setting topic and processing the stream from streetlamps
        DataStream<StreetLamp> lampStream = env
                .addSource(new FlinkKafkaConsumer010<>("lamp", new SimpleStringSchema(), properties))
                // parsing into a StreetLamp object
                .flatMap(new EmberParseLamp())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<StreetLamp>() {
                    @Override
                    public long extractAscendingTimestamp(StreetLamp lamp) {
                        return lamp.getSent();
                    }
                });

        // LUMEN SENSORS DATA PROCESSING
        // setting topic and processing the stream from light sensors
        KeyedStream<LumenData, String> lumenStream = env
                .addSource(new FlinkKafkaConsumer010<>("lumen", new SimpleStringSchema(), properties))
                // parsing into LumenData object
                .flatMap(new EmberParseLumen())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LumenData>() {
                    @Override
                    public long extractAscendingTimestamp(LumenData lumen) {
                        return lumen.getRetrieved();
                    }
                })
                // keying by address
                .keyBy(new EmberLumenAddressSelector());


        // TRAFFIC DATA
        // setting topic and processing the stream from traffic data API
        KeyedStream<TrafficData, String> trafficStream = env
                .addSource(new FlinkKafkaConsumer010<>("traffic", new SimpleStringSchema(), properties))
                // parsing into TrafficData object
                .flatMap(new EmberParseTraffic())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TrafficData>() {
                    @Override
                    public long extractAscendingTimestamp(TrafficData traffic) {
                        return traffic.getRetrieved();
                    }
                })
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

        // using Apache Kafka as a sink for control output on multiple topics
        EmberKafkaControlSink.configuration(controlStream, properties);



        // MONITORING
        // to monitor Ember results we can rank the StreetLamps by:
        // 1. Life-Span
        DataStream<EmberLampLifeSpanRank> lifeSpanStream = lampStream
                .windowAll(SlidingEventTimeWindows.of(Time.minutes(MONITOR_TIME_MINUTES_MIN), Time.minutes(MONITOR_TIME_MINUTES_MAX)))
                .apply(new EmberLampLifeSpan());
        // storing data in elasticsearch by rank position
        lifeSpanStream.addSink(new ElasticsearchSink(config, transports,
                new EmberElasticsearchRankSinkFunction("ember", "rank")));


        // 2. Mean Power Consumption
        // creating a keyed stream using global properties
        // - by lamp id selection
        // - by street aggregation

        DataStream<Object> consumptionStreamHour = null;
        DataStream<Object> consumptionStreamDay = null;
        DataStream<Object> consumptionStreamWeek = null;

        // AGGREGATION by ID or STREET
        // defining the key selector
        KeySelector meanSelector = null;
        if (streetAggregation)
            meanSelector = new EmberLampAddressSelector();
        else
            meanSelector = new EmberLampIdSelector();
        KeyedStream<Object, Object> consumptionStreamById = lampStream
                .keyBy(meanSelector);

        // windowing the keyed stream by ...
        // 1 h window
        consumptionStreamHour = consumptionStreamById
                .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_CONSUMPTION_HOUR_MINUTES)))
                .apply(new EmberEMAWindowMean(streetAggregation));

        // 1 d window
        consumptionStreamDay = consumptionStreamById
                .window(TumblingEventTimeWindows.of(Time.hours(WINDOW_CONSUMPTION_DAY_HOURS)))
                .apply(new EmberEMAWindowMean(streetAggregation));

        // 1 w window
        consumptionStreamWeek = consumptionStreamById
                .window(TumblingEventTimeWindows.of(Time.days(WINDOW_CONSUMPTION_WEEK_DAYS)))
                .apply(new EmberEMAWindowMean(streetAggregation));

        // storing data in elasticsearch
        String emaType = (streetAggregation) ? "_street" : "_id";
        consumptionStreamHour.addSink(new ElasticsearchSink(config, transports,
                new EmberElasticsearchSinkFunction("ember","consumption_hour" + emaType)));

        consumptionStreamDay.addSink(new ElasticsearchSink(config, transports,
                new EmberElasticsearchSinkFunction("ember","consumption_day" + emaType)));

        consumptionStreamWeek.addSink(new ElasticsearchSink(config, transports,
                new EmberElasticsearchSinkFunction("ember","consumption_week" + emaType)));


        // ALERT
        // retrieving and serializing alert info
        DataStream<String> alertStream = env
                .addSource(new EmberElasticsearchAlertSource("ember", "lamp", elasticConfig))
                .flatMap(new EmberSerializeAlert());

        // using Apache Kafka as a sink for alert output
        EmberKafkaProducer.configuration(alertStream, "alert", properties);

        // DASHBOARD
        // storing for visualization and triggers in persistence level
        lampStream.addSink(new ElasticsearchSink(config, transports,
                new EmberElasticsearchSinkFunction("ember","lamp")));

        lampStream.print();

        System.out.println(env.getExecutionPlan());

        env.execute("EmberCityOfLight");
    }

}
