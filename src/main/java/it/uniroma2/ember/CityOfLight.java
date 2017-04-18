package it.uniroma2.ember;

import it.uniroma2.ember.elasticsearch.EmberElasticsearchAlertSource;
import it.uniroma2.ember.elasticsearch.EmberElasticsearchRankSinkFunction;
import it.uniroma2.ember.elasticsearch.EmberElasticsearchSinkFunction;
import it.uniroma2.ember.kafka.EmberKafkaControlSink;
import it.uniroma2.ember.kafka.EmberKafkaProducer;
import it.uniroma2.ember.operators.join.EmberAggregateSensors;
import it.uniroma2.ember.operators.join.EmberControlBufferApply;
import it.uniroma2.ember.operators.join.EmberControlRoom;
import it.uniroma2.ember.operators.join.EmberLampBufferApply;
import it.uniroma2.ember.operators.parser.EmberParseLamp;
import it.uniroma2.ember.operators.parser.EmberParseLumen;
import it.uniroma2.ember.operators.parser.EmberParseTraffic;
import it.uniroma2.ember.operators.selector.*;
import it.uniroma2.ember.operators.serializer.*;
import it.uniroma2.ember.stats.*;
import it.uniroma2.ember.utils.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
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

    private static long MONITOR_TIME_MINUTES_MIN = 1;
    private static long MONITOR_TIME_MINUTES_MAX = 60;
    public static int MONITOR_MAX_LEN            = 10;
    public static int MAX_LIFE_SPAN_DAYS         = 200;

    public static long ALERT_SOURCE_PERIOD_SECONDS = 30 * 1000;
    public static long TO_FAILURE_SECONDS          = 30 * 3;

    private static String ELASTICSEARCH_NAME    = "embercluster";
    private static String ELASTICSEARCH_ADDRESS = "localhost";
    private static int ELASTICSEARCH_PORT       = 9300;

    private static String KAFKA_ADDRESS = "localhost";
    private static int KAFKA_PORT       = 9092;

    private static boolean KAFKA_ALERT = true;



    @SuppressWarnings("unchecked")
    public static void main(String[] argv) throws Exception {

        if (argv.length == 2) {
            // assuming a new property file to open
            try {
                ParameterTool parameters = ParameterTool.fromPropertiesFile(argv[1]);

                MONITOR_TIME_MINUTES_MIN    = parameters.getLong("lifespan.rank.min", 1);
                MONITOR_TIME_MINUTES_MAX    = parameters.getLong("lifespan.rank.max", 60);
                MONITOR_MAX_LEN             = parameters.getInt("lifespan.rank.size", 10);
                MAX_LIFE_SPAN_DAYS          = parameters.getInt("lifespan.days.max", 200);

                ALERT_SOURCE_PERIOD_SECONDS = parameters.getLong("alerts.period.seconds", 30)*1000;
                TO_FAILURE_SECONDS          = parameters.getLong("alerts.electricalfailure.seconds", 90);

                ELASTICSEARCH_NAME          = parameters.get("elasticsearch.cluster.name", "embercluster");
                ELASTICSEARCH_ADDRESS       = parameters.get("elasticsearch.cluster.address", "localhost");
                ELASTICSEARCH_PORT          = parameters.getInt("elasticsearch.cluster.port", 9300);

                KAFKA_ADDRESS               = parameters.get("kafka.cluster.address", "localhost");
                KAFKA_PORT                  = parameters.getInt("kafka.cluster.port", 9092);

                KAFKA_ALERT                 = parameters.getBoolean("kafka.topic.alert", true);



            } catch (Exception e){
                e.printStackTrace();
                System.out.println("Error reading property file... exiting now!");
                return;
            }
        }

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.getConfig().setLatencyTrackingInterval(500);

        // get input data
        Properties properties = new Properties();

        // setting group id
        /* to be setted by config file eventually */
        properties.setProperty("bootstrap.servers", KAFKA_ADDRESS + ":" + String.valueOf(KAFKA_PORT));
        properties.setProperty("heartbeat.interval.ms", "10000");

        // preparing elasticsearch config for Elasticsearch API only
        Map<String, Object> elasticConfig = new HashMap<>();
        elasticConfig.put("cluster.address", ELASTICSEARCH_ADDRESS);
        elasticConfig.put("cluster.port", ELASTICSEARCH_PORT);
        elasticConfig.put("cluster.name", ELASTICSEARCH_NAME);

        // preparing elasticsearch for Elasticsearch Connector
        Map<String,String> config = new HashMap<>();
        config.put("bulk.flush.max.actions","1");
        config.put("cluster.name", ELASTICSEARCH_NAME);

        List<InetSocketAddress> transports = new ArrayList<>();
        transports.add(new InetSocketAddress(InetAddress.getByName(ELASTICSEARCH_ADDRESS), ELASTICSEARCH_PORT));

        // STREETLAMPS DATA PROCESSING
        // setting topic and processing the stream from streetlamps
        DataStream<StreetLamp> lampStream = env
                .addSource(new FlinkKafkaConsumer010<>("lamp", new SimpleStringSchema(), properties))
                // parsing into a StreetLamp object
                .flatMap(new EmberParseLamp())
                .name("lampstream")
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<StreetLamp>() {
                    @Override
                    public long extractAscendingTimestamp(StreetLamp lamp) {
                        return lamp.getSent() * 1000;
                    }
                });
        // keying by address
        KeyedStream<StreetLamp, String> lampStreamByAddress = lampStream
                .keyBy(new EmberLampAddressSelector());
        // keying by id
        KeyedStream<StreetLamp, Integer> lampStreamById = lampStream
                .keyBy(new EmberLampIdSelector());


        // LUMEN SENSORS DATA PROCESSING
        // setting topic and processing the stream from light sensors
        KeyedStream<LumenData, String> lumenStream = env
                .addSource(new FlinkKafkaConsumer010<>("lumen", new SimpleStringSchema(), properties))
                // parsing into LumenData object
                .flatMap(new EmberParseLumen())
                .name("lumenstream")
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LumenData>() {
                    @Override
                    public long extractAscendingTimestamp(LumenData lumen) {
                        return lumen.getRetrieved() * 1000;
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
                .name("trafficstream")
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TrafficData>() {
                    @Override
                    public long extractAscendingTimestamp(TrafficData traffic) {
                        return traffic.getRetrieved() * 1000;
                    }
                })
                // keying by address
                .keyBy(new EmberTrafficAddressSelector());



        // AGGREGATION - LUMEN + TRAFFIC DATA & LAMP (buffering)
        // computing mean value for ambient per street by a minute interval
        DataStream<Tuple2<String, Float>> ambientMean = lumenStream
                .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_TIME_SEC)))
                .apply(new EmberAmbientMean())
                .name("ambientmean");


        // computing mean value for traffic per street by a minute interval
        DataStream<Tuple2<String, Float>> trafficMean = trafficStream
                .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_TIME_SEC)))
                .apply(new EmberTrafficMean())
                .name("trafficmean");


        // computing a buffer of the streetlamps to use them with aggregated sensors
        // (in the buffer we compute a mean of the data from the sensor to filter out
        // duplicated records in case of network issues and to provide a punctual control output)
        DataStream<StreetLamp> streetLampBuffer = lampStreamById
                .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_TIME_SEC)))
                .apply(new EmberLampBufferApply())
                .name("lampbuffer");


        // joining traffic and ambient streams in order to get the optimal light value
        DataStream<Tuple2<String,Tuple2<Float, Float>>> aggregatedSensorsStream = trafficMean
                .join(ambientMean)
                .where(new EmberTrafficMeanSelector())
                .equalTo(new EmberLumenMeanSelector())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(WINDOW_TIME_SEC*3)))
                .apply(new EmberAggregateSensors());


        // CONTROL

        // Dev notes:
        //   in Apache Flink we use EventTime to process data from the source
        //   but we must use ProcessingTime to perform transformations and operations
        //   relative to the system processing time (such as aggregations in windows etc.)

        // joining optimal light stream with lamp data
        DataStream<StreetLamp> controlStream = streetLampBuffer
                .join(aggregatedSensorsStream)
                .where(new EmberLampAddressSelector())
                .equalTo(new EmberSensorsAddressSelector())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(WINDOW_TIME_SEC*6)))
                .apply(new EmberControlRoom());

        // buffering control to filter out duplicates (due to network failures and delays)
        // and to produce an optimal average control - 1 s buffer
        DataStream<StreetLamp> controlStreamBuffered = controlStream
                .keyBy(new EmberLampIdSelector())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(1000)))
                .apply(new EmberControlBufferApply());


        // - uncomment to debug the output control -
        // controlStreamBuffered.print();

        // using Apache Kafka as a sink for control output on multiple topics
        EmberKafkaControlSink.configuration(controlStreamBuffered, properties);



        // MONITORING
        // to monitor Ember results we can rank the StreetLamps by:
        // 1. Life-Span
        DataStream<EmberLampLifeSpanRank> lifeSpanStream = lampStream
                .windowAll(SlidingProcessingTimeWindows.of(Time.minutes(MONITOR_TIME_MINUTES_MIN), Time.minutes(MONITOR_TIME_MINUTES_MAX)))
                .apply(new EmberLampLifeSpan());
        // storing data in elasticsearch by rank position
        lifeSpanStream.addSink(new ElasticsearchSink(config, transports,
                new EmberElasticsearchRankSinkFunction("ember", "rank")));


        // 2. Mean Power Consumption
        // 1 h window - by id
        DataStream<LampEMAConsumption> consumptionStreamHourId = lampStreamById
                .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_CONSUMPTION_HOUR_MINUTES)))
                .apply(new EmberEMAWindowMean())
                .name("consumptionstream_hour_id");
        // 1 h window - by address aggregation
        DataStream<LampEMAConsumptionStreet> consumptionStreamHourStreet = lampStreamByAddress
                .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_CONSUMPTION_HOUR_MINUTES)))
                .apply(new EmberEMAWindowMeanAddress())
                .name("consumptionstream_hour_address");

        // storing data in elasticsearch
        // (we will use the high integrated kibana features to visualize and retrieve consumption stats)
        consumptionStreamHourId.addSink(new ElasticsearchSink(config, transports,
                new EmberElasticsearchSinkFunction("ember","consumption_hour" + "_id")));
        consumptionStreamHourStreet.addSink(new ElasticsearchSink(config, transports,
                new EmberElasticsearchSinkFunction("ember","consumption_hour" + "_address")));


        // ALERT
        // retrieving and serializing alert info
        DataStream<String> alertStream = env
                .addSource(new EmberElasticsearchAlertSource("ember", "lamp", elasticConfig))
                .flatMap(new EmberSerializeAlert());

        if (KAFKA_ALERT)
            // using Apache Kafka as a sink for alert output
            EmberKafkaProducer.configuration(alertStream, "alert", properties);
        else
            // using Elasticsearch to store alert sequence
            alertStream.addSink(new ElasticsearchSink(config, transports,
                    new EmberElasticsearchRankSinkFunction("ember", "alert")));


        // DASHBOARD
        // storing for visualization and triggers in persistence level
        lampStream.addSink(new ElasticsearchSink(config, transports,
                new EmberElasticsearchSinkFunction("ember","lamp")));

        // uncomment to print execution plan (flink.apache.org/visualizer)
        System.out.println(env.getExecutionPlan());

        env.execute("EmberCityOfLight");
    }

}