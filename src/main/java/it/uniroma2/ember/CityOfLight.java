package it.uniroma2.ember;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

/**
 * Created by federico on 10/03/17.
 */
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

        // setting topic and processing the stream from streetlamps
        SplitStream<EmberInput.StreetLamp> selector = env
                .addSource(new FlinkKafkaConsumer010<>("lamp", new SimpleStringSchema(), properties))
                // parsing into a StreetLamp object
                .flatMap(new EmberInputFilter.EmberParseLamp())
                // filtering powered on lamps
                .split(new EmberInputFilter.EmberPowerSelector());

        // selecting streams
        DataStream<EmberInput.StreetLamp> poweredOn  = selector.select("on");
        DataStream<EmberInput.StreetLamp> poweredOff = selector.select("off");

        // redirecting streams


        env.execute("EmberInputFilter");
    }

}
