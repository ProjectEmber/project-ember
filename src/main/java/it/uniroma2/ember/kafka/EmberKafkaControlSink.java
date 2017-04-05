package it.uniroma2.ember.kafka;

import it.uniroma2.ember.utils.StreetLamp;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Properties;

/**
 * utility class to produce control-unit oriented topics on Kafka
 */
public class EmberKafkaControlSink {

    public static class ControlSerializationSchema implements KeyedSerializationSchema<StreetLamp> {

        @Override
        public byte[] serializeKey(StreetLamp lamp) {
            return lamp.getControl_unit().getBytes();
        }

        @Override
        public byte[] serializeValue(StreetLamp lamp) {
            try {
                return new ObjectMapper().writeValueAsBytes(lamp);
            } catch (Exception e) {
                e.printStackTrace();
                return "{}".getBytes();
            }
        }

        @Override
        public String getTargetTopic(StreetLamp lamp) {
            return lamp.getControl_unit();
        }
    }

    public static void configuration(DataStream<StreetLamp> stream, Properties properties) {

        // using Apache Kafka as
        FlinkKafkaProducer010.FlinkKafkaProducer010Configuration kafkaConfig = FlinkKafkaProducer010
                .writeToKafkaWithTimestamps(
                        stream,
                        "control",
                        new ControlSerializationSchema(),
                        properties
                );

        kafkaConfig.setLogFailuresOnly(false);
        kafkaConfig.setFlushOnCheckpoint(true);
    }
}
