package it.uniroma2.ember.kafka;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

/**
 * Utility class to let Ember produce results from control system on Kafka
 */

public class EmberKafkaProducer {

    public static void configuration(DataStream<String> stream, String topic, Properties properties) {

        // using Apache Kafka as a sink for ranking output
        FlinkKafkaProducer010.FlinkKafkaProducer010Configuration kafkaConfig = FlinkKafkaProducer010
                .writeToKafkaWithTimestamps(
                        stream,
                        topic,
                        new SimpleStringSchema(),
                        properties
        );
        kafkaConfig.setLogFailuresOnly(false);
        kafkaConfig.setFlushOnCheckpoint(true);
    }
}
