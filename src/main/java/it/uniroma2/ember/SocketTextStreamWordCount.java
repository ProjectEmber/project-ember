package it.uniroma2.ember;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.sling.commons.json.JSONObject;

import java.sql.Timestamp;
import java.util.Properties;

import static it.uniroma2.ember.EmberInput.parseStreetLamp;

/**
 * This example shows an implementation of WordCount with data from a text
 * socket. To run the example make sure that the service providing the text data
 * is already up and running.
 * 
 * <p>
 * To start an example socket text stream on your local machine run netcat from
 * a command line: <code>nc -lk 9999</code>, where the parameter specifies the
 * port number.
 * 
 * 
 * <p>
 * Usage:
 * <code>SocketTextStreamWordCount &lt;hostname&gt; &lt;port&gt;</code>
 * <br>
 * 
 * <p>
 * This example shows how to:
 * <ul>
 * <li>use StreamExecutionEnvironment.socketTextStream
 * <li>write a simple Flink program
 * <li>write and use user-defined functions
 * </ul>
 * 
 * @see <a href="www.openbsd.org/cgi-bin/man.cgi?query=nc">netcat</a>
 */
public class SocketTextStreamWordCount {

	//
	//	Program
	//

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		// get input data
		Properties properties = new Properties();
		/* to be setted by config file eventually */
		properties.setProperty("bootstrap.servers", "localhost:9092");
        // only required for Kafka 0.8
		properties.setProperty("zookeeper.connect", "localhost:2181");

		// setting group id
		properties.setProperty("group.id", "thegrid");
		// setting topic
		DataStream<String> stream = env
				.addSource(new FlinkKafkaConsumer010<>("lamp", new SimpleStringSchema(), properties))
				.flatMap(new FlatMapFunction<String, EmberInput.StreetLamp>() {
					@Override
					public void flatMap(String s, Collector<EmberInput.StreetLamp> collector) throws Exception {

						if (EmberInput.parseStreetLamp(s) == null) {
							collector.collect(new EmberInput.StreetLamp(5,"ciao","prova", 4, true, 5, 43,343));
						} else {
							collector.collect(EmberInput.parseStreetLamp(s));
						}
					}
				})
				.flatMap(new FlatMapFunction<EmberInput.StreetLamp, String>() {
					@Override
					public void flatMap(EmberInput.StreetLamp streetLamp, Collector<String> collector) throws Exception {
						collector.collect(streetLamp.getAddress());
					}
				});

		stream.print();

		// execute program
		env.execute("Java WordCount from SocketTextStream Example");
	}

	//
	// 	User Functions
	//

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}	
}
