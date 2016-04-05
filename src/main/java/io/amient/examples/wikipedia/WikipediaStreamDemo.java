/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.amient.examples.wikipedia;

import com.fasterxml.jackson.databind.JsonNode;
import io.amient.kafka.connect.irc.IRCFeedConnector;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.connect.api.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * This is an equivalent of hello-samza project which transforms wikipedia irc channel events into wikipedia-raw
 * and does some basic mapping and stateful aggregation of stats over this record stream.
 *
 * The pipeline consisting of 3 components which are roughly equivalents of all hello-samza tasks.
 *
 * 1.   Wikipedia Feed - Kafka Connect Source Stream for connecting external data.
 *
 * 2.1. Wikipedia Edit Parser - KStream-KStream mapper which parses incoming `wikipedia-raw` messages, parses them and
 * publishes the result into `wikipedia-parsed` record stream. Published messages are formatted as json.
 *
 * 2.2. Aggregate Number of Edits per Use - Stateful per-user counter.
 */

public class WikipediaStreamDemo {

    private static final Logger log = LoggerFactory.getLogger(WikipediaStreamDemo.class);

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) throws Exception {

        final String bootstrapServers = args.length == 1 ? args[0] : DEFAULT_BOOTSTRAP_SERVERS;

        KafkaStreams streams = createWikipediaStreamsInstance(bootstrapServers);
        try {
            streams.start();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    streams.close();
                }
            });
            while (true) Thread.sleep(1000);
        } catch (Throwable e) {
            log.error("Stopping the application due to streams initialization error ", e);
        }
    }

    private static KafkaStreams createWikipediaStreamsInstance(String bootstrapServers) {
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        KStreamBuilder builder = new KStreamBuilder();
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wikipedia-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        Properties connectorProps = new Properties();
        connectorProps.put(ConnectorConfig.NAME_CONFIG, "wikipedia-irc-source");
        connectorProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, "io.amient.kafka.connect.irc.IRCFeedConnector");
        connectorProps.put(ConnectorConfig.TASKS_MAX_CONFIG, "10");
        connectorProps.put(IRCFeedConnector.IRC_HOST_CONFIG, "irc.wikimedia.org");
        connectorProps.put(IRCFeedConnector.IRC_PORT_CONFIG, "6667");
        connectorProps.put(IRCFeedConnector.IRC_CHANNELS_CONFIG, "#en.wikipedia,#en.wiktionary,#en.wikinews");

        KStream<SchemaAndValue, SchemaAndValue> wikipediaRaw = builder.stream(connectorProps);

        KStream<String, WikipediaMessage> wikipediaParsed =
                wikipediaRaw.map(WikipediaMessage::parseIRCFromSource)
                        .filter(WikipediaMessage::filterNonNull)
                        .through(Serdes.String(), new JsonPOJOSerde<>(WikipediaMessage.class), "wikipedia-parsed");

        KTable<String, Long> totalEditsByUser = wikipediaParsed
                .filter((key, value) -> value.type == WikipediaMessage.Type.EDIT)
                .countByKey(Serdes.String(), "wikipedia-edits-by-user");

        //some print
        totalEditsByUser.toStream().process(() -> new AbstractProcessor<String, Long>() {
            @Override
            public void process(String user, Long numEdits) {
                System.out.println("USER: " + user + " num.edits: " + numEdits);
            }
        });

        return new KafkaStreams(builder, props);

    }

}
