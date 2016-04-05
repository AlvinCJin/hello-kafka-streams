package org.apache.kafka.connect.api;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KStreamBuilder extends org.apache.kafka.streams.kstream.KStreamBuilder{
    public KStream<SchemaAndValue, SchemaAndValue> stream(Properties connectorProps) {
        return null;
    }
}
