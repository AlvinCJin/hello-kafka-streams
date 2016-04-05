package org.apache.kafka.connect.api;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;

public class KStreamBuilder extends org.apache.kafka.streams.kstream.KStreamBuilder {
    public <K,V> KStream<K, V> stream(Serde<K> keySerde, Serde<V> valueSerde, EmbeddedConnectSource connectSource) {
        return null;
    }
}
