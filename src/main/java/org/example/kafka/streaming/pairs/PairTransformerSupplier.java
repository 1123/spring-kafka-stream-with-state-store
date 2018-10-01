package org.example.kafka.streaming.pairs;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;

public class PairTransformerSupplier<K,V> implements TransformerSupplier<K, V, KeyValue<K, Pair<V,V>>> {

    @Override
    public Transformer<K, V, KeyValue<K, Pair<V, V>>> get() {
        return new PairTransformer<>();
    }
}
