package org.example.kafka.streaming.pairs;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

public class PairTransformer<K,V> implements Transformer<K, V, KeyValue<K, Pair<V, V>>> {
    private V left;

    @Override
    public void init(ProcessorContext context) {
        left = null;
    }

    @Override
    public KeyValue<K, Pair<V, V>> transform(K key, V value) {
        if (left == null) { left = value; return null; }
        KeyValue<K, Pair<V,V>> result = KeyValue.pair(key, new Pair<>(left, value));
        left = null;
        return result;
    }

    public void close() {
        left = null;
    }

}

