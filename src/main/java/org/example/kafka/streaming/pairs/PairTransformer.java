package org.example.kafka.streaming.pairs;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class PairTransformer<K,V> implements Transformer<K, V, KeyValue<K, Pair<V, V>>> {
    private ProcessorContext context;
    private String storeName;
    private KeyValueStore<Integer, V> stateStore;

    public PairTransformer(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        stateStore = (KeyValueStore<Integer, V>) context.getStateStore(storeName);
    }

    @Override
    public KeyValue<K, Pair<V, V>> transform(K key, V value) {
        if (stateStore.get(1) == null) {
            stateStore.put(1, value); return null;
        }
        KeyValue<K, Pair<V,V>> result = KeyValue.pair(key, new Pair<>(stateStore.get(1), value));
        stateStore.put(1, null);
        return result;
    }

    @Override
    public void close() { }

}

