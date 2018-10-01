package org.example.kafka.streaming;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.example.kafka.streaming.pairs.Pair;
import org.example.kafka.streaming.pairs.PairTransformerSupplier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class SpringStreamConfig {

    @Bean
    public KStream<String, String> sampleStream(StreamsBuilder builder) {
        KStream<String, String> messages = builder.stream("sample-messages", Consumed.with(Serdes.String(), Serdes.String()));
        transformToPairs(messages);
        KeyValueBytesStoreSupplier store = Stores.persistentKeyValueStore("sample-stream-store");
        StoreBuilder storeBuilder = new KeyValueStoreBuilder<>(
                store,
                new Serdes.StringSerde(),
                new Serdes.StringSerde(),
                Time.SYSTEM
        );
        builder.addStateStore(storeBuilder);
        return messages;
    }

    private void transformToPairs(KStream<String, String> messages) {
        messages.transform(new PairTransformerSupplier<>())
                .filter((key, value) -> value != null)
                .mapValues(Pair::toString)
                .to("pairsTransformed");
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public StreamsConfig kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sample-stream-application");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        return new StreamsConfig(props);
    }

}
