package org.example.kafka.streaming;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.example.kafka.streaming.pairs.Pair;
import org.example.kafka.streaming.pairs.PairTransformerSupplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class SpringStreamConfig {

    @Value("${local.stream.store}")
    private String stateStoreName;
    @Value("${local.stream.input}")
    private String inputTopic;
    @Value("${local.stream.output}")
    private String outputTopic;
    @Value("${local.stream.name}")
    private String streamingAppName;
    @Value("${local.kafka.bootstrap-servers}")
    private String bootStrapServers;

    @Autowired
    private PairTransformerSupplier<String> pairTransformerSupplier;

    @Bean
    public KStream<Integer, String> sampleStream(StreamsBuilder builder) {
        KStream<Integer, String> messages = builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()));
        KeyValueBytesStoreSupplier store = Stores.persistentKeyValueStore(stateStoreName);
        StoreBuilder storeBuilder = new KeyValueStoreBuilder<>(
                store,
                new Serdes.IntegerSerde(),
                new Serdes.StringSerde(),
                Time.SYSTEM
        );
        builder.addStateStore(storeBuilder);
        transformToPairs(messages);
        return messages;
    }

    private void transformToPairs(KStream<Integer, String> messages) {
        KStream<Integer, Pair<String, String>> pairs = messages.transform(
                pairTransformerSupplier,
                stateStoreName
        );
        KStream<Integer, Pair<String, String>> filtered = pairs.filter((key, value) -> value != null);
        KStream<Integer, String> serialized = filtered.mapValues(Pair::toString);
        serialized.to(outputTopic, Produced.with(new Serdes.IntegerSerde(), new Serdes.StringSerde()));
    }

    @Bean(name = "sampleStreamsConfig")
    public StreamsConfig kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, streamingAppName);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        return new StreamsConfig(props);
    }

}
