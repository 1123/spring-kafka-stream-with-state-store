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

    @Bean
    public KStream<String, String> sampleStream(StreamsBuilder builder) {
        KStream<String, String> messages = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));
        KeyValueBytesStoreSupplier store = Stores.persistentKeyValueStore(stateStoreName);
        StoreBuilder storeBuilder = new KeyValueStoreBuilder<>(
                store,
                new Serdes.StringSerde(),
                new Serdes.StringSerde(),
                Time.SYSTEM
        );
        builder.addStateStore(storeBuilder);
        transformToPairs(messages);
        return messages;
    }

    private void transformToPairs(KStream<String, String> messages) {
        KStream<String, Pair<String, String>> pairs = messages.transform(
                new PairTransformerSupplier<>(),
                stateStoreName
        );
        KStream<String, Pair<String, String>> filtered = pairs.filter((key, value) -> value != null);
        KStream<String, String> serialized = filtered.mapValues(Pair::toString);
        serialized.to(outputTopic);
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public StreamsConfig kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, streamingAppName);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        return new StreamsConfig(props);
    }

}
