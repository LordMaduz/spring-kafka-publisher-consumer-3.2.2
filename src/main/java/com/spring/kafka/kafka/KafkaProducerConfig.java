package com.spring.kafka.kafka;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.RoutingKafkaTemplate;
import org.springframework.kafka.support.serializer.DelegatingByTopicSerializer;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.*;
import java.util.regex.Pattern;

import com.spring.kafka.InventoryProto;
import com.spring.kafka.model.Order;
import com.spring.kafka.model.avro.TransactionEvent;

@Configuration
public class KafkaProducerConfig extends KafkaBasicConfig {

    @Bean
    public ProducerFactory<Object, Object> producerFactory() {
        final Map<String, Object> props = getBasicConfig();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        final DefaultKafkaProducerFactory<Object, Object> producerFactory = new DefaultKafkaProducerFactory<>(props);
        return producerFactory;
    }

    @Bean
    public ProducerFactory<Object, Object> producerFactoryWithDelegatingSerializer() {
        final Map<String, Object> props = getBasicConfig();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        final DefaultKafkaProducerFactory<Object, Object> producerFactoryByDelegatingByTopic = new DefaultKafkaProducerFactory<>(props, null,
                new DelegatingByTopicSerializer(
                        Map.of(

                                Pattern.compile("proto-event-topic"), new KafkaProtobufSerializer<>(),
                                Pattern.compile("json-event-topic"), new JsonSerializer<>(),
                                Pattern.compile("avro-event-topic"), new KafkaAvroSerializer()),
                        new ByteArraySerializer()));

        final DefaultKafkaProducerFactory<Object, Object> producerFactoryDelegatingByType = new DefaultKafkaProducerFactory<>(props, null,
                new DelegatingByTypeSerializer(
                        Map.of(
                                InventoryProto.Inventory.class, new KafkaProtobufSerializer<>(),
                                Byte.class, new ByteArraySerializer(),
                                TransactionEvent.class, new KafkaAvroSerializer(),
                                Order.class, new JsonSerializer<>())));

        return producerFactoryByDelegatingByTopic;
    }

    @Bean
    public KafkaTemplate<?, ?> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactoryWithDelegatingSerializer());
    }


    @Bean
    public KafkaTemplate<?, ?> jsonKafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public KafkaTemplate<?, ?> byteKafkaTemplate(final ProducerFactory<?, ?> producerFactory) {
        return new KafkaTemplate<>(producerFactory(), Collections.singletonMap(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
    }

    @Bean
    public KafkaTemplate<?, ?> protoKafkaTemplate(final ProducerFactory<?, ?> producerFactory) {
        return new KafkaTemplate<>(producerFactory(), Collections.singletonMap(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class));
    }

    @Bean
    public KafkaTemplate<?, ?> avroKafkaTemplate(final ProducerFactory<?, ?> producerFactory) {
        return new KafkaTemplate<>(producerFactory(), Collections.singletonMap(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class));
    }

    @Bean
    public RoutingKafkaTemplate routingTemplate(GenericApplicationContext context,
                                                ProducerFactory<Object, Object> producerFactory) {


        // Clone the producerFactory with a different Serializer, register with Spring for shutdown
        final DefaultKafkaProducerFactory<Object, Object> protoProducerFactory = protoProducerFactory(producerFactory);
        context.registerBean("protoProducerFactory", DefaultKafkaProducerFactory.class, () -> protoProducerFactory);

        final DefaultKafkaProducerFactory<Object, Object> byteArrayProducerFactory = byteArrayProducerFactory(producerFactory);
        context.registerBean("byteArrayProducerFactory", DefaultKafkaProducerFactory.class, () -> byteArrayProducerFactory);


        final DefaultKafkaProducerFactory<Object, Object> avroProducerFactory = avroProducerFactory(producerFactory);
        context.registerBean("avroProducerFactory", DefaultKafkaProducerFactory.class, () -> avroProducerFactory);

        Map<Pattern, ProducerFactory<Object, Object>> map = new LinkedHashMap<>();
        map.put(Pattern.compile("json-event-topic"), producerFactory);
        map.put(Pattern.compile("proto-event-topic"), protoProducerFactory);
        map.put(Pattern.compile("avro-event-topic"), avroProducerFactory);
        map.put(Pattern.compile(".+"), byteArrayProducerFactory);
        return new RoutingKafkaTemplate(map);
    }

    private DefaultKafkaProducerFactory<Object, Object> protoProducerFactory(ProducerFactory<Object, Object> producerFactory) {
        final Map<String, Object> config = new HashMap<>(producerFactory.getConfigurationProperties());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);

        return new DefaultKafkaProducerFactory<>(config);
    }

    private DefaultKafkaProducerFactory<Object, Object> avroProducerFactory(ProducerFactory<Object, Object> producerFactory) {
        final Map<String, Object> config = new HashMap<>(producerFactory.getConfigurationProperties());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        return new DefaultKafkaProducerFactory<>(config);
    }


    private DefaultKafkaProducerFactory<Object, Object> byteArrayProducerFactory(ProducerFactory<Object, Object> producerFactory) {
        final Map<String, Object> config = new HashMap<>(producerFactory.getConfigurationProperties());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        return new DefaultKafkaProducerFactory<>(config);
    }
}
