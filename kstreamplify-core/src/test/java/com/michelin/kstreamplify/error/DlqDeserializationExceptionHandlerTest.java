package com.michelin.kstreamplify.error;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.utils.JsonSerdesUtils;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.michelin.kstreamplify.constants.PropertyConstants.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DlqDeserializationExceptionHandlerTest {
    @Mock
    private ConsumerRecord<byte[], byte[]> record;

    @Mock
    private ProcessorContext processorContext;

    private Producer<byte[], Object> avroErrorProducer;

    private Producer<byte[], JsonError> jsonErrorProducer;

    private DlqDeserializationExceptionHandler avroHandler;

    private DlqDeserializationExceptionHandler jsonHandler;
    
    private static Properties avroProperties;
    private static Properties jsonProperties;

    @BeforeEach
    void setUp() {
        Serializer<Object> avroSerializer = new KafkaAvroSerializer();  
        Serializer<JsonError> jsonSerializer = JsonSerdesUtils.getSerdesForValue(JsonError.class);
        avroSerializer.configure(Map.of(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://"), false);
        jsonSerializer.configure(Map.of(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://"), false);
        avroErrorProducer = new MockProducer<>(true, new ByteArraySerializer(), avroSerializer);
        jsonErrorProducer = new MockProducer<>(true, new ByteArraySerializer(), jsonSerializer);


        avroProperties = new Properties();
        avroProperties.put(ERROR + PROPERTY_SEPARATOR + FORMAT, AVRO);
        jsonProperties = new Properties();
        jsonProperties.put(ERROR + PROPERTY_SEPARATOR + FORMAT, "");

        KafkaStreamsExecutionContext.setDlqTopicName(null);
    }

    @Test
    void shouldReturnFailIfNoDlq() {
        avroHandler = new DlqDeserializationExceptionHandler(avroErrorProducer);

        KafkaStreamsExecutionContext.setProperties(avroProperties);
        DeserializationExceptionHandler.DeserializationHandlerResponse response =
                avroHandler.handle(processorContext, record, new RuntimeException("Exception..."));

        assertEquals(DeserializationExceptionHandler.DeserializationHandlerResponse.FAIL, response);
    }

    @Test
    void shouldReturnFailOnExceptionDuringHandleAvroError() {
        avroHandler = new DlqDeserializationExceptionHandler(avroErrorProducer);
        jsonHandler = new DlqDeserializationExceptionHandler(jsonErrorProducer);
        KafkaStreamsExecutionContext.setDlqTopicName("DlqTopic");

        KafkaStreamsExecutionContext.setProperties(avroProperties);
        DeserializationExceptionHandler.DeserializationHandlerResponse response =
                avroHandler.handle(processorContext, record, new KafkaException("Exception..."));

        assertEquals(DeserializationExceptionHandler.DeserializationHandlerResponse.FAIL, response);

        KafkaStreamsExecutionContext.setProperties(jsonProperties);
        response = jsonHandler.handle(processorContext, record, new KafkaException("Exception..."));

        assertEquals(DeserializationExceptionHandler.DeserializationHandlerResponse.FAIL, response);
    }

    @Test
    void shouldReturnFailOnExceptionDuringHandleJsonError() {
        jsonHandler = new DlqDeserializationExceptionHandler(jsonErrorProducer);
        KafkaStreamsExecutionContext.setDlqTopicName("DlqTopic");
        
        KafkaStreamsExecutionContext.setProperties(jsonProperties);
        DeserializationExceptionHandler.DeserializationHandlerResponse response =
                jsonHandler.handle(processorContext, record, new KafkaException("Exception..."));

        assertEquals(DeserializationExceptionHandler.DeserializationHandlerResponse.FAIL, response);
    }

    @Test
    void shouldReturnContinueOnKafkaExceptionAvroError() {
        avroHandler = new DlqDeserializationExceptionHandler(avroErrorProducer);
        KafkaStreamsExecutionContext.setDlqTopicName("DlqTopic");

        when(record.key()).thenReturn("key".getBytes(StandardCharsets.UTF_8));
        when(record.value()).thenReturn("value".getBytes(StandardCharsets.UTF_8));
        when(record.topic()).thenReturn("topic");

        KafkaStreamsExecutionContext.setProperties(avroProperties);
        DeserializationExceptionHandler.DeserializationHandlerResponse response =
                avroHandler.handle(processorContext, record, new KafkaException("Exception..."));

        assertEquals(DeserializationExceptionHandler.DeserializationHandlerResponse.CONTINUE, response);
    }

    @Test
    void shouldReturnContinueOnKafkaExceptionJsonError() {
        jsonHandler = new DlqDeserializationExceptionHandler(jsonErrorProducer);
        KafkaStreamsExecutionContext.setDlqTopicName("DlqTopic");

        when(record.key()).thenReturn("key".getBytes(StandardCharsets.UTF_8));
        when(record.value()).thenReturn("value".getBytes(StandardCharsets.UTF_8));
        when(record.topic()).thenReturn("topic");

        KafkaStreamsExecutionContext.setProperties(jsonProperties);
        DeserializationExceptionHandler.DeserializationHandlerResponse response = jsonHandler.handle(processorContext, record, new KafkaException("Exception..."));

        assertEquals(DeserializationExceptionHandler.DeserializationHandlerResponse.CONTINUE, response);
    }

    @Test
    void shouldConfigure() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("schema.registry.url", "localhost:8080");
        configs.put("acks", "all");

        avroHandler = new DlqDeserializationExceptionHandler(null);
        avroHandler.configure(configs);

        assertTrue(DlqExceptionHandler.getProducer() instanceof KafkaProducer<byte[], Object>);
    }

    /*@Test
    void shouldEnrichWithException() {
        KafkaError.Builder kafkaError = KafkaError.newBuilder()
            .setTopic("topic")
            .setStack("stack")
            .setPartition(0)
            .setOffset(0)
            .setCause("cause")
            .setValue("value");

        avroHandler = new DlqDeserializationExceptionHandler();
        KafkaError.Builder enrichedBuilder = avroHandler.initAvroErrorBuilderWithException(kafkaError,
            new RuntimeException("Exception..."), "key".getBytes(StandardCharsets.UTF_8),
            "value".getBytes(StandardCharsets.UTF_8));

        KafkaError error = enrichedBuilder.build();
        assertEquals("Unknown cause", error.getCause());
        assertNull(error.getContextMessage());
    }

    @Test
    void shouldEnrichWithRecordTooLargeException() {
        KafkaError.Builder kafkaError = KafkaError.newBuilder()
            .setTopic("topic")
            .setStack("stack")
            .setPartition(0)
            .setOffset(0)
            .setCause("cause")
            .setValue("value");

        avroHandler = new DlqDeserializationExceptionHandler();
        KafkaError.Builder enrichedBuilder = avroHandler.initAvroErrorBuilderWithException(kafkaError,
            new RecordTooLargeException("Exception..."), "key".getBytes(StandardCharsets.UTF_8),
            "value".getBytes(StandardCharsets.UTF_8));

        KafkaError error = enrichedBuilder.build();
        assertEquals("Unknown cause", error.getCause());
        assertEquals("The record is too large to be set as value (5 bytes). "
            + "The key will be used instead", error.getValue());
    }*/
}
