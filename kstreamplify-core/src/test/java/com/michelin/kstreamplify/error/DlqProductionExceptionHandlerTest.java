package com.michelin.kstreamplify.error;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.utils.JsonSerdesUtils;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
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
class DlqProductionExceptionHandlerTest {
    @Mock
    private ProducerRecord<byte[], byte[]> record;

    private Producer<byte[], Object> avroErrorProducer;
    private Producer<byte[], JsonError> jsonErrorProducer;

    private DlqProductionExceptionHandler avroHandler;
    private DlqProductionExceptionHandler jsonHandler;

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
    void shouldReturnFailIfNoDlqAvroError() {
        avroHandler = new DlqProductionExceptionHandler(avroErrorProducer);

        KafkaStreamsExecutionContext.setProperties(avroProperties);
        ProductionExceptionHandler.ProductionExceptionHandlerResponse response =
                avroHandler.handle(record, new RuntimeException("Exception..."));

        assertEquals(ProductionExceptionHandler.ProductionExceptionHandlerResponse.FAIL, response);
    }

    @Test
    void shouldReturnFailIfNoDlqJsonError() {
        jsonHandler = new DlqProductionExceptionHandler(jsonErrorProducer);

        KafkaStreamsExecutionContext.setProperties(jsonProperties);
        ProductionExceptionHandler.ProductionExceptionHandlerResponse response =
                jsonHandler.handle(record, new RuntimeException("Exception..."));

        assertEquals(ProductionExceptionHandler.ProductionExceptionHandlerResponse.FAIL, response);
    }

    @Test
    void shouldReturnContinueOnExceptionDuringHandleAvroError() {
        avroHandler = new DlqProductionExceptionHandler(avroErrorProducer);
        KafkaStreamsExecutionContext.setDlqTopicName("DlqTopic");

        KafkaStreamsExecutionContext.setProperties(avroProperties);
        ProductionExceptionHandler.ProductionExceptionHandlerResponse response =
                avroHandler.handle(record, new KafkaException("Exception..."));

        assertEquals(ProductionExceptionHandler.ProductionExceptionHandlerResponse.CONTINUE, response);
    }

    @Test
    void shouldReturnContinueOnExceptionDuringHandleJsonError() {
        jsonHandler = new DlqProductionExceptionHandler(jsonErrorProducer);
        KafkaStreamsExecutionContext.setDlqTopicName("DlqTopic");

        KafkaStreamsExecutionContext.setProperties(jsonProperties);
        ProductionExceptionHandler.ProductionExceptionHandlerResponse response =
                jsonHandler.handle(record, new KafkaException("Exception..."));

        assertEquals(ProductionExceptionHandler.ProductionExceptionHandlerResponse.CONTINUE, response);
    }

    @Test
    void shouldReturnContinueOnKafkaExceptionAvroError() {
        avroHandler = new DlqProductionExceptionHandler(avroErrorProducer);
        KafkaStreamsExecutionContext.setDlqTopicName("DlqTopic");

        when(record.key()).thenReturn("key".getBytes(StandardCharsets.UTF_8));
        when(record.value()).thenReturn("value".getBytes(StandardCharsets.UTF_8));
        when(record.topic()).thenReturn("topic");

        KafkaStreamsExecutionContext.setProperties(avroProperties);
        ProductionExceptionHandler.ProductionExceptionHandlerResponse response =
                avroHandler.handle(record, new KafkaException("Exception..."));

        assertEquals(ProductionExceptionHandler.ProductionExceptionHandlerResponse.CONTINUE, response);
    }

    @Test
    void shouldReturnContinueOnKafkaExceptionJsonError() {
        jsonHandler = new DlqProductionExceptionHandler(jsonErrorProducer);
        KafkaStreamsExecutionContext.setDlqTopicName("DlqTopic");

        when(record.key()).thenReturn("key".getBytes(StandardCharsets.UTF_8));
        when(record.value()).thenReturn("value".getBytes(StandardCharsets.UTF_8));
        when(record.topic()).thenReturn("topic");

        KafkaStreamsExecutionContext.setProperties(avroProperties);
        ProductionExceptionHandler.ProductionExceptionHandlerResponse response =
                jsonHandler.handle(record, new KafkaException("Exception..."));

        assertEquals(ProductionExceptionHandler.ProductionExceptionHandlerResponse.CONTINUE, response);
    }

    @Test
    void shouldReturnFailOnRetriableExceptionAvroError() {
        avroHandler = new DlqProductionExceptionHandler(avroErrorProducer);
        KafkaStreamsExecutionContext.setDlqTopicName("DlqTopic");

        KafkaStreamsExecutionContext.setProperties(avroProperties);
        ProductionExceptionHandler.ProductionExceptionHandlerResponse response =
                avroHandler.handle(record, new RetriableCommitFailedException("Exception..."));

        assertEquals(ProductionExceptionHandler.ProductionExceptionHandlerResponse.FAIL, response);
    }

    @Test
    void shouldReturnFailOnRetriableExceptionJsonError() {
        jsonHandler = new DlqProductionExceptionHandler(jsonErrorProducer);
        KafkaStreamsExecutionContext.setDlqTopicName("DlqTopic");

        KafkaStreamsExecutionContext.setProperties(jsonProperties);
        ProductionExceptionHandler.ProductionExceptionHandlerResponse response =
                jsonHandler.handle(record, new RetriableCommitFailedException("Exception..."));

        assertEquals(ProductionExceptionHandler.ProductionExceptionHandlerResponse.FAIL, response);
    }

    @Test
    void shouldConfigure() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("schema.registry.url", "localhost:8080");
        configs.put("acks", "all");

        // reset DlqExceptionHandler producer
        DlqExceptionHandler.producer = null;

        avroHandler = new DlqProductionExceptionHandler();
        avroHandler.configure(configs);

        assertTrue(DlqExceptionHandler.getProducer() instanceof KafkaProducer<byte[], Object>);
    }
}

