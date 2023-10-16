package com.michelin.kstreamplify.error;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.serdes.JsonSerde;
import com.michelin.kstreamplify.utils.JsonSerdesUtils;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;

import static com.michelin.kstreamplify.constants.PropertyConstants.*;
import static com.michelin.kstreamplify.constants.PropertyConstants.FORMAT;

/**
 * The class to manage DLQ exception.
 */
@Slf4j
public abstract class DlqExceptionHandler {
    /**
     * The DLQ producer.
     */
    @Getter
    protected static Producer<byte[], Object> producer;

    /**
     * Create a producer.
     *
     * @param clientId The producer client id
     * @param configs  The producer configs
     */
    public static void instantiateProducer(String clientId, Map<String, ?> configs) {
        Properties properties = new Properties();
        properties.putAll(configs);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName());
        String errorFormat = null;
        try{
            errorFormat = KafkaStreamsExecutionContext.getProperties().getProperty(ERROR + PROPERTY_SEPARATOR + FORMAT);
        }
        catch (Exception ignored){ }
        
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                AVRO.equals(errorFormat) ? 
                        KafkaAvroSerializer.class.getName() :
                        JsonSerde.class.getName());
                        //JsonSerdesUtils.getSerdesForValue().serializer().getClass().getName());
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        producer = new KafkaProducer<>(properties);
    }

    /**
     * Enrich with exception.
     *
     * @param exception the exception to add
     * @param key       the record key
     * @param value     the record value
     * @return the error enriched by the exception
     */
    public KafkaError.Builder initAvroErrorBuilderWithException(Exception exception,
                                                                byte[] key,
                                                                byte[] value) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        exception.printStackTrace(pw);

        boolean tooLarge = exception instanceof RecordTooLargeException;

        return KafkaError.newBuilder()
                .setCause(
                        exception.getCause() != null ? exception.getCause().getMessage() : "Unknown cause")
                .setValue(tooLarge ? "The record is too large to be set as value (" + value.length
                        + " bytes). The key will be used instead" : null)
                .setStack(sw.toString())
                .setByteValue(tooLarge ? ByteBuffer.wrap(key) : ByteBuffer.wrap(value));
    }

    /**
     * Enrich with exception.
     *
     * @param exception the exception to add
     * @param key       the record key
     * @param value     the record value
     * @return the error enriched by the exception
     */
    public JsonError.JsonErrorBuilder initJsonErrorBuilderWithException(Exception exception,
                                                                        byte[] key,
                                                                        byte[] value) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        exception.printStackTrace(pw);

        boolean tooLarge = exception instanceof RecordTooLargeException;

        return JsonError.builder()
                .cause(exception.getCause() != null ? exception.getCause().getMessage() : "Unknown cause")
                .value(tooLarge ? "The record is too large to be set as value (" + value.length
                        + " bytes). The key will be used instead" : null)
                .stack(sw.toString())
                .byteValue(tooLarge ? ByteBuffer.wrap(key) : ByteBuffer.wrap(value));
    }
}
