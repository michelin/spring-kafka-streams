package com.michelin.kstreamplify.error;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.io.Serializable;
import java.util.Map;

import static com.michelin.kstreamplify.constants.PropertyConstants.*;

/**
 * The class managing deserialization exceptions.
 */
@Slf4j
@NoArgsConstructor
public class DlqDeserializationExceptionHandler extends DlqExceptionHandler
        implements DeserializationExceptionHandler {

    private static final String CONTEXT_MESSAGE = "An exception occurred during the stream internal deserialization";
    private static final Object GUARD = new Object();

    /**
     * Constructor.
     *
     * @param producer A Kafka producer.
     */
    public <T> DlqDeserializationExceptionHandler(Producer<byte[], T> producer) {
        DlqExceptionHandler.producer = (Producer<byte[], Object>)producer; // todo does this cast work?
    }

    /**
     * Manage deserialization exceptions.
     *
     * @param processorContext     the processor context
     * @param consumerRecord       the record to deserialize
     * @param consumptionException the exception for the deserialization
     * @return FAIL or CONTINUE
     */
    @Override
    public DeserializationHandlerResponse handle(ProcessorContext processorContext,
                                                 ConsumerRecord<byte[], byte[]> consumerRecord,
                                                 Exception consumptionException) {
        if (StringUtils.isBlank(KafkaStreamsExecutionContext.getDlqTopicName())) {
            log.warn(
                    "Failed to route deserialization error to the designated DLQ (Dead Letter Queue) topic. "
                            +
                            "Please make sure to define a DLQ topic in your KafkaStreamsStarter bean configuration.");
            return DeserializationHandlerResponse.FAIL;
        }

        try {
            Object errorModel =
                    AVRO.equals(KafkaStreamsExecutionContext.getProperties().getProperty(ERROR + PROPERTY_SEPARATOR + FORMAT)) ?
                            buildAvroErrorModelFromException(consumerRecord, consumptionException) :
                            buildJsonErrorModelFromException(consumerRecord, consumptionException);


            boolean isCausedByKafka = consumptionException.getCause() instanceof KafkaException;
            // If the cause of this exception is a KafkaException and if getCause == sourceException
            // (see Throwable.getCause - including SerializationException)
            // use to handle poison pill => sent message into dlq and continue our life.
            if (isCausedByKafka || consumptionException.getCause() == null) {
                producer.send(new ProducerRecord<>(KafkaStreamsExecutionContext.getDlqTopicName(),
                        consumerRecord.key(), errorModel)).get();
                return DeserializationHandlerResponse.CONTINUE;
            }
        } catch (InterruptedException ie) {
            log.error(
                    "Interruption while sending the deserialization exception {} for key {}, "
                            + "value {} and topic {} to DLQ topic {}",
                    consumptionException,
                    consumerRecord.key(), consumerRecord.value(), consumerRecord.topic(),
                    KafkaStreamsExecutionContext.getDlqTopicName(), ie);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error(
                    "Cannot send the deserialization exception {} for key {}, value {} and topic {} to DLQ topic {}",
                    consumptionException,
                    consumerRecord.key(), consumerRecord.value(), consumerRecord.topic(),
                    KafkaStreamsExecutionContext.getDlqTopicName(), e);
        }

        // here we only have exception like UnknownHostException for example or TimeoutException ...
        // situation example:  we cannot ask schema registry because the url is unavailable 
        return DeserializationHandlerResponse.FAIL;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(Map<String, ?> configs) {
        synchronized (GUARD) {
            if (producer == null) {
                instantiateProducer(DlqDeserializationExceptionHandler.class.getName(), configs);
            }
        }
    }

    private JsonError buildJsonErrorModelFromException(ConsumerRecord<byte[], byte[]> consumerRecord, Exception exception) {
        return initJsonErrorBuilderWithException(exception, consumerRecord.key(), consumerRecord.value())
                .contextMessage(CONTEXT_MESSAGE)
                .offset(consumerRecord.offset())
                .partition(consumerRecord.partition())
                .topic(consumerRecord.topic())
                .build();
    }

    private KafkaError buildAvroErrorModelFromException(ConsumerRecord<byte[], byte[]> consumerRecord, Exception exception) {
        return initAvroErrorBuilderWithException(exception, consumerRecord.key(), consumerRecord.value())
                .setContextMessage(CONTEXT_MESSAGE)
                .setOffset(consumerRecord.offset())
                .setPartition(consumerRecord.partition())
                .setTopic(consumerRecord.topic())
                .build();
    }
}
