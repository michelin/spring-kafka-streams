package com.michelin.kstreamplify.error;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import java.io.Serializable;
import java.util.Map;

import static com.michelin.kstreamplify.constants.PropertyConstants.*;

/**
 * The class managing DLQ production exceptions.
 */
@Slf4j
@NoArgsConstructor
public class DlqProductionExceptionHandler extends DlqExceptionHandler
        implements ProductionExceptionHandler {
    private static final Object GUARD = new Object();

    private static final String CONTEXT_MESSAGE = "An exception occurred during the stream internal production";

    /**
     * Constructor.
     *
     * @param producer A Kafka producer
     */
    protected <T> DlqProductionExceptionHandler(Producer<byte[], T> producer) {
        DlqExceptionHandler.producer = (Producer<byte[], Object>)producer; // todo does this work?
    }

    /**
     * Manage production exceptions.
     *
     * @param producerRecord      the record to produce
     * @param productionException the exception on producing
     * @return FAIL or CONTINUE
     */
    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> producerRecord,
                                                     Exception productionException) {
        if (StringUtils.isBlank(KafkaStreamsExecutionContext.getDlqTopicName())) {
            log.warn(
                    "Failed to route production error to the designated DLQ (Dead Letter Queue) topic. "
                            +
                            "Please make sure to define a DLQ topic in your KafkaStreamsStarter bean configuration.");
            return ProductionExceptionHandlerResponse.FAIL;
        }

        boolean retryable = productionException instanceof RetriableException;

        if (!retryable) {
            try {
                Serializable errorModel =
                        AVRO.equals(KafkaStreamsExecutionContext.getProperties().getProperty(ERROR + PROPERTY_SEPARATOR + FORMAT)) ?
                                buildAvroErrorModelFromException(producerRecord, productionException) :
                                buildJsonErrorModelFromException(producerRecord, productionException);

                producer.send(new ProducerRecord<>(KafkaStreamsExecutionContext.getDlqTopicName(),
                        producerRecord.key(), errorModel)).get();
            } catch (InterruptedException ie) {
                log.error(
                        "Interruption while sending the production exception {} for key {}, value {} "
                                + "and topic {} to DLQ topic {}",
                        productionException,
                        producerRecord.key(), producerRecord.value(), producerRecord.topic(),
                        KafkaStreamsExecutionContext.getDlqTopicName(), ie);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error(
                        "Cannot send the production exception {} for key {}, value {} and topic {} to DLQ topic {}",
                        productionException,
                        producerRecord.key(), producerRecord.value(), producerRecord.topic(),
                        KafkaStreamsExecutionContext.getDlqTopicName(), e);
                return ProductionExceptionHandlerResponse.CONTINUE;
            }

            return ProductionExceptionHandlerResponse.CONTINUE;
        }

        return ProductionExceptionHandlerResponse.FAIL;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(Map<String, ?> configs) {
        synchronized (GUARD) {
            if (producer == null) {
                instantiateProducer(DlqProductionExceptionHandler.class.getName(), configs);
            }
        }
    }


    private JsonError buildJsonErrorModelFromException(ProducerRecord<byte[], byte[]> producerRecord, Exception exception) {
        return initJsonErrorBuilderWithException(exception, producerRecord.key(), producerRecord.value())
                .contextMessage(CONTEXT_MESSAGE)
                .offset(-1)
                .partition(producerRecord.partition())
                .topic(producerRecord.topic())
                .build();
    }

    private KafkaError buildAvroErrorModelFromException(ProducerRecord<byte[], byte[]> producerRecord, Exception exception) {
        return initAvroErrorBuilderWithException(exception, producerRecord.key(), producerRecord.value())
                .setContextMessage(CONTEXT_MESSAGE)
                .setOffset(-1)
                .setPartition(producerRecord.partition() == null ? -1 : producerRecord.partition())
                .setTopic(producerRecord.topic())
                .build();
    }
}
