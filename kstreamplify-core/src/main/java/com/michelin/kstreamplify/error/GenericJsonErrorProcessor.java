package com.michelin.kstreamplify.error;

import com.michelin.kstreamplify.avro.KafkaError;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;

import java.io.PrintWriter;
import java.io.StringWriter;

public class GenericJsonErrorProcessor<V>
        implements FixedKeyProcessor<String, ProcessingError<V>, JsonError> {
    private FixedKeyProcessorContext<String, JsonError> context;

    /**
     * Init context.
     *
     * @param context the context to init
     */
    @Override
    public void init(FixedKeyProcessorContext<String, JsonError> context) {
        this.context = context;
    }

    @Override
    public void process(FixedKeyRecord<String, ProcessingError<V>> fixedKeyRecord) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        fixedKeyRecord.value().getException().printStackTrace(pw);

        RecordMetadata recordMetadata = context.recordMetadata().orElse(null);

        JsonError error = JsonError.builder()
                .cause(fixedKeyRecord.value().getException().getMessage())
                .contextMessage(fixedKeyRecord.value().getContextMessage())
                .offset(recordMetadata != null ? recordMetadata.offset() : -1)
                .partition(recordMetadata != null ? recordMetadata.partition() : -1)
                .stack(sw.toString())
                .topic(
                        recordMetadata != null && recordMetadata.topic() != null ? recordMetadata.topic() :
                                "Outside topic context")
                .value(fixedKeyRecord.value().getKafkaRecord())
                .build();

        context.forward(fixedKeyRecord.withValue(error));
    }

    @Override
    public void close() {
        FixedKeyProcessor.super.close();
    }
}


