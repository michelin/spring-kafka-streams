package com.michelin.kstreamplify.error;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.avro.KafkaError;

import java.util.Optional;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GenericAvroErrorProcessorTest {
    private final GenericAvroErrorProcessor<String> errorProcessor = new GenericAvroErrorProcessor<>();

    @Mock
    private FixedKeyProcessorContext<String, KafkaError> mockContext;

    @Mock
    private FixedKeyRecord<String, ProcessingError<String>> mockRecord;

    @Mock
    private RecordMetadata mockRecordMetadata;

    @Test
    void shouldProcessError() {
        when(mockRecord.value())
            .thenReturn(new ProcessingError<>(new RuntimeException("Exception..."), "Context message", "Record"));

        // Given a mock RecordMetadata
        when(mockRecordMetadata.offset()).thenReturn(10L);
        when(mockRecordMetadata.partition()).thenReturn(0);
        when(mockRecordMetadata.topic()).thenReturn("test-topic");

        // Given that the context has a recordMetadata
        when(mockContext.recordMetadata()).thenReturn(Optional.of(mockRecordMetadata));

        // When processing the record
        errorProcessor.init(mockContext);
        errorProcessor.process(mockRecord);

        verify(mockContext).forward(any());
    }
}
