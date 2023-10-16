package com.michelin.kstreamplify.serdes;


import com.michelin.kstreamplify.utils.JsonSerdesUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
class JsonSerdeTest {
    @Test
    void shouldSerializeAndDeserialize() {

        byte[] bytes;
        TestJsonRecord jsonRecord = new TestJsonRecord("field", 0, 3L, true, Instant.now());

        JsonSerde<TestJsonRecord> testJsonRecordSerde = JsonSerdesUtils.getSerdesForValue(TestJsonRecord.class);

        bytes = testJsonRecordSerde.serialize("mock-topic", jsonRecord);

        TestJsonRecord result = testJsonRecordSerde.deserialize("mock-topic", bytes);


//        JsonSerde<TestJsonRecord> testJsonRecordSerde = JsonSerdesUtils.<TestJsonRecord>getSerdesForValue();
//        bytes = testJsonRecordSerde.serializer().serialize("mock-topic", jsonRecord);
        assertEquals(jsonRecord, result);
    }
}
