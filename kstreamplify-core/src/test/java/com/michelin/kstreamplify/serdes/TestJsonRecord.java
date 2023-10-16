package com.michelin.kstreamplify.serdes;

import lombok.*;

import java.time.Instant;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@EqualsAndHashCode
public class TestJsonRecord {
    String field1;
    int field2;
    long field3;
    boolean field4;
    Instant arrivalTime;
}
