package com.michelin.kstreamplify.error;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.nio.ByteBuffer;

@NoArgsConstructor
@Getter
@Setter
@Builder
@AllArgsConstructor
public class JsonError implements Serializable {
    private String cause;

    private String contextMessage;

    private long offset;

    private int partition;

    private String stack;

    private String topic;

    private String value;

    private ByteBuffer byteValue;
}
