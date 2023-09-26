package com.michelin.kstreamplify.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.serdes.JsonSerde;

public class JsonSerdesUtils {
    private JsonSerdesUtils() {
    }

    /**
     * Return a key serdes for a requested class
     *
     * @param <T> The class of requested serdes
     * @return a serdes for requested class
     */
    public static <T> JsonSerde<T> getSerdesForKey() {
        return getSerdes(true);
    }

    /**
     * Return a value serdes for a requested class
     *
     * @param <T> The class of requested serdes
     * @return a serdes for requested class
     */
    public static <T> JsonSerde<T> getSerdesForValue() {
        return getSerdes(false);
    }

    /**
     * Return a serdes for a requested class
     *
     * @param isSerdeForKey Is the serdes for a key or a value
     * @param <T>           The class of requested serdes
     * @return a serdes for requested class
     */
    private static <T> JsonSerde<T> getSerdes(boolean isSerdeForKey) {
        JsonSerde<T> serde = new JsonSerde<T>() {
            @Override
            protected TypeReference<T> getTypeReference() {
                return new TypeReference<>() {
                };
            }
        };

        serde.configure(KafkaStreamsExecutionContext.getSerdesConfig(), isSerdeForKey);
        return serde;
    }
}

