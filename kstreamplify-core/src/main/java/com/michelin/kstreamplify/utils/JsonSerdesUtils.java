package com.michelin.kstreamplify.utils;

import com.michelin.kstreamplify.serdes.JsonSerde;

public final class JsonSerdesUtils {
    private JsonSerdesUtils() {
    }

    /**
     * Return a value serdes for a requested class
     *
     * @param <T> The class of requested serdes
     * @return a serdes for requested class
     */
    public static <T> JsonSerde<T> getSerdesForValue(Class<T> clazz) {
        return getSerdes(clazz);
    }

    /**
     * Return a serdes for a requested class
     *
     * @param <T> The class of requested serdes
     * @return a serdes for requested class
     */
    private static <T> JsonSerde<T> getSerdes(Class<T> clazz) {
        return new JsonSerde<T>(clazz);
    }
}

