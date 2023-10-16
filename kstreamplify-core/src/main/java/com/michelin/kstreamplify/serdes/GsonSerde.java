//package com.michelin.kstreamplify.serdes;
//
//import com.google.common.reflect.TypeToken;
//import com.google.gson.Gson;
//import org.apache.kafka.common.errors.SerializationException;
//import org.apache.kafka.common.serialization.Deserializer;
//import org.apache.kafka.common.serialization.Serde;
//import org.apache.kafka.common.serialization.Serializer;
//
//import java.lang.reflect.Type;
//import java.nio.charset.StandardCharsets;
//import java.util.Map;
//
//public class GsonSerde<T> implements Serializer<T>, Deserializer<T>, Serde<T> {
//    private final Gson gson = new Gson();
//    private final Type valueType;
//
//    public GsonSerde() {
//        this.valueType = TypeToken.of(T.class).getType();
//    }
//
//    @Override
//    public T deserialize(String topic, byte[] data) {
//        if (data == null) {
//            return null;
//        }
//        try {
//            String jsonString = new String(data, StandardCharsets.UTF_8);
//            return gson.fromJson(jsonString, valueType);
//        } catch (Exception e) {
//            throw new SerializationException("Error deserializing JSON data", e);
//        }
//    }
//
//    @Override
//    public void configure(Map<String, ?> configs, boolean isKey) {
//        // nothing to do here
//    }
//
//    @Override
//    public byte[] serialize(String topic, T data) {
//        try {
//            return gson.toJson(data).getBytes();
//        } catch (Exception e) {
//            throw new SerializationException("Error serializing object to JSON", e);
//        }
//    }
//
//    @Override
//    public void close() {
//        // Nothing to close for Gson
//    }
//
//    @Override
//    public Serializer<T> serializer() {
//        return this;
//    }
//
//    @Override
//    public Deserializer<T> deserializer() {
//        return this;
//    }
//}
