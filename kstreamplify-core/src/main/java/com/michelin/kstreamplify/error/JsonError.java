package com.michelin.kstreamplify.error;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.nio.ByteBuffer;

@NoArgsConstructor
@Getter
@Setter
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



    
    public static JsonErrorBuilder builder() {
        return new JsonErrorBuilder();
    }

    
    public static class JsonErrorBuilder {
        
        private String cause;
        
        private String contextMessage;
        
        private long offset;
        
        private int partition;
        
        private String stack;
        
        private String topic;
        
        private String value;
        
        private ByteBuffer byteValue;

        
        JsonErrorBuilder() {
        }

        
        public JsonErrorBuilder cause(String cause) {
            this.cause = cause;
            return this;
        }

        
        public JsonErrorBuilder contextMessage(String contextMessage) {
            this.contextMessage = contextMessage;
            return this;
        }

        
        public JsonErrorBuilder offset(long offset) {
            this.offset = offset;
            return this;
        }

        
        public JsonErrorBuilder partition(int partition) {
            this.partition = partition;
            return this;
        }

        
        public JsonErrorBuilder stack(String stack) {
            this.stack = stack;
            return this;
        }

        
        public JsonErrorBuilder topic(String topic) {
            this.topic = topic;
            return this;
        }

        
        public JsonErrorBuilder value(String value) {
            this.value = value;
            return this;
        }

        
        public JsonErrorBuilder byteValue(ByteBuffer byteValue) {
            this.byteValue = byteValue;
            return this;
        }

        
        public JsonError build() {
            return new JsonError(this.cause, this.contextMessage, this.offset, this.partition, this.stack, this.topic, this.value, this.byteValue);
        }

        
        public String toString() {
            return "JsonError.JsonErrorBuilder(cause=" + this.cause + ", contextMessage=" + this.contextMessage + ", offset=" + this.offset + ", partition=" + this.partition + ", stack=" + this.stack + ", topic=" + this.topic + ", value=" + this.value + ", byteValue=" + this.byteValue + ")";
        }
    }
}
