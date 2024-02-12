package com.spring.kafka.mapper;

import com.google.protobuf.Message;

import java.util.function.Consumer;
import java.util.function.Supplier;

public interface ProtobufMapper<T, P extends Message> {
    Class<T> getJavaClassType();

    Class<P> getProtoClassType();

    P toProto(T e);

    T fromProto(P proto);

    default <V> void setIfNotNull(Supplier<V> supplier, Consumer<V> consumer) {
        if (supplier.get() != null) {
            consumer.accept(supplier.get());
        }
    }
}
