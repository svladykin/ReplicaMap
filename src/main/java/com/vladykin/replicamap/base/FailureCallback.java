package com.vladykin.replicamap.base;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public interface FailureCallback extends Callback {

    void onError(Throwable th);

    @Override
    default void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null)
            onError(exception);
    }
}
