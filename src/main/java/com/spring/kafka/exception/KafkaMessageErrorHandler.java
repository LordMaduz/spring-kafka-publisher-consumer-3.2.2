package com.spring.kafka.exception;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;

@Slf4j
public class KafkaMessageErrorHandler implements CommonErrorHandler {

    @Override
    public void handleOtherException(Exception thrownException, Consumer<?, ?> consumer, MessageListenerContainer container, boolean batchListener) {
        manageException(thrownException, consumer);
    }


    private void manageException(Exception ex, Consumer<?, ?> consumer) {
        log.error("Error polling message: " + ex.getMessage());
        if (ex instanceof RecordDeserializationException) {
            RecordDeserializationException rde = (RecordDeserializationException) ex;
            consumer.seek(rde.topicPartition(), rde.offset() + 1L);
            consumer.commitSync();
        } else {
            log.error("Exception not handled");
        }
    }
}
