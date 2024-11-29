package com.colak.springtutorial.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
@RequiredArgsConstructor
@Slf4j
public class RedisStreamProducer {

    @Value("${redis.stream.name}")
    String streamName;

    private final RedisTemplate<String, String> redisTemplate;

    public RecordId  produceMessage(String message) {
        // Create a map of the message fields
        ObjectRecord<String, String> record = StreamRecords.newRecord()
                .ofObject(message)
                .withStreamKey(streamName);

        RecordId recordId = this.redisTemplate.opsForStream()
                .add(record);

        log.info("recordId: {}", recordId);

        if (Objects.isNull(recordId)) {
            log.info("error sending event: {}", message);
            return null;
        }

        return recordId;
    }
}

