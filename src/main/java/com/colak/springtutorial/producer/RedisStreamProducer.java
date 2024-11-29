package com.colak.springtutorial.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class RedisStreamProducer {

    @Value("${redis.stream.name}")
    String streamName;

    private final RedisTemplate<String, String> redisTemplate;

    public void produceMessage(String user, String message) {
        // Create a map of the message fields
        Map<String, String> messageData = new HashMap<>();
        messageData.put("user", user);
        messageData.put("message", message);

        // Write the message to the stream
        StreamOperations<String, String, String> streamOps = redisTemplate.opsForStream();
        streamOps.add(streamName, messageData);

        log.info("Message added to stream: {}", messageData);
    }
}

