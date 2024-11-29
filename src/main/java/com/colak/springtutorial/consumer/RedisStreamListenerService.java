package com.colak.springtutorial.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class RedisStreamListenerService {

    @Value("${redis.stream.name}")
    String streamName;

    private final RedisTemplate<String, String> redisTemplate;

    public void consumeMessages() {
        StreamOperations<String, String, String> streamOps = redisTemplate.opsForStream();

        // Infinite loop to consume messages
        while (true) {

            StreamReadOptions streamReadOptions = StreamReadOptions.empty().block(Duration.ofMillis(500));

            // StreamOffset<String> streamOffset = StreamOffset.latest(streamName);
            StreamOffset<String> streamOffset = StreamOffset.fromStart(streamName);

            List<MapRecord<String, String, String>> messages = streamOps.read(streamReadOptions, streamOffset);

            if (messages != null && !messages.isEmpty()) {
                for (MapRecord<String, String, String> message : messages) {
                    String messageId = message.getId().getValue();
                    String user = message.getValue().get("user");
                    String messageContent = message.getValue().get("message");

                    log.info("Message ID: {}", messageId);
                    log.info("User: {}", user);
                    log.info("Message: {}", messageContent);
                }
            }
        }
    }
}
