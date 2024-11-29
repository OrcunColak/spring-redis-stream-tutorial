package com.colak.springtutorial.config;

import com.colak.springtutorial.consumer.RedisStreamListenerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;

@Configuration
@Slf4j
public class RedisStreamConfig {

    @Value("${redis.stream.name}")
    private String streamName;


    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(redisConnectionFactory);
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setValueSerializer(new StringRedisSerializer());

        return redisTemplate;
    }

    @Bean
    public StreamListener<String, ObjectRecord<String, String>> purchaseStreamListener() {
        // handle message from stream
        return new RedisStreamListenerService();
    }

    @Bean
    public Subscription subscription(RedisConnectionFactory connectionFactory)
            throws UnknownHostException {

        createConsumerGroupIfNotExists(connectionFactory);
        StreamOffset<String> streamOffset = StreamOffset.create(streamName, ReadOffset.lastConsumed());

        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String,
                ObjectRecord<String, String>> options = StreamMessageListenerContainer
                .StreamMessageListenerContainerOptions
                .builder()
                .pollTimeout(Duration.ofMillis(100))
                .targetType(String.class)
                .build();

        StreamMessageListenerContainer<String, ObjectRecord<String, String>> container =
                StreamMessageListenerContainer
                        .create(connectionFactory, options);

        // receiveAutoAck is used to auto acknowledge the messages just after being received.
        Subscription subscription =
                container.receiveAutoAck(Consumer.from(streamName, InetAddress.getLocalHost().getHostName()),
                        streamOffset, purchaseStreamListener());


        container.start();
        return subscription;
    }

    void createConsumerGroupIfNotExists(RedisConnectionFactory redisConnectionFactory) {
        try {
            try {
                redisConnectionFactory.getConnection().streamCommands()
                        .xGroupCreate(streamName.getBytes(), streamName, ReadOffset.from("0-0"), true);
            } catch (RedisSystemException exception) {
                log.warn(exception.getCause().getMessage());
            }
        } catch (RedisSystemException ex) {
            log.error(ex.getMessage());
        }
    }

}
