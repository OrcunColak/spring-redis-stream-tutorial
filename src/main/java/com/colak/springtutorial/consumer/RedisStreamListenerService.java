package com.colak.springtutorial.consumer;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class RedisStreamListenerService implements StreamListener<String, ObjectRecord<String, String>> {

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Override
    @SneakyThrows
    public void onMessage(ObjectRecord<String, String> record) {
        String purchaseEvent = record.getValue();
        log.info(" - consumed : {}", purchaseEvent);

        redisTemplate.opsForStream().acknowledge(record.getStream(), record);
    }

}
