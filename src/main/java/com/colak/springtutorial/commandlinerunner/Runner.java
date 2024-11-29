package com.colak.springtutorial.commandlinerunner;

import com.colak.springtutorial.producer.RedisStreamProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class Runner implements CommandLineRunner {

    private final RedisStreamProducer producer;

    @Override
    public void run(String... args) {
        producer.produceMessage("hello world");
    }
}
