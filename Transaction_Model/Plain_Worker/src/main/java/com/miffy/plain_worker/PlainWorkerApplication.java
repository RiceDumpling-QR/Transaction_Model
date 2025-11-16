package com.miffy.plain_worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class PlainWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(PlainWorkerApplication.class, args);
    }

}
