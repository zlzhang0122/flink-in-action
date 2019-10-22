package com.github.flink;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.github.flink")
public class WebActionsApplication {

	public static void main(String[] args) {
		SpringApplication.run(WebActionsApplication.class, args);
	}

}
