package com.spring.kafka.controller;

import com.spring.kafka.service.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@Slf4j
public class KafkaController {
    private final ProductService productService;

    @GetMapping("/product/order")
    public ResponseEntity<String> test() {

        productService.publishMessages();
        return ResponseEntity.ok("NEW PRODUCT ORDER RECEIVED");
    }

}
