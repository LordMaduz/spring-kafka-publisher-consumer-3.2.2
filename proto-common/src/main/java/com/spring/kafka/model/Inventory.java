package com.spring.kafka.model;

import lombok.*;

@Data
@Builder
public class Inventory {
    private String inventoryId;
    private String name;
}
