package org.example.kafka.streaming;

import lombok.*;

@Builder
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
class Message {

    private String source;
    private float lng;
    private float lat;
    private long timestamp;

}
