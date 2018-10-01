package org.example.kafka.streaming.pairs;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class Pair<X, Y> {
    private X x;
    private Y y;
}
