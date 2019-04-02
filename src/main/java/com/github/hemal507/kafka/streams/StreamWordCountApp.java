package com.github.hemal507.kafka.streams;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Arrays;
import java.util.Properties;

import static org.apache.kafka.common.serialization.Serdes.*;
import org.apache.kafka.common.serialization.Serdes;

public class StreamWordCountApp {
    public static void main(String[] args) {
        String bootstrap_server="localhost:9092" ;
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"wordcount-app");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        StreamsBuilder builder = new StreamsBuilder();

        // 1. Stream from Kafka                 <null,"hey Kafka its Kafka Streams"
        KStream<String,String> wordCountInput = builder.stream("word-count-input");

        // 2. MapValues to lower case           <null,"hey kafka its kafka streams"
        KTable<String,Long> wordcounts = wordCountInput.mapValues(value -> value.toLowerCase() )
        // 3. FlatMapValues by split            <null,"hey"><null,"kafka">,<null,"its">,<null,"kafka">, <null,"streams">
                            .flatMapValues(value -> Arrays.asList(value.split(" ") ))
        // 4. selectKey to apply key            <"hey","hey"><"kafka","kafka">,<"its","its">,<"kafka","kafka">, <"streams","streams">
                            .selectKey((key,value) -> value )
        // 5. GroupByKey aggregation            <"hey",1><"kafka",1>,<"its",1>,<"kafka",1>, <"streams",1>
                            .groupByKey()
        // 6. Count occurrences                 <"hey,1>, <"kafka",2><"its",1>,<"streams",1>
                            .count();
        // 7. To write to a topic               write it to another kafka topic

        wordcounts.toStream().to("word-count-output", Produced.with(String(), Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }
}
