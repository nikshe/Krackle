package com.jointsky.bigdata.api;

import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * 发送消息至kafka集群
 * Created by on 2017/5/16.
 */
public class EdpsKafkaServiceImpl implements EdpsKafkaService {
    private Logger logger = LoggerFactory.getLogger(EdpsKafkaServiceImpl.class);
    private static KafkaProducer producer = null;
    private static PropertiesLoader loader = new PropertiesLoader("kafka.properties");

    @Override
    public void establishConnect() throws Exception {
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers",loader.getProperty("bootstrap.servers"));
        producerProps.setProperty("compression.codec", loader.getProperty("compression"));
        producerProps.setProperty("queue.buffering.max.ms", loader.getProperty("queue.buffering.max.ms"));
        producerProps.setProperty("queue.enqueue.timeout.ms", loader.getProperty("queue.enqueue.timeout.ms"));
        producerProps.setProperty("request.required.acks", loader.getProperty("request.required.acks"));
        producerProps.setProperty("producer.type", loader.getProperty("producer.type"));
        producerProps.setProperty("key.serializer", loader.getProperty("key.serializer"));
        producerProps.setProperty("value.serializer", loader.getProperty("value.serializer"));
        producer = new KafkaProducer(producerProps);
    }

    @Override
    public void send(MessageData messageData) throws Exception {
        String topic = messageData.getResourceName();
        String messageStr = messageData.getData();
        producer.send(new ProducerRecord<String,String>(topic,messageStr));
    }

    @Override
    public void send(List<MessageData> messageDataList) throws Exception {
        for (MessageData messageData : messageDataList) {
            send(messageData);
        }
    }

    @Override
    public void closeConnect() {
        producer.close();
    }

}
