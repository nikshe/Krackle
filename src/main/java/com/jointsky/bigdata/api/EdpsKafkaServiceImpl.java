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
    private static final EdpsKafkaService edpsKafkaService= new EdpsKafkaServiceImpl();

    public void sendData(String message){

    }

    @Override
    public void establishConnect() throws Exception {
        Properties producerProps = new Properties();
        producerProps.setProperty("metadata.broker.list", loader.getProperty("metadata.broker.list"));
        producerProps.setProperty("compression.codec", loader.getProperty("compression"));
        producerProps.setProperty("queue.buffering.max.ms", "100");
        producerProps.setProperty("queue.enqueue.timeout.ms", "-1");
        producerProps.setProperty("request.required.acks", "1");
        producerProps.setProperty("producer.type", "async");
        producerProps.setProperty("serializer.class", "kafka.serializer.StringEncoder");
        producer = new KafkaProducer(producerProps);
    }

    @Override
    public void send(MessageData messageData) throws Exception {
        String topic = messageData.getResourceName();
        String messageStr = messageData.getData();
        producer.send(new ProducerRecord(topic, messageStr));
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

    @Override
    public EdpsKafkaService getInstance() {
        return edpsKafkaService;
    }
}
