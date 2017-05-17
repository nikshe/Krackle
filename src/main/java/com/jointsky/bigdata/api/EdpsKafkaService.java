package com.jointsky.bigdata.api;

import java.util.List;

/**
 * 企业级KAFKA数据服务
 * API使用说明:
 *          1. 创建对象EdpsKafkaService edpsKafkaService = new EdpsKafkaServiceImpl();
 *          2. 建立连接 edpsKafkaService.establishConnect();
 *          3. 发送数据 数据是由消息封装体来构成,包括要发送的消息和消息主题 topic
 *              提供了发送单条和批量多条接口 send(MessageData messageData) 和 send(List<MessageData> messageDataList)
 *          4. 连接关闭,释放资源 edpsKafkaService.closeConnect()
 *
 * Created by on 2017/5/16.
 */
public interface EdpsKafkaService {
    /**
     * 建立连接,设置参数,并返回消息发送服务实例
     * @param
     */
    public void establishConnect() throws  Exception;

    /**
     * 发送一条消息
     * @param messageData 消息数据
     */
    public void send(MessageData messageData) throws  Exception;

    /**
     * 批量发送接口，一次发送多条消息
     * @param messageDataList  存放多条消息到list中
     */
    public void send(List<MessageData> messageDataList) throws Exception;

    /**
     * 连接关闭,释放资源
     */
    public void closeConnect();


}
