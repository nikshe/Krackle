package com.jointsky.bigdata.api;

/**
 * 消息封装体
 * Created by  on 2017/5/16.
 */
public class MessageData {
    private String resourceName;

    private String messageId;

    private String data;

    public String getResourceName() {
        return resourceName;
    }

    public MessageData setResourceName(String resourceName) {
        this.resourceName = resourceName;
        return this;
    }

    public String getMessageId() {
        return messageId;
    }

    public MessageData setMessageId(String messageId) {
        this.messageId = messageId;
        return this;
    }

    public String getData() {
        return data;
    }

    public MessageData setData(String data) {
        this.data = data;
        return this;
    }
}
