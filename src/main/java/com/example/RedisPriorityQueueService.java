package com.example;

import java.io.IOException;
import java.util.Properties;
import java.io.InputStream;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class RedisPriorityQueueService implements PriorityQueueService {

    private final Jedis jedis;
    private final Gson gson = new GsonBuilder().serializeNulls().create();
    private final Integer visibilityTimeout;

    public RedisPriorityQueueService() {
        String propFileName = "config.properties";
        Properties confInfo = new Properties();
        try (InputStream inStream = getClass().getClassLoader().getResourceAsStream(propFileName)) {
            confInfo.load(inStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.visibilityTimeout = Integer.parseInt(confInfo.getProperty("visibilityTimeout", "30"));
        //Fetching upstash redis db connection details
        final String endpoint = confInfo.getProperty("endpoint", "musical-moth-26539.upstash.io");
        final int port = Integer.parseInt(confInfo.getProperty("port", "6379"));
        final boolean isSSL = Boolean.parseBoolean(confInfo.getProperty("ssl", "true"));

        this.jedis = new Jedis(endpoint, port, isSSL); // Create connection
        this.jedis.auth(confInfo.getProperty("password", "None"));  //Authentication
    }

    public RedisPriorityQueueService(Jedis jedis, int visibilityTimeout) {
        this.jedis = jedis;
        this.visibilityTimeout = visibilityTimeout;
    }

    @Override
    public void push(String queueUrl, String msgBody, int priority)
    {
        PriorityMessage priorityMessage = new PriorityMessage(new Message(msgBody), priority, System.nanoTime());
        try
        {
            double score = score(priorityMessage);
            String serializedMessage = gson.toJson(priorityMessage);
            this.jedis.zadd(queueUrl, score, serializedMessage);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }

    @Override
    public Message pull(String queueUrl) {

        try
        {
            Set<String> members = this.jedis.zrange(queueUrl, 0, -1);
            for (String member : members) {
                PriorityMessage priorityMessage = gson.fromJson(member, PriorityMessage.class);
                if (priorityMessage != null && priorityMessage.getMessage() != null) {
                    Message msg = priorityMessage.getMessage();
                    if(msg.isVisibleAt(System.nanoTime()))   //Currently Visible
                    {
                        this.jedis.zrem(queueUrl, gson.toJson(priorityMessage));
                        msg.setReceiptId(UUID.randomUUID().toString());
                        msg.incrementAttempts();
                        msg.setVisibleFrom(System.nanoTime() + TimeUnit.SECONDS.toNanos(visibilityTimeout));

                        //Update message with new entries
                        PriorityMessage newPriorityMessage = new PriorityMessage(msg, priorityMessage.getPriority(), priorityMessage.getTimestamp());
                        String updatedMessage = gson.toJson(newPriorityMessage);

                        this.jedis.zadd(queueUrl, score(newPriorityMessage), updatedMessage);

                        return new Message(msg.getBody(), msg.getReceiptId());
                    }
                }
            }
        }
         catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void delete(String queueUrl, String receiptId){
            try {
                Set<String> members = this.jedis.zrange(queueUrl, 0, -1);
                for (String member : members) {
                    PriorityMessage priorityMessage = gson.fromJson(member, PriorityMessage.class);
                    Message msg = priorityMessage.getMessage();
                    if (!msg.isVisibleAt(System.nanoTime()) && msg.getReceiptId() != null && msg.getReceiptId().equals(receiptId)) {
                        this.jedis.zrem(queueUrl, member);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    private double score(PriorityMessage priorityMessage)
    {
        return (double)priorityMessage.getPriority() + (double)priorityMessage.getTimestamp() * 1e-9;
    }

    public void flushAll() {
        jedis.flushAll();
    }
}