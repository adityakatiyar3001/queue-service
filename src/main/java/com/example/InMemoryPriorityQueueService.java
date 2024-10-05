package com.example;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

public class InMemoryPriorityQueueService implements PriorityQueueService{

    private final Map<String, PriorityBlockingQueue<PriorityMessage>> queues;
    private final long visibilityTimeout;


    public InMemoryPriorityQueueService() {
        this.queues = new ConcurrentHashMap<>();
        String propFileName = "config.properties";
        Properties confInfo = new Properties();

        try (InputStream inStream = getClass().getClassLoader().getResourceAsStream(propFileName)) {
            confInfo.load(inStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.visibilityTimeout = Integer.parseInt(confInfo.getProperty("visibilityTimeout", "30"));

    }

    @Override
    public void push(String queueUrl, String msgBody, int priority)
    {
        PriorityBlockingQueue<PriorityMessage> queue = queues.get(queueUrl);
        if (queue == null)
        {
            queue = new PriorityBlockingQueue<PriorityMessage>();
            queues.put(queueUrl, queue);
        }
        PriorityMessage priorityMessage = new PriorityMessage(new Message(msgBody), priority, System.nanoTime());
        queue.add(priorityMessage);
    }

    @Override
    public Message pull(String queueUrl)
    {
        PriorityBlockingQueue<PriorityMessage> queue = queues.get(queueUrl);
        if(queue == null) return null;
        else if(!queue.isEmpty())
        {
            PriorityMessage priorityMessage = queue.peek();
            if (priorityMessage == null) return null;
            else
            {
                Message msg = priorityMessage.getMessage();
                msg.setReceiptId(UUID.randomUUID().toString());
                msg.incrementAttempts();
                msg.setVisibleFrom(System.nanoTime() + TimeUnit.SECONDS.toNanos(visibilityTimeout));
                return new Message(msg.getBody(), msg.getReceiptId());
            }
        }
        else
            return null;
    }

    @Override
    public void delete(String queueUrl, String receiptId)
    {
        PriorityBlockingQueue<PriorityMessage> queue = queues.get(queueUrl);
        if (queue != null)
        {
            for (PriorityMessage priorityMessage : queue)
            {
                Message msg = priorityMessage.getMessage();
                if (!msg.isVisibleAt(System.nanoTime()) && msg.getReceiptId().equals(receiptId))
                {
                    boolean x = queue.remove(priorityMessage);
                    break;
                }
            }
        }
    }
}
