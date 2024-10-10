package com.example;

public class PriorityMessage implements Comparable<PriorityMessage> {
    private final Message message;
    private final int priority;
    private final long timestamp; // FCFS based on timestamp

    public PriorityMessage(Message message, int priority, long timestamp)
    {
        this.message = message;
        this.priority = priority;
        this.timestamp = timestamp;
    }

    public Message getMessage()
    {
        return message;
    }
    public Integer getPriority()
    {
        return priority;
    }
    public long getTimestamp()
    {
        return timestamp;
    }

    @Override
    public int compareTo(PriorityMessage other)
    {
        if (this.priority != other.priority)
        {
            return Integer.compare(this.priority, other.priority); // higher priority first
        }
        return Long.compare(this.timestamp, other.timestamp); // FCFS
    }
}
