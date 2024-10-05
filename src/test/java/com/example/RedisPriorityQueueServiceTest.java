package com.example;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class RedisPriorityQueueServiceTest {

    private RedisPriorityQueueService queueService;
    private final String testQueueUrl = "testQueue";

    @Before
    public void setup() {
        // Initialize RedisPriorityQueueService and clear Redis database
        queueService = new RedisPriorityQueueService();
        queueService.flushAll();  // Clear all data in Redis before each test
    }

    @Test
    public void testPushMessage() {
        // Arrange
        String messageBody = "Test Message";
        int priority = 1;

        // Act
        queueService.push(testQueueUrl, messageBody, priority);

        // Assert
        Message pulledMessage = queueService.pull(testQueueUrl);
        assertNotNull("Message should be pulled from the queue", pulledMessage);
        assertEquals("Message body should match", messageBody, pulledMessage.getBody());
    }

    @Test
    public void testPullMessage() {
        // Arrange
        String messageBody = "Test Message";
        queueService.push(testQueueUrl, messageBody, 1);

        // Act
        Message pulledMessage = queueService.pull(testQueueUrl);

        // Assert
        assertNotNull("Message should not be null", pulledMessage);
        assertEquals("Message body should match", messageBody, pulledMessage.getBody());
        assertNotNull("Receipt ID should not be null", pulledMessage.getReceiptId());
        assertFalse("Message should have a receipt ID", pulledMessage.getReceiptId().isEmpty());
    }

    @Test
    public void testDeleteMessage() {
        // Arrange
        String messageBody = "Test Message";
        queueService.push(testQueueUrl, messageBody, 1);

        Message pulledMessage = queueService.pull(testQueueUrl);
        assertNotNull(pulledMessage);
        String receiptId = pulledMessage.getReceiptId();

        // Act
        queueService.delete(testQueueUrl, receiptId);

        // Assert
        Message afterDeleteMessage = queueService.pull(testQueueUrl);
        assertNull("Queue should be empty after deletion", afterDeleteMessage);
    }

    @Test
    public void testDeleteNonExistentMessage() {
        // Arrange
        queueService.push(testQueueUrl, "Test Message", 1);
        String nonExistentReceiptId = "non-existent-receipt-id";

        // Act
        queueService.delete(testQueueUrl, nonExistentReceiptId);

        // Assert
        Message pulledMessage = queueService.pull(testQueueUrl);
        assertNotNull("Message should still exist since the receipt ID did not match", pulledMessage);
    }

    @Test
    public void testPullFromEmptyQueue() {
        // Act
        Message message = queueService.pull(testQueueUrl);

        // Assert
        assertNull("Message should be null when pulling from an empty queue", message);
    }

    @Test
    public void testMultipleMessagesSamePriority() {
        // Arrange
        queueService.push(testQueueUrl, "First Message", 2);
        queueService.push(testQueueUrl, "Second Message", 2);
        queueService.push(testQueueUrl, "Third Message", 2);

        // Act & Assert
        Message firstMessage = queueService.pull(testQueueUrl);
        assertEquals("First Message", firstMessage.getBody());
    }

    @Test
    public void testQueueBehaviorAcrossMultipleQueues() {
        // Arrange
        String secondQueueUrl = "secondQueue";
        queueService.push(testQueueUrl, "Message in Queue 1", 1);
        queueService.push(secondQueueUrl, "Message in Queue 2", 2);

        // Act & Assert
        Message messageInQueue1 = queueService.pull(testQueueUrl);
        assertEquals("Message in Queue 1", messageInQueue1.getBody());

        Message messageInQueue2 = queueService.pull(secondQueueUrl);
        assertEquals("Message in Queue 2", messageInQueue2.getBody());
    }
}
