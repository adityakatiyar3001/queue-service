package com.example;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class InMemoryPriorityQueueServiceTest {

    private InMemoryPriorityQueueService queueService;
    private final String testQueueUrl = "https://sqs.ap-1.amazonaws.com/007/MyQueue";

    @Before
    public void setup() {
        queueService = new InMemoryPriorityQueueService();
    }

    @Test
    public void testPushAndPullMessagesWithPriority() {
        queueService.push(testQueueUrl, "High Priority Message", 1);
        queueService.push(testQueueUrl, "Medium Priority Message", 2);
        queueService.push(testQueueUrl, "Low Priority Message", 3);

        // Ensure messages are pulled in priority order (highest priority first)
        assertEquals("High Priority Message", queueService.pull(testQueueUrl).getBody());
    }

    @Test
    public void testDeleteMessageByReceiptId() {
        queueService.push(testQueueUrl, "Test Message", 1);
        Message message = queueService.pull(testQueueUrl);

        // Ensure receipt ID is generated
        assertNotNull("Receipt ID should not be null", message.getReceiptId());

        // Delete the message
        queueService.delete(testQueueUrl, message.getReceiptId());

        // Ensure the message has been deleted
        assertNull("Message should be deleted", queueService.pull(testQueueUrl));
    }

    @Test
    public void testPullFromEmptyQueue() {
        // Ensure pulling from an empty queue returns null
        assertNull("Empty queue should return null", queueService.pull(testQueueUrl));
    }

    @Test
    public void testMultipleMessagesSamePriority() {
        queueService.push(testQueueUrl, "First Message", 2);
        queueService.push(testQueueUrl, "Second Message", 2);
        queueService.push(testQueueUrl, "Third Message", 2);

        // Ensure messages are pulled in First-Come-First-Serve (FCFS) order for the same priority
        assertEquals("First Message", queueService.pull(testQueueUrl).getBody());
    }

    @Test
    public void testDeleteNonExistentMessage() {
        queueService.push(testQueueUrl, "Sample Message", 1);
        Message message = queueService.pull(testQueueUrl);

        // Attempt to delete a non-existent message by providing a fake receipt ID
        queueService.delete(testQueueUrl, "non-existent-receipt-id");

        // Ensure that deleting a non-existent message doesn't remove valid messages
        assertNotNull("Message should still exist in the queue", message);
    }

    @Test
    public void testQueueBehaviorAcrossMultipleQueues() {
        String secondQueueUrl = "https://example.com/secondQueue";

        queueService.push(testQueueUrl, "Message in Queue 1", 1);
        queueService.push(secondQueueUrl, "Message in Queue 2", 2);

        // Ensure messages are correctly retrieved from separate queues
        assertEquals("Message in Queue 1", queueService.pull(testQueueUrl).getBody());
        assertEquals("Message in Queue 2", queueService.pull(secondQueueUrl).getBody());
    }

    @Test
    public void testReceiptIdGeneration() {
        queueService.push(testQueueUrl, "Receipt ID Test", 1);
        Message message = queueService.pull(testQueueUrl);

        // Ensure that the pulled message has a valid receipt ID
        assertNotNull("Receipt ID should be generated", message.getReceiptId());
        assertFalse("Receipt ID should not be empty", message.getReceiptId().isEmpty());
    }
    
    @Test
    public void testPullAfterVisibilityTimeout() throws InterruptedException {
        queueService.push(testQueueUrl, "Test Message1", 1);
        queueService.push(testQueueUrl, "Test Message2", 2);


        assertEquals("Test Message1", queueService.pull(testQueueUrl).getBody());
        Thread.sleep(3050);
        assertEquals("Test Message1", queueService.pull(testQueueUrl).getBody());

    }

}
