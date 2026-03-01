package learning.rabbitmq;

import com.rabbitmq.client.Channel; // Import Channel for AMQP operations
import com.rabbitmq.client.Connection; // Import Connection for broker connectivity
import com.rabbitmq.client.ConnectionFactory; // Import factory for creating connections
import com.rabbitmq.client.DeliverCallback; // Import callback for message delivery
import java.io.IOException; // Import standard IO exception
import java.util.concurrent.TimeoutException; // Import exception for connection timeouts
import java.util.concurrent.ExecutorService; // Import for managed thread pools
import java.util.concurrent.Executors; // Import to create executor instances

/**
 * A robust Message Consumer using RabbitMQ Java Client.
 * Follows best practices: Managed thread pools, manual acknowledgments, and
 * graceful shutdown.
 */
public class MessageConsumer {
    // Define the constant name for the source queue
    private final static String QUEUE_NAME = "task_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        // Create a thread pool to handle message processing asynchronously
        // This prevents the connection thread from being blocked by heavy tasks
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        // Instantiate the connection factory
        ConnectionFactory factory = new ConnectionFactory();

        // Configure the broker host address
        factory.setHost("rabbitmq");

        // Assign the executor service to the factory for internal callback handling
        factory.setSharedExecutor(executorService);

        // Establish a long-lived connection to the broker
        Connection connection = factory.newConnection();

        // Create a communication channel
        Channel channel = connection.createChannel();

        // Ensure the queue exists before attempting to consume from it
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);

        // Set Quality of Service: only send 1 message to this consumer at a time
        // This ensures fair dispatching among multiple consumers
        channel.basicQos(1);

        // Print status message to indicate the consumer is active
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        // Define the logic to execute when a message is received
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            // Extract the message body from the delivery object
            String message = new String(delivery.getBody(), "UTF-8");

            // Log the received message
            System.out.println(" [x] Received: '" + message + "'");

            try {
                // Simulate a time-consuming task
                doWork(message);
            } finally {
                // Log completion
                System.out.println(" [x] Done");
                // Manually acknowledge the message to remove it from the queue
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };

        // Start the consumer with manual acknowledgments enabled
        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {
        });

        // Add a shutdown hook to close resources cleanly when the JVM exits
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println("Shutting down...");
                channel.close(); // Close the channel
                connection.close(); // Close the connection
                executorService.shutdown(); // Shutdown the thread pool
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
    }

    // Helper method to simulate processing work
    private static void doWork(String task) {
        try {
            // Sleep for 1 second to simulate processing time
            Thread.sleep(1000);
        } catch (InterruptedException _ignored) {
            // Restore interrupted status
            Thread.currentThread().interrupt();
        }
    }
}
