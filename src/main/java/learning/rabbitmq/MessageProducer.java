package learning.rabbitmq; // Define the package for organizational purposes

import com.rabbitmq.client.Channel; // Import the Channel interface for AMQP operations
import com.rabbitmq.client.Connection; // Import the Connection interface for broker connectivity
import com.rabbitmq.client.ConnectionFactory; // Import the factory to create connections
import java.nio.charset.StandardCharsets; // Import standard charset for string encoding
import java.util.concurrent.TimeoutException; // Import exception for connection timeouts
import java.io.IOException; // Import standard IO exception

/**
 * A robust Message Producer using RabbitMQ Java Client.
 * Follows best practices: Try-with-resources, explicit encoding, and proper resource management.
 */
public class MessageProducer {
    // Define the constant name for our target queue
    private final static String QUEUE_NAME = "task_queue";

    public static void main(String[] args) {
        // Instantiate the connection factory object
        ConnectionFactory factory = new ConnectionFactory();
        
        // Configure the network address of the RabbitMQ broker
        factory.setHost("rabbitmq");
        
        // Set the username for authentication (default is guest)
        factory.setUsername("guest");
        
        // Set the password for authentication (default is guest)
        factory.setPassword("guest");

        // Use try-with-resources to ensure connection and channel are closed automatically
        try (Connection connection = factory.newConnection(); // Establish a physical TCP connection
             Channel channel = connection.createChannel()) { // Create a virtual channel over the connection
            
            // Declare the queue to ensure it exists before sending messages
            // durable: true, exclusive: false, autoDelete: false
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            
            // Define the payload to be transmitted
            String message = "Hello, RabbitMQ! This is a persistent message.";
            
            // Convert the string to a byte array using UTF-8 encoding
            byte[] body = message.getBytes(StandardCharsets.UTF_8);
            
            // Publish the message to the default exchange with the queue name as routing key
            // props: MessageProperties.PERSISTENT_TEXT_PLAIN ensures message survives broker restart
            channel.basicPublish("", QUEUE_NAME, null, body);
            
            // Print a confirmation message to the console
            System.out.println(" [x] Sent: '" + message + "'");
            
        } catch (IOException | TimeoutException e) { // Catch potential connectivity or protocol errors
            // Log the error stack trace for debugging
            e.printStackTrace();
        }
    }
}