use lapin::{options::*, types::FieldTable, BasicProperties, Connection, ConnectionProperties};
use tokio;
use tokio_stream::StreamExt;
use uuid::Uuid;

// Repo
// mongolian_rabbit_search 

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    
    // RabbitMQ connection string
    let addr_rabbit = "amqp://guest:guest@localhost:5672/%2f";

    // Connect to RabbitMQ
    let connection = Connection::connect(addr_rabbit, ConnectionProperties::default())
        .await?;

    println!("Connected to RabbitMQ!");

    // Create a channel
    let channel = connection.create_channel().await?;

    // Declare the request queue
    let queue_name = "my_queue";
    channel.queue_declare(
        queue_name,
        QueueDeclareOptions::default(),
        FieldTable::default(),
    ).await?;

    println!("Queue declared: {}", queue_name);

    // Declare a response queue for this publisher
    let response_queue = format!("response_queue_{}", Uuid::new_v4());
    channel.queue_declare(
        &response_queue,
        QueueDeclareOptions {
            auto_delete: true,
            ..Default::default()
        },
        FieldTable::default(),
    ).await?;

    println!("Response queue declared: {}", response_queue);

    // Generate a unique correlation ID
    let correlation_id = Uuid::new_v4().to_string();

    // Publish the Ping message with reply_to and correlation_id
    let message = b"Hola me llamo Dylan!";
    channel.basic_publish(
        "", // Default exchange
        queue_name,
        BasicPublishOptions::default(),
        message,
        BasicProperties::default()
            .with_reply_to(response_queue.clone().into())
            .with_correlation_id(correlation_id.clone().into()),
    ).await?;

    println!("Message sent: {:?}", std::str::from_utf8(message)?);

    // Setup consumer to receive the Pong response
    let mut consumer = channel.basic_consume(
        &response_queue,
        "publisher_consumer",
        BasicConsumeOptions::default(),
        FieldTable::default(),
    ).await?;

    println!("Waiting for response...");

    // Wait for the response
    while let Some(delivery) = consumer.next().await {
        match delivery {
            Ok(delivery) => {
                let response = std::str::from_utf8(&delivery.data)?;
                if let Some(cid) = delivery.properties.correlation_id() {
                    if cid.as_str() == correlation_id {
                        println!("Received response: {}", response);
                        delivery.ack(BasicAckOptions::default()).await?;
                        break; // Exit after receiving the correct response
                    }
                }
            }
            Err(e) => {
                eprintln!("Error receiving response: {}", e);
                break;
            }
        }
    }

    // Cleanup
    channel.close(200, "OK").await?;
    connection.close(200, "OK").await?;

    Ok(())
}