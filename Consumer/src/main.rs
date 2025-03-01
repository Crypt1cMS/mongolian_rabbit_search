use lapin::{options::*, types::FieldTable, BasicProperties, Connection, ConnectionProperties};
use mongodb::{bson::doc, Client};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use elasticsearch::{
    Elasticsearch, IndexParts, http::transport::{TransportBuilder, SingleNodeConnectionPool}, auth::Credentials
};
use url::Url;
use tokio;
use tokio_stream::StreamExt;

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    content: String,
    timestamp: DateTime<Utc>
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // Elasticsearch connection with authentication

    // Api Key - ejRnWFQ1VUJzd3FlOTRKam8yODQ6SkNNSFAwRDlRQVM1dUx4U3FlWS1wQQ==
    // endpoint - https://localhost:9200

    let es_username = "elastic";
    let es_password = "dylan123";
    let es_url = "https://localhost:9200";
    let url = Url::parse(es_url)?;

    let conn_pool = SingleNodeConnectionPool::new(url);
    let transport = TransportBuilder::new(conn_pool)
        .auth(Credentials::Basic(es_username.into(), es_password.into()))
        .build()?;
    let es_client = Elasticsearch::new(transport);
    

    // Connect to MongoDB
    let client = Client::with_uri_str("mongodb://mongoadmin:Admin@localhost:27017/?authSource=admin").await?;
    let database = client.database("DB_Messages");
    let collection = database.collection::<Message>("Messages");

    println!("Connected to MongoDB!");

    // Connect to RabbitMQ
    let addr = "amqp://guest:guest@localhost:5672/%2f";
    let connection = Connection::connect(addr, ConnectionProperties::default()).await?;
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

    // Setup consumer
    let mut consumer = channel.basic_consume(
        queue_name,
        "my_consumer",
        BasicConsumeOptions::default(),
        FieldTable::default(),
    ).await?;

    println!("Waiting for messages...");

    while let Some(delivery) = consumer.next().await {
        match delivery {
            Ok(delivery) => {
                let message = std::str::from_utf8(&delivery.data)?;
                println!("Received message: {}", message);

                // Save received message to MongoDB
                let received_msg = Message {
                    content: message.to_string(),
                    timestamp: Utc::now()
                };
                
                collection.insert_one(received_msg, None).await?;

                let es_msg = json!({
                    "content": message,
                    "timestamp": Utc::now().to_rfc3339()
                });
                
                es_client
                    .index(IndexParts::Index("messages"))
                    .body(&es_msg)
                    .send()
                    .await?;

                if let Some(reply_to) = delivery.properties.reply_to() {
                    let acknowledge = b"Message Received - Consumer!";
                    channel.basic_publish(
                        "",
                        reply_to.as_str(),
                        BasicPublishOptions::default(),
                        acknowledge,
                        BasicProperties::default()
                            .with_correlation_id(delivery.properties.correlation_id().clone().unwrap_or_default()),
                    ).await?;
                    println!("Sent Acknowledgement");
                }

                // Acknowledge the original message
                delivery.ack(BasicAckOptions::default()).await?;
            }
            Err(e) => {
                eprintln!("Error receiving message: {}", e);
                break;
            }
        }
    }

    // Cleanup
    channel.close(200, "OK").await?;
    connection.close(200, "OK").await?;

    Ok(())
}