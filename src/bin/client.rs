use bytes::Bytes;
use mini_redis::client;
use tokio::sync::{mpsc, oneshot};

/// Multiple different commands are multiplexed over a single channel.
#[derive(Debug)]
enum Command {
    Get {
        key: String,
        res: Responder<Option<Bytes>>,
    },

    Set {
        key: String,
        value: Bytes,
        res: Responder<()>,
    },
}

/// Provided by the requester and used by the manager task to send
/// the command response back to the requester.
type Responder<T> = oneshot::Sender<mini_redis::Result<T>>;

#[tokio::main]
async fn main() {
    // Create a new channel with a capacity of at most 32.
    let (tx, mut rx) = mpsc::channel(32);

    let manager = tokio::spawn(async move {
        // Establish a connection to the server
        let mut client = client::connect("127.0.0.1:6379").await.unwrap();

        // Start receiving messages
        while let Some(cmd) = rx.recv().await {
            use Command::*;

            match cmd {
                Get { key, res } => {
                    let resp = client.get(&key).await;

                    // Ignore errors
                    let _ = res.send(resp);
                }

                Set { key, value, res } => {
                    let resp = client.set(&key, value).await;
                    // Ignore errors
                    let _ = res.send(resp);
                }
            }
        }
    });

    // The `Sender` handles are moved into the tasks.
    // As there are two tasks, we need a second `Sender`.
    let tx2 = tx.clone();

    // Spawn two tasks, one gets a key, the other sets a key
    let t1 = tokio::spawn(async move {
        let (res_tx, res_rx) = oneshot::channel();

        let cmd = Command::Get {
            key: "foo".to_string(),
            res: res_tx,
        };
        // Send the GET request
        if tx.send(cmd).await.is_err() {
            eprintln!("connection task shutdown");
            return;
        }

        // Await the response
        let response = res_rx.await;
        println!("GOT (GET) = {:?}", response);
    });

    let t2 = tokio::spawn(async move {
        let (res_tx, res_rx) = oneshot::channel();
        let cmd = Command::Set {
            key: "foo".to_string(),
            value: "bar".into(),
            res: res_tx,
        };

        // Send the SET request
        if tx2.send(cmd).await.is_err() {
            eprintln!("connection task shutdown");
            return;
        };

        // Await the response
        let response = res_rx.await;
        println!("GOT (SET) = {:?}", response);
    });

    t1.await.unwrap();
    t2.await.unwrap();
    manager.await.unwrap();
}
