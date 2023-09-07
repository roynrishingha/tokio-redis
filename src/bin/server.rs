use bytes::Bytes;
use mini_redis::{Connection, Frame};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

type ShardedDb = Arc<Vec<Mutex<HashMap<String, Bytes>>>>;

#[tokio::main]
async fn main() {
    // Bind thr listener to the address
    let listner = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    println!("Listening");

    let num_shards = 16;
    let db = new_sharded_db(num_shards);

    loop {
        // The second item contains the IP and port of the new connection.
        let (socket, _) = listner.accept().await.unwrap();
        // Clone the handle to the hash map.
        let db = db.clone();

        println!("Accepted");

        // A new task is spawned for each inbound socket.
        // The socket is moved to the new task and processes there.
        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
}

async fn process(socket: TcpStream, db: ShardedDb) {
    use mini_redis::Command::{self, Get, Set};

    // The `Connection` lets us read/write redis **frames** instead of
    // byte streams. The `Connection` type is defined by mini-redis.
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                // Calculate shard index using the hash_key function
                let shard_index = hash_key(cmd.key(), db.len());
                let mut shard_db = db[shard_index].lock().unwrap();
                shard_db.insert(cmd.key().to_string(), cmd.value().clone());

                //let mut db = db.lock().unwrap();
                //db.insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                let shard_index = hash_key(cmd.key(), db.len());
                let shard_db = db[shard_index].lock().unwrap();

                if let Some(value) = shard_db.get(cmd.key()) {
                    // `Frame::Bulk` expects data to be of type `Bytes`.
                    Frame::Bulk(value.clone())
                } else {
                    Frame::Null
                }
            }
            cmd => panic!("unimplemented {:?}", cmd),
        };

        connection.write_frame(&response).await.unwrap();
    }
}

fn new_sharded_db(num_shards: usize) -> ShardedDb {
    let mut db = Vec::with_capacity(num_shards);
    for _ in 0..num_shards {
        db.push(Mutex::new(HashMap::new()));
    }
    Arc::new(db)
}

// Hashing function to determine the shard index
fn hash_key(key: &str, num_shards: usize) -> usize {
    let mut hash: usize = 0;
    for byte in key.bytes() {
        hash = (hash.wrapping_mul(31) + byte as usize) % num_shards;
    }
    hash
}
