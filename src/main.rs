use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(async move {
                    handle_connection(stream)
                        .await
                        .inspect_err(|e| eprintln!("{e}"))
                });
            }
            Err(e) => {
                eprintln!("error: {}", e);
            }
        }
    }
}

async fn handle_connection(mut stream: TcpStream) -> anyhow::Result<()> {
    let mut buf = BytesMut::with_capacity(1024);
    loop {
        if let Ok(0) = stream.read_buf(&mut buf).await {
            break;
        }
        stream.write_all(b"+PONG\r\n").await?;
    }
    Ok(())
}
