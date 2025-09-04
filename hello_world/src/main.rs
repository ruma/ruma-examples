use std::{env, process::exit};

use ruma::{
    OwnedRoomOrAliasId, TransactionId,
    api::client::{membership::join_room_by_id_or_alias, message::send_message_event},
    events::room::message::RoomMessageEventContent,
};

type HttpClient = ruma_client::http_client::HyperNativeTls;

async fn hello_world(
    homeserver_url: String,
    username: &str,
    password: &str,
    room_id_or_alias: OwnedRoomOrAliasId,
) -> anyhow::Result<()> {
    // Construct and log in the client.
    let client = ruma_client::Client::builder()
        .homeserver_url(homeserver_url)
        .build::<HttpClient>()
        .await?;
    client
        .log_in(username, password, None, Some("ruma-example-client"))
        .await?;

    // Join the room.
    let room_id = client
        .send_request(join_room_by_id_or_alias::v3::Request::new(
            room_id_or_alias.clone(),
        ))
        .await?
        .room_id;

    // Send the message.
    client
        .send_request(send_message_event::v3::Request::new(
            room_id,
            TransactionId::new(),
            &RoomMessageEventContent::text_plain("Hello World!"),
        )?)
        .await?;

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let (homeserver_url, username, password, room) = match (
        env::args().nth(1),
        env::args().nth(2),
        env::args().nth(3),
        env::args().nth(4),
    ) {
        (Some(a), Some(b), Some(c), Some(d)) => (a, b, c, d),
        _ => {
            eprintln!(
                "Usage: {} <homeserver_url> <username> <password> <room>",
                env::args().next().unwrap()
            );
            exit(1)
        }
    };

    hello_world(homeserver_url, &username, &password, room.try_into()?).await
}
