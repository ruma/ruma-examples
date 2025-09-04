use std::{error::Error, io, sync::LazyLock, time::Duration};

use futures_util::future::{join, join_all};
use http_body_util::BodyExt as _;
use ruma::{
    OwnedRoomId, OwnedUserId, TransactionId,
    api::client::{
        filter::FilterDefinition, membership::join_room_by_id, message::send_message_event,
        sync::sync_events,
    },
    assign,
    events::{
        AnySyncMessageLikeEvent, AnySyncTimelineEvent, SyncMessageLikeEvent,
        room::message::{MessageType, RoomMessageEventContent},
    },
    presence::PresenceState,
    serde::Raw,
};
use ruma_client::DefaultConstructibleHttpClient as _;
use serde_json::Value as JsonValue;
use tokio::fs;
use tokio_stream::StreamExt as _;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let bot = Bot::build().await?;
    bot.run().await?;

    Ok(())
}

/// The URI used to request a new joke.
static JOKE_API_URI: LazyLock<hyper::Uri> = LazyLock::new(|| {
    "https://v2.jokeapi.dev/joke/Programming,Pun,Misc?safe-mode&type=single"
        .parse()
        .expect("URI should be valid")
});

type HttpClient = ruma_client::http_client::HyperNativeTls;
type MatrixClient = ruma_client::Client<HttpClient>;

/// The bot.
struct Bot {
    /// The client to use to make HTTP requests outside of the Matrix API.
    http_client: HttpClient,
    /// The client to use to make requests against the Matrix API.
    matrix_client: MatrixClient,
    /// The user ID of the Matrix account used by the bot.
    user_id: OwnedUserId,
}

impl Bot {
    /// Build the `Bot` from the config.
    async fn build() -> Result<Self, Box<dyn Error>> {
        let config = Config::read()
            .await
            .map_err(|e| format!("configuration in ./config is invalid: {e}"))?;
        let http_client = HttpClient::default();

        let matrix_client = if let Some(state) = State::read().await.ok().flatten() {
            ruma_client::Client::builder()
                .homeserver_url(config.homeserver)
                .access_token(Some(state.access_token))
                .http_client(http_client.clone())
                .await?
        } else if config.password.is_some() {
            let client = Self::create_matrix_session(http_client.clone(), &config).await?;
            let state = State {
                access_token: client
                    .access_token()
                    .expect("Matrix access token is missing"),
            };

            if let Err(err) = state.write().await {
                eprintln!(
                    "Failed to persist access token to disk. \
                     Re-authentication will be required on the next startup: {err}",
                );
            }
            client
        } else {
            return Err("No previous session found and no credentials stored in config".into());
        };

        Ok(Self {
            http_client,
            matrix_client,
            user_id: config.username,
        })
    }

    /// Create a new matrix session, aka log in.
    ///
    /// This methods panics if the password is not set in the config.
    async fn create_matrix_session(
        http_client: HttpClient,
        config: &Config,
    ) -> Result<MatrixClient, Box<dyn Error>> {
        let client = ruma_client::Client::builder()
            .homeserver_url(config.homeserver.clone())
            .http_client(http_client)
            .await?;

        if let Err(e) = client
            .log_in(
                config.username.as_ref(),
                config
                    .password
                    .as_deref()
                    .expect("we should have already checked that the password is set"),
                None,
                None,
            )
            .await
        {
            let reason = match e {
                ruma_client::Error::AuthenticationRequired => {
                    "invalid credentials specified".to_owned()
                }
                ruma_client::Error::Response(response_err) => {
                    format!("failed to get a response from the server: {response_err}")
                }
                ruma_client::Error::FromHttpResponse(parse_err) => {
                    format!("failed to parse log in response: {parse_err}")
                }
                _ => e.to_string(),
            };
            return Err(format!("Failed to log in: {reason}").into());
        }

        Ok(client)
    }

    /// Run the bot.
    async fn run(&self) -> Result<(), Box<dyn Error>> {
        // Perform an initial sync to ignore messages before the bot was launched.
        let filter = FilterDefinition::ignore_all().into();
        let initial_sync_response = self
            .matrix_client
            .send_request(assign!(sync_events::v3::Request::new(), {
                filter: Some(filter),
            }))
            .await?;

        // Ignore events from our bot.
        let not_senders = vec![self.user_id.clone()];
        let filter = {
            let mut filter = FilterDefinition::empty();
            filter.room.timeline.not_senders = not_senders;
            filter
        }
        .into();

        // Launch a sync loop to listen to messages and invites.
        let mut sync_stream = Box::pin(self.matrix_client.sync(
            Some(filter),
            initial_sync_response.next_batch,
            PresenceState::Online,
            Some(Duration::from_secs(30)),
        ));

        println!("Listening...");
        while let Some(response) = sync_stream.try_next().await? {
            let message_futures =
                response
                    .rooms
                    .join
                    .iter()
                    .map(|(room_id, room_info)| async move {
                        // Use a regular for loop for the messages within one room to handle them sequentially
                        for e in &room_info.timeline.events {
                            if let Err(err) = self.handle_message(e, room_id.to_owned()).await {
                                eprintln!("failed to respond to message: {err}");
                            }
                        }
                    });

            let invite_futures = response.rooms.invite.into_keys().map(|room_id| async move {
                if let Err(err) = self.handle_invitations(room_id.clone()).await {
                    eprintln!("failed to accept invitation for room {room_id}: {err}");
                }
            });

            // Handle messages from different rooms as well as invites concurrently
            join(join_all(message_futures), join_all(invite_futures)).await;
        }

        Ok(())
    }

    /// Handle the given message from the given room.
    async fn handle_message(
        &self,
        ev: &Raw<AnySyncTimelineEvent>,
        room_id: OwnedRoomId,
    ) -> Result<(), Box<dyn Error>> {
        // We are only interested in text messages that contain the word "joke".
        let Ok(AnySyncTimelineEvent::MessageLike(AnySyncMessageLikeEvent::RoomMessage(
            SyncMessageLikeEvent::Original(m),
        ))) = ev.deserialize()
        else {
            return Ok(());
        };
        let MessageType::Text(t) = m.content.msgtype else {
            return Ok(());
        };

        println!("{}:\t{}", m.sender, t.body);

        if !t.body.to_ascii_lowercase().contains("joke") {
            return Ok(());
        }

        let joke = self
            .get_joke()
            .await
            .unwrap_or_else(|_| "I thought of a joke... but I just forgot it.".to_owned());
        let joke_content = RoomMessageEventContent::notice_plain(joke);

        let txn_id = TransactionId::new();
        let req = send_message_event::v3::Request::new(room_id.to_owned(), txn_id, &joke_content)?;
        // Do nothing if we can't send the message.
        let _ = self.matrix_client.send_request(req).await;

        Ok(())
    }

    /// Handle an invitation to the given room.
    async fn handle_invitations(&self, room_id: OwnedRoomId) -> Result<(), Box<dyn Error>> {
        println!("invited to {room_id}");
        self.matrix_client
            .send_request(join_room_by_id::v3::Request::new(room_id.clone()))
            .await?;

        let greeting = "Hello! My name is Mr. Bot! I like to tell jokes. Like this one: ";
        let joke = self
            .get_joke()
            .await
            .unwrap_or_else(|_| "err... never mind.".to_owned());
        let content = RoomMessageEventContent::notice_plain(format!("{greeting}\n{joke}"));
        let txn_id = TransactionId::new();
        let message = send_message_event::v3::Request::new(room_id, txn_id, &content)?;
        self.matrix_client.send_request(message).await?;
        Ok(())
    }

    /// Get a new joke from the API.
    async fn get_joke(&self) -> Result<String, Box<dyn Error>> {
        let rsp = self.http_client.get(JOKE_API_URI.clone()).await?;
        let bytes = rsp.into_body().collect().await?.to_bytes();

        let joke_obj = serde_json::from_slice::<JsonValue>(&bytes)
            .map_err(|_| "invalid JSON returned from joke API")?;
        let joke = joke_obj["joke"]
            .as_str()
            .ok_or("joke field missing from joke API response")?;

        Ok(joke.to_owned())
    }
}

/// The session data to persist.
struct State {
    /// The token used for authorizing requests.
    access_token: String,
}

impl State {
    /// The path of the file where the session is persisted.
    const PATH: &str = "./session";

    /// Persist the [`State`] to a file.
    async fn write(&self) -> io::Result<()> {
        let content = &self.access_token;
        fs::write(Self::PATH, content).await?;
        Ok(())
    }

    /// Try to read the persisted [`State`] from a file.
    async fn read() -> io::Result<Option<Self>> {
        match fs::read_to_string(Self::PATH).await {
            Ok(access_token) => Ok(Some(Self { access_token })),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
}

/// The bot configuration.
struct Config {
    /// The URL of the homeserver to interact with.
    homeserver: String,
    /// The user ID of the account to use for the bot.
    username: OwnedUserId,
    /// The password of the account.
    ///
    /// Only required the first time.
    password: Option<String>,
}

impl Config {
    /// Read the config file.
    async fn read() -> io::Result<Self> {
        let content = fs::read_to_string("./config").await?;
        let lines = content.split('\n');

        let mut homeserver = None;
        let mut username = Err("required field `username` is missing".to_owned());
        let mut password = None;
        for line in lines {
            if let Some((key, value)) = line.split_once('=') {
                match key.trim() {
                    "homeserver" => homeserver = Some(value.trim().to_owned()),
                    // TODO: infer domain from `homeserver`
                    "username" => {
                        username = value.trim().to_owned().try_into().map_err(|e| {
                            format!("invalid Matrix user ID format for `username`: {e}")
                        });
                    }
                    "password" => password = Some(value.trim().to_owned()),
                    _ => {}
                }
            }
        }

        match (homeserver, username) {
            (Some(homeserver), Ok(username)) => Ok(Self {
                homeserver,
                username,
                password,
            }),
            (homeserver, username) => {
                let mut error = String::from("Invalid config specified:");
                if homeserver.is_none() {
                    error.push_str("\n  required field `homeserver` is missing");
                }
                if let Err(e) = username {
                    error.push_str("\n  ");
                    error.push_str(&e);
                }
                Err(io::Error::new(io::ErrorKind::InvalidData, error))
            }
        }
    }
}
