.. _tls:

TLS
===

A critical component of Prototype 3 will be authentication. To ensure that
credentials and other sensitive data **can** be transmitted securely, we
will implement TLS (Transport Layer Security) using the following crates
to assist us:

- `rustls` - A modern TLS library written in Rust.
- `tokio-rustls` - Asynchronous TLS support for Tokio.
- `webpki` - A library for verifying X.509 certificates.

.. mermaid::

   sequenceDiagram
       participant C as Client
       participant S as Server (TcpListener)
       participant TLS as TlsAcceptor
       participant H as Connection Handler

       Note over S: Server loads cert & private key<br/>via load_tls_acceptor()

       C->>S: TCP Connection Request
       S->>S: listener.accept()

       alt TLS Enabled
         S->>TLS: tls.accept(stream)
         TLS->>C: ServerHello + Certificate
         Note over TLS,C: Certificate contains public key
         C->>TLS: ClientKeyExchange
         Note over C,TLS: Client verifies certificate<br/>Generates shared secret
         C->>TLS: ChangeCipherSpec + Finished
         TLS->>C: ChangeCipherSpec + Finished
         Note over C,TLS: Encrypted channel established

         TLS->>H: TLS stream (encrypted)
         H->>C: Application Data (encrypted)
         C->>H: Application Data (encrypted)
       else Plain TCP
            S->>H: TCP stream (plaintext)
            H->>C: Application Data (plaintext)
            C->>H: Application Data (plaintext)
       end

This diagram illustrates the TLS handshake process between a client and a server,
highlighting the steps taken to establish a secure connection.


Codebase Changes
^^^^^^^^^^^^^^^^

In this prototype, we've had to restructure some parts of the server daemon and
networking module to accomodate both plain TCP and TLS connections. This involved
abstracting the connection handling logic to support both types of connections
seamlessly.

To abstract this, we changed the function signature for `handle_connection` from:

.. code-block:: rust

    pub async fn handle_connection(
      mut stream: TcpStream,
      storage_engine: Arc<StorageEngine>,
      sessions: Arc<DashMap<u64, Session>>,
      mut shutdown_rx: broadcast::Receiver<()>,
  ) -> Result<()>

To:

.. code-block:: rust

    pub async fn handle_connection<S>(
      mut stream: S,
      storage_engine: Arc<StorageEngine>,
      sessions: Arc<DashMap<u64, Session>>,
      mut shutdown_rx: broadcast::Receiver<()>,
  ) -> Result<()>
  where
      S: AsyncRead + AsyncWrite + Unpin + Send + 'static,

This allows `handle_connection` to accept any stream type that implements
the `AsyncRead` and `AsyncWrite` traits, enabling it to work with both
`TcpStream` and `TlsStream<TcpStream>`.

With these changes, the server can now handle both secure and non-secure
connections.

In the `Server::run()` method, we now check if TLS is enabled. If it is,
we wrap the accepted TCP stream with the TLS acceptor before passing it to
`handle_connection`. If TLS is not enabled, we pass the plain TCP stream
directly.

.. code-block:: rust

  if let Some(tls) = self.tls_acceptor.clone() {
      // Spawn a new task to handle the TLS connection
      tokio::spawn(async move {
          // Wrap the stream with TLS
          match tls.accept(stream).await {
              Ok(tls_stream) => {
                  // Handle the TLS connection
                  if let Err(e) = crate::network::handle_connection(
                      tls_stream, sessions, connection_shutdown
                  ).await {
                      tracing::error!("TLS connection error: {e}");
                  }
              }
              Err(e) => tracing::warn!("TLS handshake failed from {addr}: {e}"),
          }
      });
  } else {
      // Spawn a new task to handle the plain connection
      tokio::spawn(async move {
          // Handle the plain TCP connection
          if let Err(e) = crate::network::handle_connection(
              stream, sessions, connection_shutdown
          ).await {
              tracing::error!("Connection error: {e}");
          }
      });
  }

As we've already adjusted our `handle_connection` function to be generic over the stream type,
this integration allows us to support both TLS and plain TCP connections with minimal changes
to the existing codebase.

We introduced a new method `load_tls_acceptor` in the server module to load the TLS
certificate and private key from specified file paths. This method initializes
the `TlsAcceptor` which is then used to wrap incoming TCP connections.

.. code-block:: rust

  pub fn load_tls_acceptor(cfg: &TlsConfig) -> Result<TlsAcceptor> {
      // Load certificates from path
      let cert_iter =
          CertificateDer::pem_file_iter(&cfg.cert_path).map_err(|e| MonoError::Io(e.to_string()))?;

      let certs = cert_iter
          .collect::<std::result::Result<Vec<_>, _>>()
          .map_err(|e| MonoError::Io(e.to_string()))?;

      // Load private key (RSA/PKCS8/EC, etc.) from path
      let key: PrivateKeyDer<'static> =
          PrivateKeyDer::from_pem_file(&cfg.key_path).map_err(|e| MonoError::Io(e.to_string()))?;

      let config = ServerConfig::builder()
          .with_no_client_auth()
          .with_single_cert(certs, key)
          .map_err(|e| MonoError::Io(e.to_string()))?;

      Ok(TlsAcceptor::from(Arc::new(config)))
  }

Finally, we had to update the server configuration to include (optionally) the
TLS certificate and private key paths, allowing the server to be configured
for TLS at startup.

.. code-block:: rust

  pub struct TlsConfig {
      pub cert_path: PathBuf,
      pub key_path: PathBuf,
  }

  pub struct ServerConfig {
      pub host: String,
      pub port: u16,
      pub tls: Option<TlsConfig>,
  }

Shown in the `config.toml` file as:

.. code-block:: toml

  # [server.tls]
  # Path to TLS certificate file (PEM format).
  # cert_path = "path/to/cert.pem"

  # Path to TLS private key file (PEM format).
  # key_path = "path/to/key.pem"

With these changes, the server is now capable of handling both TLS-encrypted
and plain TCP connections, allowing for secure communication when needed.
