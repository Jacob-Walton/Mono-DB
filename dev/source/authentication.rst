.. _authentication:

Authentication
==============

This section covers how authentication was implemented in Prototype 3. If
you haven't already, please read the :ref:`tls` section first, as it provides
important context for understanding the authentication process.

Authentication Flow
^^^^^^^^^^^^^^^^^^^

.. mermaid::

  sequenceDiagram
          participant C as Client
          participant S as Server
          participant A as Auth Handler
          participant SM as Session Manager
          participant DB as Storage Engine
  
          Note over C,S: Connection already established<br/>(via TLS or plaintext)
  
          C->>S: AUTH command<br/>{username, password/token}
          S->>A: Validate credentials
  
          alt Using password
              A->>DB: Query user credentials
              DB->>A: Return hashed password + salt
              A->>A: Hash provided password<br/>Compare with stored hash
          else Using token
              A->>DB: Query token metadata
              DB->>A: Return token info + expiry
              A->>A: Validate token & check expiry
          end
  
          alt Valid credentials
              A->>SM: Create session
              Note over SM: Generate session_id<br/>Store in DashMap
              SM->>A: session_id + permissions
              A->>C: AUTH_OK<br/>{session_id, user_info}
              Note over C: Client stores session_id<br/>for future requests
  
              loop Authenticated requests
                  C->>S: Command + session_id
                  S->>SM: Validate session
                  SM->>S: Session valid + permissions
                  S->>DB: Execute command (if authorised)
                  DB->>S: Result
                  S->>C: Response
              end
          else Invalid credentials
              A->>C: AUTH_FAILED<br/>{error_message}
              Note over S: May implement rate limit<br/>or temporary ban
              S->>C: Connection closed
          end
  
          Note over C,S: Session tiemout or<br/>explicit LOGOUT
          C->>S: LOGOUT or timeout
          S->>SM: Remove session
          SM->>S: Session destroyed
          S->>C: Connection closed

The above diagram illustrates the complete authentication flow, from the initial
AUTH command to session management and eventual logout or timeout. However, we
can see that there are some decisions we need to make when implementing this
flow, these will probably be unstable for the course of Prototype 3 development
until we finalise our authentication requirements.

Extending Session
^^^^^^^^^^^^^^^^^

To work with this new authentication system, we need to refactor many parts
of the existing server codebase. First of all, the session needs to gain the
following attributes:

.. code-block:: rust

    // New session struct
    pub struct Session {
        pub id: u64,
        pub tx_id: Option<u64>,
        pub user_id: String,
        pub permissions: Permissions,
        pub created_at: Instant,
        pub last_activity: Instant,
        pub authenticated: bool,
    }

    // Original session struct for reference
    pub struct Session {
        pub id: u64,
        pub tx_id: Option<u64>,
        pub user: Option<String>,
        pub database: Option<String>,
    }

Authentication Methods
^^^^^^^^^^^^^^^^^^^^^^

**Password-based**

Username and bcrypt/argon2 hashed password authentication. This will be the
primary method of authentication for most users. The server will store user
credentials securely in the database.

Users will be able to configure the hashing algorithm used and the relevant
parameters (e.g., cost factor for bcrypt) in the server settings. This allows
for flexibility and adaptability to different security requirements.

**Token-based**

Token-based authentication is typically used for service accounts or automated
processes. The server will issue API keys that can be used to authenticate
requests without needing to provide a username and password each time.

Tokens will have an expiry time and can be revoked by administrators as needed.

**Certificate-based (Stretch Goal)**

If time permits, we may implement certificate-based authentication using
mutual TLS (mTLS). This method provides a high level of security by requiring
both the client and server to present valid certificates during the TLS handshake.

This method is particularly useful for internal services and microservices
that need to communicate securely.

Storing User Credentials
^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

    This section has not yet been written.