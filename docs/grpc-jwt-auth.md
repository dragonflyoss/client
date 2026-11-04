# gRPC JWT authentication

Dfdaemon can attach a JSON Web Token (JWT) to outgoing gRPC calls and verify JWTs on its peer upload gRPC server. The implementation currently supports HS256 and requires the `exp` claim. If `nbf` is present, it is also validated.

Authentication remains backward compatible for local development: when no token is configured, clients send no `authorization` metadata; when no server secret is configured, the upload server accepts unauthenticated calls. Both cases emit a warning. Configuring a token or secret explicitly enables the corresponding behavior.

The Unix-domain download API is local and is unchanged. Manager and scheduler servers must apply the same HS256 verification policy in their own implementations.

## Enable authentication

Configure `security.jwt` in `dfdaemon.yaml`:

```yaml
security:
  jwt:
    # Used by outgoing manager, scheduler, health, and peer gRPC clients.
    tokenPath: /etc/dragonfly/client/grpc.jwt

    # Used by the upload gRPC server to verify incoming peer calls.
    # Mount this from your secret manager; do not commit a real secret.
    secret: replace-with-the-shared-hs256-secret
```

A token can also be configured directly, although a mounted file is recommended to avoid putting credentials in a configuration manifest:

```yaml
security:
  jwt:
    token: replace-with-an-hs256-jwt
    secret: replace-with-the-shared-hs256-secret
```

`token` and `tokenPath` are mutually exclusive. A request made with both configured fails with `UNAUTHENTICATED`. Token files are read when a gRPC client is constructed, and a trailing newline is ignored. Restart dfdaemon (or otherwise recreate its gRPC clients) after rotating a token file.

## Issue and distribute tokens

Issue tokens through your existing identity or secret-management system with:

- algorithm `HS256` in the JOSE header;
- an integer `exp` claim containing the Unix expiration time in seconds;
- an optional integer `nbf` claim for not-before validation;
- a signature created with the same high-entropy secret configured in `security.jwt.secret`.

Audience (`aud`), issuer (`iss`), and subject (`sub`) claims are not restricted; authentication is based on the HS256 signature and token lifetime. Deployments that require audience or issuer isolation must enforce it at token issuance or use distinct verification secrets.

Distribute the token and verification secret through separate secret-management channels where possible. Token files should be readable only by the dfdaemon user (for example, mode `0600` on Linux). Every peer that must accept the token needs the same verification secret; every client needs a currently valid token. Do not log tokens or secrets.

Expired tokens, malformed tokens, missing credentials on an authenticated server, and invalid or tampered signatures are rejected with gRPC status `UNAUTHENTICATED`.
