# ZRUSTDB OAuth Integration System

A comprehensive OAuth integration system for ZRUSTDB with multi-provider support, unique ID generation, account linking, and seamless integration with the existing zauth system.

## Features

- **Multi-Provider Support**: Google, GitHub, Discord, Apple
- **Unique ID Generation**: Pattern `{provider}_{userPlatformId}` hashed to 32-char ID
- **Account Linking**: Link multiple OAuth accounts to single ZRUSTDB user
- **Unified User Profiles**: Combined view of user data across providers
- **Platform-Specific Metadata**: Store provider-specific user information
- **Secure OAuth Flows**: PKCE, CSRF protection, secure token exchange
- **Seamless Integration**: Works with existing zauth JWT authentication

## Quick Start

### 1. Environment Configuration

```bash
# Google OAuth
export GOOGLE_CLIENT_ID="your_google_client_id"
export GOOGLE_CLIENT_SECRET="your_google_client_secret"
export GOOGLE_REDIRECT_URI="http://localhost:3000/auth/oauth/google/callback"

# GitHub OAuth
export GITHUB_CLIENT_ID="your_github_client_id"
export GITHUB_CLIENT_SECRET="your_github_client_secret"
export GITHUB_REDIRECT_URI="http://localhost:3000/auth/oauth/github/callback"

# Discord OAuth
export DISCORD_CLIENT_ID="your_discord_client_id"
export DISCORD_CLIENT_SECRET="your_discord_client_secret"
export DISCORD_REDIRECT_URI="http://localhost:3000/auth/oauth/discord/callback"

# Apple OAuth
export APPLE_CLIENT_ID="your_apple_client_id"
export APPLE_CLIENT_SECRET="your_apple_client_secret"
export APPLE_REDIRECT_URI="http://localhost:3000/auth/oauth/apple/callback"
```

### 2. Basic Integration

```rust
use zauth::{
    oauth::{oauth_routes, OAuthService, OAuthProvider},
    AuthConfig, AuthState,
};
use axum::Router;
use zcore_storage::Store;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize storage and auth config
    let store = Box::leak(Box::new(Store::open_memory("app")?));
    let auth_config = AuthConfig::new(/* config params */);
    let auth_state = AuthState { config: auth_config, store };

    // Create application with OAuth routes
    let app = Router::new()
        .nest("/auth", oauth_routes())
        .with_state(auth_state);

    // Start server
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    axum::serve(listener, app).await?;
    Ok(())
}
```

## API Endpoints

### OAuth Flow Endpoints

- `GET /auth/oauth/:provider/authorize` - Start OAuth authorization flow
- `GET /auth/oauth/:provider/callback` - OAuth callback (JSON response)
- `GET /auth/oauth/:provider/redirect` - OAuth callback (frontend redirect)

### User Management Endpoints

- `GET /auth/profile` - Get unified user profile
- `POST /auth/oauth/unlink` - Unlink OAuth account

## OAuth Flow

### 1. Authorization Request

```http
GET /auth/oauth/google/authorize?env_slug=live&org_id=1
```

Response:
```json
{
  "authorization_url": "https://accounts.google.com/o/oauth2/v2/auth?...",
  "state": "csrf-token-here",
  "session_id": "session-id-here"
}
```

### 2. User Authorization

User is redirected to provider (Google, GitHub, etc.) for authorization.

### 3. Callback Handling

```http
GET /auth/oauth/google/callback?code=auth_code&state=csrf-token
```

Response:
```json
{
  "success": true,
  "user_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
  "is_new_user": true,
  "is_linked_account": false,
  "access_token": "jwt_access_token",
  "refresh_token": "jwt_refresh_token",
  "access_expires_at": 1640995200,
  "refresh_expires_at": 1641600000,
  "profile": {
    "id": "unique_32_char_oauth_id",
    "provider": "google",
    "email": "user@example.com",
    "name": "John Doe",
    "avatar_url": "https://example.com/avatar.jpg",
    "username": null
  }
}
```

## Unique ID Generation

OAuth users get unique 32-character IDs generated from provider and platform user ID:

```rust
use zauth::oauth::generate_oauth_user_id;

let id = generate_oauth_user_id("google", "123456789");
// Results in: "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6" (32 chars)
```

The pattern is: `SHA256(provider + "_" + platform_user_id)[0..32]`

## Account Linking

Users can link multiple OAuth accounts to their ZRUSTDB account:

```rust
use zauth::oauth::OAuthService;

let oauth_service = OAuthService::new(store, auth_config);

// Start linking flow (user must be authenticated)
let flow = oauth_service.initiate_oauth_flow(
    OAuthProvider::GitHub,
    org_id,
    None, // env_slug
    Some(user_id), // linking_user_id
).await?;
```

## Unified User Profile

Get comprehensive user information across all linked accounts:

```http
GET /auth/profile
Authorization: Bearer jwt_token
```

Response:
```json
{
  "user_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
  "email": "user@example.com",
  "primary_name": "John Doe",
  "primary_avatar": "https://example.com/avatar.jpg",
  "oauth_profiles": [
    {
      "id": "google_profile_id",
      "provider": "google",
      "email": "user@example.com",
      "name": "John Doe",
      "avatar_url": "https://example.com/avatar.jpg",
      "username": null
    },
    {
      "id": "github_profile_id",
      "provider": "github",
      "email": "user@example.com",
      "name": "John Doe",
      "avatar_url": "https://github.com/avatar.jpg",
      "username": "johndoe"
    }
  ],
  "created_at": 1640995200,
  "updated_at": 1640995200
}
```

## Advanced Usage

### Custom Provider Configuration

```rust
use zauth::oauth::{OAuthService, OAuthConfig, OAuthProvider};

let mut oauth_service = OAuthService::new(store, auth_config);

oauth_service.add_provider(OAuthConfig {
    provider: OAuthProvider::Google,
    client_id: "custom_client_id".to_string(),
    client_secret: "custom_client_secret".to_string(),
    redirect_uri: "https://myapp.com/oauth/callback".to_string(),
    scopes: vec!["openid".to_string(), "email".to_string(), "profile".to_string()],
    auth_url: "https://accounts.google.com/o/oauth2/v2/auth".to_string(),
    token_url: "https://oauth2.googleapis.com/token".to_string(),
    user_info_url: "https://openidconnect.googleapis.com/v1/userinfo".to_string(),
});
```

### Account Unlinking

```http
POST /auth/oauth/unlink
Authorization: Bearer jwt_token
Content-Type: application/json

{
  "provider": "github"
}
```

### Error Handling

The OAuth system provides comprehensive error handling:

```rust
use zauth::oauth::OAuthError;

match oauth_service.handle_oauth_callback(code, state, session_id).await {
    Ok(login_result) => {
        // Success - user authenticated
    }
    Err(OAuthError::InvalidState) => {
        // CSRF token validation failed
    }
    Err(OAuthError::StateExpired) => {
        // OAuth state expired (10 minute timeout)
    }
    Err(OAuthError::EmailConflict) => {
        // Email already registered with different provider
    }
    Err(OAuthError::AccountAlreadyLinked) => {
        // OAuth account already linked to different user
    }
    Err(e) => {
        // Other errors
    }
}
```

## Security Features

### CSRF Protection
- Unique state tokens for each OAuth flow
- State tokens expire after 10 minutes
- State tokens are consumed on use

### PKCE (Proof Key for Code Exchange)
- SHA256 code challenges for enhanced security
- Prevents authorization code interception attacks

### Token Security
- JWT access tokens (short-lived, 1 hour default)
- JWT refresh tokens (long-lived, 7 days default)
- Secure session management with revocation

### Data Protection
- Provider-specific metadata stored securely
- Email verification status tracked
- User consent respected for data sharing

## Database Schema

The OAuth system uses the following database tables:

- `oauth_profiles` - OAuth user profiles from providers
- `oauth_account_links` - Links between ZRUSTDB users and OAuth profiles
- `oauth_states` - CSRF state tokens for OAuth flows
- `oauth_sessions` - OAuth session data with PKCE verifiers
- `oauth_profile_email` - Email lookup index
- `oauth_provider_user` - Provider user ID lookup index

## Provider-Specific Notes

### Google OAuth
- Provides: ID, email, name, picture, verified email status
- Requires: `openid`, `email`, `profile` scopes
- Well-supported with reliable user info

### GitHub OAuth
- Provides: ID, email, name, username, avatar
- Requires: `user:email` scope
- Email verification status available via separate API

### Discord OAuth
- Provides: ID, email, username, avatar, verified status
- Requires: `identify`, `email` scopes
- Username format: `username#discriminator`

### Apple OAuth
- Provides: ID, email, verified status (minimal for privacy)
- Requires: `email`, `name` scopes
- Name only provided on first authorization

## Performance Considerations

- OAuth profiles cached in database for fast access
- Unique ID generation is deterministic (no random generation)
- Email and provider lookups use dedicated indices
- Expired state/session cleanup runs automatically
- Provider user info cached to reduce API calls

## Testing

Run OAuth integration tests:

```bash
cargo test oauth_integration --package zauth
```

Example test coverage:
- Unique ID generation
- OAuth service initialization
- Profile creation and retrieval
- Account linking workflows
- State and session management
- Cleanup and expiration
- Integration with existing auth system

## Troubleshooting

### Common Issues

1. **Invalid redirect URI**
   - Ensure redirect URIs match exactly in provider configuration
   - Check for trailing slashes or protocol mismatches

2. **Email conflicts**
   - User tries to OAuth with email already registered via password
   - Implement account migration flow or require unlinking

3. **State validation failures**
   - Check that state tokens aren't being cached/reused
   - Verify 10-minute expiration isn't being exceeded

4. **Provider configuration errors**
   - Verify all required environment variables are set
   - Check client ID/secret validity with provider
   - Ensure scopes match what's configured in provider dashboard

### Debug Mode

Enable debug logging:

```rust
std::env::set_var("RUST_LOG", "zauth::oauth=debug");
tracing_subscriber::fmt::init();
```

This will log OAuth flow details, token exchanges, and error details for troubleshooting.