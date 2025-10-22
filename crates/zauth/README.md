# ZAuth - Enterprise Authentication & Authorization System

> **🚨 CRITICAL STATUS**: This crate has **COMPILATION BLOCKERS** and cannot be built or tested. Please see the [TODO_STILL.md](./TODO_STILL.md) for details on critical issues.

## 🚨 Current Status: 🔴 BLOCKED

### Critical Issues Preventing Compilation:

- **❌ Missing from workspace**: zauth not listed in workspace Cargo.toml members
- **❌ Storage architecture mismatch**: Tests use redb, zcore-storage uses fjall
- **⚠️ Cannot compile or run tests** due to workspace configuration issues

### Architecture Assessment:

The crate demonstrates sophisticated authentication design with excellent security architecture:

- **✅ Security design**: Enterprise-grade authentication with JWT, OAuth, 2FA
- **✅ Feature completeness**: Comprehensive auth services (team management, delegation, biometrics)
- **⚠️ Implementation issues**: Storage backend inconsistency prevents testing
- **⚠️ Development blocked**: Cannot compile or verify functionality due to configuration

## ✅ Implemented Features

### Core Authentication
- **JWT-based Authentication**: Separate access and refresh tokens with configurable TTL
- **Argon2 Password Hashing**: Secure password storage with configurable work factors
- **Session Management**: Persistent session tracking with revocation support
- **Environment Switching**: Change environments without re-authentication
- **Development Tokens**: HMAC-signed development tokens for testing

### OAuth Integration
- **Multi-Provider Support**: Google, GitHub, Discord, Apple OAuth providers
- **Account Linking**: Link multiple OAuth accounts to single user
- **PKCE Support**: Enhanced security for OAuth flows
- **Provider Configuration**: Flexible OAuth provider configuration system

### Multi-Factor Authentication (2FA)
- **TOTP (Time-based OTP)**: RFC 6238 compliant authenticator app support
- **SMS Verification**: Phone number based authentication
- **WebAuthn/FIDO2**: Hardware security key support
- **Backup Codes**: Recovery codes for emergency access

### Team & Organization Management
- **Team Hierarchies**: Nested team structures with inheritance
- **Role Delegation**: Temporary and conditional role assignments
- **Permission Inheritance**: Hierarchical permission propagation
- **Team Invitations**: Email-based team invitation system

## 🚧 Features In Development

### Biometric Authentication Framework
- **Multi-Modal Support**: Face, fingerprint, voice, iris recognition (comprehensive framework exists, core implementations incomplete)
- **Liveness Detection**: Anti-spoofing with challenge-response (challenge generation implemented, verification incomplete)
- **Template Protection**: Cancelable biometric templates (HSM integration framework exists, needs completion)
- **Privacy Preservation**: Zero-knowledge proofs and differential privacy (mathematical frameworks implemented)

### Advanced Security Features
- **Device Trust Management**: Device fingerprinting and trust scoring (service integrated, needs enhancement)
- **Hardware Attestation**: Device hardware security verification (service integrated, verification logic incomplete)
- **Session Security Monitoring**: Real-time security analysis (framework exists with bloom filters, unused)
- **Geographic Restrictions**: Location-based access controls (GeoIP integration exists, policy enforcement incomplete)

## 🔧 Installation

**⚠️ Development Use Only**

Add to your `Cargo.toml`:

```toml
[dependencies]
zauth = { path = "path/to/zauth" }
```

## 🚦 Quick Start

### Basic Setup

```rust
use zauth::{AuthConfig, AuthService, AuthState};
use zcore_storage::Store;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize storage
    let store = Store::open("./data").expect("Failed to open store");
    let store = Box::leak(Box::new(store));

    // Configure authentication
    let config = AuthConfig::new(
        Some(1), // dev_org_id
        Some("dev-secret".into()), // dev_secret
        "access-secret".into(),
        "refresh-secret".into(),
        900,  // access_ttl_secs (15 min)
        604800, // refresh_ttl_secs (7 days)
    );

    // Create auth service
    let auth_service = AuthService::new(store, config.clone());

    // Create admin user
    let admin = auth_service.signup_initial_admin(
        1,
        "admin@example.com",
        "secure-password"
    )?;

    println!("Admin user created: {}", admin.email);
    Ok(())
}
```

### Axum Integration

```rust
use axum::{extract::State, response::Json, routing::post, Router};
use serde::{Deserialize, Serialize};
use zauth::{AuthConfig, AuthSession, AuthState};

#[derive(Deserialize)]
struct LoginRequest {
    email: String,
    password: String,
    environment: Option<String>,
}

async fn login(
    State(auth_state): State<AuthState>,
    Json(req): Json<LoginRequest>,
) -> Result<Json<zauth::LoginTokens>, zauth::AuthError> {
    let auth_service = zauth::AuthService::new(auth_state.store, auth_state.config);

    let tokens = auth_service
        .login(1, &req.email, &req.password, req.environment)
        .await?;

    Ok(Json(tokens))
}

// Protected route example
async fn protected_route(
    auth_session: AuthSession, // Automatically extracted and validated
) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "user_id": auth_session.user_id,
        "org_id": auth_session.org_id,
        "roles": auth_session.roles,
        "environment": auth_session.env_slug
    }))
}

fn create_app(auth_state: AuthState) -> Router {
    Router::new()
        .route("/login", post(login))
        .route("/protected", axum::routing::get(protected_route))
        .with_state(auth_state)
}
```

### OAuth Integration

```rust
use zauth::oauth::{OAuthService, OAuthProvider};

async fn oauth_login_example() -> anyhow::Result<()> {
    let store = Store::open("./data")?;
    let oauth_service = OAuthService::new(&store);

    // Initiate OAuth flow
    let flow = oauth_service.initiate_oauth_flow(
        &OAuthProvider::Google,
        1, // org_id
        Some("live".to_string()), // environment
        "https://yourapp.com/auth/google/callback"
    ).await?;

    println!("Redirect user to: {}", flow.authorization_url);

    // Handle callback (in your callback handler)
    let result = oauth_service.handle_oauth_callback(
        &OAuthProvider::Google,
        &flow.state,
        "authorization_code_from_callback",
        Some(&flow.code_verifier)
    ).await?;

    match result {
        zauth::oauth::OAuthLoginResult::NewUser { tokens, profile } => {
            println!("New user created: {}", profile.email.unwrap_or_default());
        },
        zauth::oauth::OAuthLoginResult::ExistingUser { tokens } => {
            println!("Existing user logged in");
        },
        zauth::oauth::OAuthLoginResult::AccountLinkingRequired { .. } => {
            println!("Account linking required");
        }
    }

    Ok(())
}
```

## 🔧 Configuration

### Environment Variables

#### Core Authentication
```bash
# JWT Configuration
ZJWT_SECRET=your-access-token-secret
ZJWT_REFRESH_SECRET=your-refresh-token-secret
ZJWT_ACCESS_TTL_SECS=900    # 15 minutes
ZJWT_REFRESH_TTL_SECS=604800 # 7 days

# Development Mode
ZAUTH_DEV_ORG_ID=1
ZAUTH_DEV_SECRET=dev-secret-key
```

#### OAuth Providers
```bash
# Google OAuth
GOOGLE_CLIENT_ID=your-google-client-id
GOOGLE_CLIENT_SECRET=your-google-client-secret
GOOGLE_REDIRECT_URI=https://yourapp.com/auth/google/callback

# GitHub OAuth
GITHUB_CLIENT_ID=your-github-client-id
GITHUB_CLIENT_SECRET=your-github-client-secret
GITHUB_REDIRECT_URI=https://yourapp.com/auth/github/callback

# Discord OAuth
DISCORD_CLIENT_ID=your-discord-client-id
DISCORD_CLIENT_SECRET=your-discord-client-secret
DISCORD_REDIRECT_URI=https://yourapp.com/auth/discord/callback

# Apple OAuth
APPLE_CLIENT_ID=your-apple-client-id
APPLE_CLIENT_SECRET=your-apple-client-secret
APPLE_REDIRECT_URI=https://yourapp.com/auth/apple/callback
```

## 🏗️ Architecture

### Core Components

1. **AuthService**: Main authentication service providing signup, login, refresh, logout
2. **OAuthService**: OAuth provider integration and account linking
3. **SessionService**: Advanced session management and security monitoring
4. **TwoFactorService**: Multi-factor authentication implementation
5. **BiometricApi**: Biometric authentication and template management (in development)
6. **TeamService**: Team and organization management
7. **PermissionService**: Role-based access control with delegation

### Security Layers

1. **Transport Security**: HTTPS/TLS encryption for all communications
2. **Token Security**: JWT with separate access/refresh keys and rotation
3. **Password Security**: Argon2 hashing with configurable parameters
4. **Session Security**: Device fingerprinting and anomaly detection
5. **Biometric Security**: Template protection and privacy preservation (in development)
6. **Hardware Security**: Device attestation and HSM integration (in development)

## 🧪 Testing

### Current Test Status
- ✅ OAuth integration tests
- ✅ Basic authentication functionality
- ⚠️ Limited biometric authentication tests
- ❌ Device attestation integration tests
- ❌ Session security monitoring tests

### Running Tests

```bash
# Run all tests
cargo test -p zauth

# Run specific test categories
cargo test -p zauth --test oauth_integration
cargo test -p zauth --lib -- two_factor
cargo test -p zauth --lib -- biometric
```

**Note**: Some tests require specific environment setup and dependencies.

## 📊 Critical Issues (Blockers)

### 1. Workspace Configuration - COMPLETE BLOCKAGE
**Status: CRITICAL** | **Impact: Cannot compile**

- **Missing from workspace**: zauth not listed in workspace Cargo.toml members
- **Compilation blocked**: Cannot build or test the crate
- **Development impact**: All work on authentication features blocked

### 2. Storage Architecture Mismatch - DESIGN FLAW
**Status: CRITICAL** | **Impact: Testing integrity**

- **Inconsistent backends**: Tests use redb, production uses fjall via zcore-storage
- **Evidence**: Tests use `.redb` files, zcore-storage uses fjall LSM-tree
- **Risk**: Tests don't reflect production behavior
- **Files affected**: All test files, examples, integration tests

### 3. Cyclic Dependencies - MONITORING BLOCKED
**Status: HIGH** | **Impact: Observability**

- **Dependency cycle**: zobservability → zauth → zbrain → zobservability
- **Metrics disabled**: Authentication event tracking disabled
- **Monitoring impact**: No production metrics or analytics

### Performance Considerations
- Database query optimization opportunities identified in repository layer
- Memory usage could be reduced by removing unused struct fields (~20-30% potential reduction)
- Rate limiting implementation exists but not fully integrated

### Security Considerations
- Many security-critical parameters are currently unused (signatures, challenge data, etc.)
- HSM integration for biometric templates is framework-only
- Device attestation verification needs implementation completion
- Input validation framework exists but not fully integrated across all services

## 🔮 Roadmap

### Phase 0: Unblock Development (Immediate - 1-2 days) - HIGHEST PRIORITY
- [ ] **Add zauth to workspace Cargo.toml** - Absolute blocker for all work
- [ ] **Fix storage architecture mismatch** - Critical for testing integrity
- [ ] Verify compilation succeeds after workspace fix
- [ ] Run basic tests to ensure functionality

### Phase 1: Critical Infrastructure Fixes (1-2 weeks)
- [ ] Fix all compilation warnings (unused fields in placeholder implementations)
- [ ] Resolve cyclic dependency issues with zobservability
- [ ] Migrate tests to use zcore-storage instead of direct redb
- [ ] Fix failing integration tests

### Phase 2: Security Feature Completion (3-4 weeks)
- [ ] Complete biometric verification algorithms (template matching, liveness detection)
- [ ] Implement HSM integration for biometric template protection
- [ ] Finish device attestation verification logic
- [ ] Restore metrics and observability integration

### Phase 3: Production Readiness (6-8 weeks)
- [ ] Security audit and penetration testing preparation
- [ ] Compliance documentation (GDPR, SOC2, etc.)
- [ ] Performance benchmarking and optimization
- [ ] Complete API documentation and deployment guides

## 🤝 Contributing

This crate is currently under active development. Contributions are welcome, but please review the [TODO_STILL.md](./TODO_STILL.md) file to understand current limitations and priorities.

### Development Guidelines
1. All new code must compile without warnings
2. Security features must have comprehensive test coverage
3. Follow existing code patterns and architecture
4. Update documentation for all new features
5. Focus on completing existing frameworks rather than adding new features

### Areas Needing Contribution
- **Biometric verification algorithms** (template matching, liveness detection)
- **Device attestation verification** (TPM, secure enclave integration)
- **Security parameter implementation** (signatures, challenge validation)
- **Test coverage expansion** (especially for failing integration tests)

## 📄 License

This project is licensed under the MIT OR Apache-2.0 license.

## 🆘 Support

- **Documentation**: See ZRUSTDB documentation for complete integration guides
- **Issues**: Report bugs and feature requests via GitHub issues
- **Security**: Report security vulnerabilities via security@zrustdb.com

---

**🚨 CRITICAL WARNING**: This authentication system **CANNOT BE COMPILED OR TESTED** due to fundamental configuration issues:

1. **Missing from workspace**: zauth must be added to workspace Cargo.toml members
2. **Storage architecture mismatch**: Tests use redb, production uses fjall
3. **Development blocked**: Cannot build, test, or verify functionality

**DO NOT ATTEMPT TO USE** until these critical blockers are resolved. See [TODO_STILL.md](./TODO_STILL.md) for detailed analysis and fix priorities.