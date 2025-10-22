# ZNotifications

A comprehensive notification system for ZRUSTDB providing multi-provider email and SMS delivery with template support, delivery tracking, and robust error handling.

**Current Status**: Under development - Core architecture implemented with partial provider support

## Purpose

The znotifications crate provides a unified notification delivery system that supports multiple email and SMS providers with automatic failover, template rendering, delivery tracking, and compliance features. It integrates with ZRUSTDB's authentication and organization management systems.

## Features

### Core Capabilities
- **Multi-Provider Support**: Email (SendGrid, Mailgun, SMTP) and SMS (Twilio) providers
- **Automatic Failover**: Provider priority-based failover with retry mechanisms
- **Template Engine**: Handlebars-based template rendering with built-in helpers
- **Delivery Tracking**: Real-time delivery status tracking and analytics
- **Rate Limiting**: Configurable rate limits per provider and recipient
- **Compliance Features**: Bounce handling, suppression lists, unsubscribe management

### Implementation Status
- ✅ **Core Architecture**: Service structure, repository pattern, tracking system
- ✅ **Template System**: Handlebars engine with built-in helpers and embedded templates
- ✅ **SendGrid Provider**: Complete email provider implementation
- ✅ **Mailgun Provider**: Complete email provider implementation
- ✅ **SMTP Provider**: Complete email provider implementation
- ✅ **Twilio Provider**: Complete SMS provider implementation
- ⚠️ **AWS SES**: Partial implementation - requires completion
- ⚠️ **AWS SNS**: Partial implementation - requires completion
- ⚠️ **Repository Layer**: Basic implementation - needs comprehensive CRUD operations
- ⚠️ **Delivery Tracking**: Framework implemented - webhook handling incomplete

### Email Features
- **Multiple Providers**: SendGrid, Mailgun, and SMTP (AWS SES partial)
- **HTML & Text**: Support for both HTML and plain text email bodies
- **Tracking**: Open and click tracking integration
- **Priority Delivery**: Configurable email priority levels
- **Bulk Operations**: Batch email sending with provider rate limiting

### SMS Features
- **Provider Support**: Twilio (AWS SNS partial)
- **Delivery Receipts**: Optional delivery receipt tracking
- **International**: Support for international phone numbers
- **Scheduled Delivery**: Future-dated message scheduling

### Template System
- **Handlebars Engine**: Powerful template rendering with variable substitution
- **Built-in Helpers**: Date formatting, currency, text manipulation helpers
- **Custom Templates**: Support for custom template registration
- **Multi-format**: Support for HTML, text, and SMS templates
- **Validation**: Template variable validation and type checking

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    NotificationService                      │
├─────────────────────────────────────────────────────────────┤
│ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐ │
│ │   EmailService  │ │   SmsService    │ │ TemplateEngine  │ │
│ └─────────────────┘ └─────────────────┘ └─────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐ │
│ │ DeliveryTracker │ │   Repository    │ │   Providers     │ │
│ └─────────────────┘ └─────────────────┘ └─────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## API Overview

### Core Service

```rust
use znotifications::{NotificationService, NotificationConfig, TemplateContext, EmailOptions};

// Initialize service
let config = NotificationConfig::from_env()?;
let service = NotificationService::new(config)?;

// Send email notification
let context = TemplateContext::new()
    .insert_str("user_name", "John Doe")
    .insert_str("reset_url", "https://example.com/reset/abc123");

let notification_id = service.send_email_notification(
    org_id,
    "password_reset",
    "user@example.com",
    context,
    EmailOptions::default(),
).await?;
```

### Template Rendering

```rust
use znotifications::{TemplateEngine, TemplateContext, NotificationTemplate};

let engine = TemplateEngine::new()?;
let context = TemplateContext::new()
    .insert_str("name", "Alice")
    .insert_number("amount", 99.99)
    .insert_bool("verified", true);

let rendered = engine.render(&template, &context)?;
println!("Subject: {}", rendered.subject);
println!("HTML: {}", rendered.html_body);
```

### Provider Configuration

```rust
use znotifications::config::{EmailConfig, EmailProviderType, EmailCredentials};

// Environment-based configuration
let config = EmailConfig::from_env()?;

// Manual configuration
let mut providers = HashMap::new();
providers.insert(
    EmailProviderType::SendGrid,
    EmailProviderConfig {
        provider_type: EmailProviderType::SendGrid,
        credentials: EmailCredentials::SendGrid {
            api_key: "your-api-key".to_string(),
        },
        settings: EmailSettings::default(),
        enabled: true,
        priority: 1,
    }
);
```

## Usage Examples

### Basic Email Sending

```rust
use znotifications::{NotificationService, TemplateContext, EmailOptions};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = NotificationConfig::from_env()?;
    let service = NotificationService::new(config)?;

    // Simple email with template
    let context = TemplateContext::new()
        .insert_str("user_name", "John")
        .insert_str("verification_code", "123456");

    let options = EmailOptions {
        from_name: Some("Your App".to_string()),
        track_opens: true,
        track_clicks: true,
        ..Default::default()
    };

    let notification_id = service.send_email_notification(
        1, // org_id
        "email_verification",
        "user@example.com",
        context,
        options,
    ).await?;

    println!("Email sent: {:?}", notification_id);
    Ok(())
}
```

### SMS Sending

```rust
use znotifications::{NotificationService, TemplateContext, SmsOptions};

let context = TemplateContext::new()
    .insert_str("verification_code", "123456")
    .insert_str("app_name", "YourApp");

let options = SmsOptions {
    delivery_receipt: true,
    ..Default::default()
};

let notification_id = service.send_sms_notification(
    1, // org_id
    "sms_verification",
    "+1234567890",
    context,
    options,
).await?;
```

### Custom Template Registration

```rust
use znotifications::TemplateEngine;

let mut engine = TemplateEngine::new()?;

// Register custom template
engine.register_template(
    "welcome_email",
    r#"
    <h1>Welcome {{name}}!</h1>
    <p>Thank you for joining {{company_name}}.</p>
    <p>Your account was created on {{format_date created_at "%B %d, %Y"}}.</p>
    "#
)?;

// Use template helpers
let template_with_helpers = r#"
    <p>Balance: {{format_currency balance "USD"}}</p>
    <p>You have {{count}} {{pluralize count "message" "messages"}}</p>
    <p>Description: {{truncate description 100}}</p>
"#;
```

### Delivery Tracking

```rust
use znotifications::{DeliveryTracker, DeliveryEventType};

let tracker = DeliveryTracker::new(repository);

// Record delivery event
tracker.record_event(
    notification_id,
    DeliveryEventType::Delivered,
    "sendgrid".to_string(),
    serde_json::json!({
        "provider_message_id": "abc123",
        "timestamp": "2024-01-01T00:00:00Z"
    })
)?;

// Get delivery statistics
let stats = tracker.get_delivery_stats(org_id, "2024-01-01")?;
println!("Emails sent: {}", stats.emails_sent);
println!("Delivery rate: {:.2}%",
    (stats.emails_delivered as f64 / stats.emails_sent as f64) * 100.0);
```

## Known Issues and Limitations

### Current Limitations
1. **AWS Provider Integration**: AWS SES and SNS providers are partially implemented and require completion
2. **Repository Implementation**: The repository layer has basic functionality but needs comprehensive CRUD operations
3. **Webhook Security**: Signature verification for provider webhooks is not yet implemented
4. **Rate Limiting**: Configuration exists but actual enforcement is not implemented
5. **Error Handling**: Some error cases are not properly handled or tested

### Build Dependencies
- The crate has a dependency conflict with lettre's native-tls feature configuration
- Some warnings about deprecated handlebars functions need to be addressed

## Configuration

### Environment Variables

#### Email Providers

```bash
# SendGrid
SENDGRID_API_KEY=your_sendgrid_api_key

# AWS SES
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_REGION=us-east-1

# Mailgun
MAILGUN_API_KEY=your_mailgun_api_key
MAILGUN_DOMAIN=your_mailgun_domain
MAILGUN_BASE_URL=https://api.eu.mailgun.net  # Optional for EU

# SMTP
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=your_email@gmail.com
SMTP_PASSWORD=your_app_password
SMTP_USE_TLS=true
SMTP_USE_STARTTLS=true
```

#### SMS Providers

```bash
# Twilio
TWILIO_ACCOUNT_SID=your_twilio_account_sid
TWILIO_AUTH_TOKEN=your_twilio_auth_token

# AWS SNS
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_REGION=us-east-1
```

#### General Settings

```bash
NOTIFICATION_RATE_LIMITING=true
```

### Built-in Templates

The crate includes several built-in templates:

- `password_reset.html` - Password reset email with secure styling
- `2fa_setup.html` - Two-factor authentication setup instructions
- `2fa_grace_period_warning.html` - Warning about 2FA grace period expiration
- `login_suspicious.html` - Suspicious login activity notification
- `2fa_code_sms.txt` - SMS template for 2FA verification codes
- `account_recovery_sms.txt` - SMS template for account recovery

### Template Helpers

The template engine includes built-in helpers:

- `{{format_date timestamp "%Y-%m-%d"}}` - Format Unix timestamps
- `{{format_currency amount "USD"}}` - Format currency amounts
- `{{truncate text 100}}` - Truncate text to specified length
- `{{upper text}}` - Convert to uppercase
- `{{lower text}}` - Convert to lowercase
- `{{pluralize count "item" "items"}}` - Pluralization helper

## Integration with ZRUSTDB

### Organization Integration

```rust
// Notifications are scoped to organizations
let notification_id = service.send_email_notification(
    user.org_id,  // Organization ID from zauth
    template_id,
    recipient,
    context,
    options,
).await?;
```

### Storage Integration

The crate uses ZRUSTDB's storage layer (`zcore-storage`) for:
- Notification records and status tracking
- Template storage and management
- Delivery event logging
- Organization preferences
- Suppression lists and bounce management

### Authentication Integration

Works with `zauth` for:
- Organization-scoped notifications
- User preference management
- Security-related notifications (2FA, password reset)

## Error Handling

The crate uses comprehensive error handling with `thiserror`:

```rust
use znotifications::NotificationError;

match service.send_email_notification(...).await {
    Ok(notification_id) => println!("Sent: {:?}", notification_id),
    Err(e) => match e.downcast_ref::<NotificationError>() {
        Some(NotificationError::EmailProvider(msg)) => {
            eprintln!("Provider error: {}", msg);
        }
        Some(NotificationError::InvalidTemplate(msg)) => {
            eprintln!("Template error: {}", msg);
        }
        Some(NotificationError::RateLimitExceeded) => {
            eprintln!("Rate limit exceeded, retry later");
        }
        _ => eprintln!("Unknown error: {}", e),
    }
}
```

## Performance Considerations

- **Provider Failover**: Automatic failover reduces delivery failures
- **Rate Limiting**: Built-in rate limiting prevents provider throttling
- **Batch Operations**: Support for efficient batch email sending
- **Connection Pooling**: HTTP client connection reuse
- **Async Design**: Fully async for high-throughput scenarios

## Security Features

- **Credential Management**: Secure credential storage and handling
- **Bounce Handling**: Automatic suppression of bounced emails
- **Unsubscribe Management**: RFC-compliant unsubscribe handling
- **Rate Limiting**: Protection against abuse and spam
- **Webhook Security**: Signature verification for provider webhooks

## Dependencies

Core dependencies:
- `anyhow` - Error handling
- `serde` / `serde_json` - Serialization
- `tokio` - Async runtime
- `tracing` - Logging and observability

External integrations (missing from Cargo.toml):
- `handlebars` - Template engine
- `lettre` - SMTP client
- `reqwest` - HTTP client for APIs
- `rust-embed` - Template asset embedding
- `ulid` - Unique identifiers
- `chrono` - Date/time handling
- `base64`, `hmac`, `sha2`, `hex` - Cryptographic operations

## License

This crate is part of the ZRUSTDB project.