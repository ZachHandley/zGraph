pub mod models;
pub mod providers;
pub mod service;
pub mod repository;
pub mod handlers;
pub mod routes;

pub use models::*;
pub use providers::*;
pub use service::*;
pub use repository::*;
pub use handlers::*;
pub use routes::*;

use anyhow::{anyhow, Result};
use sha2::{Digest, Sha256};

/// Generate a unique 32-character ID from provider and platform user ID
/// Pattern: `{provider}_{platform_user_id}` -> SHA256 -> 32 char hex
pub fn generate_oauth_user_id(provider: &str, platform_user_id: &str) -> String {
    let input = format!("{}_{}", provider, platform_user_id);
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let result = hasher.finalize();
    format!("{:x}", result)[..32].to_string()
}

/// Validate OAuth provider name
pub fn validate_provider(provider: &str) -> Result<()> {
    match provider {
        "google" | "github" | "discord" | "apple" => Ok(()),
        _ => Err(anyhow!("Unsupported OAuth provider: {}", provider)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_oauth_user_id() {
        let id1 = generate_oauth_user_id("google", "12345");
        let id2 = generate_oauth_user_id("google", "12345");
        let id3 = generate_oauth_user_id("github", "12345");

        // Same input should generate same ID
        assert_eq!(id1, id2);
        assert_eq!(id1.len(), 32);

        // Different provider should generate different ID
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_validate_provider() {
        assert!(validate_provider("google").is_ok());
        assert!(validate_provider("github").is_ok());
        assert!(validate_provider("discord").is_ok());
        assert!(validate_provider("apple").is_ok());
        assert!(validate_provider("invalid").is_err());
    }
}