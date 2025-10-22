use anyhow::{anyhow, Result};
use std::collections::HashSet;

/// Input validation and sanitization utilities for authentication functions
pub struct AuthValidator;

impl AuthValidator {
    /// Sanitize and validate email addresses
    pub fn validate_email(email: &str) -> Result<String> {
        if email.is_empty() {
            return Err(anyhow!("Email cannot be empty"));
        }

        if email.len() > 254 {
            return Err(anyhow!("Email too long: {} characters", email.len()));
        }

        // Basic email format validation
        if !email.contains('@') {
            return Err(anyhow!("Email must contain @ symbol"));
        }

        let parts: Vec<&str> = email.split('@').collect();
        if parts.len() != 2 {
            return Err(anyhow!("Email must contain exactly one @ symbol"));
        }

        let (local_part, domain) = (parts[0], parts[1]);

        if local_part.is_empty() {
            return Err(anyhow!("Email local part cannot be empty"));
        }

        if domain.is_empty() {
            return Err(anyhow!("Email domain cannot be empty"));
        }

        if !domain.contains('.') {
            return Err(anyhow!("Email domain must contain at least one dot"));
        }

        // Check for potentially dangerous characters
        let dangerous_chars = ['<', '>', '"', '\'', ';', '(', ')', '\\', '/'];
        for dangerous_char in dangerous_chars {
            if email.contains(dangerous_char) {
                return Err(anyhow!("Email contains invalid character: {}", dangerous_char));
            }
        }

        // Return trimmed email
        Ok(email.trim().to_lowercase())
    }

    /// Sanitize and validate passwords
    pub fn validate_password(password: &str) -> Result<String> {
        if password.is_empty() {
            return Err(anyhow!("Password cannot be empty"));
        }

        if password.len() < 8 {
            return Err(anyhow!("Password too short: minimum 8 characters"));
        }

        if password.len() > 128 {
            return Err(anyhow!("Password too long: maximum 128 characters"));
        }

        // Check for common weak passwords
        let weak_passwords = [
            "password", "12345678", "qwerty", "letmein", "admin", "welcome",
            "password123", "11111111", "12344321", "password1", "admin123"
        ];

        let lower_password = password.to_lowercase();
        if weak_passwords.contains(&lower_password.as_str()) {
            return Err(anyhow!("Password is too common and easily guessable"));
        }

        // Return the password as-is (will be hashed later)
        Ok(password.to_string())
    }

    /// Sanitize and validate user agent strings
    pub fn validate_user_agent(user_agent: &Option<String>) -> Result<Option<String>> {
        match user_agent {
            Some(ua) => {
                if ua.is_empty() {
                    Ok(None)
                } else if ua.len() > 1024 {
                    Err(anyhow!("User agent too long: maximum 1024 characters"))
                } else {
                    // Check for potentially dangerous characters
                    let dangerous_chars = ['<', '>', '"', '\'', '\\', '\n', '\r', '\t'];
                    for dangerous_char in dangerous_chars {
                        if ua.contains(dangerous_char) {
                            return Err(anyhow!("User agent contains invalid character: {}", dangerous_char));
                        }
                    }
                    Ok(Some(ua.trim().to_string()))
                }
            }
            None => Ok(None),
        }
    }

    /// Sanitize and validate IP addresses
    pub fn validate_ip_address(ip: &Option<String>) -> Result<Option<String>> {
        match ip {
            Some(ip_str) => {
                if ip_str.is_empty() {
                    Ok(None)
                } else if ip_str.len() > 45 { // IPv6 addresses can be up to 45 chars
                    Err(anyhow!("IP address too long: maximum 45 characters"))
                } else {
                    // Basic IP address validation
                    if ip_str.contains(":::") || ip_str.starts_with(":") || ip_str.ends_with(":") {
                        return Err(anyhow!("Invalid IP address format"));
                    }

                    // Check for potentially dangerous characters
                    let valid_chars: HashSet<char> = "0123456789abcdef:.:".chars().collect();
                    for c in ip_str.to_lowercase().chars() {
                        if !valid_chars.contains(&c) {
                            return Err(anyhow!("IP address contains invalid character: {}", c));
                        }
                    }

                    Ok(Some(ip_str.trim().to_string()))
                }
            }
            None => Ok(None),
        }
    }

    /// Sanitize and validate environment names
    pub fn validate_environment_slug(slug: &str) -> Result<String> {
        if slug.is_empty() {
            return Err(anyhow!("Environment slug cannot be empty"));
        }

        if slug.len() > 50 {
            return Err(anyhow!("Environment slug too long: maximum 50 characters"));
        }

        // Check for valid characters (alphanumeric, hyphens, underscores)
        let valid_chars: HashSet<char> = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_".chars().collect();
        for c in slug.chars() {
            if !valid_chars.contains(&c) {
                return Err(anyhow!("Environment slug contains invalid character: {}", c));
            }
        }

        // Must start and end with alphanumeric
        if !slug.chars().next().unwrap_or('a').is_alphanumeric() {
            return Err(anyhow!("Environment slug must start with alphanumeric character"));
        }

        if !slug.chars().last().unwrap_or('a').is_alphanumeric() {
            return Err(anyhow!("Environment slug must end with alphanumeric character"));
        }

        Ok(slug.trim().to_string())
    }

    /// Sanitize and validate device fingerprints
    pub fn validate_device_fingerprint(fingerprint: &str) -> Result<String> {
        if fingerprint.is_empty() {
            return Err(anyhow!("Device fingerprint cannot be empty"));
        }

        if fingerprint.len() > 256 {
            return Err(anyhow!("Device fingerprint too long: maximum 256 characters"));
        }

        // Basic validation for hex-based fingerprints
        let valid_chars: HashSet<char> = "0123456789abcdefABCDEF-:".chars().collect();
        for c in fingerprint.chars() {
            if !valid_chars.contains(&c) {
                return Err(anyhow!("Device fingerprint contains invalid character: {}", c));
            }
        }

        Ok(fingerprint.trim().to_string())
    }

    /// Sanitize and validate session tokens
    pub fn validate_session_token(token: &str) -> Result<String> {
        if token.is_empty() {
            return Err(anyhow!("Session token cannot be empty"));
        }

        if token.len() > 4096 {
            return Err(anyhow!("Session token too long: maximum 4096 characters"));
        }

        // JWT tokens typically contain base64url characters
        let valid_chars: HashSet<char> = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~+/=".chars().collect();
        for c in token.chars() {
            if !valid_chars.contains(&c) {
                return Err(anyhow!("Session token contains invalid character: {}", c));
            }
        }

        Ok(token.trim().to_string())
    }

    /// Sanitize string input to prevent injection attacks
    pub fn sanitize_string_input(input: &str, max_length: usize) -> Result<String> {
        if input.len() > max_length {
            return Err(anyhow!("Input too long: maximum {} characters", max_length));
        }

        // Remove potentially dangerous characters for general string inputs
        let dangerous_chars = ['<', '>', '"', '\'', '\\', '\n', '\r', '\t'];
        let mut sanitized = String::new();

        for c in input.chars() {
            if dangerous_chars.contains(&c) {
                // Replace with safe alternatives
                match c {
                    '<' => sanitized.push_str("&lt;"),
                    '>' => sanitized.push_str("&gt;"),
                    '"' => sanitized.push_str("&quot;"),
                    '\'' => sanitized.push_str("&#39;"),
                    '\\' => sanitized.push_str("&#92;"),
                    '\n' => sanitized.push_str("\\n"),
                    '\r' => sanitized.push_str("\\r"),
                    '\t' => sanitized.push_str("\\t"),
                    _ => sanitized.push(c),
                }
            } else {
                sanitized.push(c);
            }
        }

        Ok(sanitized)
    }

    /// Validate numeric IDs (ULID strings)
    pub fn validate_ulid_string(ulid_str: &str) -> Result<String> {
        if ulid_str.is_empty() {
            return Err(anyhow!("ULID string cannot be empty"));
        }

        if ulid_str.len() != 26 {
            return Err(anyhow!("ULID string must be 26 characters long"));
        }

        // ULIDs use Crockford's base32 encoding
        let valid_chars: HashSet<char> = "0123456789ABCDEFGHJKMNPQRSTVWXYZabcdefghjkmnpqrstvwxyz".chars().collect();
        for c in ulid_str.chars() {
            if !valid_chars.contains(&c) {
                return Err(anyhow!("ULID string contains invalid character: {}", c));
            }
        }

        Ok(ulid_str.to_string())
    }

    /// Validate role names
    pub fn validate_role_name(role: &str) -> Result<String> {
        if role.is_empty() {
            return Err(anyhow!("Role name cannot be empty"));
        }

        if role.len() > 50 {
            return Err(anyhow!("Role name too long: maximum 50 characters"));
        }

        // Check for valid characters
        let valid_chars: HashSet<char> = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_: ".chars().collect();
        for c in role.chars() {
            if !valid_chars.contains(&c) {
                return Err(anyhow!("Role name contains invalid character: {}", c));
            }
        }

        Ok(role.trim().to_string())
    }

    /// Validate label names
    pub fn validate_label_name(label: &str) -> Result<String> {
        if label.is_empty() {
            return Err(anyhow!("Label name cannot be empty"));
        }

        if label.len() > 100 {
            return Err(anyhow!("Label name too long: maximum 100 characters"));
        }

        // Labels typically use dot notation and alphanumeric characters
        let valid_chars: HashSet<char> = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-".chars().collect();
        for c in label.chars() {
            if !valid_chars.contains(&c) {
                return Err(anyhow!("Label name contains invalid character: {}", c));
            }
        }

        // Must start and end with alphanumeric
        if !label.chars().next().unwrap_or('a').is_alphanumeric() {
            return Err(anyhow!("Label name must start with alphanumeric character"));
        }

        if !label.chars().last().unwrap_or('a').is_alphanumeric() {
            return Err(anyhow!("Label name must end with alphanumeric character"));
        }

        Ok(label.trim().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_email_validation() {
        // Valid emails
        assert!(AuthValidator::validate_email("user@example.com").is_ok());
        assert!(AuthValidator::validate_email("test.user+tag@example.co.uk").is_ok());
        assert!(AuthValidator::validate_email("USER@EXAMPLE.COM").is_ok()); // Case normalization

        // Invalid emails
        assert!(AuthValidator::validate_email("").is_err());
        assert!(AuthValidator::validate_email("user@").is_err());
        assert!(AuthValidator::validate_email("@example.com").is_err());
        assert!(AuthValidator::validate_email("user example.com").is_err());
        assert!(AuthValidator::validate_email("user<script>@example.com").is_err());
    }

    #[test]
    fn test_password_validation() {
        // Valid passwords
        assert!(AuthValidator::validate_password("securePassword123!").is_ok());
        assert!(AuthValidator::validate_password("MyP@ssw0rd").is_ok());

        // Invalid passwords
        assert!(AuthValidator::validate_password("").is_err());
        assert!(AuthValidator::validate_password("short").is_err());
        assert!(AuthValidator::validate_password("password").is_err()); // Too common
        assert!(AuthValidator::validate_password("12345678").is_err()); // Too common
    }

    #[test]
    fn test_environment_slug_validation() {
        // Valid slugs
        assert!(AuthValidator::validate_environment_slug("production").is_ok());
        assert!(AuthValidator::validate_environment_slug("staging-env").is_ok());
        assert!(AuthValidator::validate_environment_slug("dev_123").is_ok());

        // Invalid slugs
        assert!(AuthValidator::validate_environment_slug("").is_err());
        assert!(AuthValidator::validate_environment_slug("prod env").is_err()); // Space
        assert!(AuthValidator::validate_environment_slug("prod@env").is_err()); // Invalid char
        assert!(AuthValidator::validate_environment_slug("-invalid").is_err()); // Starts with hyphen
    }
}