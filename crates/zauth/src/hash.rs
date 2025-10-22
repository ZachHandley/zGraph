use anyhow::{anyhow, Result};
use argon2::{password_hash::SaltString, Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use password_hash::rand_core::OsRng;

pub fn hash_password(plain: &str) -> Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    let argon = Argon2::default();
    argon
        .hash_password(plain.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|e| anyhow!("argon2 hash failure: {e}"))
}

pub fn verify_password(hash: &str, candidate: &str) -> Result<bool> {
    let parsed = PasswordHash::new(hash).map_err(|e| anyhow!("bad password hash: {e}"))?;
    Ok(Argon2::default()
        .verify_password(candidate.as_bytes(), &parsed)
        .is_ok())
}
