use argon2::{Argon2, Params};
use chacha20poly1305::{
    AeadCore, ChaCha20Poly1305, Key,
    aead::{Aead, KeyInit, OsRng},
};

const MAGIC: &[u8; 8] = b"MONODB01";

pub fn encrypt_file(master_key: &[u8; 32], plaintext: &[u8]) -> Vec<u8> {
    let cipher = ChaCha20Poly1305::new(Key::from_slice(master_key));

    let nonce = ChaCha20Poly1305::generate_nonce(&mut OsRng);

    let ciphertext = cipher
        .encrypt(&nonce, plaintext)
        .expect("encryption failed");

    let mut out = Vec::with_capacity(MAGIC.len() + nonce.len() + ciphertext.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ciphertext);
    out
}

pub fn decrypt_file(master_key: &[u8; 32], data: &[u8]) -> Option<Vec<u8>> {
    if data.len() < MAGIC.len() + 12 {
        return None;
    }
    if &data[..MAGIC.len()] != MAGIC {
        return None;
    }

    let cipher = ChaCha20Poly1305::new(Key::from_slice(master_key));
    let nonce = &data[MAGIC.len()..MAGIC.len() + 12];
    let ciphertext = &data[MAGIC.len() + 12..];

    cipher.decrypt(nonce.into(), ciphertext).ok()
}

pub fn derive_key_from_password(password: &str, out_key: &mut [u8; 32]) {
    const SALT: &[u8] = b"user-db-file-key-v1";

    let params = Params::new(
        64 * 1024, // m_cost
        3,         // t_cost
        1,         // p_cost
        Some(32),  // output length
    )
    .expect("valid params");

    let argon2 = Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);

    argon2
        .hash_password_into(password.as_bytes(), SALT, out_key)
        .expect("key derivation failed");
}
