use std::mem;

use anyhow::Result;
use argon2::Argon2;
use fake::{Fake, faker};
use get_size::GetSize;
use inquire::Password;
use monodb_client::ClientBuilder;
use monodb_common::Value;
use serde::{Deserialize, Serialize};

use crate::utils::{get_namespaces, hash_password, value_heap_size};
use indicatif::{ProgressBar, ProgressStyle};

mod db;
mod encrypt;
mod utils;

const TARGET_SIZE: u64 = 100 * 1024 * 1024; // 100MB

#[derive(Debug, Serialize, Deserialize)]
pub struct UserEntry {
    pub id: Value,            // u64
    pub first_name: Value,    // String
    pub last_name: Value,     // String
    pub email: Value,         // String
    pub password_hash: Value, // String
}

impl GetSize for UserEntry {
    fn get_heap_size(&self) -> usize {
        value_heap_size(&self.id)
            + value_heap_size(&self.first_name)
            + value_heap_size(&self.last_name)
            + value_heap_size(&self.email)
            + value_heap_size(&self.password_hash)
    }

    fn get_size(&self) -> usize {
        mem::size_of::<Self>() + self.get_heap_size()
    }

    fn get_stack_size() -> usize {
        mem::size_of::<Self>()
    }
}

#[derive(Debug)]
pub struct EnvironmentEntry {
    pub measured_at: Value, // UNIX timestamp (u64)
    pub temperature: Value, // Degrees Celsius (f64)
    pub humidity: Value,    // RH% (f64)
}

impl GetSize for EnvironmentEntry {
    fn get_heap_size(&self) -> usize {
        value_heap_size(&self.measured_at)
            + value_heap_size(&self.temperature)
            + value_heap_size(&self.humidity)
    }

    fn get_size(&self) -> usize {
        mem::size_of::<Self>() + self.get_heap_size()
    }

    fn get_stack_size() -> usize {
        mem::size_of::<Self>()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let file_exists = std::fs::metadata("user_data.enc").is_ok();

    let mut client = ClientBuilder::new("127.0.0.1:6432")
        .max_connections(10)
        .build()
        .await?;

    db::create_tables(&mut client)
        .await
        .expect("failed to create tables");

    if file_exists {
        println!("Encrypted user data file 'user_data.enc' found.");
        let encryption_password = Password::new("Enter encryption password for client")
            .without_confirmation()
            .prompt()?;

        let mut master_key = [0u8; 32];
        encrypt::derive_key_from_password(&encryption_password, &mut master_key);

        let encrypted_data = std::fs::read("user_data.enc")?;
        let decrypted_data = encrypt::decrypt_file(&master_key, &encrypted_data)
            .ok_or_else(|| anyhow::anyhow!("Failed to decrypt data. Incorrect password?"))?;

        let loaded: Vec<UserEntry> = serde_json::from_slice(&decrypted_data)?;
        println!("Loaded {} user records from encrypted file.", loaded.len());
        for record in &loaded {
            db::insert_user(&mut client, record).await.unwrap();
        }
        return Ok(());
    }

    let namespaces = get_namespaces(&client).await?;

    for ns in namespaces {
        println!("{}:", ns.name);
        for table in ns.tables {
            println!("  - {}", table.name);
        }
    }

    println!();

    let params = argon2::Params::new(4 * 1024, 1, 1, None).expect("valid params");
    let argon2 = Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);
    let sample_record = UserEntry {
        id: Value::Int64(1),
        first_name: Value::String(faker::name::en::FirstName().fake()),
        last_name: Value::String(faker::name::en::LastName().fake()),
        email: Value::String(faker::internet::en::SafeEmail().fake()),
        password_hash: Value::String(
            hash_password(
                &argon2,
                faker::internet::en::Password(8..16).fake::<String>(),
                1,
            )
            .await,
        ),
    };

    let size_in_memory = sample_record.get_size();
    let needed = ((TARGET_SIZE as f64 / size_in_memory as f64) / 10_000.0).ceil() * 10_000.0;
    println!(
        "Size in memory: {} B, needed records: {}",
        size_in_memory, needed
    );

    let fake_names: Vec<String> = (0..needed as u64)
        .map(|_| faker::name::en::Name().fake())
        .collect();

    let mut passwords = Vec::with_capacity(needed as usize);
    let mut password_hashes = Vec::with_capacity(needed as usize);

    // Progress bar for hashing passwords
    let pb = ProgressBar::new(needed as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{msg}\n[{elapsed_precise}] {bar:30.green/black} {percent:>3}% | {pos}/{len} | ETA {eta}",
            )
            .unwrap()
            .progress_chars("▉▊▋▌▍▎▏ "),
    );
    pb.set_message("Hashing passwords");

    for i in 0..needed as u64 {
        let password: String = faker::internet::en::Password(8..16).fake();
        let hash = hash_password(&argon2, password.clone(), i).await;
        passwords.push(password);
        password_hashes.push(hash);
        pb.inc(1);
    }
    pb.finish_with_message("Password hashing complete");

    let generated: Vec<UserEntry> = (0..needed as u64)
        .map(|i| UserEntry {
            id: Value::Int64(i.try_into().unwrap()),
            first_name: Value::String(
                fake_names[i as usize]
                    .split_whitespace()
                    .next()
                    .unwrap()
                    .to_string(),
            ),
            last_name: Value::String(
                fake_names[i as usize]
                    .split_whitespace()
                    .last()
                    .unwrap()
                    .to_string(),
            ),
            email: Value::String(format!(
                "{}.{}@example.com",
                fake_names[i as usize]
                    .split_whitespace()
                    .next()
                    .unwrap()
                    .to_lowercase(),
                fake_names[i as usize]
                    .split_whitespace()
                    .last()
                    .unwrap()
                    .to_lowercase()
            )),
            password_hash: Value::String(password_hashes[i as usize].clone()),
        })
        .collect();

    // Get actual size in memory
    println!(
        "Records take {} MB in memory",
        generated.get_size() / (1024 * 1024)
    );

    for record in &generated[0..5] {
        println!("{:?}", record);
    }

    let encryption_password =
        Password::new("Enter encryption password to store client data").prompt()?;

    client.close().await?;

    let json = serde_json::to_vec(&generated)?;
    let mut master_key = [0u8; 32];
    encrypt::derive_key_from_password(&encryption_password, &mut master_key);
    let encrypted_data = encrypt::encrypt_file(&master_key, &json);

    std::fs::write("user_data.enc", &encrypted_data)?;

    Ok(())
}
