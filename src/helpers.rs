use std::error::Error;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use serde_json::Value;

// Helper function to read existing JSON array or create a new one
pub fn read_json_array(path: &str) -> Result<Vec<Value>, Box<dyn Error>> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(_) => return Ok(Vec::new()), // Return empty array if file doesn't exist
    };
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    let array: Vec<Value> = serde_json::from_str(&contents).unwrap_or_default();
    Ok(array)
}

// Helper function to write JSON array to file
pub fn write_json_array(path: &str, array: &[Value]) -> Result<(), Box<dyn Error>> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    let json_str = serde_json::to_string_pretty(array)?;
    file.write_all(json_str.as_bytes())?;
    Ok(())
}

/// Finds the HIPS key (coldkey) by checking files with the "68697073" prefix.
pub fn find_hips_key(keystore_path: &str) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let target_prefix = "68697073"; // "hips" in hex
    let dir_entries = fs::read_dir(keystore_path)?;

    for entry in dir_entries {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                if file_name.starts_with(target_prefix) {
                    return Ok(Some(file_name.to_string()));
                }
            }
        }
    }

    Ok(None)
}