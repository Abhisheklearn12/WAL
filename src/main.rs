// Trying to implement WAL(Write-AHead Log)
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

// WAL ensures durability: writes are logged before being applied to main storage
// This allows recovery after crashes by replaying the log
pub struct WAL {
    file: File,  // The log file on disk
    offset: u64, // Current write position in the file
}

impl WAL {
    // Opens or creates a WAL file at the given path
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .create(true) // Create if doesn't exist
            .read(true) // Need read for recovery
            .append(true) // Always append, never overwrite
            .open(path)?;

        // Get current file size to know where to append next
        let offset = file.seek(SeekFrom::End(0))?;

        Ok(WAL { file, offset })
    }

    // Writes a log entry to disk with length prefix for easy reading
    // Format: [4 bytes length][data bytes]
    pub fn append(&mut self, data: &[u8]) -> io::Result<u64> {
        let len = data.len() as u32;
        let entry_offset = self.offset;

        // Write length prefix (4 bytes, big-endian for portability)
        self.file.write_all(&len.to_be_bytes())?;

        // Write actual data
        self.file.write_all(data)?;

        // Force write to disk immediately (durability guarantee)
        // Without this, data might sit in OS buffers and be lost on crash
        self.file.sync_all()?;

        // Update our position tracker
        self.offset += 4 + data.len() as u64;

        Ok(entry_offset) // Return where this entry was written
    }

    // Reads all entries from the log (used during recovery)
    pub fn read_all(&mut self) -> io::Result<Vec<Vec<u8>>> {
        let mut entries = Vec::new();

        // Start from beginning of file
        self.file.seek(SeekFrom::Start(0))?;

        loop {
            // Read 4-byte length prefix
            let mut len_buf = [0u8; 4];
            match self.file.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let len = u32::from_be_bytes(len_buf) as usize;

            // Read entry data
            let mut data = vec![0u8; len];
            self.file.read_exact(&mut data)?;

            entries.push(data);
        }

        Ok(entries)
    }

    // Truncates the log after recovery (when entries have been applied to main storage)
    pub fn truncate(&mut self) -> io::Result<()> {
        self.file.set_len(0)?;
        self.file.sync_all()?;
        self.offset = 0;
        Ok(())
    }
}

// Example usage demonstrating crash recovery pattern
fn main() -> io::Result<()> {
    let mut wal = WAL::open("test.wal")?;

    // Phase 1: Recovery - replay any uncommitted operations
    println!("Recovering from WAL...");
    let entries = wal.read_all()?;
    for (i, entry) in entries.iter().enumerate() {
        println!(
            "Recovered entry {}: {:?}",
            i,
            String::from_utf8_lossy(entry)
        );
        // Here you would apply each entry to your main data structure, this is imp.
    }

    // Phase 2: Normal operation - log new writes
    println!("\nWriting new entries...");
    wal.append(b"SET key1 = value1")?;
    wal.append(b"SET key2 = value2")?;
    println!("Entries written and synced to disk");

    // Phase 3: After successful checkpoint, clear the log
    // (In real systems, you'd verify main storage is consistent first)
    println!("\nClearing WAL after checkpoint...");

    // You can comment this line to simulate a crash before the checkpoint (just for experimentation)
    // TESTING WAL RECOVERY:
    // 1. Comment out wal.truncate()? line
    // 2. Run: cargo build --release (only needed ONCE after code change)
    // 3. Run: cargo run --release (entries accumulate each run)
    // 4. The more you run cargo run --release, the more entries you'll see recovered
    //
    // Each run adds 2 new entries, so you'll see:
    // Run 1: 0 recovered → writes 2 → total 2 in WAL
    // Run 2: 2 recovered → writes 2 → total 4 in WAL
    // Run 3: 4 recovered → writes 2 → total 6 in WAL
    // And so on...
    // wal.truncate()?;

    Ok(())
}

// Note for myself (Key WAL properties I tried to implement):
// 1. Durability - sync_all() forces data to disk immediately
// 2. Append-only - Uses append mode, never overwrites
// 3. Recovery - read_all() replays log entries after crashes
// 4. Length-prefixed entries - Standard format for variable-length records
// 5. Truncation - Clears log after successful checkpoint
//
// How it works:
// a. Each write is logged with a 4-byte length prefix before the data
// b. sync_all() ensures data reaches physical disk (survives power loss)
// c. On restart, all entries are read back and replayed to restore state
// d. After applying entries to main storage, the log is truncated
