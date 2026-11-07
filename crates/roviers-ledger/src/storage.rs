use crc32fast::Hasher;
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use memmap2::Mmap;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

const MAGIC: &[u8; 4] = b"ROV1";
const HEADER_SIZE: usize = 32;

#[derive(Error, Debug)]
pub enum LedgerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("CRC32 validation failed: expected {expected}, calculated {actual}")]
    CrcMismatch { expected: u32, actual: u32 },
    #[error("Corrupt record header at offset {0}")]
    CorruptHeader(u64),
    #[error("Decompression failed: {0}")]
    DecompressError(String),
}

#[derive(Debug, Clone)]
pub struct LedgerRecord {
    pub offset: u64,
    pub timestamp: i64,
    pub topic: String,
    pub key: String,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
pub struct SegmentInfo {
    pub base_offset: u64,
    pub end_offset: u64,
    pub path: PathBuf,
}

pub struct Ledger {
    dir: PathBuf,
    segment_max_bytes: u64,
    active_path: PathBuf,
    active_base: u64,
    active_size: u64,
    writer: Option<BufWriter<File>>,
    segments: Vec<SegmentInfo>,
    mmaps: HashMap<PathBuf, Mmap>,
    next_offset: AtomicU64,
}

impl Ledger {
    pub fn open(dir: impl AsRef<Path>, segment_max_bytes: u64) -> Result<Self, LedgerError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let mut segments = Vec::new();
        let mut entries = fs::read_dir(&dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "log"))
            .collect::<Vec<_>>();

        entries.sort();

        let mut active_base = 0;
        let mut active_size = 0;
        let mut next_offset = 0;

        for path in &entries {
            let metadata = fs::metadata(path)?;
            let file_size = metadata.len();
            let parsed = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|stem| stem.parse::<u64>().ok());
            if let Some(base) = parsed {
                segments.push(SegmentInfo {
                    base_offset: base,
                    end_offset: base + file_size,
                    path: path.clone(),
                });
                active_base = base;
                active_size = file_size;
                next_offset = base + file_size;
            }
        }

        let active_path = if segments.is_empty() {
            let p = dir.join(format!("{:020}.log", 0));
            let mut f = File::create(&p)?;
            f.write_all(MAGIC)?;
            f.flush()?;
            active_size = 4;
            segments.push(SegmentInfo {
                base_offset: 0,
                end_offset: 4,
                path: p.clone(),
            });
            next_offset = 4;
            p
        } else {
            entries.last().unwrap().clone()
        };

        let file = OpenOptions::new().append(true).open(&active_path)?;
        let writer = BufWriter::new(file);

        let mut ledger = Self {
            dir,
            segment_max_bytes: segment_max_bytes.max(1024 * 1024),
            active_path,
            active_base,
            active_size,
            writer: Some(writer),
            segments,
            mmaps: HashMap::new(),
            next_offset: AtomicU64::new(next_offset),
        };

        ledger.reload_mmaps()?;
        Ok(ledger)
    }

    pub fn append(&mut self, topic: &str, key: &str, payload: &[u8]) -> Result<u64, LedgerError> {
        let compressed = compress_prepend_size(payload);
        let topic_bytes = topic.as_bytes();
        let key_bytes = key.as_bytes();

        let record_data_len = topic_bytes.len() + key_bytes.len() + compressed.len();
        let total_record_len = HEADER_SIZE + record_data_len;

        if self.active_size + total_record_len as u64 > self.segment_max_bytes {
            self.roll_segment()?;
        }

        let record_offset = self
            .next_offset
            .fetch_add(total_record_len as u64, Ordering::SeqCst);
        let timestamp = chrono::Utc::now().timestamp_millis();

        // Calculate CRC32 of payload and metadata
        let mut hasher = Hasher::new();
        hasher.update(&timestamp.to_le_bytes());
        hasher.update(topic_bytes);
        hasher.update(key_bytes);
        hasher.update(&compressed);
        let crc = hasher.finalize();

        let writer = self.writer.as_mut().unwrap();

        // Header: [MAGIC: 4][Offset: 8][Timestamp: 8][TopicLen: 2][KeyLen: 2][DataLen: 4][CRC: 4] = 32 bytes
        writer.write_all(MAGIC)?;
        writer.write_all(&record_offset.to_le_bytes())?;
        writer.write_all(&timestamp.to_le_bytes())?;
        writer.write_all(&(topic_bytes.len() as u16).to_le_bytes())?;
        writer.write_all(&(key_bytes.len() as u16).to_le_bytes())?;
        writer.write_all(&(compressed.len() as u32).to_le_bytes())?;
        writer.write_all(&crc.to_le_bytes())?;

        // Body
        writer.write_all(topic_bytes)?;
        writer.write_all(key_bytes)?;
        writer.write_all(&compressed)?;
        writer.flush()?;

        self.active_size += total_record_len as u64;
        if let Some(last) = self.segments.last_mut() {
            last.end_offset = self.active_base + self.active_size;
        }

        Ok(record_offset)
    }

    fn roll_segment(&mut self) -> Result<(), LedgerError> {
        if let Some(mut w) = self.writer.take() {
            w.flush()?;
        }

        let new_base = self.next_offset.load(Ordering::SeqCst);
        let new_path = self.dir.join(format!("{:020}.log", new_base));
        let mut f = File::create(&new_path)?;
        f.write_all(MAGIC)?;
        f.flush()?;

        self.active_base = new_base;
        self.active_size = 4;
        self.active_path = new_path.clone();

        self.segments.push(SegmentInfo {
            base_offset: new_base,
            end_offset: new_base + 4,
            path: new_path.clone(),
        });

        let file = OpenOptions::new().append(true).open(&new_path)?;
        self.writer = Some(BufWriter::new(file));

        self.reload_mmaps()?;
        Ok(())
    }

    pub fn reload_mmaps(&mut self) -> Result<(), LedgerError> {
        self.mmaps.clear();
        for seg in &self.segments {
            let f = File::open(&seg.path)?;
            if f.metadata()?.len() > 0 {
                let mmap = unsafe { Mmap::map(&f)? };
                self.mmaps.insert(seg.path.clone(), mmap);
            }
        }
        Ok(())
    }

    pub fn read_record(&mut self, target_offset: u64) -> Result<Option<LedgerRecord>, LedgerError> {
        // Find segment containing this offset
        let segment = match self
            .segments
            .iter()
            .find(|s| target_offset >= s.base_offset && target_offset < s.end_offset)
        {
            Some(s) => s,
            None => {
                // If not found, try reloading mmaps in case new writes occurred
                self.reload_mmaps()?;
                match self
                    .segments
                    .iter()
                    .find(|s| target_offset >= s.base_offset && target_offset < s.end_offset)
                {
                    Some(s) => s,
                    None => return Ok(None),
                }
            }
        };

        let mmap = match self.mmaps.get(&segment.path) {
            Some(m) => m,
            None => return Ok(None),
        };

        let rel_offset = (target_offset - segment.base_offset) as usize;
        if rel_offset + HEADER_SIZE > mmap.len() {
            return Ok(None);
        }

        let header = &mmap[rel_offset..rel_offset + HEADER_SIZE];
        if &header[0..4] != MAGIC {
            return Err(LedgerError::CorruptHeader(target_offset));
        }

        let offset = u64::from_le_bytes(header[4..12].try_into().unwrap());
        let timestamp = i64::from_le_bytes(header[12..20].try_into().unwrap());
        let topic_len = u16::from_le_bytes(header[20..22].try_into().unwrap()) as usize;
        let key_len = u16::from_le_bytes(header[22..24].try_into().unwrap()) as usize;
        let data_len = u32::from_le_bytes(header[24..28].try_into().unwrap()) as usize;
        let expected_crc = u32::from_le_bytes(header[28..32].try_into().unwrap());

        let data_start = rel_offset + HEADER_SIZE;
        let data_end = data_start + topic_len + key_len + data_len;

        if data_end > mmap.len() {
            return Ok(None);
        }

        let topic_bytes = &mmap[data_start..data_start + topic_len];
        let key_bytes = &mmap[data_start + topic_len..data_start + topic_len + key_len];
        let compressed_payload = &mmap[data_start + topic_len + key_len..data_end];

        // Validate CRC32
        let mut hasher = Hasher::new();
        hasher.update(&timestamp.to_le_bytes());
        hasher.update(topic_bytes);
        hasher.update(key_bytes);
        hasher.update(compressed_payload);
        let actual_crc = hasher.finalize();

        if actual_crc != expected_crc {
            return Err(LedgerError::CrcMismatch {
                expected: expected_crc,
                actual: actual_crc,
            });
        }

        let decompressed = decompress_size_prepended(compressed_payload)
            .map_err(|e| LedgerError::DecompressError(e.to_string()))?;

        Ok(Some(LedgerRecord {
            offset,
            timestamp,
            topic: String::from_utf8_lossy(topic_bytes).into_owned(),
            key: String::from_utf8_lossy(key_bytes).into_owned(),
            payload: decompressed,
        }))
    }
}
