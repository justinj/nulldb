use std::{
    collections::BTreeMap,
    error::Error,
    fmt::{self, Display, Formatter},
    io::SeekFrom,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use futures::{stream::FuturesUnordered, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter},
};

#[derive(Debug)]
enum NdbError {
    Io(std::io::Error),
    Serde(serde_json::Error),
}

impl Display for NdbError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            NdbError::Io(err) => write!(f, "IO error: {}", err),
            NdbError::Serde(err) => write!(f, "Serde error: {}", err),
        }
    }
}

impl Error for NdbError {}

impl From<std::io::Error> for NdbError {
    fn from(err: std::io::Error) -> NdbError {
        NdbError::Io(err)
    }
}

impl From<serde_json::Error> for NdbError {
    fn from(err: serde_json::Error) -> NdbError {
        NdbError::Serde(err)
    }
}

#[tokio::main]
async fn main() -> Result<(), NdbError> {
    let mut db = Db::new("db").await?;

    db.put("foo".as_bytes(), "bar".as_bytes()).await?;
    db.put("baz".as_bytes(), "qux".as_bytes()).await?;
    db.put("foo".as_bytes(), "goo".as_bytes()).await?;

    println!(
        "{:?}",
        String::from_utf8(db.get("foo".as_bytes()).await?.unwrap()).unwrap()
    );

    db.flush_memtable().await?;

    println!(
        "{:?}",
        String::from_utf8(db.get("foo".as_bytes()).await?.unwrap()).unwrap()
    );

    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Put {
    key: Vec<u8>,
    value: Vec<u8>,
}

trait Queryable {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, NdbError>;
}

#[derive(Serialize, Deserialize)]
struct SSTableMetadata {
    written_timestamp: u64,
    meta_path: PathBuf,
    data_path: PathBuf,
    index_path: PathBuf,
}

struct SSTable {
    meta: SSTableMetadata,
    data_file: File,
    index_file: File,
    index: Vec<(Vec<u8>, u64)>,
}

impl SSTable {
    async fn open(path: impl AsRef<Path>) -> Result<SSTable, NdbError> {
        let meta_path = path.as_ref().with_extension("meta");
        let mut meta_file = File::open(&meta_path).await?;
        let mut contents = String::new();
        meta_file.read_to_string(&mut contents).await?;
        let meta: SSTableMetadata = serde_json::from_str(&contents)?;

        let data_file = File::open(&meta.data_path).await?;
        let mut index_file = File::open(&meta.index_path).await?;
        let mut index_contents = String::new();
        index_file.read_to_string(&mut index_contents).await?;
        let index: Vec<(Vec<u8>, u64)> = serde_json::from_str(&index_contents)?;

        Ok(SSTable {
            meta,
            data_file,
            index_file,
            index,
        })
    }
}

impl Queryable for SSTable {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, NdbError> {
        // TODO: fix this, when you read from something too small
        let loc = match self.index.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
            Ok(i) => i,
            Err(i) => i,
        } - 1;

        let mut location = self.index[loc].1;

        let mut data_file = BufReader::new(self.data_file.try_clone().await?);

        data_file.seek(SeekFrom::Start(location)).await?;

        let file_len = self.data_file.metadata().await?.len();
        while location < file_len {
            let key_len = data_file.read_u32().await?;
            location += 4;
            let mut current_key = vec![0; key_len as usize];
            data_file.read_exact(&mut current_key).await?;
            location += key_len as u64;

            let value_len = data_file.read_u32().await?;
            location += 4;
            let mut value = vec![0; value_len as usize];
            data_file.read_exact(&mut value).await?;
            location += value_len as u64;

            println!("at: {:?} {:?}", current_key, value);
            println!("seeking: {:?}", key);

            if current_key == key {
                return Ok(Some(value));
            } else if current_key.as_slice() > key {
                break;
            }
        }

        Ok(None)
    }
}

impl PartialOrd for SSTable {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(
            self.meta
                .written_timestamp
                .cmp(&other.meta.written_timestamp)
                .reverse(),
        )
    }
}

impl PartialEq for SSTable {
    fn eq(&self, other: &Self) -> bool {
        self.meta.written_timestamp == other.meta.written_timestamp
    }
}

impl Eq for SSTable {}

impl Ord for SSTable {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl SSTable {
    // `data` must be ordered by key.
    async fn construct(
        dir: impl AsRef<Path>,
        data: impl Iterator<Item = (Vec<u8>, Vec<u8>)>,
    ) -> Result<SSTable, NdbError> {
        // Get the current unix epoch.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let data_path = dir.as_ref().join(format!("{}.sst", now));
        let index_path = dir.as_ref().join(format!("{}.idx", now));

        let mut data_file = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .open(&data_path)
                .await?,
        );

        let mut index = Vec::new();

        for (i, (key, value)) in data.enumerate() {
            let offset = data_file.seek(SeekFrom::Current(0)).await?;
            data_file.write_u32(key.len() as u32).await?;
            data_file.write_all(&key).await?;
            data_file.write_u32(value.len() as u32).await?;
            data_file.write_all(&value).await?;
            if i % 16 == 0 {
                index.push((key, offset));
            }
        }

        data_file.flush().await?;
        data_file.get_ref().sync_all().await?;

        let mut index_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&index_path)
            .await?;

        index_file
            .write_all(serde_json::to_string(&index)?.as_bytes())
            .await?;

        index_file.sync_all().await?;

        let meta_path = dir.as_ref().join(format!("{}.meta", now));
        let meta = SSTableMetadata {
            meta_path: meta_path.clone(),
            data_path,
            index_path,
            written_timestamp: now,
        };
        let mut meta_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&meta_path)
            .await?;
        meta_file
            .write_all(serde_json::to_string(&meta)?.as_bytes())
            .await?;

        Ok(SSTable {
            meta,
            data_file: data_file.into_inner(),
            index_file,
            index,
        })
    }
}

#[derive(Default)]
struct Memtable {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Queryable for Memtable {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, NdbError> {
        Ok(self.data.get(key).cloned())
    }
}

impl Memtable {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.data.insert(key, value);
    }
}

impl Memtable {
    async fn hydrate(log: &Log) -> Result<Memtable, NdbError> {
        let mut data = BTreeMap::new();
        let reader = File::open(&log.path).await?;
        let reader = BufReader::new(reader);
        let mut lines = reader.lines();
        while let Some(line) = lines.next_line().await? {
            let put: Put = serde_json::from_str(&line)?;
            data.insert(put.key, put.value);
        }

        Ok(Memtable { data })
    }
}

struct Log {
    path: PathBuf,
    log: BufWriter<File>,
}

impl Log {
    async fn open(path: impl AsRef<Path>) -> Result<Log, NdbError> {
        let log = BufWriter::new(
            OpenOptions::new()
                .append(true)
                .create(true)
                .open(&path)
                .await?,
        );
        Ok(Log {
            path: path.as_ref().to_path_buf(),
            log,
        })
    }

    async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), NdbError> {
        let put = Put {
            key: key.into(),
            value: value.into(),
        };

        let serialized = serde_json::to_string(&put)?;
        self.log.write_all(serialized.as_bytes()).await?;
        self.log.write_all(b"\n").await?;
        self.log.flush().await?;
        self.log.get_ref().sync_all().await?;

        Ok(())
    }
}

impl Queryable for Log {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, NdbError> {
        let reader = File::open(&self.path).await?;
        let reader = BufReader::new(reader);
        let mut lines = reader.lines();
        let mut result = None;
        while let Some(line) = lines.next_line().await? {
            let put: Put = serde_json::from_str(&line)?;
            if put.key == key {
                result = Some(put.value);
            }
        }

        Ok(result)
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct DbMeta {
    sstables: Vec<String>,
    wal: PathBuf,
}

struct Db {
    dir: PathBuf,
    log: Log,
    memtable: Memtable,
    sstables: Vec<SSTable>,
    meta: DbMeta,
}

impl Db {
    async fn new(db_dir: impl AsRef<Path>) -> Result<Db, NdbError> {
        if !db_dir.as_ref().exists() {
            tokio::fs::create_dir_all(&db_dir).await?;
        }
        let meta_path = db_dir.as_ref().join("meta.json");
        let meta = if meta_path.exists() {
            let mut meta_file = File::open(&meta_path).await?;
            let mut contents = String::new();
            meta_file.read_to_string(&mut contents).await?;
            serde_json::from_str(&contents)?
        } else {
            let meta = DbMeta {
                sstables: Vec::new(),
                wal: db_dir.as_ref().join("log"),
            };
            let mut meta_file = File::create(&meta_path).await?;
            meta_file
                .write_all(serde_json::to_string(&meta)?.as_bytes())
                .await?;
            meta
        };

        let log = Log::open(db_dir.as_ref().join("log")).await?;
        let memtable = Memtable::hydrate(&log).await?;
        let sstable_results: Vec<_> = meta
            .sstables
            .iter()
            .map(|path| SSTable::open(path))
            .collect::<FuturesUnordered<_>>()
            .collect()
            .await;

        let mut sstables = sstable_results.into_iter().collect::<Result<Vec<_>, _>>()?;
        sstables.sort();

        Ok(Db {
            dir: db_dir.as_ref().into(),
            log,
            memtable,
            sstables,
            meta,
        })
    }

    async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), NdbError> {
        self.log.put(key, value).await?;
        self.memtable.data.insert(key.into(), value.into());

        Ok(())
    }

    async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, NdbError> {
        if let Some(value) = self.memtable.get(key).await? {
            return Ok(Some(value));
        }
        for sstable in &self.sstables {
            if let Some(value) = sstable.get(key).await? {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    async fn update_meta(&mut self, meta: DbMeta) -> Result<(), NdbError> {
        let meta_path = self.dir.join("meta.json");
        let mut meta_file = File::create(&meta_path).await?;
        meta_file
            .write_all(serde_json::to_string(&meta)?.as_bytes())
            .await?;
        self.meta = meta;
        Ok(())
    }

    fn get_filename(&self, prefix: &str) -> String {
        format!(
            "{}-{}",
            prefix,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        )
    }

    async fn flush_memtable(&mut self) -> Result<SSTable, NdbError> {
        let data = std::mem::take(&mut self.memtable);
        let sstable = SSTable::construct("db", data.data.into_iter()).await?;
        // Start a fresh log.
        let log_path = self.dir.join(self.get_filename("log"));
        self.log = Log::open(&log_path).await?;

        let mut new_meta = self.meta.clone();
        new_meta
            .sstables
            .push(sstable.meta.meta_path.to_string_lossy().into_owned());
        new_meta.wal = log_path;
        self.update_meta(new_meta).await?;

        self.memtable = Memtable::hydrate(&self.log).await?;
        Ok(sstable)
    }
}
