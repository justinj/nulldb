use std::{
    collections::HashMap,
    error::Error,
    fmt::{self, Display, Formatter},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
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

    // db.put("foo", "bar").await?;
    // db.put("baz", "qux").await?;
    // db.put("foo", "goo").await?;

    println!("{:?}", db.get("foo").await?);

    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Put {
    key: String,
    value: String,
}

trait Queryable {
    async fn get(&self, key: &str) -> Result<Option<String>, NdbError>;
}

struct Memtable {
    memtable: HashMap<String, String>,
}

impl Memtable {
    fn new(memtable: HashMap<String, String>) -> Memtable {
        Memtable { memtable }
    }

    fn insert(&mut self, key: String, value: String) {
        self.memtable.insert(key, value);
    }
}

impl Queryable for Memtable {
    async fn get(&self, key: &str) -> Result<Option<String>, NdbError> {
        Ok(self.memtable.get(key).map(|s| s.to_string()))
    }
}

struct Log {
    path: PathBuf,
    log: File,
}

impl Log {
    async fn open(path: impl AsRef<Path>) -> Result<Log, NdbError> {
        let log = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&path)
            .await?;
        Ok(Log {
            path: path.as_ref().to_path_buf(),
            log,
        })
    }

    async fn put(&mut self, key: String, value: String) -> Result<(), NdbError> {
        let put = Put { key, value };

        let serialized = serde_json::to_string(&put)?;
        self.log.write_all(serialized.as_bytes()).await?;
        self.log.write_all(b"\n").await?;
        self.log.sync_all().await?;

        Ok(())
    }

    async fn hydrate(&self) -> Result<Memtable, NdbError> {
        let reader = File::open(&self.path).await?;
        let reader = BufReader::new(reader);
        let mut lines = reader.lines();
        let mut memtable = HashMap::new();
        while let Some(line) = lines.next_line().await? {
            let put: Put = serde_json::from_str(&line)?;
            memtable.insert(put.key, put.value);
        }

        Ok(Memtable::new(memtable))
    }
}

impl Queryable for Log {
    async fn get(&self, key: &str) -> Result<Option<String>, NdbError> {
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

struct Db {
    log: Log,
    memtable: Memtable,
}

impl Db {
    async fn new(db_dir: impl AsRef<Path>) -> Result<Db, NdbError> {
        if !db_dir.as_ref().exists() {
            tokio::fs::create_dir_all(&db_dir).await?;
        }
        let log = Log::open(db_dir.as_ref().join("log")).await?;
        let memtable = log.hydrate().await?;
        Ok(Db { log, memtable })
    }

    async fn put(&mut self, key: &str, value: &str) -> Result<(), NdbError> {
        self.log.put(key.into(), value.into()).await?;
        self.memtable.insert(key.into(), value.into());

        Ok(())
    }

    async fn get(&mut self, key: &str) -> Result<Option<String>, NdbError> {
        Ok(self.memtable.get(key.as_ref()).await?)
    }
}
