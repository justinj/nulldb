use std::{
    collections::BTreeMap,
    error::Error,
    fmt::{self, Display, Formatter},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
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
        log.get_ref().sync_all().await?;
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
        let memtable = Memtable::hydrate(&log).await?;
        Ok(Db { log, memtable })
    }

    async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), NdbError> {
        self.log.put(key, value).await?;
        self.memtable.put(key.into(), value.into());

        Ok(())
    }

    async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, NdbError> {
        Ok(self.memtable.get(key).await?)
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
