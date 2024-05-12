use std::{collections::{BTreeMap, HashMap}, error::Error, fmt::{self, Display, Formatter}, hash::Hash, path::{Path, PathBuf}};

use serde::{Deserialize, Serialize};
use tokio::{fs::{File, OpenOptions}, io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt, BufReader}};

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

    db.put("foo", "bar").await?;
    db.put("baz", "qux").await?;
    db.put("foo", "goo").await?;
    db.put("foo", "bar").await?;
    db.put("baz", "qux").await?;
    db.put("foo", "goo").await?;
    for i in 0..100 {
        db.put(format!("key{}", i), format!("value{}", i)).await?;
    }

    println!("{:?}", db.get("foo").await?);

    db.flush().await?;

    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Put {
    key: String,
    value: String,
}

#[derive(Serialize, Deserialize)]
struct IndexEntry {
    key: String,
    position: u64,
}

trait Queryable {
    async fn get(&self, key: impl AsRef<str>) -> Result<Option<String>, NdbError>;
}

struct Sst {
    data_path: PathBuf,
    index_path: PathBuf,
    index: Vec<(String, u64)>,
}

// impl Sst {
//     async fn construct(name: String, data: impl Iterator<Item = (String, String)>) -> Result<Sst, NdbError> {
//         let data_path = PathBuf::from(format!("{}.data", name));
//         let index_path = PathBuf::from(format!("{}.index", name));

//         let mut data_file = OpenOptions::new().write(true).create(true).open(&data_path).await?;
//         let mut index_file = OpenOptions::new().write(true).create(true).open(&index_path).await?;

//         let mut index = HashMap::new();

//         const INDEX_SPLIT: usize = 16;
//         for (idx, (key, value)) in data.enumerate() {
//             if idx % INDEX_SPLIT == 0 {
//                 index.insert(key.clone(), data_file.seek(tokio::io::SeekFrom::Current(0)).await?);
//             }
//             let put = Put { key, value };
//             let serialized = serde_json::to_string(&put)?;
//             data_file.write_all(serialized.as_bytes()).await?;
//             data_file.write_all(b"\n").await?;
//         }

//         data_file.sync_all().await?;

//         let index_data = serde_json::to_string(&index)?;
//         index_file.write_all(index_data.to_string().as_bytes()).await?;
//         index_file.write_all(b"\n").await?;
//         index_file.sync_all().await?;


//         Ok(Sst { data_path, index_path, index })
//     }
// }

// impl Queryable for Sst {
//     async fn get(&self, key: impl AsRef<str>) -> Result<Option<String>, NdbError> {
//         let idx = self.index.get(key.as_ref());
//         let file = File::open(&self.data_path).await?;
//         file.seek(tokio::io::SeekFrom::Start(*idx.unwrap())).await?;
//     }
// }

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
    async fn get(&self, key: impl AsRef<str>) -> Result<Option<String>, NdbError> {
        Ok(self.memtable.get(key.as_ref()).map(|s| s.to_string()))
    }
}

struct Log {
    path: PathBuf,
    log: File,
}

impl Log {
    async fn open(path: impl AsRef<Path>) -> Result<Log, NdbError> {
        let log = OpenOptions::new().append(true).create(true).open(&path).await?;
        Ok(Log { path: path.as_ref().to_path_buf(), log })
    }

    async fn put(&mut self, key: impl AsRef<str>, value: impl AsRef<str>) -> Result<(), NdbError> {
        let put = Put {
            key: key.as_ref().to_string(),
            value: value.as_ref().to_string(),
        };

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
    async fn get(&self, key: impl AsRef<str>) -> Result<Option<String>, NdbError> {
        let reader = File::open(&self.path).await?;
        let reader = BufReader::new(reader);
        let mut lines = reader.lines();
        let mut result = None;
        while let Some(line) = lines.next_line().await? {
            let put: Put = serde_json::from_str(&line)?;
            if put.key == key.as_ref() {
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

    async fn put(&mut self, key: impl AsRef<str>, value: impl AsRef<str>) -> Result<(), NdbError> {
        self.log.put(key.as_ref(), value.as_ref()).await?;
        self.memtable.insert(key.as_ref().to_string(), value.as_ref().to_string());

        Ok(())
    }

    async fn get(&mut self, key: impl AsRef<str>) -> Result<Option<String>, NdbError> {
        Ok(self.memtable.get(key.as_ref()).await?)
    }

    async fn flush(&mut self) -> Result<(), NdbError> {
        let data = self.memtable.memtable.drain().collect::<Vec<_>>();
        // let sst = Sst::construct("sst".to_string(), data.into_iter()).await?;

        Ok(())
    }
}