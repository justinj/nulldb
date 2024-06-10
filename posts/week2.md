I didn't realize how hard it would be to bin-pack episodes of this thing into the roughly ~750 word chunks that I try to keep issues of this newsletter at.
Lots of things are like, oh that's small, so I'm going to stuff it in with
something else, but then that something else is sort of big and so it's iffy to
cram something else in there too. Alas!

When we [last left our heroes](https://buttondown.email/jaffray/archive/null-bitmap-builds-a-database-1-the-log-is/) we were writing all of our writes into a log, and
serving reads by scanning the entire log to figure out what the value of a given
key should be.

Today we're going to fix that with a little fella we like to call the memtable.

There's a fairly natural solution to this which is to just keep an in-memory data structure that supports fast reads and fast writes.
This is the titular "memtable."
This thing won't actually be stored durably anywhere, but we still have the log for that.


```rust
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
```

I think of this as like, there's a bunch of boxes we need ticked, and [no man has all three](https://buttondown.email/jaffray/archive/the-three-places-for-data-in-an-lsm/):

* Fast reads,
* fast writes,
* durability.

Our log gave us fast writes (because they're just appends) and durability (because it's stored on disk).
Our memtable gives us fast reads and fast writes, but no durability.

So we have to update our database to use the memtable instead of the log.

```rust
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
        let memtable = Memtable::default();
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
```

There's another thing we have to do though, if we write data into the database, and then restart it, we need to be able to access the data from before.
This was easy before, because we just always consulted the on-disk index, but now, we need to "rehydrate" the memtable so that it contains all the data from our log.
We can do this by just replaying the entire log so that it's as though we're seeing each of its entries for the first time again.

```rust
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
```

```rust
async fn new(db_dir: impl AsRef<Path>) -> Result<Db, NdbError> {
    if !db_dir.as_ref().exists() {
        tokio::fs::create_dir_all(&db_dir).await?;
    }
    let log = Log::open(db_dir.as_ref().join("log")).await?;
    let memtable = Memtable::hydrate(&log).await?;
    Ok(Db { log, memtable })
}
```

I need to emphasize again we're skipping some important steps here in the name of expediency (and we're not even going very fast!).
If you're following along to make a database anyone actually intends to use then
you should make sure you actually know what you're doing first and consult a
source that doesn't have a graffiti-style logo.

This handling of the log-replay is actually not safe because of
the way fsync works (the way we write into the log at *all* is actually not safe, but this problem is different and subtle so I want to talk about it).

Happy path number one is something like this:

I do a write, it gets written into the log, fsync'd, then we crash.
The database comes back up, we rehydrate the log, it contains my write, and if I try to read again, I'll see it. This is great. All working as intended.

Happy path number two is something like this:

I do a write, before you're able to apply it to the log, we crash.
The database comes back up, and we rehydrate the log, it doesn't contain my
write, and so reads don't see it. This is fine, because the writer never got an
acknowledgment that the write succeeded, and so they shouldn't have taken any action assuming it had, so this is all fine.

Now here's sad path:

I do a write, and it goes into the log, and then the database crashes *before we fsync*.
We come back up, and the reader, having not gotten an acknowledgment that their write succeeded, must do a read to see if it did or not.
They do a read, and then the write, having made it to the OS's in-memory buffers, is returned.
Now the reader would be justified in believing that the write is durable: they saw it, after all.
But now we *hard crash*, and the whole server goes down, losing the contents of the file buffers.
Now the write is lost, even though we served it!

The solution is easy: just fsync the log on startup so that any reads we do are based off of data that has made it to disk:

```rust
log.get_ref().sync_all().await?;
```

