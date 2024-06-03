use anyhow::Result;
use async_trait::async_trait;
use lolraft::process::*;
use redb::{Database, ReadableTable, TableDefinition};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

mod entry {
    use lolraft::process::Entry;
    #[derive(serde::Deserialize, serde::Serialize)]
    struct OnDiskStruct {}
    pub fn ser(x: Entry) -> Vec<u8> {
        todo!()
    }
    pub fn desr(bin: &[u8]) -> Entry {
        todo!()
    }
}

struct LazyEntry {
    inner: Entry,
    space: String,
    notifier: oneshot::Sender<()>,
}
struct Reaper {
    db: Arc<Database>,
    recv: mpsc::Receiver<LazyEntry>,
}
impl Reaper {
    fn table_def(space: &str) -> TableDefinition<u64, Vec<u8>> {
        TableDefinition::new(space)
    }
    fn reap(&self, du: Duration) -> Result<()> {
        let deadline = Instant::now() + du;
        let mut notifiers = vec![];
        let tx = self.db.begin_write()?;
        while let Ok(e) = self.recv.recv_timeout(deadline - Instant::now()) {
            let mut tbl = tx.open_table(Self::table_def(&e.space))?;
            tbl.insert(0, entry::ser(e.inner))?;
            notifiers.push(e.notifier);
        }
        tx.commit()?;

        for notifier in notifiers {
            notifier.send(()).ok();
        }
        Ok(())
    }
}

struct LogStore {
    db: Arc<Database>,
    space: String,
    reaper_queue: mpsc::Sender<LazyEntry>,
    insert_lazy: bool,
}
impl LogStore {
    fn table_def(&self) -> TableDefinition<u64, Vec<u8>> {
        TableDefinition::new(&self.space)
    }
    async fn insert_entry_immediate(&self, i: Index, e: Entry) -> Result<()> {
        let tx = self.db.begin_write()?;
        {
            let mut tbl = tx.open_table(self.table_def())?;
            tbl.insert(i, entry::ser(e))?;
        }
        tx.commit()?;
        Ok(())
    }
    async fn insert_entry_lazy(&self, i: Index, e: Entry) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let e = LazyEntry {
            inner: e,
            space: self.space.clone(),
            notifier: tx,
        };
        self.reaper_queue.send(e).ok();
        Ok(())
    }
}
#[async_trait]
impl RaftLogStore for LogStore {
    async fn insert_entry(&self, i: Index, e: Entry) -> Result<()> {
        if self.insert_lazy {
            self.insert_entry_lazy(i, e).await?;
        } else {
            self.insert_entry_immediate(i, e).await?;
        }
        Ok(())
    }
    async fn delete_entries_before(&self, i: Index) -> Result<()> {
        let tx = self.db.begin_write()?;
        {
            let mut tbl = tx.open_table(self.table_def())?;
            tbl.retain(|k, _| k >= i)?;
        }
        tx.commit()?;
        Ok(())
    }
    async fn get_entry(&self, i: Index) -> Result<Option<Entry>> {
        let tx = self.db.begin_read()?;
        let tbl = tx.open_table(self.table_def())?;
        match tbl.get(i)? {
            Some(bin) => Ok(Some(entry::desr(&bin.value()))),
            None => Ok(None),
        }
    }
    async fn get_head_index(&self) -> Result<Index> {
        let tx = self.db.begin_read()?;
        let tbl = tx.open_table(self.table_def())?;
        let out = tbl.first()?;
        Ok(match out {
            Some((k, v)) => k.value(),
            None => 0,
        })
    }
    async fn get_last_index(&self) -> Result<Index> {
        let tx = self.db.begin_read()?;
        let tbl = tx.open_table(self.table_def())?;
        let out = tbl.last()?;
        Ok(match out {
            Some((k, v)) => k.value(),
            None => 0,
        })
    }
}

mod ballot {
    use lolraft::process::Ballot;
    #[derive(serde::Deserialize, serde::Serialize)]
    struct OnDiskStruct {}
    pub fn ser(x: Ballot) -> Vec<u8> {
        todo!()
    }
    pub fn desr(bin: &[u8]) -> Ballot {
        todo!()
    }
}

struct BallotStore {
    db: Arc<Database>,
    space: String,
}
impl BallotStore {
    fn table_def(&self) -> TableDefinition<(), Vec<u8>> {
        TableDefinition::new(&self.space)
    }
}
#[async_trait]
impl RaftBallotStore for BallotStore {
    async fn save_ballot(&self, ballot: Ballot) -> Result<()> {
        let tx = self.db.begin_write()?;
        {
            let mut tbl = tx.open_table(self.table_def())?;
            tbl.insert((), ballot::ser(ballot))?;
        }
        tx.commit()?;
        Ok(())
    }
    async fn load_ballot(&self) -> Result<Ballot> {
        let tx = self.db.begin_read()?;
        let tbl = tx.open_table(self.table_def())?;
        match tbl.get(())? {
            Some(bin) => Ok(ballot::desr(&bin.value())),
            None => Err(anyhow::anyhow!("No ballot")),
        }
    }
}

pub fn new(
    db: redb::Database,
    lane_id: u32,
    insert_lazy: bool,
) -> (impl RaftLogStore, impl RaftBallotStore) {
    let db = Arc::new(db);
    let (tx, rx) = mpsc::channel();
    let log = LogStore {
        space: format!("log-{lane_id}"),
        db: db.clone(),
        reaper_queue: tx,
        insert_lazy,
    };
    let ballot = BallotStore {
        space: format!("ballot-{lane_id}"),
        db: db.clone(),
    };
    (log, ballot)
}
