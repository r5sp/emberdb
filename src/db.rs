//! The public database handle: write path, read path, recovery and background work.
//!
//! Concurrency model:
//!
//! * `state` (an `RwLock`) guards the active memtable, its WAL, the immutable memtable
//!   awaiting flush, and the current `Arc<Version>`. Writers hold it exclusively for the
//!   duration of a WAL append and memtable insert. Readers hold it shared only long
//!   enough to check the memtable and clone `Arc`s; SSTable I/O happens lock-free.
//! * `flush_lock` serialises memtable flushes; the compaction mutex serialises
//!   compactions. A flush and a compaction may run concurrently: each builds its output
//!   outside the state lock and then installs a version edit (add/remove specific
//!   tables) under it, so the edits commute.
//! * Flushes and compactions run on a background thread, or inline on the writing thread
//!   when `Options::background_compaction` is false.

use std::collections::HashSet;
use std::fmt;
use std::fs::{self, File, TryLockError};
use std::io;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread::JoinHandle;

use crate::compaction::{self, Compaction};
use crate::error::{Error, Result};
use crate::iterator::{BoxedIter, MergeIterator};
use crate::manifest::{
    read_manifest, sync_dir, write_manifest, FileMeta, ManifestState, MANIFEST_TMP_FILE,
};
use crate::memtable::Memtable;
use crate::options::Options;
use crate::sstable::TableBuilder;
use crate::types::Value;
use crate::version::{log_path, table_path, TableHandle, Version};
use crate::wal::{self, WalWriter};

const LOCK_FILE: &str = "LOCK";

/// A handle to an open database.
///
/// `Db` is `Send + Sync`; share it between threads with an `Arc<Db>`. Dropping the handle
/// stops the background thread (waiting for any in-progress flush or compaction) and
/// syncs the write-ahead log.
pub struct Db {
    inner: Arc<Inner>,
    worker: Option<Worker>,
}

struct Worker {
    tx: Sender<Msg>,
    handle: JoinHandle<()>,
}

enum Msg {
    Work,
    Shutdown,
}

struct Immutable {
    mem: Arc<Memtable>,
    wal_number: u64,
}

struct State {
    mem: Memtable,
    wal: WalWriter,
    wal_number: u64,
    imm: Option<Immutable>,
    version: Arc<Version>,
}

impl State {
    /// Oldest log that may still hold writes not yet in an SSTable.
    fn log_number(&self) -> u64 {
        self.imm.as_ref().map_or(self.wal_number, |i| i.wal_number)
    }
}

#[derive(Default)]
struct Counters {
    user_bytes: AtomicU64,
    wal_bytes: AtomicU64,
    flush_bytes: AtomicU64,
    compaction_bytes: AtomicU64,
    flushes: AtomicU64,
    compactions: AtomicU64,
    trivial_moves: AtomicU64,
}

struct Inner {
    dir: PathBuf,
    opts: Options,
    state: RwLock<State>,
    next_file: AtomicU64,
    flush_lock: Mutex<()>,
    /// Per-level round-robin compaction pointers. Holding this mutex also serialises
    /// compactions.
    compaction: Mutex<Vec<Vec<u8>>>,
    bg_error: Mutex<Option<String>>,
    counters: Counters,
    /// Held for the lifetime of the database to keep other processes out.
    _lock: File,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Db {
    /// Opens (or creates) the database in directory `path`, replaying any write-ahead
    /// logs left by a previous process.
    pub fn open(path: impl AsRef<Path>, opts: Options) -> Result<Db> {
        opts.validate()?;
        let dir = path.as_ref().to_path_buf();
        if !dir.exists() {
            if !opts.create_if_missing {
                return Err(Error::InvalidArgument(format!(
                    "{} does not exist and create_if_missing is false",
                    dir.display()
                )));
            }
            fs::create_dir_all(&dir)?;
        }

        let lock_file = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join(LOCK_FILE))?;
        lock_file.try_lock().map_err(|e| match e {
            TryLockError::WouldBlock => Error::InvalidArgument(format!(
                "{} is already open in another handle or process",
                dir.display()
            )),
            TryLockError::Error(e) => Error::Io(e),
        })?;

        let manifest =
            read_manifest(&dir)?.unwrap_or_else(|| ManifestState::empty(opts.num_levels));
        if manifest
            .levels
            .iter()
            .skip(opts.num_levels)
            .any(|l| !l.is_empty())
        {
            return Err(Error::InvalidArgument(format!(
                "database has {} levels but num_levels is {}",
                manifest.levels.len(),
                opts.num_levels
            )));
        }
        let mut added = Vec::new();
        for (level, files) in manifest.levels.iter().enumerate().take(opts.num_levels) {
            for meta in files {
                added.push((level, TableHandle::open(&dir, meta.clone())?));
            }
        }
        let mut version = Version::new(opts.num_levels).apply(&HashSet::new(), added);

        // Find logs that may hold unflushed writes, and the highest file number in use.
        let mut logs = Vec::new();
        let mut max_number = 0;
        for (number, kind) in list_files(&dir)? {
            max_number = max_number.max(number);
            if kind == FileKind::Log && number >= manifest.log_number {
                logs.push(number);
            }
        }
        logs.sort_unstable();
        let next_file = AtomicU64::new(manifest.next_file.max(max_number + 1));

        let mut recovered = Memtable::new();
        for &number in &logs {
            wal::replay(&log_path(&dir, number), |k, v| recovered.insert(k, v))?;
        }
        let counters = Counters::default();
        // Flush recovered writes straight to L0 so the old logs (and any torn tail) can be
        // discarded instead of carried forward.
        if !recovered.is_empty() {
            let table = write_level0_table(&dir, &opts, &next_file, &recovered)?;
            counters
                .flush_bytes
                .fetch_add(table.meta.file_size, Ordering::Relaxed);
            version = version.apply(&HashSet::new(), vec![(0, table)]);
        }

        let wal_number = next_file.fetch_add(1, Ordering::SeqCst);
        let wal = WalWriter::create(&log_path(&dir, wal_number), opts.sync_writes)?;
        write_manifest(
            &dir,
            &ManifestState {
                next_file: next_file.load(Ordering::SeqCst),
                log_number: wal_number,
                levels: version.manifest_levels(),
            },
        )?;
        remove_obsolete_files(&dir, &version.live_files(), wal_number)?;

        let inner = Arc::new(Inner {
            state: RwLock::new(State {
                mem: Memtable::new(),
                wal,
                wal_number,
                imm: None,
                version: Arc::new(version),
            }),
            next_file,
            flush_lock: Mutex::new(()),
            compaction: Mutex::new(vec![Vec::new(); opts.num_levels]),
            bg_error: Mutex::new(None),
            counters,
            _lock: lock_file,
            dir,
            opts,
        });

        let worker = if inner.opts.background_compaction {
            let (tx, rx) = mpsc::channel();
            let worker_inner = inner.clone();
            let handle = std::thread::Builder::new()
                .name("emberdb-compaction".into())
                .spawn(move || worker_loop(worker_inner, rx))?;
            Some(Worker { tx, handle })
        } else {
            None
        };
        let db = Db { inner, worker };
        // The reopened tree may already be over a compaction threshold.
        db.schedule_background_work();
        Ok(db)
    }

    /// Inserts or overwrites `key`.
    pub fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        let needs_work = self
            .inner
            .write(key.as_ref(), Value::Put(value.as_ref().to_vec()))?;
        if needs_work {
            self.schedule_background_work();
        }
        Ok(())
    }

    /// Deletes `key`. Deleting a missing key is not an error.
    pub fn delete(&self, key: impl AsRef<[u8]>) -> Result<()> {
        let needs_work = self.inner.write(key.as_ref(), Value::Delete)?;
        if needs_work {
            self.schedule_background_work();
        }
        Ok(())
    }

    /// Returns the current value of `key`, if any.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        let (imm, version) = {
            let st = self.inner.state_read();
            if let Some(v) = st.mem.get(key) {
                return Ok(v.clone().into_option());
            }
            (st.imm.as_ref().map(|i| i.mem.clone()), st.version.clone())
        };
        if let Some(v) = imm.as_ref().and_then(|m| m.get(key)) {
            return Ok(v.clone().into_option());
        }
        Ok(version.get(key)?.and_then(Value::into_option))
    }

    /// Returns an iterator over the live key/value pairs in `range`, in key order.
    ///
    /// Any key type that is `AsRef<[u8]>` can bound the range: `"a".."m"`,
    /// `b"a".as_slice()..`, `start_vec..=end_vec`. Use [`Db::iter`] for a full scan.
    ///
    /// The iterator observes a consistent snapshot of the tables and memtables taken when
    /// `scan` is called; later writes are not visible to it. Memtable entries in the range
    /// are copied up front, SSTable entries are read lazily.
    ///
    /// ```
    /// # let dir = tempfile::tempdir().unwrap();
    /// let db = emberdb::Db::open(dir.path(), emberdb::Options::default())?;
    /// db.put("apple", "1")?;
    /// db.put("banana", "2")?;
    /// db.put("cherry", "3")?;
    /// let keys: Vec<Vec<u8>> = db
    ///     .scan("b"..)?
    ///     .map(|kv| kv.map(|(k, _)| k))
    ///     .collect::<emberdb::Result<_>>()?;
    /// assert_eq!(keys, vec![b"banana".to_vec(), b"cherry".to_vec()]);
    /// # Ok::<(), emberdb::Error>(())
    /// ```
    pub fn scan<K, R>(&self, range: R) -> Result<DbIterator>
    where
        K: AsRef<[u8]>,
        R: RangeBounds<K>,
    {
        let start = range.start_bound().map(|k| k.as_ref().to_vec());
        let end = range.end_bound().map(|k| k.as_ref().to_vec());
        if range_is_empty(&start, &end) {
            return Ok(DbIterator::empty());
        }
        let (start_ref, end_ref) = (bound_ref(&start), bound_ref(&end));

        let (mem_entries, imm_entries, version) = {
            let st = self.inner.state_read();
            (
                st.mem.range_entries(start_ref, end_ref),
                st.imm
                    .as_ref()
                    .map(|i| i.mem.range_entries(start_ref, end_ref)),
                st.version.clone(),
            )
        };

        // Sources newest first: memtable, immutable memtable, L0 newest-first, L1, L2, ...
        let mut sources: Vec<BoxedIter> = vec![Box::new(mem_entries.into_iter().map(Ok))];
        if let Some(entries) = imm_entries {
            sources.push(Box::new(entries.into_iter().map(Ok)));
        }
        let seek = match &start {
            Bound::Included(k) | Bound::Excluded(k) => Some(k.clone()),
            Bound::Unbounded => None,
        };
        for t in version.overlapping(0, start_ref, end_ref) {
            sources.push(Box::new(t.table.iter(seek.clone())));
        }
        for level in 1..version.levels.len() {
            let tables = version.overlapping(level, start_ref, end_ref);
            if !tables.is_empty() {
                let seek = seek.clone();
                sources.push(Box::new(
                    tables
                        .into_iter()
                        .flat_map(move |t| t.table.iter(seek.clone())),
                ));
            }
        }
        Ok(DbIterator {
            merged: Some(MergeIterator::new(sources)),
            start,
            end,
        })
    }

    /// Iterates over every live key/value pair. Equivalent to `scan(..)`.
    pub fn iter(&self) -> Result<DbIterator> {
        self.scan::<&[u8], _>(..)
    }

    /// Flushes the active memtable to an L0 SSTable and waits for the flush to finish.
    pub fn flush(&self) -> Result<()> {
        self.inner.check_bg_error()?;
        loop {
            let mut st = self.inner.state_write();
            if st.mem.is_empty() {
                break;
            }
            if st.imm.is_some() {
                drop(st);
                self.inner.flush_immutable()?;
                continue;
            }
            self.inner.freeze(&mut st)?;
            break;
        }
        self.inner.flush_immutable()?;
        match &self.worker {
            Some(w) => {
                let _ = w.tx.send(Msg::Work);
                Ok(())
            }
            None => self.inner.maybe_compact(),
        }
    }

    /// Flushes the memtable and merges every SSTable into the deepest level, discarding
    /// all overwritten values and tombstones. Blocks until finished.
    pub fn compact(&self) -> Result<()> {
        self.flush()?;
        self.inner.compact_all()
    }

    /// Returns a snapshot of size and I/O statistics.
    pub fn stats(&self) -> Stats {
        let st = self.inner.state_read();
        let c = &self.inner.counters;
        Stats {
            levels: (0..st.version.levels.len())
                .map(|l| LevelStats {
                    files: st.version.levels[l].len(),
                    bytes: st.version.level_bytes(l),
                })
                .collect(),
            memtable_bytes: st.mem.approximate_size(),
            user_bytes_written: c.user_bytes.load(Ordering::Relaxed),
            wal_bytes_written: c.wal_bytes.load(Ordering::Relaxed),
            flush_bytes_written: c.flush_bytes.load(Ordering::Relaxed),
            compaction_bytes_written: c.compaction_bytes.load(Ordering::Relaxed),
            flushes: c.flushes.load(Ordering::Relaxed),
            compactions: c.compactions.load(Ordering::Relaxed),
            trivial_moves: c.trivial_moves.load(Ordering::Relaxed),
        }
    }

    /// The database directory.
    pub fn path(&self) -> &Path {
        &self.inner.dir
    }

    fn schedule_background_work(&self) {
        match &self.worker {
            Some(w) => {
                let _ = w.tx.send(Msg::Work);
            }
            None => {
                // The triggering write is already durable in the WAL, so a failure here is
                // recorded and surfaced on the next write rather than failing this one.
                if let Err(e) = self.inner.background_work() {
                    self.inner.set_bg_error(&e);
                }
            }
        }
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        if let Some(w) = self.worker.take() {
            let _ = w.tx.send(Msg::Shutdown);
            let _ = w.handle.join();
        }
        let _ = self.inner.state_write().wal.sync();
    }
}

fn worker_loop(inner: Arc<Inner>, rx: Receiver<Msg>) {
    while let Ok(msg) = rx.recv() {
        let mut shutdown = matches!(msg, Msg::Shutdown);
        // Coalesce queued requests: one pass handles all pending work.
        while let Ok(msg) = rx.try_recv() {
            shutdown |= matches!(msg, Msg::Shutdown);
        }
        if shutdown {
            return;
        }
        if let Err(e) = inner.background_work() {
            inner.set_bg_error(&e);
        }
    }
}

impl Inner {
    fn state_read(&self) -> RwLockReadGuard<'_, State> {
        self.state.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn state_write(&self) -> RwLockWriteGuard<'_, State> {
        self.state.write().unwrap_or_else(PoisonError::into_inner)
    }

    fn check_bg_error(&self) -> Result<()> {
        match &*lock(&self.bg_error) {
            Some(msg) => Err(Error::Background(msg.clone())),
            None => Ok(()),
        }
    }

    fn set_bg_error(&self, e: &Error) {
        lock(&self.bg_error).get_or_insert_with(|| e.to_string());
    }

    /// Applies one write. Returns true if the memtable was frozen and a flush is needed.
    fn write(&self, key: &[u8], value: Value) -> Result<bool> {
        self.check_bg_error()?;
        let mut needs_work = false;
        loop {
            let mut st = self.state_write();
            if st.mem.approximate_size() >= self.opts.memtable_size {
                if st.imm.is_some() {
                    // The previous memtable is still being flushed. Stall this writer and
                    // help finish it rather than letting memory grow without bound.
                    drop(st);
                    self.flush_immutable()?;
                    continue;
                }
                self.freeze(&mut st)?;
                needs_work = true;
            }
            let written = match st.wal.append(key, &value) {
                Ok(n) => n,
                Err(e) => {
                    // A partial append would leave garbage mid-log that hides every later
                    // record on replay, so stop accepting writes.
                    self.set_bg_error(&e);
                    return Err(e);
                }
            };
            self.counters
                .wal_bytes
                .fetch_add(written as u64, Ordering::Relaxed);
            self.counters.user_bytes.fetch_add(
                (key.len() + value.payload().len()) as u64,
                Ordering::Relaxed,
            );
            st.mem.insert(key.to_vec(), value);
            return Ok(needs_work);
        }
    }

    /// Makes the active memtable immutable and starts a new memtable and WAL.
    fn freeze(&self, st: &mut State) -> Result<()> {
        debug_assert!(st.imm.is_none());
        let number = self.next_file.fetch_add(1, Ordering::SeqCst);
        let wal = WalWriter::create(&log_path(&self.dir, number), self.opts.sync_writes)?;
        if self.opts.sync_writes {
            sync_dir(&self.dir)?;
        }
        let old_wal_number = std::mem::replace(&mut st.wal_number, number);
        st.wal = wal;
        st.imm = Some(Immutable {
            mem: Arc::new(std::mem::take(&mut st.mem)),
            wal_number: old_wal_number,
        });
        Ok(())
    }

    fn background_work(&self) -> Result<()> {
        self.flush_immutable()?;
        self.maybe_compact()
    }

    fn persist(&self, version: &Version, log_number: u64) -> Result<()> {
        write_manifest(
            &self.dir,
            &ManifestState {
                next_file: self.next_file.load(Ordering::SeqCst),
                log_number,
                levels: version.manifest_levels(),
            },
        )
    }

    /// Writes the immutable memtable (if any) to an L0 table and installs it.
    fn flush_immutable(&self) -> Result<()> {
        let _guard = lock(&self.flush_lock);
        let (mem, wal_number) = match &self.state_read().imm {
            Some(imm) => (imm.mem.clone(), imm.wal_number),
            None => return Ok(()),
        };
        let mut added = Vec::new();
        if !mem.is_empty() {
            let table = write_level0_table(&self.dir, &self.opts, &self.next_file, &mem)?;
            self.counters
                .flush_bytes
                .fetch_add(table.meta.file_size, Ordering::Relaxed);
            added.push((0, table));
        }
        {
            let mut st = self.state_write();
            let version = st.version.apply(&HashSet::new(), added);
            // Once this manifest is durable, the immutable memtable's log is obsolete.
            self.persist(&version, st.wal_number)?;
            st.version = Arc::new(version);
            st.imm = None;
        }
        self.counters.flushes.fetch_add(1, Ordering::Relaxed);
        remove_if_exists(&log_path(&self.dir, wal_number))
    }

    /// Runs compactions until every level is within its budget.
    fn maybe_compact(&self) -> Result<()> {
        let mut pointers = lock(&self.compaction);
        loop {
            let version = self.state_read().version.clone();
            let Some(c) = compaction::pick(&version, &self.opts, &mut pointers) else {
                return Ok(());
            };
            self.run_compaction(c)?;
        }
    }

    fn compact_all(&self) -> Result<()> {
        let _pointers = lock(&self.compaction);
        let version = self.state_read().version.clone();
        match compaction::full(&version) {
            Some(c) => self.run_compaction(c),
            None => Ok(()),
        }
    }

    fn run_compaction(&self, c: Compaction) -> Result<()> {
        let removed: HashSet<u64> = c.inputs().map(|t| t.meta.number).collect();
        if c.is_trivial_move() {
            let table = c.runs[0][0].clone();
            self.install(&removed, vec![(c.output_level, table)])?;
            self.counters.trivial_moves.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        let outputs = compaction::execute(&c, &self.dir, &self.opts, &self.next_file)?;
        let written: u64 = outputs.iter().map(|t| t.meta.file_size).sum();
        self.install(
            &removed,
            outputs.into_iter().map(|t| (c.output_level, t)).collect(),
        )?;
        self.counters
            .compaction_bytes
            .fetch_add(written, Ordering::Relaxed);
        self.counters.compactions.fetch_add(1, Ordering::Relaxed);
        // Inputs are unreferenced once the new manifest is durable. Readers that still hold
        // an old Version keep their open file descriptors, which remain valid after unlink.
        for number in removed {
            remove_if_exists(&table_path(&self.dir, number))?;
        }
        Ok(())
    }

    fn install(&self, removed: &HashSet<u64>, added: Vec<(usize, Arc<TableHandle>)>) -> Result<()> {
        let mut st = self.state_write();
        let version = st.version.apply(removed, added);
        self.persist(&version, st.log_number())?;
        st.version = Arc::new(version);
        Ok(())
    }
}

fn write_level0_table(
    dir: &Path,
    opts: &Options,
    next_file: &AtomicU64,
    mem: &Memtable,
) -> Result<Arc<TableHandle>> {
    let number = next_file.fetch_add(1, Ordering::SeqCst);
    let mut builder = TableBuilder::create(&table_path(dir, number), opts)?;
    for (k, v) in mem.iter() {
        builder.add(k, v)?;
    }
    let info = builder.finish()?;
    TableHandle::open(
        dir,
        FileMeta {
            number,
            file_size: info.file_size,
            num_entries: info.num_entries,
            smallest: info.smallest,
            largest: info.largest,
        },
    )
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum FileKind {
    Log,
    Table,
}

fn parse_file_name(name: &str) -> Option<(u64, FileKind)> {
    let (stem, ext) = name.split_once('.')?;
    if stem.is_empty() || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let number = stem.parse().ok()?;
    match ext {
        "log" => Some((number, FileKind::Log)),
        "sst" => Some((number, FileKind::Table)),
        _ => None,
    }
}

fn list_files(dir: &Path) -> Result<Vec<(u64, FileKind)>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let name = entry?.file_name();
        if let Some(parsed) = name.to_str().and_then(parse_file_name) {
            out.push(parsed);
        }
    }
    Ok(out)
}

/// Deletes logs older than `log_number`, tables not referenced by the manifest, and any
/// manifest left half-written by a crash.
fn remove_obsolete_files(dir: &Path, live: &HashSet<u64>, log_number: u64) -> Result<()> {
    for (number, kind) in list_files(dir)? {
        let obsolete = match kind {
            FileKind::Log => number < log_number,
            FileKind::Table => !live.contains(&number),
        };
        if obsolete {
            let path = match kind {
                FileKind::Log => log_path(dir, number),
                FileKind::Table => table_path(dir, number),
            };
            remove_if_exists(&path)?;
        }
    }
    remove_if_exists(&dir.join(MANIFEST_TMP_FILE))
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

fn bound_ref(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Bound::Included(k) => Bound::Included(k.as_slice()),
        Bound::Excluded(k) => Bound::Excluded(k.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn range_is_empty(start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>) -> bool {
    match (start, end) {
        (Bound::Included(s), Bound::Included(e)) => s > e,
        (Bound::Included(s), Bound::Excluded(e))
        | (Bound::Excluded(s), Bound::Included(e))
        | (Bound::Excluded(s), Bound::Excluded(e)) => s >= e,
        _ => false,
    }
}

/// Iterator returned by [`Db::scan`]. Yields `(key, value)` pairs in ascending key order;
/// an I/O or corruption error is yielded once and ends the iteration.
pub struct DbIterator {
    merged: Option<MergeIterator>,
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
}

impl DbIterator {
    fn empty() -> Self {
        DbIterator {
            merged: None,
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }
}

impl Iterator for DbIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (key, value) = match self.merged.as_mut()?.next() {
                None => {
                    self.merged = None;
                    return None;
                }
                Some(Err(e)) => {
                    self.merged = None;
                    return Some(Err(e));
                }
                Some(Ok(entry)) => entry,
            };
            let past_end = match &self.end {
                Bound::Included(e) => key > *e,
                Bound::Excluded(e) => key >= *e,
                Bound::Unbounded => false,
            };
            if past_end {
                self.merged = None;
                return None;
            }
            if matches!(&self.start, Bound::Excluded(s) if key == *s) {
                continue;
            }
            match value {
                Value::Put(v) => return Some(Ok((key, v))),
                Value::Delete => continue,
            }
        }
    }
}

/// Per-level size information.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LevelStats {
    /// Number of SSTables in the level.
    pub files: usize,
    /// Total size of those SSTables in bytes.
    pub bytes: u64,
}

/// Statistics returned by [`Db::stats`]. Byte counters are cumulative since `open`.
#[derive(Clone, Debug)]
pub struct Stats {
    /// Size of each level, L0 first.
    pub levels: Vec<LevelStats>,
    /// Approximate size of the active memtable.
    pub memtable_bytes: usize,
    /// Key and value bytes passed to `put`/`delete`.
    pub user_bytes_written: u64,
    /// Bytes appended to write-ahead logs.
    pub wal_bytes_written: u64,
    /// Bytes of L0 tables written by memtable flushes.
    pub flush_bytes_written: u64,
    /// Bytes of tables written by compactions.
    pub compaction_bytes_written: u64,
    /// Number of memtable flushes.
    pub flushes: u64,
    /// Number of compactions that rewrote data.
    pub compactions: u64,
    /// Number of compactions satisfied by moving a table to the next level unchanged.
    pub trivial_moves: u64,
}

impl Stats {
    /// Total bytes written to disk (WAL + flushes + compactions) per user byte.
    pub fn write_amplification(&self) -> f64 {
        if self.user_bytes_written == 0 {
            return 0.0;
        }
        (self.wal_bytes_written + self.flush_bytes_written + self.compaction_bytes_written) as f64
            / self.user_bytes_written as f64
    }
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "level  files        bytes")?;
        for (i, l) in self.levels.iter().enumerate() {
            if l.files > 0 {
                writeln!(f, "L{i:<5} {:>5} {:>12}", l.files, l.bytes)?;
            }
        }
        writeln!(f, "memtable bytes:    {}", self.memtable_bytes)?;
        writeln!(f, "user bytes:        {}", self.user_bytes_written)?;
        writeln!(f, "wal bytes:         {}", self.wal_bytes_written)?;
        writeln!(f, "flush bytes:       {}", self.flush_bytes_written)?;
        writeln!(f, "compaction bytes:  {}", self.compaction_bytes_written)?;
        writeln!(
            f,
            "flushes / compactions / trivial moves: {} / {} / {}",
            self.flushes, self.compactions, self.trivial_moves
        )?;
        write!(f, "write amplification: {:.2}", self.write_amplification())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_only_engine_file_names() {
        assert_eq!(parse_file_name("000012.log"), Some((12, FileKind::Log)));
        assert_eq!(parse_file_name("000007.sst"), Some((7, FileKind::Table)));
        assert_eq!(parse_file_name("MANIFEST"), None);
        assert_eq!(parse_file_name("MANIFEST.tmp"), None);
        assert_eq!(parse_file_name("+1.log"), None);
        assert_eq!(parse_file_name("12.txt"), None);
        assert_eq!(parse_file_name(".log"), None);
    }

    #[test]
    fn empty_range_detection() {
        use Bound::*;
        let k = |s: &str| s.as_bytes().to_vec();
        assert!(range_is_empty(&Included(k("b")), &Included(k("a"))));
        assert!(!range_is_empty(&Included(k("a")), &Included(k("a"))));
        assert!(range_is_empty(&Included(k("a")), &Excluded(k("a"))));
        assert!(range_is_empty(&Excluded(k("a")), &Excluded(k("a"))));
        assert!(range_is_empty(&Excluded(k("a")), &Included(k("a"))));
        assert!(!range_is_empty(&Unbounded, &Excluded(k(""))));
        assert!(!range_is_empty(&Included(k("a")), &Unbounded));
    }

    #[test]
    fn db_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Db>();
        fn assert_send<T: Send>() {}
        assert_send::<DbIterator>();
    }
}
