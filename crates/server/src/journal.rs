use crate::*;
use object_store::{
    Error as ObjectStoreError, ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion,
    aws::{AmazonS3Builder, S3ConditionalPut},
    path::Path as ObjectPath,
};
use sha2::{Digest, Sha256};
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as std_mpsc;

const JOURNAL_FORMAT: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct JournalHead {
    pub(crate) format: u32,
    pub(crate) partition: u32,
    pub(crate) owner_epoch: u64,
    pub(crate) position: u64,
    pub(crate) record_path: String,
}

#[derive(Debug, Clone)]
pub(crate) struct JournalCursor {
    pub(crate) head: JournalHead,
    version: UpdateVersion,
}

#[derive(Debug, Serialize, Deserialize)]
struct JournalRecord {
    format: u32,
    partition: u32,
    owner_epoch: u64,
    position: u64,
    parent: Option<String>,
    payload: Vec<u8>,
}

pub(crate) struct RecoveredRecord {
    pub(crate) owner_epoch: u64,
    pub(crate) position: u64,
    pub(crate) payload: Vec<u8>,
}

enum JournalCommand {
    Append {
        partition: u32,
        owner_epoch: u64,
        payload: Vec<u8>,
        response: std_mpsc::Sender<Result<u64>>,
    },
    PublishCheckpoint {
        manifest: CheckpointManifest,
        source: PathBuf,
        response: std_mpsc::Sender<Result<()>>,
    },
    Sync {
        partition: u32,
        after_position: u64,
        response: std_mpsc::Sender<Result<Vec<RecoveredRecord>>>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct RemoteCheckpoint {
    format: u32,
    manifest: CheckpointManifest,
    files: Vec<CheckpointFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CheckpointFile {
    path: String,
    digest: String,
    size: u64,
}

fn content_digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(crate) struct RemoteJournal {
    commands: mpsc::Sender<JournalCommand>,
}

impl RemoteJournal {
    pub(crate) fn restore_latest(uri: &str, destination: &FsPath) -> Result<bool> {
        let uri = uri.to_owned();
        let destination = destination.to_path_buf();
        std::thread::Builder::new()
            .name("highwater-checkpoint-restore".to_owned())
            .spawn(move || -> Result<bool> {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                let journal = ConditionalJournal::s3(&uri)?;
                runtime.block_on(journal.restore_latest_checkpoint(&destination))
            })?
            .join()
            .map_err(|_| anyhow!("checkpoint restore thread panicked"))?
    }

    pub(crate) fn open(
        uri: &str,
        after_positions: &[u64],
    ) -> Result<(Self, Vec<Vec<RecoveredRecord>>)> {
        let journal = ConditionalJournal::s3(uri)?;
        let (commands, mut receiver) = mpsc::channel(4_096);
        let (ready_sender, ready_receiver) = std_mpsc::sync_channel(1);
        let positions = after_positions.to_vec();
        std::thread::Builder::new()
            .name("highwater-object-journal".to_owned())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready_sender.send(Err(format!("start journal runtime: {error}")));
                        return;
                    }
                };
                let recovered = runtime.block_on(async {
                    let mut cursors = Vec::with_capacity(positions.len());
                    let mut records = Vec::with_capacity(positions.len());
                    for (partition, after) in positions.into_iter().enumerate() {
                        let (cursor, recovered) =
                            journal.recover(u32::try_from(partition)?, after).await?;
                        cursors.push(cursor);
                        records.push(recovered);
                    }
                    Ok::<_, anyhow::Error>((cursors, records))
                });
                let (mut cursors, records) = match recovered {
                    Ok(recovered) => recovered,
                    Err(error) => {
                        let _ = ready_sender.send(Err(format!("recover journal: {error:#}")));
                        return;
                    }
                };
                if ready_sender.send(Ok(records)).is_err() {
                    return;
                }
                let cursors = Arc::new(
                    cursors
                        .drain(..)
                        .map(tokio::sync::Mutex::new)
                        .collect::<Vec<_>>(),
                );
                runtime.block_on(async move {
                    while let Some(command) = receiver.recv().await {
                        let journal = journal.clone();
                        let cursors = cursors.clone();
                        tokio::spawn(async move {
                            match command {
                                JournalCommand::Append {
                                    partition,
                                    owner_epoch,
                                    payload,
                                    response,
                                } => {
                                    let mut cursor = cursors[partition as usize].lock().await;
                                    let result = journal
                                        .append(partition, cursor.as_ref(), owner_epoch, payload)
                                        .await
                                        .map(|next| {
                                            let position = next.head.position;
                                            *cursor = Some(next);
                                            position
                                        });
                                    let _ = response.send(result);
                                }
                                JournalCommand::PublishCheckpoint {
                                    manifest,
                                    source,
                                    response,
                                } => {
                                    let result =
                                        journal.publish_checkpoint(&manifest, &source).await;
                                    let _ = response.send(result);
                                }
                                JournalCommand::Sync {
                                    partition,
                                    after_position,
                                    response,
                                } => {
                                    let mut cursor = cursors[partition as usize].lock().await;
                                    let result = journal
                                        .recover(partition, after_position)
                                        .await
                                        .map(|(next, records)| {
                                            *cursor = next;
                                            records
                                        });
                                    let _ = response.send(result);
                                }
                            }
                        });
                    }
                });
            })?;
        let recovered = ready_receiver
            .recv()
            .context("journal service stopped during startup")?;
        let recovered = recovered.map_err(anyhow::Error::msg)?;
        Ok((Self { commands }, recovered))
    }

    pub(crate) fn append(&self, partition: u32, owner_epoch: u64, payload: Vec<u8>) -> Result<u64> {
        let (response, result) = std_mpsc::channel();
        self.commands
            .try_send(JournalCommand::Append {
                partition,
                owner_epoch,
                payload,
                response,
            })
            .context("journal service stopped")?;
        result
            .recv()
            .context("journal service stopped during append")?
    }

    pub(crate) fn publish_checkpoint(
        &self,
        manifest: CheckpointManifest,
        source: PathBuf,
    ) -> Result<()> {
        let (response, result) = std_mpsc::channel();
        self.commands
            .try_send(JournalCommand::PublishCheckpoint {
                manifest,
                source,
                response,
            })
            .context("journal service stopped")?;
        result
            .recv()
            .context("journal service stopped during checkpoint publication")?
    }

    pub(crate) fn sync(&self, partition: u32, after_position: u64) -> Result<Vec<RecoveredRecord>> {
        let (response, result) = std_mpsc::channel();
        self.commands
            .try_send(JournalCommand::Sync {
                partition,
                after_position,
                response,
            })
            .context("journal service stopped")?;
        result
            .recv()
            .context("journal service stopped during synchronization")?
    }
}

#[derive(Clone)]
pub(crate) struct ConditionalJournal {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    #[cfg(test)]
    fail_after_head_commit: Arc<AtomicBool>,
    #[cfg(test)]
    ambiguous_head_pause: Arc<Mutex<Option<Arc<HeadResponsePause>>>>,
    #[cfg(test)]
    fail_before_head_commit: Arc<AtomicBool>,
    #[cfg(test)]
    fail_before_checkpoint_pointer: Arc<AtomicBool>,
    #[cfg(test)]
    fail_after_checkpoint_pointer_commit: Arc<AtomicBool>,
    #[cfg(test)]
    checkpoint_before_pause: Arc<Mutex<Option<Arc<HeadResponsePause>>>>,
    #[cfg(test)]
    checkpoint_after_pause: Arc<Mutex<Option<Arc<HeadResponsePause>>>>,
}

#[cfg(test)]
#[derive(Default)]
struct HeadResponsePause {
    committed: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

impl ConditionalJournal {
    pub(crate) fn s3(uri: &str) -> Result<Self> {
        let location = uri
            .strip_prefix("s3://")
            .ok_or_else(|| anyhow!("journal URI must start with s3://"))?;
        let (bucket, prefix) = location.split_once('/').unwrap_or((location, ""));
        if bucket.is_empty() {
            bail!("journal URI has no bucket");
        }
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()?;
        Ok(Self {
            store: Arc::new(store),
            prefix: prefix.trim_matches('/').to_owned(),
            #[cfg(test)]
            fail_after_head_commit: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            ambiguous_head_pause: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            fail_before_head_commit: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            fail_before_checkpoint_pointer: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            fail_after_checkpoint_pointer_commit: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            checkpoint_before_pause: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            checkpoint_after_pause: Arc::new(Mutex::new(None)),
        })
    }

    #[cfg(test)]
    fn memory() -> Self {
        Self {
            store: Arc::new(object_store::memory::InMemory::new()),
            prefix: "test".to_owned(),
            fail_after_head_commit: Arc::new(AtomicBool::new(false)),
            ambiguous_head_pause: Arc::new(Mutex::new(None)),
            fail_before_head_commit: Arc::new(AtomicBool::new(false)),
            fail_before_checkpoint_pointer: Arc::new(AtomicBool::new(false)),
            fail_after_checkpoint_pointer_commit: Arc::new(AtomicBool::new(false)),
            checkpoint_before_pause: Arc::new(Mutex::new(None)),
            checkpoint_after_pause: Arc::new(Mutex::new(None)),
        }
    }

    #[cfg(test)]
    fn inject_ambiguous_head_commit(&self) {
        self.fail_after_head_commit.store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    fn inject_checkpoint_pointer_failure(&self) {
        self.fail_before_checkpoint_pointer
            .store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    fn inject_ambiguous_checkpoint_commit(&self) {
        self.fail_after_checkpoint_pointer_commit
            .store(true, Ordering::SeqCst);
    }

    fn path(&self, suffix: &str) -> ObjectPath {
        if self.prefix.is_empty() {
            ObjectPath::from(suffix)
        } else {
            ObjectPath::from(format!("{}/{suffix}", self.prefix))
        }
    }

    fn head_path(&self, partition: u32) -> ObjectPath {
        self.path(&format!("partitions/{partition:04}/head.json"))
    }

    fn checkpoint_pointer_path(&self) -> ObjectPath {
        self.path("checkpoints/current.json")
    }

    async fn read_remote_checkpoint(&self) -> Result<Option<(RemoteCheckpoint, UpdateVersion)>> {
        let pointer = match self.store.get(&self.checkpoint_pointer_path()).await {
            Ok(pointer) => pointer,
            Err(ObjectStoreError::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let version = UpdateVersion {
            e_tag: pointer.meta.e_tag.clone(),
            version: pointer.meta.version.clone(),
        };
        let checkpoint: RemoteCheckpoint = serde_json::from_slice(&pointer.bytes().await?)?;
        if checkpoint.format != JOURNAL_FORMAT || checkpoint.manifest.format != 1 {
            bail!("unsupported remote checkpoint format");
        }
        Ok(Some((checkpoint, version)))
    }

    async fn restore_latest_checkpoint(&self, destination: &FsPath) -> Result<bool> {
        let Some((checkpoint, _)) = self.read_remote_checkpoint().await? else {
            return Ok(false);
        };
        if destination.exists() {
            fs::remove_dir_all(destination)?;
        }
        fs::create_dir_all(destination)?;
        for checkpoint_file in &checkpoint.files {
            let relative_path = FsPath::new(&checkpoint_file.path);
            if relative_path.is_absolute()
                || relative_path
                    .components()
                    .any(|component| matches!(component, std::path::Component::ParentDir))
            {
                bail!("remote checkpoint contains an unsafe file path");
            }
            if checkpoint_file.digest.len() != 64
                || !checkpoint_file
                    .digest
                    .bytes()
                    .all(|value| value.is_ascii_hexdigit())
            {
                bail!("remote checkpoint contains an invalid content digest");
            }
            let object = self.path(&format!("checkpoints/objects/{}", checkpoint_file.digest));
            let bytes = self.store.get(&object).await?.bytes().await?;
            if bytes.len() as u64 != checkpoint_file.size
                || content_digest(&bytes) != checkpoint_file.digest
            {
                bail!(
                    "remote checkpoint content is corrupt: {}",
                    checkpoint_file.path
                );
            }
            let path = destination.join(relative_path);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut file = File::create(path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
        }
        File::open(destination)?.sync_all()?;
        Ok(true)
    }

    async fn publish_checkpoint(
        &self,
        manifest: &CheckpointManifest,
        source: &FsPath,
    ) -> Result<()> {
        let current = self.read_remote_checkpoint().await?;
        if let Some((checkpoint, _)) = &current {
            for (partition, position) in &checkpoint.manifest.shard_sequences {
                if manifest
                    .shard_sequences
                    .get(partition)
                    .is_none_or(|next| next < position)
                {
                    bail!("checkpoint publication would regress partition {partition}");
                }
            }
        }
        let mut pending = vec![(source.to_path_buf(), PathBuf::new())];
        let mut files = Vec::new();
        while let Some((directory, relative_directory)) = pending.pop() {
            for entry in fs::read_dir(directory)? {
                let entry = entry?;
                let relative = relative_directory.join(entry.file_name());
                if entry.file_type()?.is_dir() {
                    pending.push((entry.path(), relative));
                    continue;
                }
                let relative = relative
                    .to_str()
                    .ok_or_else(|| anyhow!("checkpoint path is not UTF-8"))?
                    .replace('\\', "/");
                let bytes = fs::read(entry.path())?;
                let digest = content_digest(&bytes);
                let result = self
                    .store
                    .put_opts(
                        &self.path(&format!("checkpoints/objects/{digest}")),
                        bytes.clone().into(),
                        PutOptions::from(PutMode::Create),
                    )
                    .await;
                if !matches!(&result, Ok(_) | Err(ObjectStoreError::AlreadyExists { .. })) {
                    result?;
                }
                files.push(CheckpointFile {
                    path: relative,
                    digest,
                    size: bytes.len() as u64,
                });
            }
        }
        files.sort_by(|left, right| left.path.cmp(&right.path));
        let checkpoint = RemoteCheckpoint {
            format: JOURNAL_FORMAT,
            manifest: manifest.clone(),
            files,
        };
        let mode = current.as_ref().map_or(PutMode::Create, |(_, version)| {
            PutMode::Update(version.clone())
        });
        #[cfg(test)]
        if self
            .fail_before_checkpoint_pointer
            .swap(false, Ordering::SeqCst)
        {
            bail!("injected checkpoint pointer failure");
        }
        #[cfg(test)]
        {
            let pause = self.checkpoint_before_pause.lock().unwrap().take();
            if let Some(pause) = pause {
                pause.committed.notify_one();
                pause.resume.notified().await;
            }
        }
        let result = self
            .store
            .put_opts(
                &self.checkpoint_pointer_path(),
                serde_json::to_vec(&checkpoint)?.into(),
                PutOptions::from(mode),
            )
            .await;
        #[cfg(test)]
        let result = if result.is_ok()
            && self
                .fail_after_checkpoint_pointer_commit
                .swap(false, Ordering::SeqCst)
        {
            let pause = self.checkpoint_after_pause.lock().unwrap().take();
            if let Some(pause) = pause {
                pause.committed.notify_one();
                pause.resume.notified().await;
            }
            Err(ObjectStoreError::Generic {
                store: "injected",
                source: anyhow!("ambiguous checkpoint commit response").into(),
            })
        } else {
            result
        };
        if let Err(error) = result {
            let observed = self.read_remote_checkpoint().await?;
            if observed.as_ref().map(|(value, _)| value) != Some(&checkpoint) {
                return Err(anyhow::Error::new(error).context("checkpoint publication was fenced"));
            }
        }
        Ok(())
    }

    async fn read_head(&self, partition: u32) -> Result<Option<JournalCursor>> {
        let path = self.head_path(partition);
        let result = match self.store.get(&path).await {
            Ok(result) => result,
            Err(ObjectStoreError::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let head: JournalHead = serde_json::from_slice(&result.bytes().await?)?;
        if head.format != JOURNAL_FORMAT || head.partition != partition || head.position == 0 {
            bail!("partition {partition} has an invalid journal head");
        }
        Ok(Some(JournalCursor { head, version }))
    }

    pub(crate) async fn append(
        &self,
        partition: u32,
        expected: Option<&JournalCursor>,
        owner_epoch: u64,
        payload: Vec<u8>,
    ) -> Result<JournalCursor> {
        if owner_epoch == 0 {
            bail!("journal owner epoch must be positive");
        }
        let position = expected.map_or(1, |cursor| cursor.head.position + 1);
        if let Some(cursor) = expected {
            if cursor.head.partition != partition {
                bail!("journal cursor belongs to another partition");
            }
            if owner_epoch != cursor.head.owner_epoch
                && owner_epoch != cursor.head.owner_epoch.saturating_add(1)
            {
                bail!(
                    "partition {partition} owner epoch must remain {} or advance to {}",
                    cursor.head.owner_epoch,
                    cursor.head.owner_epoch.saturating_add(1)
                );
            }
        }
        let record_path = self.path(&format!(
            "partitions/{partition:04}/records/{position:020}-{}.mpk",
            Uuid::new_v4()
        ));
        let record = JournalRecord {
            format: JOURNAL_FORMAT,
            partition,
            owner_epoch,
            position,
            parent: expected.map(|cursor| cursor.head.record_path.clone()),
            payload,
        };
        self.store
            .put_opts(
                &record_path,
                rmp_serde::to_vec_named(&record)?.into(),
                PutOptions::from(PutMode::Create),
            )
            .await?;

        let head = JournalHead {
            format: JOURNAL_FORMAT,
            partition,
            owner_epoch,
            position,
            record_path: record_path.to_string(),
        };
        let mode = expected.map_or(PutMode::Create, |cursor| {
            PutMode::Update(cursor.version.clone())
        });
        #[cfg(test)]
        if self.fail_before_head_commit.swap(false, Ordering::SeqCst) {
            bail!("injected crash before head commit");
        }
        let committed = self
            .store
            .put_opts(
                &self.head_path(partition),
                serde_json::to_vec(&head)?.into(),
                PutOptions::from(mode),
            )
            .await;
        #[cfg(test)]
        let committed =
            if committed.is_ok() && self.fail_after_head_commit.swap(false, Ordering::SeqCst) {
                let pause = self.ambiguous_head_pause.lock().unwrap().take();
                if let Some(pause) = pause {
                    pause.committed.notify_one();
                    pause.resume.notified().await;
                }
                Err(ObjectStoreError::Generic {
                    store: "injected",
                    source: anyhow!("ambiguous head commit response").into(),
                })
            } else {
                committed
            };
        let result = match committed {
            Ok(result) => result,
            Err(error) => {
                let observed = self.read_head(partition).await?;
                if observed.as_ref().is_some_and(|cursor| cursor.head == head) {
                    return Ok(observed.expect("matching journal head"));
                }
                // A different head does not prove this record was rejected: our CAS
                // may have succeeded before its response was lost, with another
                // writer subsequently appending a descendant. Keep the immutable
                // record until reachability-aware garbage collection can prove it
                // is unused. Do not return the successor's cursor as our success:
                // the caller has not applied that successor's payload locally.
                return Err(anyhow::Error::new(error).context(format!(
                    "partition {partition} conditional append was fenced"
                )));
            }
        };
        Ok(JournalCursor {
            head,
            version: UpdateVersion {
                e_tag: result.e_tag,
                version: result.version,
            },
        })
    }

    pub(crate) async fn recover(
        &self,
        partition: u32,
        after_position: u64,
    ) -> Result<(Option<JournalCursor>, Vec<RecoveredRecord>)> {
        let cursor = self.read_head(partition).await?;
        let mut path = cursor
            .as_ref()
            .map(|cursor| cursor.head.record_path.clone());
        let mut expected_position = cursor.as_ref().map_or(0, |cursor| cursor.head.position);
        let mut records = Vec::new();
        while expected_position > after_position {
            let record_path = path.ok_or_else(|| {
                anyhow!(
                    "partition {partition} journal chain ended before position {after_position}"
                )
            })?;
            let result = self
                .store
                .get(&ObjectPath::from(record_path.as_str()))
                .await?;
            let record: JournalRecord = rmp_serde::from_slice(&result.bytes().await?)?;
            if record.format != JOURNAL_FORMAT
                || record.partition != partition
                || record.position != expected_position
                || record.owner_epoch == 0
            {
                bail!(
                    "partition {partition} has an invalid record at position {expected_position}"
                );
            }
            records.push(RecoveredRecord {
                owner_epoch: record.owner_epoch,
                position: record.position,
                payload: record.payload,
            });
            path = record.parent;
            expected_position -= 1;
        }
        records.reverse();
        for pair in records.windows(2) {
            if pair[1].owner_epoch < pair[0].owner_epoch
                || pair[1].owner_epoch > pair[0].owner_epoch.saturating_add(1)
            {
                bail!("partition {partition} journal contains a non-monotonic owner epoch");
            }
        }
        Ok((cursor, records))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn checkpoint_manifest(id: &str, position: u64) -> CheckpointManifest {
        CheckpointManifest {
            format: 1,
            checkpoint_id: id.to_owned(),
            sequence: position,
            shard_sequences: BTreeMap::from([(0, position)]),
            created_at: 1.0,
            object_path: String::new(),
            state_handles: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn conditional_head_fences_a_stale_owner() -> Result<()> {
        let journal = ConditionalJournal::memory();
        let first = journal.append(3, None, 1, b"one".to_vec()).await?;
        let second = journal.append(3, Some(&first), 2, b"two".to_vec()).await?;
        let stale = journal.append(3, Some(&first), 1, b"stale".to_vec()).await;
        assert!(stale.is_err());
        let (head, records) = journal.recover(3, 0).await?;
        assert_eq!(head.expect("head").head, second.head);
        assert_eq!(records.len(), 2);
        assert_eq!(records[1].payload, b"two");
        Ok(())
    }

    #[tokio::test]
    async fn ambiguous_head_response_is_resolved_by_readback() -> Result<()> {
        let journal = ConditionalJournal::memory();
        journal.inject_ambiguous_head_commit();

        let committed = journal.append(1, None, 1, b"committed".to_vec()).await?;
        let (head, records) = journal.recover(1, 0).await?;

        assert_eq!(head.expect("head").head, committed.head);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].payload, b"committed");
        Ok(())
    }

    #[tokio::test]
    async fn ambiguous_response_after_takeover_preserves_committed_ancestor() -> Result<()> {
        let journal = ConditionalJournal::memory();
        let pause = Arc::new(HeadResponsePause::default());
        *journal.ambiguous_head_pause.lock().unwrap() = Some(pause.clone());
        journal.inject_ambiguous_head_commit();
        let writer = journal.clone();
        let pending = tokio::spawn(async move {
            writer
                .append(1, None, 1, b"accepted-before-response-loss".to_vec())
                .await
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            pause.committed.notified(),
        )
        .await?;
        let first = journal.read_head(1).await?.expect("committed head");
        let second = journal
            .append(1, Some(&first), 2, b"takeover".to_vec())
            .await?;
        pause.resume.notify_one();
        // The original caller may receive an uncertain result, but must not destroy
        // a record that the acknowledged successor now depends on.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), pending).await??;
        let (head, records) = journal.recover(1, 0).await?;
        assert_eq!(head.expect("head").head, second.head);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].payload, b"accepted-before-response-loss");
        assert_eq!(records[1].payload, b"takeover");
        Ok(())
    }

    #[tokio::test]
    async fn conditional_head_allows_only_one_competing_owner() -> Result<()> {
        let journal = ConditionalJournal::memory();
        let first = journal.append(2, None, 1, b"first".to_vec()).await?;

        let left_journal = journal.clone();
        let left_cursor = first.clone();
        let right_journal = journal.clone();
        let right_cursor = first.clone();
        let (left, right) = tokio::join!(
            left_journal.append(2, Some(&left_cursor), 2, b"left".to_vec()),
            right_journal.append(2, Some(&right_cursor), 2, b"right".to_vec())
        );

        assert_ne!(left.is_ok(), right.is_ok());
        let (_, records) = journal.recover(2, 0).await?;
        assert_eq!(records.len(), 2);
        assert!(records[1].payload == b"left" || records[1].payload == b"right");
        Ok(())
    }

    // A small executable oracle for the real journal implementation. The schedule
    // is seeded, and the injected failure points never rely on wall-clock sleeps.
    // Set HIGHWATER_JOURNAL_SEED to replay one history from a failing CI log.
    #[tokio::test]
    async fn seeded_journal_histories_match_reference_log() -> Result<()> {
        let seeds = match std::env::var("HIGHWATER_JOURNAL_SEED") {
            Ok(seed) => vec![seed.parse::<u64>()?],
            Err(_) => {
                let count = std::env::var("HIGHWATER_JOURNAL_SEED_COUNT")
                    .ok()
                    .map(|count| count.parse::<u64>())
                    .transpose()?
                    .unwrap_or(128);
                anyhow::ensure!(count > 0, "journal simulation seed count must be positive");
                (0..count).collect()
            }
        };
        for seed in seeds {
            let mut trace = Vec::new();
            generated_history(seed, &mut trace).await.with_context(|| {
                format!(
                    "journal simulation seed={seed}; replay with HIGHWATER_JOURNAL_SEED={seed}\n{}",
                    trace.join("\n")
                )
            })?;
        }
        Ok(())
    }

    async fn generated_history(seed: u64, trace: &mut Vec<String>) -> Result<()> {
        let journal = ConditionalJournal::memory();
        let mut random = seed;
        let mut model: Vec<(u64, Vec<u8>)> = Vec::new();
        let mut cursors: [Option<JournalCursor>; 3] = [None, None, None];
        for step in 0..64 {
            random = random
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let node = (random >> 32) as usize % cursors.len();
            let action = (random >> 48) % 8;
            trace.push(format!(
                "step={step} node={node} action={action} committed={}",
                model.len()
            ));
            match action {
                0 => {
                    // Read the current head; other nodes retain their stale views.
                    cursors[node] = journal.read_head(1).await?;
                }
                1 => {
                    // Drop local state and rebuild it using a reference checkpoint
                    // prefix plus the actual retained journal tail.
                    cursors[node] = None;
                    let cut = random as usize % (model.len() + 1);
                    let (head, tail) = journal.recover(1, cut as u64).await?;
                    let mut restored = model[..cut].to_vec();
                    restored.extend(
                        tail.into_iter()
                            .map(|record| (record.owner_epoch, record.payload)),
                    );
                    anyhow::ensure!(
                        restored == model,
                        "checkpoint prefix plus tail differs from reference"
                    );
                    cursors[node] = head;
                }
                7 => {
                    // Lose a successful response while a different owner advances
                    // the head. The first writer must preserve its committed record.
                    let current = journal.read_head(1).await?;
                    let epoch = model.last().map_or(1, |entry| entry.0);
                    let payload = format!("{seed}:{step}:ambiguous").into_bytes();
                    let pause = Arc::new(HeadResponsePause::default());
                    *journal.ambiguous_head_pause.lock().unwrap() = Some(pause.clone());
                    journal.inject_ambiguous_head_commit();
                    let writer = journal.clone();
                    let sent = payload.clone();
                    let pending = tokio::spawn(async move {
                        writer.append(1, current.as_ref(), epoch, sent).await
                    });
                    tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        pause.committed.notified(),
                    )
                    .await?;
                    model.push((epoch, payload));
                    let first = journal
                        .read_head(1)
                        .await?
                        .expect("head committed before pause");
                    let successor = format!("{seed}:{step}:successor").into_bytes();
                    let next = journal
                        .append(1, Some(&first), epoch + 1, successor.clone())
                        .await;
                    pause.resume.notify_one();
                    let uncertain =
                        tokio::time::timeout(std::time::Duration::from_secs(5), pending).await??;
                    cursors[node] = Some(next?);
                    model.push((epoch + 1, successor));
                    anyhow::ensure!(
                        uncertain.is_err(),
                        "must not hand the caller an unapplied successor cursor"
                    );
                }
                _ => {
                    let cursor = cursors[node].as_ref();
                    let previous_position = cursor.map_or(0, |cursor| cursor.head.position);
                    let previous_epoch = cursor.map_or(1, |cursor| cursor.head.owner_epoch);
                    let epoch =
                        previous_epoch + u64::from(action == 3) + 2 * u64::from(action == 4);
                    let crash = action == 5;
                    let ambiguous = action == 6;
                    journal
                        .fail_before_head_commit
                        .store(crash, Ordering::SeqCst);
                    journal
                        .fail_after_head_commit
                        .store(ambiguous, Ordering::SeqCst);
                    let payload = format!("{seed}:{step}:{node}").into_bytes();
                    let result = journal.append(1, cursor, epoch, payload.clone()).await;
                    // Invalid epochs can reject before reaching an injected hook.
                    journal
                        .fail_before_head_commit
                        .store(false, Ordering::SeqCst);
                    journal
                        .fail_after_head_commit
                        .store(false, Ordering::SeqCst);
                    let accepted = previous_position == model.len() as u64
                        && (cursor.is_none() || action != 4)
                        && !crash;
                    anyhow::ensure!(
                        result.is_ok() == accepted,
                        "append result differs from reference: {result:?}"
                    );
                    if let Ok(next) = result {
                        model.push((epoch, payload));
                        cursors[node] = Some(next);
                    }
                }
            }
            let (head, records) = journal.recover(1, 0).await?;
            anyhow::ensure!(
                head.as_ref().map_or(0, |cursor| cursor.head.position) == model.len() as u64,
                "head position differs from reference"
            );
            anyhow::ensure!(
                records.len() == model.len(),
                "committed record count differs from reference"
            );
            for (index, (record, expected)) in records.iter().zip(&model).enumerate() {
                anyhow::ensure!(
                    record.position == index as u64 + 1
                        && record.owner_epoch == expected.0
                        && record.payload == expected.1,
                    "recovered record {index} differs from reference"
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn owner_epoch_cannot_skip_fencing_generation() -> Result<()> {
        let journal = ConditionalJournal::memory();
        let first = journal.append(4, None, 1, b"first".to_vec()).await?;

        let skipped = journal
            .append(4, Some(&first), 3, b"skipped".to_vec())
            .await;

        assert!(skipped.is_err());
        let (head, records) = journal.recover(4, 0).await?;
        assert_eq!(head.expect("head").head, first.head);
        assert_eq!(records.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn recovery_fails_closed_when_committed_record_is_missing() -> Result<()> {
        let journal = ConditionalJournal::memory();
        let committed = journal.append(5, None, 1, b"committed".to_vec()).await?;
        journal
            .store
            .delete(&ObjectPath::from(committed.head.record_path.as_str()))
            .await?;

        assert!(journal.recover(5, 0).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn recovery_fails_closed_when_committed_record_is_corrupt() -> Result<()> {
        let journal = ConditionalJournal::memory();
        let committed = journal.append(6, None, 1, b"committed".to_vec()).await?;
        journal
            .store
            .put(
                &ObjectPath::from(committed.head.record_path.as_str()),
                b"not-messagepack".to_vec().into(),
            )
            .await?;

        assert!(journal.recover(6, 0).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_publication_is_monotonic_and_restorable() -> Result<()> {
        let journal = ConditionalJournal::memory();
        let root = std::env::temp_dir().join(format!("highwater-journal-{}", Uuid::new_v4()));
        let source = root.join("source");
        let restored = root.join("restored");
        fs::create_dir_all(&source)?;
        fs::write(source.join("CURRENT"), b"MANIFEST-000001\n")?;
        let manifest = checkpoint_manifest("checkpoint-2", 2);
        journal.publish_checkpoint(&manifest, &source).await?;
        assert!(journal.restore_latest_checkpoint(&restored).await?);
        assert_eq!(fs::read(restored.join("CURRENT"))?, b"MANIFEST-000001\n");

        let mut older = manifest;
        older.checkpoint_id = "checkpoint-1".to_owned();
        older.shard_sequences.insert(0, 1);
        assert!(journal.publish_checkpoint(&older, &source).await.is_err());
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_files_are_reused_by_content_digest() -> Result<()> {
        let journal = ConditionalJournal::memory();
        let root = std::env::temp_dir().join(format!("highwater-journal-{}", Uuid::new_v4()));
        let source = root.join("source");
        fs::create_dir_all(&source)?;
        fs::write(source.join("CURRENT"), b"MANIFEST-000001\n")?;
        fs::write(source.join("OPTIONS"), b"stable-options\n")?;
        journal
            .publish_checkpoint(&checkpoint_manifest("checkpoint-1", 1), &source)
            .await?;
        let (first, _) = journal.read_remote_checkpoint().await?.expect("checkpoint");

        fs::write(source.join("CURRENT"), b"MANIFEST-000002\n")?;
        journal
            .publish_checkpoint(&checkpoint_manifest("checkpoint-2", 2), &source)
            .await?;
        let (second, _) = journal.read_remote_checkpoint().await?.expect("checkpoint");

        let first_options = first
            .files
            .iter()
            .find(|file| file.path == "OPTIONS")
            .expect("OPTIONS");
        let second_options = second
            .files
            .iter()
            .find(|file| file.path == "OPTIONS")
            .expect("OPTIONS");
        assert_eq!(first_options.digest, second_options.digest);
        assert_ne!(
            first
                .files
                .iter()
                .find(|file| file.path == "CURRENT")
                .expect("CURRENT")
                .digest,
            second
                .files
                .iter()
                .find(|file| file.path == "CURRENT")
                .expect("CURRENT")
                .digest,
        );
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_restore_rejects_corrupt_content() -> Result<()> {
        let journal = ConditionalJournal::memory();
        let root = std::env::temp_dir().join(format!("highwater-journal-{}", Uuid::new_v4()));
        let source = root.join("source");
        fs::create_dir_all(&source)?;
        fs::write(source.join("CURRENT"), b"MANIFEST-000001\n")?;
        journal
            .publish_checkpoint(&checkpoint_manifest("checkpoint-1", 1), &source)
            .await?;
        let (checkpoint, _) = journal.read_remote_checkpoint().await?.expect("checkpoint");
        let file = checkpoint.files.first().expect("checkpoint file");
        journal
            .store
            .put(
                &journal.path(&format!("checkpoints/objects/{}", file.digest)),
                b"corrupt".to_vec().into(),
            )
            .await?;

        assert!(
            journal
                .restore_latest_checkpoint(&root.join("restored"))
                .await
                .is_err()
        );
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn interrupted_checkpoint_publication_preserves_previous_pointer() -> Result<()> {
        let journal = ConditionalJournal::memory();
        let root = std::env::temp_dir().join(format!("highwater-journal-{}", Uuid::new_v4()));
        let source = root.join("source");
        let restored = root.join("restored");
        fs::create_dir_all(&source)?;
        fs::write(source.join("CURRENT"), b"old-checkpoint\n")?;
        journal
            .publish_checkpoint(&checkpoint_manifest("checkpoint-1", 1), &source)
            .await?;

        fs::write(source.join("CURRENT"), b"unpublished-checkpoint\n")?;
        journal.inject_checkpoint_pointer_failure();
        let interrupted = journal
            .publish_checkpoint(&checkpoint_manifest("checkpoint-2", 2), &source)
            .await;

        assert!(interrupted.is_err());
        assert!(journal.restore_latest_checkpoint(&restored).await?);
        assert_eq!(fs::read(restored.join("CURRENT"))?, b"old-checkpoint\n");
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn ambiguous_checkpoint_response_is_resolved_by_readback() -> Result<()> {
        let journal = ConditionalJournal::memory();
        let root = std::env::temp_dir().join(format!("highwater-journal-{}", Uuid::new_v4()));
        let source = root.join("source");
        let restored = root.join("restored");
        fs::create_dir_all(&source)?;
        fs::write(source.join("CURRENT"), b"committed-checkpoint\n")?;
        journal.inject_ambiguous_checkpoint_commit();

        journal
            .publish_checkpoint(&checkpoint_manifest("checkpoint-1", 1), &source)
            .await?;

        assert!(journal.restore_latest_checkpoint(&restored).await?);
        assert_eq!(
            fs::read(restored.join("CURRENT"))?,
            b"committed-checkpoint\n"
        );
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_races_preserve_winning_contents_and_vector() -> Result<()> {
        // Exercise both a stale conditional update and an ambiguous successful
        // update overtaken before readback. Also try reusing a checkpoint ID.
        for after_commit in [false, true] {
            for reuse_id in [false, true] {
                let journal = ConditionalJournal::memory();
                let root = std::env::temp_dir()
                    .join(format!("highwater-checkpoint-race-{}", Uuid::new_v4()));
                let result = checkpoint_race(&journal, &root, after_commit, reuse_id).await;
                let _ = fs::remove_dir_all(&root);
                result
                    .with_context(|| format!("after_commit={after_commit}, reuse_id={reuse_id}"))?;
            }
        }
        Ok(())
    }

    async fn checkpoint_race(
        journal: &ConditionalJournal,
        root: &FsPath,
        after_commit: bool,
        reuse_id: bool,
    ) -> Result<()> {
        let first_source = root.join("first");
        let second_source = root.join("second");
        fs::create_dir_all(&first_source)?;
        fs::create_dir_all(&second_source)?;
        fs::write(first_source.join("CURRENT"), b"first snapshot")?;
        fs::write(second_source.join("CURRENT"), b"second snapshot")?;
        let pause = Arc::new(HeadResponsePause::default());
        if after_commit {
            *journal.checkpoint_after_pause.lock().unwrap() = Some(pause.clone());
            journal.inject_ambiguous_checkpoint_commit();
        } else {
            *journal.checkpoint_before_pause.lock().unwrap() = Some(pause.clone());
        }
        let writer = journal.clone();
        let first = tokio::spawn(async move {
            writer
                .publish_checkpoint(&checkpoint_manifest("first", 1), &first_source)
                .await
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            pause.committed.notified(),
        )
        .await?;
        let winning = checkpoint_manifest(if reuse_id { "first" } else { "second" }, 2);
        let published = journal.publish_checkpoint(&winning, &second_source).await;
        pause.resume.notify_one();
        let first_result = tokio::time::timeout(std::time::Duration::from_secs(5), first).await??;
        published?;
        anyhow::ensure!(
            first_result.is_err(),
            "different checkpoint contents incorrectly acknowledged as this publication"
        );
        let (current, _) = journal.read_remote_checkpoint().await?.expect("checkpoint");
        anyhow::ensure!(
            current.manifest.shard_sequences == winning.shard_sequences,
            "checkpoint vector regressed"
        );
        let restored = root.join("restored");
        anyhow::ensure!(
            journal.restore_latest_checkpoint(&restored).await?,
            "checkpoint disappeared"
        );
        anyhow::ensure!(
            fs::read(restored.join("CURRENT"))? == b"second snapshot",
            "restored losing checkpoint contents"
        );
        let mut regressing = checkpoint_manifest("regression", 1);
        regressing.shard_sequences.insert(1, 100);
        anyhow::ensure!(
            journal
                .publish_checkpoint(&regressing, &second_source)
                .await
                .is_err(),
            "larger total position masked a regressing partition"
        );
        Ok(())
    }
}
