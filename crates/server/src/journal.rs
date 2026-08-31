use crate::*;
use object_store::{
    Error as ObjectStoreError, ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion,
    aws::{AmazonS3Builder, S3ConditionalPut},
    path::Path as ObjectPath,
};
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
        response: std_mpsc::Sender<Result<u64, String>>,
    },
    PublishCheckpoint {
        manifest: CheckpointManifest,
        source: PathBuf,
        response: std_mpsc::Sender<Result<(), String>>,
    },
    Sync {
        partition: u32,
        after_position: u64,
        response: std_mpsc::Sender<Result<Vec<RecoveredRecord>, String>>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RemoteCheckpoint {
    format: u32,
    manifest: CheckpointManifest,
    files: Vec<String>,
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
                                        })
                                        .map_err(|error| format!("{error:#}"));
                                    let _ = response.send(result);
                                }
                                JournalCommand::PublishCheckpoint {
                                    manifest,
                                    source,
                                    response,
                                } => {
                                    let result = journal
                                        .publish_checkpoint(&manifest, &source)
                                        .await
                                        .map_err(|error| format!("{error:#}"));
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
                                        })
                                        .map_err(|error| format!("{error:#}"));
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
            .map_err(anyhow::Error::msg)
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
            .map_err(anyhow::Error::msg)
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
            .map_err(anyhow::Error::msg)
    }
}

#[derive(Clone)]
pub(crate) struct ConditionalJournal {
    store: Arc<dyn ObjectStore>,
    prefix: String,
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
        })
    }

    #[cfg(test)]
    fn memory() -> Self {
        Self {
            store: Arc::new(object_store::memory::InMemory::new()),
            prefix: "test".to_owned(),
        }
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
        for relative in &checkpoint.files {
            let relative_path = FsPath::new(relative);
            if relative_path.is_absolute()
                || relative_path
                    .components()
                    .any(|component| matches!(component, std::path::Component::ParentDir))
            {
                bail!("remote checkpoint contains an unsafe file path");
            }
            let object = self.path(&format!(
                "checkpoints/{}/files/{relative}",
                checkpoint.manifest.checkpoint_id
            ));
            let bytes = self.store.get(&object).await?.bytes().await?;
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
                self.store
                    .put_opts(
                        &self.path(&format!(
                            "checkpoints/{}/files/{relative}",
                            manifest.checkpoint_id
                        )),
                        fs::read(entry.path())?.into(),
                        PutOptions::from(PutMode::Create),
                    )
                    .await?;
                files.push(relative);
            }
        }
        files.sort();
        let checkpoint = RemoteCheckpoint {
            format: JOURNAL_FORMAT,
            manifest: manifest.clone(),
            files,
        };
        let mode = current.as_ref().map_or(PutMode::Create, |(_, version)| {
            PutMode::Update(version.clone())
        });
        let result = self
            .store
            .put_opts(
                &self.checkpoint_pointer_path(),
                serde_json::to_vec(&checkpoint)?.into(),
                PutOptions::from(mode),
            )
            .await;
        if let Err(error) = result {
            let observed = self.read_remote_checkpoint().await?;
            if observed
                .as_ref()
                .map(|(value, _)| &value.manifest.checkpoint_id)
                != Some(&manifest.checkpoint_id)
            {
                return Err(anyhow!("checkpoint publication was fenced: {error}"));
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
        let committed = self
            .store
            .put_opts(
                &self.head_path(partition),
                serde_json::to_vec(&head)?.into(),
                PutOptions::from(mode),
            )
            .await;
        let result = match committed {
            Ok(result) => result,
            Err(error) => {
                let observed = self.read_head(partition).await?;
                if observed.as_ref().is_some_and(|cursor| cursor.head == head) {
                    return Ok(observed.expect("matching journal head"));
                }
                let _ = self.store.delete(&record_path).await;
                return Err(anyhow!(
                    "partition {partition} conditional append was fenced: {error}"
                ));
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
    async fn checkpoint_publication_is_monotonic_and_restorable() -> Result<()> {
        let journal = ConditionalJournal::memory();
        let root = std::env::temp_dir().join(format!("highwater-journal-{}", Uuid::new_v4()));
        let source = root.join("source");
        let restored = root.join("restored");
        fs::create_dir_all(&source)?;
        fs::write(source.join("CURRENT"), b"MANIFEST-000001\n")?;
        let manifest = CheckpointManifest {
            format: 1,
            checkpoint_id: "checkpoint-2".to_owned(),
            sequence: 2,
            shard_sequences: BTreeMap::from([(0, 2)]),
            created_at: 1.0,
            object_path: String::new(),
            state_handles: BTreeMap::new(),
        };
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
}
