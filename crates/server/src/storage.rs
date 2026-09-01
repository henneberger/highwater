use crate::*;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Mutation {
    pub(crate) op: String,
    pub(crate) key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) end_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) value: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) encoded_value: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct WalRecord {
    pub(crate) format: u32,
    #[serde(default)]
    pub(crate) shard: u32,
    pub(crate) sequence: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) mutations: Vec<Mutation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) puts: Vec<(String, Vec<u8>)>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) deletes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) delete_ranges: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CheckpointManifest {
    pub(crate) format: u32,
    pub(crate) checkpoint_id: String,
    pub(crate) sequence: u64,
    #[serde(default)]
    pub(crate) shard_sequences: BTreeMap<u32, u64>,
    pub(crate) created_at: f64,
    pub(crate) object_path: String,
    #[serde(default)]
    pub(crate) state_handles: BTreeMap<String, String>,
}

pub(crate) struct DurableStore {
    pub(crate) db: DB,
    pub(crate) wal_dir: PathBuf,
    pub(crate) checkpoint_dir: PathBuf,
    pub(crate) manifest_path: PathBuf,
    pub(crate) sequences: Vec<Mutex<u64>>,
    journal: Option<RemoteJournal>,
}

impl DurableStore {
    pub(crate) fn durability_mode(&self) -> &'static str {
        if self.journal.is_some() {
            "replicated_object_log"
        } else {
            "local_checkpoint"
        }
    }

    pub(crate) fn open_sharded_with_journal(
        state_dir: &FsPath,
        object_dir: &FsPath,
        log_shards: usize,
        journal_uri: Option<&str>,
    ) -> Result<Self> {
        if log_shards == 0 {
            bail!("log_shards must be positive");
        }
        let checkpoint_dir = object_dir.join("rust-core/checkpoints");
        let manifest_path = object_dir.join("rust-core/checkpoint-manifest.json");
        fs::create_dir_all(&checkpoint_dir)?;
        if !state_dir.exists() || fs::read_dir(state_dir)?.next().is_none() {
            let restored_remote = journal_uri
                .map(|uri| RemoteJournal::restore_latest(uri, state_dir))
                .transpose()?
                .unwrap_or(false);
            if !restored_remote && let Some(manifest) = Self::read_manifest(&manifest_path)? {
                if state_dir.exists() {
                    fs::remove_dir_all(state_dir)?;
                }
                Self::copy_checkpoint(&checkpoint_dir.join(&manifest.checkpoint_id), state_dir)?;
            }
        }
        fs::create_dir_all(state_dir)?;
        let wal_dir = object_dir.join("rust-core/wal");
        fs::create_dir_all(&wal_dir)?;
        let existing_shards = fs::read_dir(&wal_dir)?
            .filter_map(std::result::Result::ok)
            .filter(|entry| {
                entry.file_type().is_ok_and(|kind| kind.is_dir())
                    && entry
                        .file_name()
                        .to_str()
                        .is_some_and(|name| name.starts_with("shard-"))
            })
            .count();
        if existing_shards != 0 && existing_shards != log_shards {
            bail!(
                "configured log shard count {log_shards} does not match durable state {existing_shards}"
            );
        }
        let mut options = Options::default();
        options.create_if_missing(true);
        let parallelism = std::thread::available_parallelism()
            .map_or(4, std::num::NonZeroUsize::get)
            .min(i32::MAX as usize) as i32;
        options.increase_parallelism(parallelism);
        options.optimize_level_style_compaction(256 * 1024 * 1024);
        options.set_write_buffer_size(64 * 1024 * 1024);
        options.set_max_write_buffer_number(4);
        options.set_disable_auto_compactions(true);
        let db = DB::open(&options, state_dir)?;
        let applied_sequences = (0..log_shards)
            .map(|shard| -> Result<u64> {
                Ok(db
                    .get(format!("meta/applied_sequence/{shard:04}").as_bytes())?
                    .map(|bytes| rmp_serde::from_slice::<u64>(&bytes))
                    .transpose()?
                    .unwrap_or(0))
            })
            .collect::<Result<Vec<_>>>()?;
        let (journal, mut remote_records) = match journal_uri {
            Some(uri) => {
                let (journal, records) = RemoteJournal::open(uri, &applied_sequences)?;
                (Some(journal), Some(records))
            }
            None => (None, None),
        };
        let mut sequences = Vec::with_capacity(log_shards);
        for shard in 0..log_shards {
            let shard_dir = wal_dir.join(format!("shard-{shard:04}"));
            fs::create_dir_all(&shard_dir)?;
            let applied = applied_sequences[shard];
            if let Some(records) = remote_records.as_mut() {
                let mut sequence = applied;
                for recovered in std::mem::take(&mut records[shard]) {
                    let record: WalRecord = rmp_serde::from_slice(&recovered.payload)
                        .with_context(|| {
                            format!(
                                "decode remote WAL shard {shard} position {}",
                                recovered.position
                            )
                        })?;
                    if record.shard as usize != shard || record.sequence != recovered.position {
                        bail!("remote WAL record does not match its journal position");
                    }
                    Self::apply_record(&db, &record, shard)?;
                    sequence = record.sequence;
                }
                sequences.push(Mutex::new(sequence));
                continue;
            }
            let mut records = Vec::new();
            for entry in fs::read_dir(&shard_dir)? {
                let path = entry?.path();
                if path.extension().is_some_and(|value| value == "mpk") {
                    let record: WalRecord = rmp_serde::from_slice(&fs::read(&path)?)
                        .with_context(|| format!("decode WAL record {}", path.display()))?;
                    if record.sequence > applied {
                        records.push(record);
                    }
                }
            }
            records.sort_by_key(|record| record.sequence);
            let mut sequence = applied;
            for record in records {
                if record.shard as usize != shard {
                    bail!("WAL record is stored in the wrong shard");
                }
                if record.sequence != sequence + 1 {
                    bail!("WAL shard {shard} sequence {} is missing", sequence + 1);
                }
                Self::apply_record(&db, &record, shard)?;
                sequence = record.sequence;
            }
            sequences.push(Mutex::new(sequence));
        }
        Ok(Self {
            db,
            wal_dir,
            checkpoint_dir,
            manifest_path,
            sequences,
            journal,
        })
    }

    pub(crate) fn read_manifest(path: &FsPath) -> Result<Option<CheckpointManifest>> {
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(serde_json::from_slice(&fs::read(path)?)?))
    }

    pub(crate) fn copy_checkpoint(source: &FsPath, destination: &FsPath) -> Result<()> {
        fs::create_dir_all(destination)?;
        for entry in fs::read_dir(source)? {
            let entry = entry?;
            let source_path = entry.path();
            let destination_path = destination.join(entry.file_name());
            if entry.file_type()?.is_dir() {
                Self::copy_checkpoint(&source_path, &destination_path)?;
            } else {
                fs::copy(source_path, destination_path)?;
            }
        }
        File::open(destination)?.sync_all()?;
        Ok(())
    }

    pub(crate) fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        self.db
            .get(key.as_bytes())?
            .map(|bytes| rmp_serde::from_slice(&bytes).map_err(Into::into))
            .transpose()
    }

    pub(crate) fn scan<T: DeserializeOwned>(&self, prefix: &str) -> Result<Vec<(String, T)>> {
        self.scan_limit(prefix, usize::MAX)
    }

    pub(crate) fn keys(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        for item in self.db.iterator(IteratorMode::From(
            prefix.as_bytes(),
            rocksdb::Direction::Forward,
        )) {
            let (key, _) = item?;
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }
            keys.push(String::from_utf8(key.to_vec())?);
        }
        Ok(keys)
    }

    pub(crate) fn scan_limit<T: DeserializeOwned>(
        &self,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<(String, T)>> {
        let mut values = Vec::new();
        if limit == 0 {
            return Ok(values);
        }
        for item in self.db.iterator(IteratorMode::From(
            prefix.as_bytes(),
            rocksdb::Direction::Forward,
        )) {
            let (key, value) = item?;
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }
            values.push((
                String::from_utf8(key.to_vec())?,
                rmp_serde::from_slice(&value)?,
            ));
            if values.len() >= limit {
                break;
            }
        }
        Ok(values)
    }

    pub(crate) fn commit(&self, mutations: Vec<Mutation>) -> Result<()> {
        self.commit_shard(0, mutations)
    }

    pub(crate) fn sync_remote_shard(&self, shard: usize) -> Result<()> {
        let Some(journal) = &self.journal else {
            return Ok(());
        };
        let mut current = self
            .sequences
            .get(shard)
            .ok_or_else(|| anyhow!("log shard {shard} does not exist"))?
            .lock()
            .map_err(|_| anyhow!("sequence lock poisoned"))?;
        let records = journal.sync(u32::try_from(shard)?, *current)?;
        for recovered in records {
            let record: WalRecord = rmp_serde::from_slice(&recovered.payload)?;
            if record.shard as usize != shard
                || record.sequence != recovered.position
                || record.sequence != *current + 1
            {
                bail!("remote WAL shard {shard} returned a non-contiguous record");
            }
            Self::apply_record(&self.db, &record, shard)?;
            *current = record.sequence;
        }
        Ok(())
    }

    pub(crate) fn sync_all_remote(&self) -> Result<()> {
        for shard in 0..self.sequences.len() {
            self.sync_remote_shard(shard)?;
        }
        Ok(())
    }

    pub(crate) fn commit_shard(&self, shard: usize, mutations: Vec<Mutation>) -> Result<()> {
        if mutations.is_empty() {
            return Ok(());
        }
        let mut current = self
            .sequences
            .get(shard)
            .ok_or_else(|| anyhow!("log shard {shard} does not exist"))?
            .lock()
            .map_err(|_| anyhow!("sequence lock poisoned"))?;
        let sequence = *current + 1;
        let mut record = WalRecord {
            format: 2,
            shard: u32::try_from(shard)?,
            sequence,
            mutations: Vec::new(),
            puts: Vec::new(),
            deletes: Vec::new(),
            delete_ranges: Vec::new(),
        };
        for mutation in mutations {
            match mutation.op.as_str() {
                "put" => {
                    let value = match mutation.encoded_value {
                        Some(value) => value,
                        None => rmp_serde::to_vec_named(
                            mutation
                                .value
                                .as_ref()
                                .ok_or_else(|| anyhow!("put mutation has no value"))?,
                        )?,
                    };
                    record.puts.push((mutation.key, value));
                }
                "delete" => record.deletes.push(mutation.key),
                "delete_range" => record.delete_ranges.push((
                    mutation.key,
                    mutation
                        .end_key
                        .ok_or_else(|| anyhow!("range deletion has no end key"))?,
                )),
                operation => bail!("unknown mutation operation: {operation}"),
            }
        }
        let shard_dir = self.wal_dir.join(format!("shard-{shard:04}"));
        let final_path = shard_dir.join(format!("{sequence:020}.mpk"));
        let temporary = shard_dir.join(format!(".{sequence:020}-{}.tmp", Uuid::new_v4()));
        let encoded = rmp_serde::to_vec_named(&record)?;
        if let Some(journal) = &self.journal {
            let owner_epoch = if shard == 0 {
                1
            } else {
                self.owner_epoch_for(shard, &record)?
            };
            let position = journal.append(u32::try_from(shard)?, owner_epoch, encoded)?;
            if position != sequence {
                bail!("remote WAL shard {shard} advanced to {position}, expected {sequence}");
            }
            Self::apply_record(&self.db, &record, shard)?;
            *current = sequence;
            return Ok(());
        }
        let mut file = File::create(&temporary)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
        fs::rename(&temporary, &final_path)?;
        File::open(&shard_dir)?.sync_all()?;
        Self::apply_record(&self.db, &record, shard)?;
        *current = sequence;
        Ok(())
    }

    fn owner_epoch_for(&self, shard: usize, record: &WalRecord) -> Result<u64> {
        let owner_key = process_partition_owner_key(shard);
        for (key, value) in &record.puts {
            if key == &owner_key {
                return Ok(rmp_serde::from_slice::<ProcessPartitionOwner>(value)?.epoch);
            }
        }
        self.get::<ProcessPartitionOwner>(&owner_key)?
            .map(|owner| owner.epoch)
            .ok_or_else(|| anyhow!("process partition {shard} has no durable owner epoch"))
    }

    pub(crate) fn apply_record(db: &DB, record: &WalRecord, shard: usize) -> Result<()> {
        let mut batch = WriteBatch::default();
        if record.format == 1 {
            for mutation in &record.mutations {
                match mutation.op.as_str() {
                    "put" => {
                        let value = match mutation.encoded_value.as_ref() {
                            Some(value) => value.clone(),
                            None => rmp_serde::to_vec_named(
                                mutation
                                    .value
                                    .as_ref()
                                    .ok_or_else(|| anyhow!("put mutation has no value"))?,
                            )?,
                        };
                        batch.put(mutation.key.as_bytes(), value);
                    }
                    "delete" => batch.delete(mutation.key.as_bytes()),
                    "delete_range" => batch.delete_range(
                        mutation.key.as_bytes(),
                        mutation
                            .end_key
                            .as_deref()
                            .ok_or_else(|| anyhow!("range deletion has no end key"))?
                            .as_bytes(),
                    ),
                    operation => bail!("unknown mutation operation: {operation}"),
                }
            }
        } else if record.format == 2 {
            for (key, value) in &record.puts {
                batch.put(key.as_bytes(), value);
            }
            for key in &record.deletes {
                batch.delete(key.as_bytes());
            }
            for (start, end) in &record.delete_ranges {
                batch.delete_range(start.as_bytes(), end.as_bytes());
            }
        } else {
            bail!("unsupported WAL format: {}", record.format);
        }
        batch.put(
            format!("meta/applied_sequence/{shard:04}").as_bytes(),
            rmp_serde::to_vec(&record.sequence)?,
        );
        let mut options = WriteOptions::default();
        // The object WAL is synced before this cache update and is replayed after a crash.
        options.set_sync(false);
        options.disable_wal(true);
        db.write_opt(batch, &options)?;
        Ok(())
    }

    pub(crate) fn prepare_checkpoint(&self) -> Result<CheckpointManifest> {
        self.sync_all_remote()?;
        let currents = self
            .sequences
            .iter()
            .map(|sequence| {
                sequence
                    .lock()
                    .map_err(|_| anyhow!("sequence lock poisoned"))
            })
            .collect::<Result<Vec<_>>>()?;
        let shard_sequences = currents
            .iter()
            .enumerate()
            .map(|(shard, sequence)| Ok((u32::try_from(shard)?, **sequence)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        let total_sequence = shard_sequences.values().sum();
        self.db.flush()?;
        let checkpoint_id = format!("{total_sequence:020}-{}", Uuid::new_v4());
        let final_path = self.checkpoint_dir.join(&checkpoint_id);
        let temporary = self.checkpoint_dir.join(format!(".{checkpoint_id}.tmp"));
        Checkpoint::new(&self.db)?.create_checkpoint(&temporary)?;
        fs::rename(&temporary, &final_path)?;
        File::open(&self.checkpoint_dir)?.sync_all()?;
        let manifest = CheckpointManifest {
            format: 1,
            checkpoint_id,
            sequence: total_sequence,
            shard_sequences,
            created_at: now(),
            object_path: final_path.to_string_lossy().into_owned(),
            state_handles: BTreeMap::new(),
        };
        Ok(manifest)
    }

    pub(crate) fn publish_checkpoint(&self, manifest: &CheckpointManifest) -> Result<()> {
        if let Some(journal) = &self.journal {
            journal.publish_checkpoint(
                manifest.clone(),
                self.checkpoint_dir.join(&manifest.checkpoint_id),
            )?;
        }
        let manifest_temporary = self
            .manifest_path
            .with_extension(format!("{}.tmp", Uuid::new_v4()));
        let mut file = File::create(&manifest_temporary)?;
        file.write_all(&serde_json::to_vec(manifest)?)?;
        file.sync_all()?;
        fs::rename(&manifest_temporary, &self.manifest_path)?;
        File::open(
            self.manifest_path
                .parent()
                .ok_or_else(|| anyhow!("checkpoint manifest has no parent"))?,
        )?
        .sync_all()?;
        for (shard, checkpointed) in &manifest.shard_sequences {
            let shard_dir = self.wal_dir.join(format!("shard-{shard:04}"));
            for entry in fs::read_dir(&shard_dir)? {
                let path = entry?.path();
                let sequence = path
                    .file_stem()
                    .and_then(|value| value.to_str())
                    .and_then(|value| value.parse::<u64>().ok());
                if sequence.is_some_and(|sequence| sequence <= *checkpointed) {
                    fs::remove_file(path)?;
                }
            }
            File::open(shard_dir)?.sync_all()?;
        }
        let mut checkpoints: Vec<_> = fs::read_dir(&self.checkpoint_dir)?
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
            .collect();
        checkpoints.sort_by_key(|entry| entry.file_name());
        let remove_count = checkpoints.len().saturating_sub(2);
        for entry in checkpoints.into_iter().take(remove_count) {
            fs::remove_dir_all(entry.path())?;
        }
        self.db.compact_range::<&[u8], &[u8]>(None, None);
        Ok(())
    }

    pub(crate) fn checkpoint(&self) -> Result<CheckpointManifest> {
        let manifest = self.prepare_checkpoint()?;
        self.publish_checkpoint(&manifest)?;
        Ok(manifest)
    }

    pub(crate) fn checkpoint_if_needed(
        &self,
        transitions: u64,
    ) -> Result<Option<CheckpointManifest>> {
        let sequences = self
            .sequences
            .iter()
            .map(|sequence| {
                sequence
                    .lock()
                    .map(|value| *value)
                    .map_err(|_| anyhow!("sequence lock poisoned"))
            })
            .collect::<Result<Vec<_>>>()?;
        let checkpointed = Self::read_manifest(&self.manifest_path)?
            .map(|manifest| manifest.shard_sequences)
            .unwrap_or_default();
        let uncheckpointed = sequences
            .iter()
            .enumerate()
            .map(|(shard, sequence)| {
                sequence.saturating_sub(checkpointed.get(&(shard as u32)).copied().unwrap_or(0))
            })
            .sum::<u64>();
        if uncheckpointed < transitions {
            return Ok(None);
        }
        self.checkpoint().map(Some)
    }
}
