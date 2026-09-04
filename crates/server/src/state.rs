use crate::*;
pub(crate) type QueryResultSender = oneshot::Sender<Result<Value, String>>;

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) store: Arc<DurableStore>,
    pub(crate) mutation_lock: Arc<Mutex<()>>,
    pub(crate) shard_locks: Arc<Vec<Mutex<()>>>,
    pub(crate) partition_senders: Arc<Vec<Option<mpsc::Sender<ProcessPartitionCommand>>>>,
    pub(crate) node_id: String,
    pub(crate) runtime_id: String,
    pub(crate) endpoint: String,
    pub(crate) control_plane: bool,
    pub(crate) execution_identities: Arc<Vec<ExecutionIdentity>>,
    pub(crate) cluster_token: Option<String>,
    pub(crate) http_client: HttpClient,
    pub(crate) key_group_count: u32,
    pub(crate) lease_seconds: f64,
    pub(crate) query_queue: Arc<Mutex<VecDeque<(String, QueryTask)>>>,
    pub(crate) query_results: Arc<Mutex<HashMap<String, QueryResultSender>>>,
}

impl AppState {
    pub(crate) fn authorize_poll(&self, request: &PollRequest) -> Result<()> {
        if self.execution_identities.is_empty() {
            return Ok(());
        }
        let token = request
            .execution_token
            .as_deref()
            .ok_or_else(|| anyhow!("execution identity is required"))?;
        let identity = self
            .execution_identities
            .iter()
            .find(|identity| constant_time_equal(identity.token.as_bytes(), token.as_bytes()))
            .ok_or_else(|| anyhow!("execution identity is invalid"))?;
        if request.task_queue.as_deref() != Some(identity.task_queue.as_str())
            || request
                .build_ids
                .iter()
                .any(|build_id| !identity.build_ids.contains(build_id))
        {
            bail!("execution identity is not authorized for this deployment");
        }
        Ok(())
    }

    pub(crate) fn authorize_cluster(&self, headers: &HeaderMap) -> Result<()> {
        let expected = self
            .cluster_token
            .as_deref()
            .ok_or_else(|| anyhow!("cluster transport is disabled"))?;
        let supplied = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .ok_or_else(|| anyhow!("cluster identity is required"))?;
        if !constant_time_equal(expected.as_bytes(), supplied.as_bytes()) {
            bail!("cluster identity is invalid");
        }
        Ok(())
    }

    pub(crate) fn commit<F>(&self, operation: F) -> Result<()>
    where
        F: FnOnce(&mut Transaction<'_>) -> Result<()>,
    {
        let _guard = self
            .mutation_lock
            .lock()
            .map_err(|_| anyhow!("mutation lock poisoned"))?;
        self.store.sync_remote_shard(0)?;
        let mut transaction = Transaction {
            store: &self.store,
            changes: BTreeMap::new(),
            encoded_changes: BTreeMap::new(),
            range_deletions: Vec::new(),
            defer_process_dispatch: false,
        };
        operation(&mut transaction)?;
        self.store.commit(transaction.into_mutations())
    }

    pub(crate) fn commit_shard<F>(&self, shard: usize, operation: F) -> Result<()>
    where
        F: FnOnce(&mut Transaction<'_>) -> Result<()>,
    {
        if shard == 0 {
            return self.commit(operation);
        }
        let _guard = self
            .shard_locks
            .get(shard)
            .ok_or_else(|| anyhow!("log shard {shard} does not exist"))?
            .lock()
            .map_err(|_| anyhow!("shard mutation lock poisoned"))?;
        self.store.sync_remote_shard(0)?;
        self.store.sync_remote_shard(shard)?;
        let mut transaction = Transaction {
            store: &self.store,
            changes: BTreeMap::new(),
            encoded_changes: BTreeMap::new(),
            range_deletions: Vec::new(),
            defer_process_dispatch: false,
        };
        operation(&mut transaction)?;
        self.store.commit_shard(shard, transaction.into_mutations())
    }

    pub(crate) fn process_shard(&self, key: &str) -> usize {
        if self.shard_locks.len() <= 1 {
            return 0;
        }
        1 + key_group_for(
            Some(key),
            0,
            u32::try_from(self.shard_locks.len() - 1).unwrap(),
        ) as usize
    }

    // Only use for replayable operations with no effects outside the transaction.
    // Rebuild mutations from synchronized state; never retry a stale mutation list.
    pub(crate) fn commit_output<F, T>(&self, shard: usize, mut operation: F) -> Result<T>
    where
        F: FnMut(&mut Transaction<'_>) -> Result<T>,
    {
        for attempt in 0..8 {
            let mut result = None;
            match self.commit_shard(shard, |transaction| {
                result = Some(operation(transaction)?);
                Ok(())
            }) {
                Ok(()) => return Ok(result.expect("output transaction committed")),
                Err(error)
                    if attempt < 7
                        && error.to_string().contains("conditional append was fenced") => {}
                Err(error) => return Err(error),
            }
        }
        unreachable!("last attempt returns its result")
    }
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    for index in 0..left.len().max(right.len()) {
        let left = left.get(index).copied().unwrap_or(0);
        let right = right.get(index).copied().unwrap_or(0);
        difference |= usize::from(left ^ right);
    }
    difference == 0
}

pub(crate) struct Transaction<'a> {
    pub(crate) store: &'a DurableStore,
    pub(crate) changes: BTreeMap<String, Option<Value>>,
    pub(crate) encoded_changes: BTreeMap<String, Vec<u8>>,
    pub(crate) range_deletions: Vec<(String, String)>,
    pub(crate) defer_process_dispatch: bool,
}

impl Transaction<'_> {
    pub(crate) fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        if let Some(value) = self.encoded_changes.get(key) {
            return Ok(Some(rmp_serde::from_slice(value)?));
        }
        if let Some(value) = self.changes.get(key) {
            return value
                .as_ref()
                .map(|value| serde_json::from_value(value.clone()).map_err(Into::into))
                .transpose();
        }
        self.store.get(key)
    }

    pub(crate) fn multi_get<T: DeserializeOwned>(&self, keys: &[String]) -> Result<Vec<Option<T>>> {
        let mut values: Vec<Option<T>> = (0..keys.len()).map(|_| None).collect();
        let mut missing = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            if let Some(value) = self.encoded_changes.get(key) {
                values[index] = Some(rmp_serde::from_slice(value)?);
            } else if let Some(value) = self.changes.get(key) {
                values[index] = value
                    .as_ref()
                    .map(|value| serde_json::from_value(value.clone()).map_err(anyhow::Error::from))
                    .transpose()?;
            } else {
                missing.push((index, key));
            }
        }
        let stored = self
            .store
            .db
            .multi_get(missing.iter().map(|(_, key)| key.as_bytes()));
        for ((index, _), value) in missing.into_iter().zip(stored) {
            values[index] = value?
                .map(|value| rmp_serde::from_slice(&value).map_err(anyhow::Error::from))
                .transpose()?;
        }
        Ok(values)
    }

    pub(crate) fn scan<T: DeserializeOwned>(&self, prefix: &str) -> Result<Vec<(String, T)>> {
        let mut values: BTreeMap<String, T> = self.store.scan(prefix)?.into_iter().collect();
        for (key, value) in self.changes.range(prefix.to_owned()..) {
            if !key.starts_with(prefix) {
                break;
            }
            if let Some(value) = value {
                values.insert(key.clone(), serde_json::from_value(value.clone())?);
            } else {
                values.remove(key);
            }
        }
        for (key, value) in self.encoded_changes.range(prefix.to_owned()..) {
            if !key.starts_with(prefix) {
                break;
            }
            values.insert(key.clone(), rmp_serde::from_slice(value)?);
        }
        Ok(values.into_iter().collect())
    }

    pub(crate) fn scan_limit<T: DeserializeOwned>(
        &self,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<(String, T)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut values = BTreeMap::new();
        for item in self.store.db.iterator(IteratorMode::From(
            prefix.as_bytes(),
            rocksdb::Direction::Forward,
        )) {
            let (key, value) = item?;
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }
            let key = String::from_utf8(key.to_vec())?;
            match (self.changes.get(&key), self.encoded_changes.get(&key)) {
                (Some(Some(value)), _) => {
                    values.insert(key, serde_json::from_value(value.clone())?);
                }
                (Some(None), _) => {}
                (None, Some(value)) => {
                    values.insert(key, rmp_serde::from_slice(value)?);
                }
                (None, None) => {
                    values.insert(key, rmp_serde::from_slice(&value)?);
                }
            }
            if values.len() >= limit {
                break;
            }
        }
        for (key, value) in self.changes.range(prefix.to_owned()..) {
            if !key.starts_with(prefix) {
                break;
            }
            if let Some(value) = value {
                values.insert(key.clone(), serde_json::from_value(value.clone())?);
            } else {
                values.remove(key);
            }
        }
        for (key, value) in self.encoded_changes.range(prefix.to_owned()..) {
            if !key.starts_with(prefix) {
                break;
            }
            values.insert(key.clone(), rmp_serde::from_slice(value)?);
        }
        Ok(values.into_iter().take(limit).collect())
    }

    pub(crate) fn put<T: Serialize>(&mut self, key: impl Into<String>, value: &T) -> Result<()> {
        let key = key.into();
        self.changes.remove(&key);
        self.encoded_changes
            .insert(key, rmp_serde::to_vec_named(value)?);
        Ok(())
    }

    pub(crate) fn put_encoded<T: Serialize>(
        &mut self,
        key: impl Into<String>,
        value: &T,
    ) -> Result<()> {
        self.put(key, value)
    }

    pub(crate) fn delete(&mut self, key: impl Into<String>) {
        let key = key.into();
        self.encoded_changes.remove(&key);
        self.changes.insert(key, None);
    }

    pub(crate) fn delete_range(&mut self, start: impl Into<String>, end: impl Into<String>) {
        self.range_deletions.push((start.into(), end.into()));
    }

    pub(crate) fn into_mutations(self) -> Vec<Mutation> {
        let mut mutations: Vec<_> = self
            .changes
            .into_iter()
            .map(|(key, value)| Mutation {
                op: if value.is_some() { "put" } else { "delete" }.to_owned(),
                key,
                end_key: None,
                value,
                encoded_value: None,
            })
            .collect();
        mutations.extend(
            self.encoded_changes
                .into_iter()
                .map(|(key, encoded_value)| Mutation {
                    op: "put".to_owned(),
                    key,
                    end_key: None,
                    value: None,
                    encoded_value: Some(encoded_value),
                }),
        );
        mutations.extend(
            self.range_deletions
                .into_iter()
                .map(|(key, end_key)| Mutation {
                    op: "delete_range".to_owned(),
                    key,
                    end_key: Some(end_key),
                    value: None,
                    encoded_value: None,
                }),
        );
        mutations
    }
}
