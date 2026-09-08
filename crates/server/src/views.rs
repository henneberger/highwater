//! Read projections of native operator changelogs at a durable, named cut.
use crate::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ViewRow {
    key: Option<String>,
    value: Value,
    count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ViewSnapshot {
    snapshot_id: String,
    created_at: f64,
    consistency: String,
    input_positions: BTreeMap<String, Vec<PartitionState>>,
    views: BTreeMap<String, Vec<ViewRow>>,
}

#[derive(Deserialize)]
pub(crate) struct ViewSnapshotRequest {
    operators: Vec<String>,
}

fn materialize(changes: Vec<DifferentialChange>) -> Result<Vec<ViewRow>> {
    let mut rows: BTreeMap<String, ViewRow> = BTreeMap::new();
    for change in changes {
        let identity = serde_json::to_string(&(&change.key, &change.row))?;
        let row = rows.entry(identity).or_insert(ViewRow {
            key: change.key,
            value: change.row,
            count: 0,
        });
        row.count = row
            .count
            .checked_add(change.diff)
            .ok_or_else(|| anyhow!("view row multiplicity overflow"))?;
    }
    if rows.values().any(|row| row.count < 0) {
        bail!("view contains unmatched retractions; publish balanced row changes before reading");
    }
    Ok(rows.into_values().filter(|row| row.count > 0).collect())
}

fn capture(transaction: &Transaction<'_>, operators: &[String]) -> Result<ViewSnapshot> {
    if operators.is_empty() || operators.len() > 32 {
        bail!("snapshot requires between 1 and 32 operators");
    }
    let mut ids = operators.to_vec();
    ids.sort();
    ids.dedup();
    if ids.len() != operators.len() || ids.iter().any(|id| id.trim().is_empty()) {
        bail!("snapshot operator identifiers must be nonempty and unique");
    }
    let mut views = BTreeMap::new();
    let mut input_positions = BTreeMap::new();
    let mut shared_filter_stream = None;
    let mut shared_records_received = None;
    for id in ids {
        // Process lanes execute asynchronously and cannot share this control-shard cut.
        if transaction
            .get::<DurableProcess>(&process_key(&id))?
            .is_some()
        {
            bail!("Process outputs do not support view snapshots yet");
        }
        // Operator types have separate registration namespaces but share a changelog.
        // Refuse collisions rather than assign another operator's rows to this view.
        let definitions = [
            stream_schedule_key(&id),
            stream_filter_key(&id),
            deduplicate_key(&id),
            temporal_join_key(&id),
            interval_join_key(&id),
        ]
        .into_iter()
        .map(|key| transaction.get::<Value>(&key))
        .collect::<Result<Vec<_>>>()?;
        if definitions
            .iter()
            .filter(|definition| definition.is_some())
            .count()
            > 1
        {
            bail!("view operator identifier is ambiguous across operator types: {id}");
        }
        let inputs = operator_input_streams(transaction, &id)?;
        if operators.len() > 1 {
            let filter = transaction
                .get::<StreamFilter>(&stream_filter_key(&id))?
                .ok_or_else(|| {
                    anyhow!("multi-view snapshots require native filters over the same stream")
                })?;
            if shared_filter_stream
                .as_ref()
                .is_some_and(|stream| stream != &filter.stream)
            {
                bail!("multi-view snapshots require native filters over the same stream");
            }
            if filter.status != "ACTIVE" {
                bail!("multi-view snapshots require active native filters");
            }
            if shared_records_received.is_some_and(|count| count != filter.records_received) {
                bail!("multi-view snapshots require equal historical input coverage");
            }
            shared_records_received = Some(filter.records_received);
            shared_filter_stream = Some(filter.stream);
        }
        for stream in inputs {
            let config = transaction
                .get::<StreamConfig>(&stream_config_key(&stream))?
                .ok_or_else(|| anyhow!("view input stream missing: {stream}"))?;
            let partitions = (0..config.partitions)
                .map(|partition| {
                    transaction
                        .get::<PartitionState>(&stream_partition_key(&stream, partition))?
                        .ok_or_else(|| anyhow!("view input partition missing"))
                })
                .collect::<Result<Vec<_>>>()?;
            input_positions.insert(stream, partitions);
        }
        let changes = transaction
            .scan::<DifferentialChange>(&operator_change_prefix(&id))?
            .into_iter()
            .map(|(_, change)| change)
            .collect();
        let rows = materialize(changes)?;
        if rows.len() > 10_000 {
            bail!("view snapshot exceeds 10000 distinct rows per operator");
        }
        views.insert(id, rows);
    }
    Ok(ViewSnapshot {
        snapshot_id: Uuid::new_v4().to_string(),
        created_at: now(),
        consistency: if operators.len() > 1 {
            "shared_input_cut"
        } else {
            "operator_output_cut"
        }
        .into(),
        input_positions,
        views,
    })
}

pub(crate) async fn create_view_snapshot(
    State(app): State<AppState>,
    Json(request): Json<ViewSnapshotRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let snapshot = app.commit_output(0, |transaction| {
        let snapshot = capture(transaction, &request.operators)?;
        transaction.put(
            format!("view-snapshot/{}", encoded(&snapshot.snapshot_id)),
            &snapshot,
        )?;
        Ok(snapshot)
    })?;
    Ok((StatusCode::CREATED, Json(snapshot)))
}

pub(crate) async fn read_view_snapshot(
    State(app): State<AppState>,
    Path(snapshot_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let snapshot = app.commit_output(0, |transaction| {
        transaction
            .get::<ViewSnapshot>(&format!("view-snapshot/{}", encoded(&snapshot_id)))?
            .ok_or_else(|| anyhow!("view snapshot not found: {snapshot_id}"))
    })?;
    Ok(Json(snapshot))
}

pub(crate) async fn delete_view_snapshot(
    State(app): State<AppState>,
    Path(snapshot_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    app.commit_output(0, |transaction| {
        transaction.delete(format!("view-snapshot/{}", encoded(&snapshot_id)));
        Ok(())
    })?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn change(kind: ChangeKind, row: Value) -> DifferentialChange {
        DifferentialChange {
            operator_id: "view".into(),
            sequence: 1,
            key: Some("a".into()),
            event_time: 1.0,
            kind,
            diff: kind.weight(),
            row,
        }
    }

    #[test]
    fn capture_rejects_invalid_and_ambiguous_operator_sets() -> Result<()> {
        let root = std::env::temp_dir().join(format!("highwater-view-boundary-{}", Uuid::new_v4()));
        let result = (|| -> Result<()> {
            let store = DurableStore::open_sharded_with_journal(
                &root.join("state"),
                &root.join("objects"),
                1,
                None,
            )?;
            let mut transaction = Transaction {
                store: &store,
                changes: BTreeMap::new(),
                encoded_changes: BTreeMap::new(),
                range_deletions: Vec::new(),
                defer_process_dispatch: false,
            };
            assert!(capture(&transaction, &[]).is_err());
            assert!(capture(&transaction, &["same".into(), "same".into()]).is_err());
            assert!(capture(&transaction, &["missing".into()]).is_err());
            let config: StreamConfig = serde_json::from_value(json!({
                "name": "source", "created_at": 0.0,
            }))?;
            transaction.put(stream_config_key("source"), &config)?;
            transaction.put(
                stream_partition_key("source", 0),
                &PartitionState::new(0, 0.0),
            )?;
            let mut filter: StreamFilter = serde_json::from_value(json!({
                "operator_id": "a", "stream": "source", "workflow_type": "sink",
                "task_queue": "default", "field": "amount", "comparison": "greater_than",
                "operand": 0, "status": "ACTIVE", "created_at": 0.0,
                "records_received": 3, "records_emitted": 0,
            }))?;
            transaction.put(stream_filter_key("a"), &filter)?;
            filter.operator_id = "b".into();
            transaction.put(stream_filter_key("b"), &filter)?;
            let ids = vec!["a".into(), "b".into()];
            assert_eq!(capture(&transaction, &ids)?.consistency, "shared_input_cut");
            filter.records_received = 2;
            transaction.put(stream_filter_key("b"), &filter)?;
            assert!(
                capture(&transaction, &ids)
                    .unwrap_err()
                    .to_string()
                    .contains("historical input coverage")
            );
            filter.records_received = 3;
            filter.stream = "another-source".into();
            transaction.put(stream_filter_key("b"), &filter)?;
            assert!(
                capture(&transaction, &ids)
                    .unwrap_err()
                    .to_string()
                    .contains("same stream")
            );
            transaction.put(stream_filter_key("collision"), &json!({}))?;
            transaction.put(deduplicate_key("collision"), &json!({}))?;
            let error = capture(&transaction, &["collision".into()]).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("ambiguous across operator types")
            );
            Ok(())
        })();
        let _ = fs::remove_dir_all(root);
        result
    }

    #[test]
    fn materialization_retracts_old_rows_and_preserves_multiplicity() -> Result<()> {
        let rows = materialize(vec![
            change(ChangeKind::Insert, json!({"total": 1})),
            change(ChangeKind::UpdateBefore, json!({"total": 1})),
            change(ChangeKind::UpdateAfter, json!({"total": 2})),
            change(ChangeKind::Insert, json!({"total": 2})),
        ])?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].value, json!({"total": 2}));
        assert_eq!(rows[0].count, 2);
        assert!(materialize(vec![change(ChangeKind::Delete, json!(1))]).is_err());
        Ok(())
    }
}
