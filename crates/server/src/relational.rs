//! Native collection operators. State, maintained output and edge deltas share a transaction.
use crate::*;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CollectionMode {
    Append,
    Retract,
    Upsert,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RelationalKind {
    Normalize,
    TopN,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RelationalSpec {
    pub(crate) operator_id: String,
    pub(crate) stream: String,
    pub(crate) kind: RelationalKind,
    pub(crate) input_mode: CollectionMode,
    #[serde(default)]
    pub(crate) primary_key: Vec<String>,
    #[serde(default)]
    pub(crate) partition_by: Vec<String>,
    #[serde(default)]
    pub(crate) order_by: Vec<(String, SortDirection)>,
    #[serde(default)]
    pub(crate) n: Option<usize>,
}

impl RelationalSpec {
    fn validate(&self) -> Result<()> {
        if self.operator_id.trim().is_empty() || self.stream.trim().is_empty() {
            bail!("operator_id and stream must not be empty");
        }
        for fields in [&self.primary_key, &self.partition_by] {
            if fields.iter().any(|s| s.trim().is_empty())
                || fields.iter().collect::<HashSet<_>>().len() != fields.len()
            {
                bail!("key fields must be nonempty and unique");
            }
        }
        if self.input_mode == CollectionMode::Upsert && self.primary_key.is_empty() {
            bail!("upsert mode requires a primary_key");
        }
        match self.kind {
            RelationalKind::Normalize => {
                if self.primary_key.is_empty()
                    || self.input_mode == CollectionMode::Append
                    || self.n.is_some()
                    || !self.order_by.is_empty()
                    || !self.partition_by.is_empty()
                {
                    bail!("normalize requires primary_key fields and upsert or retract mode");
                }
            }
            RelationalKind::TopN => {
                if !self.n.is_some_and(|n| (1..=10_000).contains(&n)) || self.order_by.is_empty() {
                    bail!("top_n requires n between 1 and 10000 and order_by fields");
                }
                let names: Vec<_> = self.order_by.iter().map(|(field, _)| field).collect();
                if names.iter().any(|s| s.trim().is_empty())
                    || names.iter().collect::<HashSet<_>>().len() != names.len()
                {
                    bail!("sort fields must be nonempty and unique");
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct CollectionRow {
    pub(crate) key: Option<String>,
    pub(crate) value: Value,
    pub(crate) event_time: f64,
    pub(crate) count: i64,
}

pub(crate) fn relational_key(id: &str) -> String {
    format!("relational/{}", encoded(id))
}
fn state_prefix(id: &str, namespace: &str) -> String {
    format!("collection/{}/{namespace}/", encoded(id))
}
pub(crate) fn maintained_prefix(id: &str) -> String {
    state_prefix(id, "output")
}
fn row_identity(row: &CollectionRow) -> Result<String> {
    Ok(serde_json::to_string(&(
        &row.key,
        &row.value,
        row.event_time,
    ))?)
}
fn project(value: &Value, fields: &[String]) -> Result<Vec<Value>> {
    fields
        .iter()
        .map(|field| {
            field_value(value, field)
                .cloned()
                .ok_or_else(|| anyhow!("missing field: {field}"))
        })
        .collect()
}
fn primary_identity(spec: &RelationalSpec, value: &Value) -> Result<String> {
    let values = project(value, &spec.primary_key)?;
    if values
        .iter()
        .any(|v| v.is_null() || v.is_array() || v.is_object())
    {
        bail!("primary key fields must be non-null scalars");
    }
    Ok(encoded(&serde_json::to_string(&values)?))
}
fn group_key(spec: &RelationalSpec, row: &CollectionRow) -> Result<Option<String>> {
    let values = project(&row.value, &spec.partition_by)?;
    if values.iter().any(|v| v.is_array() || v.is_object()) {
        bail!("partition fields must be scalars");
    }
    Ok(Some(serde_json::to_string(&values)?))
}

// Sorting is byte-ordered in RocksDB. Strings use a terminator below every hex digit;
// reversing also reverses that terminator, so prefixes sort correctly in either direction.
fn scalar_sort(value: &Value, direction: SortDirection) -> Result<(String, Option<&'static str>)> {
    if value.is_null() {
        return Ok(("1".into(), None));
    } // nulls last in both directions
    let (mut text, kind) = match value {
        Value::String(value) => (
            value
                .as_bytes()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
                + "!",
            "string",
        ),
        Value::Bool(value) => (if *value { "1" } else { "0" }.into(), "bool"),
        Value::Number(value) => {
            let number = value
                .as_f64()
                .ok_or_else(|| anyhow!("invalid numeric sort value"))?;
            if !number.is_finite()
                || value.is_i64() && value.as_i64().unwrap().unsigned_abs() > (1_u64 << 53)
                || value.is_u64() && value.as_u64().unwrap() > (1_u64 << 53)
            {
                bail!(
                    "numeric sort values must be finite; integer sort values must be within +/-2^53"
                );
            }
            let bits = (if number == 0.0 { 0.0 } else { number }).to_bits();
            let ordered = if bits >> 63 == 1 {
                !bits
            } else {
                bits ^ (1 << 63)
            };
            (format!("{ordered:016x}"), "number")
        }
        _ => bail!("sort values must be scalar or null"),
    };
    if direction == SortDirection::Desc {
        text = text
            .bytes()
            .map(|b| {
                if b == b'!' {
                    '~'
                } else {
                    char::from_digit(15 - (b as char).to_digit(16).unwrap(), 16).unwrap()
                }
            })
            .collect();
    }
    Ok((format!("0{text}"), Some(kind)))
}

fn candidate_key(
    transaction: &mut Transaction<'_>,
    spec: &RelationalSpec,
    row: &CollectionRow,
) -> Result<String> {
    let mut sort = String::new();
    for (field, direction) in &spec.order_by {
        let value =
            field_value(&row.value, field).ok_or_else(|| anyhow!("missing sort field: {field}"))?;
        let (component, kind) = scalar_sort(value, *direction)?;
        if let Some(kind) = kind {
            let key = format!(
                "{}{}",
                state_prefix(&spec.operator_id, "sort-type"),
                encoded(field)
            );
            if transaction
                .get::<String>(&key)?
                .is_some_and(|prior| prior != kind)
            {
                bail!("sort field {field} changed scalar type");
            }
            transaction.put(key, &kind)?;
        }
        sort.push_str(&component);
        sort.push('|');
    }
    // Full row identity is a stable final tie-breaker, including original row time.
    Ok(format!(
        "{}{sort}/{}",
        candidate_prefix(spec, row.key.as_deref()),
        encoded(&row_identity(row)?)
    ))
}
fn candidate_prefix(spec: &RelationalSpec, group: Option<&str>) -> String {
    format!(
        "{}{}/",
        state_prefix(&spec.operator_id, "ordered"),
        encoded(group.unwrap_or(""))
    )
}

fn winners(
    transaction: &Transaction<'_>,
    spec: &RelationalSpec,
    group: Option<&str>,
) -> Result<BTreeMap<String, CollectionRow>> {
    let mut remaining = spec.n.expect("validated top_n") as i64;
    let mut result = BTreeMap::new();
    for (key, mut row) in transaction
        .scan_limit::<CollectionRow>(&candidate_prefix(spec, group), remaining as usize)?
    {
        row.count = row.count.min(remaining);
        remaining -= row.count;
        result.insert(key, row);
        if remaining == 0 {
            break;
        }
    }
    Ok(result)
}

// Both candidate arrangements and maintained outputs use the same checked bag update.
fn update_counted_row(
    transaction: &mut Transaction<'_>,
    key: String,
    row: &CollectionRow,
    diff: i64,
) -> Result<()> {
    let mut current = transaction
        .get::<CollectionRow>(&key)?
        .unwrap_or(CollectionRow {
            count: 0,
            ..row.clone()
        });
    current.count = current
        .count
        .checked_add(diff)
        .ok_or_else(|| anyhow!("row multiplicity overflow"))?;
    if current.count < 0 {
        bail!("collection received unmatched row retraction (including original event time)");
    }
    if current.count == 0 {
        transaction.delete(key);
    } else {
        transaction.put(key, &current)?;
    }
    Ok(())
}

// Shared maintained-result primitive. Each row retains its original time for deletion.
fn emit_delta(
    transaction: &mut Transaction<'_>,
    spec: &RelationalSpec,
    row: &CollectionRow,
    diff: i64,
) -> Result<()> {
    let key = format!(
        "{}{}/{}",
        maintained_prefix(&spec.operator_id),
        encoded(&serde_json::to_string(&row.key)?),
        encoded(&row_identity(row)?)
    );
    update_counted_row(transaction, key, row, diff)?;
    // The legacy transport is unit-weight. Deliberately encode every contribution rather
    // than produce a consolidated weight that its StreamRecord cannot represent.
    for _ in 0..diff.unsigned_abs() {
        append_operator_change(
            transaction,
            &spec.operator_id,
            row.key.clone(),
            row.event_time,
            if diff > 0 {
                ChangeKind::Insert
            } else {
                ChangeKind::Delete
            },
            row.value.clone(),
        )?;
    }
    Ok(())
}

fn normalize(
    transaction: &mut Transaction<'_>,
    spec: &RelationalSpec,
    record: &StreamRecord,
) -> Result<Vec<(CollectionRow, i64)>> {
    let current = CollectionRow {
        key: record.key.clone(),
        value: record.value.clone(),
        event_time: record.event_time,
        count: 1,
    };
    if spec.input_mode == CollectionMode::Append && record.kind != ChangeKind::Insert {
        bail!("append collection accepts only insert changes");
    }
    if spec.input_mode == CollectionMode::Retract && record.kind == ChangeKind::Upsert {
        bail!("retract collection does not accept ambiguous upsert; normalize keyed upserts first");
    }
    if spec.primary_key.is_empty() {
        return Ok(vec![(current, record.kind.weight())]);
    }
    let key = format!(
        "{}{}",
        state_prefix(&spec.operator_id, "primary"),
        primary_identity(spec, &record.value)?
    );
    let prior = transaction.get::<CollectionRow>(&key)?;
    if spec.input_mode == CollectionMode::Upsert {
        if record.kind == ChangeKind::UpdateBefore {
            bail!(
                "upsert collection does not accept update_before; use retract mode for full changelogs"
            );
        }
        if record.kind == ChangeKind::Delete {
            let prior = prior.ok_or_else(|| anyhow!("delete of unknown primary key"))?;
            transaction.delete(key);
            return Ok(vec![(prior, -1)]);
        }
        // Identical upserts do not move the logical row's original timestamp.
        if prior
            .as_ref()
            .is_some_and(|row| row.key == current.key && row.value == current.value)
        {
            return Ok(vec![]);
        }
        transaction.put(key, &current)?;
        let mut changes = prior.into_iter().map(|row| (row, -1)).collect::<Vec<_>>();
        changes.push((current, 1));
        return Ok(changes);
    }
    if record.kind.is_addition() {
        if prior.is_some() {
            bail!("duplicate primary key; retract the old row before inserting its replacement");
        }
        transaction.put(key, &current)?;
    } else {
        let prior = prior
            .filter(|row| row.key == current.key && row.value == current.value)
            .ok_or_else(|| anyhow!("retraction does not match primary key row"))?;
        transaction.delete(key);
        // A keyed changelog identifies the previous row independently of when the
        // correction arrived. Preserve that row's time for windows and interval joins.
        return Ok(vec![(prior, -1)]);
    }
    Ok(vec![(current, record.kind.weight())])
}

fn apply_record(
    transaction: &mut Transaction<'_>,
    spec: &RelationalSpec,
    record: &StreamRecord,
) -> Result<()> {
    let changes = normalize(transaction, spec, record)?;
    if spec.kind == RelationalKind::Normalize {
        for (row, diff) in changes {
            emit_delta(transaction, spec, &row, diff)?;
        }
        return Ok(());
    }
    let mut old = BTreeMap::new();
    let mut groups = HashSet::new();
    let mut grouped = Vec::new();
    for (mut row, diff) in changes {
        row.key = group_key(spec, &row)?;
        if groups.insert(row.key.clone()) {
            old.extend(winners(transaction, spec, row.key.as_deref())?);
        }
        grouped.push((row, diff));
    }
    for (row, diff) in grouped {
        let key = candidate_key(transaction, spec, &row)?;
        update_counted_row(transaction, key, &row, diff)?;
    }
    let mut new = BTreeMap::new();
    for group in groups {
        new.extend(winners(transaction, spec, group.as_deref())?);
    }
    for (key, row) in &old {
        let diff = new.get(key).map_or(0, |row| row.count) - row.count;
        if diff < 0 {
            emit_delta(transaction, spec, row, diff)?;
        }
    }
    for (key, row) in &new {
        let diff = row.count - old.get(key).map_or(0, |row| row.count);
        if diff > 0 {
            emit_delta(transaction, spec, row, diff)?;
        }
    }
    Ok(())
}

pub(crate) fn refresh_relational(
    transaction: &mut Transaction<'_>,
    record: Option<&StreamRecord>,
) -> Result<()> {
    if let Some(record) = record {
        for (_, id) in
            transaction.scan::<String>(&format!("relational-input/{}/", encoded(&record.stream)))?
        {
            let spec = transaction
                .get::<RelationalSpec>(&relational_key(&id))?
                .ok_or_else(|| anyhow!("relational operator missing"))?;
            apply_record(transaction, &spec, record)?;
        }
    }
    Ok(())
}

pub(crate) fn ensure_operator_id_available(transaction: &Transaction<'_>, id: &str) -> Result<()> {
    for key in [
        process_key(id),
        stream_schedule_key(id),
        stream_filter_key(id),
        deduplicate_key(id),
        temporal_join_key(id),
        interval_join_key(id),
    ] {
        if transaction.get::<Value>(&key)?.is_some() {
            bail!("operator identifier already used: {id}");
        }
    }
    Ok(())
}

pub(crate) fn reject_relational_id(transaction: &Transaction<'_>, id: &str) -> Result<()> {
    if transaction
        .get::<RelationalSpec>(&relational_key(id))?
        .is_some()
    {
        bail!("operator identifier already used by a collection: {id}");
    }
    Ok(())
}

pub(crate) async fn create_relational(
    State(app): State<AppState>,
    Json(spec): Json<RelationalSpec>,
) -> Result<impl IntoResponse, ApiError> {
    spec.validate()?;
    let created = app.commit_output(0, |transaction| {
        ensure_operator_id_available(transaction, &spec.operator_id)?;
        if let Some(existing) =
            transaction.get::<RelationalSpec>(&relational_key(&spec.operator_id))?
        {
            if existing != spec {
                bail!("operator already exists with a different specification");
            }
            return Ok(false);
        }
        if transaction
            .get::<StreamConfig>(&stream_config_key(&spec.stream))?
            .is_none()
        {
            bail!("input stream not found: {}", spec.stream);
        }
        transaction.put(relational_key(&spec.operator_id), &spec)?;
        validate_collection_edges(transaction)?;
        // Registration and backfill share the source's transaction; tail admission cannot interleave.
        let mut records: Vec<_> = transaction
            .scan::<StreamRecord>(&stream_record_prefix(&spec.stream))?
            .into_iter()
            .map(|(_, r)| r)
            .collect();
        records.sort_by_key(|record| record.sequence);
        // The retained stream prefix contains admitted records; this also includes
        // internal corrections whose historical timestamp is marked too_late.
        for record in records {
            apply_record(transaction, &spec, &record)?;
        }
        transaction.put(relational_key(&spec.operator_id), &spec)?;
        transaction.put(
            format!(
                "relational-input/{}/{}",
                encoded(&spec.stream),
                encoded(&spec.operator_id)
            ),
            &spec.operator_id,
        )?;
        validate_collection_edges(transaction)?;
        Ok(true)
    })?;
    Ok((
        if created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(spec),
    ))
}

pub(crate) async fn get_relational(
    State(app): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let spec = app.commit_output(0, |transaction| {
        transaction
            .get::<RelationalSpec>(&relational_key(&id))?
            .ok_or_else(|| anyhow!("relational operator not found: {id}"))
    })?;
    Ok(Json(
        json!({"spec": spec, "algorithm": if spec.kind == RelationalKind::TopN { "retractable_ordered_multiset" } else { "primary_key_normalizer" }, "output_mode": "retract", "retention": "all_live_rows", "frontier": "sealed_input_only"}),
    ))
}

#[derive(Deserialize)]
pub(crate) struct CollectionRead {
    key: Option<String>,
    limit: Option<usize>,
}
pub(crate) async fn read_collection_rows(
    State(app): State<AppState>,
    Path(id): Path<String>,
    axum::extract::Query(query): axum::extract::Query<CollectionRead>,
) -> Result<impl IntoResponse, ApiError> {
    let limit = query.limit.unwrap_or(10_000);
    if !(1..=10_000).contains(&limit) {
        return Err(ApiError(anyhow!("limit must be between 1 and 10000")));
    }
    let rows = app.commit_output(0, |transaction| {
        if transaction
            .get::<RelationalSpec>(&relational_key(&id))?
            .is_none()
        {
            bail!("relational operator not found: {id}");
        }
        let prefix = query.key.as_ref().map_or_else(
            || maintained_prefix(&id),
            |key| {
                format!(
                    "{}{}/",
                    maintained_prefix(&id),
                    encoded(&serde_json::to_string(&Some(key)).expect("string serialization"))
                )
            },
        );
        let rows = transaction.scan_limit::<CollectionRow>(&prefix, limit + 1)?;
        if rows.len() > limit {
            bail!("result exceeds limit; select a smaller group or increase limit");
        }
        Ok(rows.into_iter().map(|(_, row)| row).collect::<Vec<_>>())
    })?;
    Ok(Json(rows))
}

// Only new collection paths opt into this validation; legacy source declarations keep
// their historical behavior. Retractions propagate through filters and other native edges.
pub(crate) fn validate_collection_edges(transaction: &Transaction<'_>) -> Result<()> {
    let specs = transaction.scan::<RelationalSpec>("relational/")?;
    let edges = transaction.scan::<OperatorEdge>("operator-edge/")?;
    let mut retracting = HashSet::new();
    for (_, spec) in &specs {
        if let Some((_, edge)) = edges
            .iter()
            .find(|(_, edge)| edge.operator_id == spec.operator_id)
        {
            retracting.insert(edge.output_stream.clone());
        }
        if spec.input_mode != CollectionMode::Retract
            && edges
                .iter()
                .any(|(_, edge)| edge.output_stream == spec.stream)
        {
            bail!(
                "native changelog input requires retract mode; upsert/append modes are for declared source records"
            );
        }
    }
    loop {
        let before = retracting.len();
        for (_, edge) in &edges {
            if operator_input_streams(transaction, &edge.operator_id)?
                .iter()
                .any(|input| retracting.contains(input))
            {
                retracting.insert(edge.output_stream.clone());
            }
        }
        if before == retracting.len() {
            break;
        }
    }
    for (_, operator) in transaction.scan::<Deduplicate>("deduplicate/")? {
        if retracting.contains(&operator.stream) {
            bail!("retracting collection cannot feed append-only deduplicate");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(sequence: u64, row: Value, kind: ChangeKind, event_time: f64) -> StreamRecord {
        serde_json::from_value(
            json!({"stream": "input", "partition": 0, "offset": sequence,
            "sequence": sequence, "event_time": event_time, "ingestion_time": 0.0,
            "key": "source-key", "value": row, "kind": kind, "late": false, "too_late": false}),
        )
        .unwrap()
    }
    fn spec(mode: CollectionMode) -> RelationalSpec {
        RelationalSpec {
            operator_id: "leaders".into(),
            stream: "input".into(),
            kind: RelationalKind::TopN,
            input_mode: mode,
            primary_key: if mode == CollectionMode::Upsert {
                vec!["id".into()]
            } else {
                vec![]
            },
            partition_by: vec!["group".into()],
            order_by: vec![
                ("score".into(), SortDirection::Desc),
                ("id".into(), SortDirection::Asc),
            ],
            n: Some(3),
        }
    }
    fn with_transaction(test: impl FnOnce(&mut Transaction<'_>) -> Result<()>) -> Result<()> {
        let root = std::env::temp_dir().join(format!("highwater-collection-{}", Uuid::new_v4()));
        let result = (|| {
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
                range_deletions: vec![],
                defer_process_dispatch: false,
            };
            test(&mut transaction)
        })();
        let _ = fs::remove_dir_all(root);
        result
    }

    #[test]
    fn ordered_scalar_encoding_matches_comparisons() -> Result<()> {
        for values in [
            vec![json!(-20), json!(-1), json!(0), json!(0.5), json!(17)],
            vec![json!(""), json!("a"), json!("aa"), json!("b"), json!("é")],
            vec![json!(false), json!(true)],
        ] {
            for direction in [SortDirection::Asc, SortDirection::Desc] {
                let mut encoded_values = values
                    .iter()
                    .map(|v| Ok((scalar_sort(v, direction)?.0, v.clone())))
                    .collect::<Result<Vec<_>>>()?;
                encoded_values.push((scalar_sort(&Value::Null, direction)?.0, Value::Null));
                encoded_values.sort_by(|a, b| a.0.cmp(&b.0));
                let mut expected = values.clone();
                if direction == SortDirection::Desc {
                    expected.reverse();
                }
                expected.push(Value::Null);
                assert_eq!(
                    encoded_values
                        .into_iter()
                        .map(|(_, v)| v)
                        .collect::<Vec<_>>(),
                    expected
                );
            }
        }
        assert!(scalar_sort(&json!(9_007_199_254_740_993_u64), SortDirection::Asc).is_err());
        assert!(scalar_sort(&json!([]), SortDirection::Asc).is_err());
        assert_eq!(
            scalar_sort(&json!(-0.0), SortDirection::Asc)?.0,
            scalar_sort(&json!(0), SortDirection::Asc)?.0
        );
        Ok(())
    }

    #[test]
    fn retractable_top_n_matches_recomputation_over_generated_upserts() -> Result<()> {
        with_transaction(|tx| {
            let spec = spec(CollectionMode::Upsert);
            let mut expected: BTreeMap<u64, Value> = BTreeMap::new();
            let mut seed = 741_u64;
            for sequence in 1..=350 {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let id = (seed >> 32) % 25;
                let delete = seed.is_multiple_of(5) && expected.contains_key(&id);
                let value = if delete {
                    expected.remove(&id);
                    json!({"id": id})
                } else {
                    let value = json!({"id": id, "score": (seed >> 16) % 13, "group": if seed.is_multiple_of(3) { "a" } else { "b" }});
                    expected.insert(id, value.clone());
                    value
                };
                apply_record(
                    tx,
                    &spec,
                    &record(
                        sequence,
                        value,
                        if delete {
                            ChangeKind::Delete
                        } else {
                            ChangeKind::Upsert
                        },
                        sequence as f64,
                    ),
                )?;
                for group in ["a", "b"] {
                    let mut oracle = expected
                        .values()
                        .filter(|row| row["group"] == group)
                        .cloned()
                        .collect::<Vec<_>>();
                    oracle.sort_by(|a, b| {
                        b["score"]
                            .as_u64()
                            .cmp(&a["score"].as_u64())
                            .then(a["id"].as_u64().cmp(&b["id"].as_u64()))
                    });
                    oracle.truncate(3);
                    let mut actual = tx
                        .scan::<CollectionRow>(&maintained_prefix(&spec.operator_id))?
                        .into_iter()
                        .map(|(_, r)| r)
                        .filter(|r| {
                            r.key.as_deref()
                                == Some(serde_json::to_string(&vec![group]).unwrap().as_str())
                        })
                        .map(|r| {
                            assert_eq!(r.count, 1);
                            r.value
                        })
                        .collect::<Vec<_>>();
                    actual.sort_by(|a, b| {
                        b["score"]
                            .as_u64()
                            .cmp(&a["score"].as_u64())
                            .then(a["id"].as_u64().cmp(&b["id"].as_u64()))
                    });
                    assert_eq!(actual, oracle, "at input {sequence} group {group}");
                }
            }
            Ok(())
        })
    }

    #[test]
    fn multiplicity_and_original_time_survive_promotion_and_normalization() -> Result<()> {
        with_transaction(|tx| {
            let mut spec = spec(CollectionMode::Retract);
            spec.n = Some(2);
            let a = json!({"id": "a", "group": "g", "score": 100});
            let b = json!({"id": "b", "group": "g", "score": 90});
            for (seq, value) in [(1, a.clone()), (2, a.clone()), (3, b.clone())] {
                apply_record(tx, &spec, &record(seq, value, ChangeKind::Insert, 10.0))?;
            }
            let rows = tx.scan::<CollectionRow>(&maintained_prefix("leaders"))?;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1.count, 2);
            apply_record(tx, &spec, &record(4, a.clone(), ChangeKind::Delete, 10.0))?;
            assert_eq!(
                tx.scan::<CollectionRow>(&maintained_prefix("leaders"))?
                    .len(),
                2
            );
            apply_record(tx, &spec, &record(5, a, ChangeKind::Delete, 10.0))?;
            assert_eq!(
                tx.scan::<CollectionRow>(&maintained_prefix("leaders"))?[0]
                    .1
                    .value,
                b
            );
            assert!(
                apply_record(tx, &spec, &record(6, b.clone(), ChangeKind::Delete, 20.0)).is_err()
            );
            // Failed transactions are discarded by the caller; exercise normalization independently.
            let mut normalizer = spec.clone();
            normalizer.operator_id = "norm".into();
            normalizer.kind = RelationalKind::Normalize;
            normalizer.input_mode = CollectionMode::Upsert;
            normalizer.primary_key = vec!["id".into()];
            let changes = normalize(
                tx,
                &normalizer,
                &record(1, b.clone(), ChangeKind::Upsert, 10.0),
            )?;
            assert_eq!(changes.len(), 1);
            assert!(
                normalize(tx, &normalizer, &record(2, b, ChangeKind::Upsert, 30.0))?.is_empty()
            );
            let deleted = normalize(
                tx,
                &normalizer,
                &record(3, json!({"id": "b"}), ChangeKind::Delete, 40.0),
            )?;
            assert_eq!(deleted[0].0.event_time, 10.0);
            assert_eq!(deleted[0].1, -1);
            Ok(())
        })
    }

    #[test]
    fn recovery_preserves_losers_and_pending_changes_before_delivery() -> Result<()> {
        let root =
            std::env::temp_dir().join(format!("highwater-collection-recovery-{}", Uuid::new_v4()));
        let result = (|| -> Result<()> {
            let mut definition = spec(CollectionMode::Upsert);
            definition.n = Some(2);
            let open = || {
                DurableStore::open_sharded_with_journal(
                    &root.join("state"),
                    &root.join("objects"),
                    1,
                    None,
                )
            };
            {
                let store = open()?;
                let mut tx = Transaction {
                    store: &store,
                    changes: BTreeMap::new(),
                    encoded_changes: BTreeMap::new(),
                    range_deletions: vec![],
                    defer_process_dispatch: false,
                };
                tx.put(relational_key("leaders"), &definition)?;
                tx.put(
                    operator_edge_key("leaders"),
                    &OperatorEdge {
                        operator_id: "leaders".into(),
                        output_stream: "output".into(),
                        status: "ACTIVE".into(),
                        created_at: 0.0,
                        changes_forwarded: 0,
                    },
                )?;
                for (id, score) in [(1, 100), (2, 90), (3, 80)] {
                    apply_record(
                        &mut tx,
                        &definition,
                        &record(
                            id,
                            json!({"id": id, "score": score, "group": "g"}),
                            ChangeKind::Upsert,
                            id as f64,
                        ),
                    )?;
                }
                store.commit(tx.into_mutations())?;
            }
            {
                let store = open()?;
                let mut tx = Transaction {
                    store: &store,
                    changes: BTreeMap::new(),
                    encoded_changes: BTreeMap::new(),
                    range_deletions: vec![],
                    defer_process_dispatch: false,
                };
                assert_eq!(
                    tx.scan::<DifferentialChange>(&operator_edge_pending_prefix("leaders"))?
                        .len(),
                    2
                );
                assert_eq!(
                    tx.scan::<CollectionRow>(&state_prefix("leaders", "ordered"))?
                        .len(),
                    3
                );
                apply_record(
                    &mut tx,
                    &definition,
                    &record(4, json!({"id": 1}), ChangeKind::Delete, 40.0),
                )?;
                let changes =
                    tx.scan::<DifferentialChange>(&operator_edge_pending_prefix("leaders"))?;
                assert_eq!(changes.len(), 4);
                assert_eq!(changes[2].1.kind, ChangeKind::Delete);
                assert_eq!(changes[2].1.event_time, 1.0);
                assert_eq!(changes[3].1.row["id"], 3);
                store.commit(tx.into_mutations())?;
            }
            let store = open()?;
            let rows = store.scan::<CollectionRow>(&maintained_prefix("leaders"))?;
            assert_eq!(rows.len(), 2);
            assert!(rows.iter().all(|(_, row)| row.value["id"] != 1));
            Ok(())
        })();
        let _ = fs::remove_dir_all(root);
        result
    }
}
