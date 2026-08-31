use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LatePolicy {
    Drop,
    SideOutput,
    Accept,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WatermarkMode {
    Bounded,
    Monotonic,
    SourceManaged,
}

fn default_watermark_mode() -> WatermarkMode {
    WatermarkMode::Bounded
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    Insert,
    Upsert,
    UpdateBefore,
    UpdateAfter,
    Delete,
}

impl ChangeKind {
    pub fn weight(self) -> i64 {
        match self {
            Self::Insert | Self::Upsert | Self::UpdateAfter => 1,
            Self::UpdateBefore | Self::Delete => -1,
        }
    }

    pub fn is_addition(self) -> bool {
        self.weight() > 0
    }
}

fn default_change_kind() -> ChangeKind {
    ChangeKind::Upsert
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TemporalJoinType {
    Inner,
    Left,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IntervalJoinType {
    Inner,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WindowAggregation {
    Count,
    Sum,
    Max,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Comparison {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

fn default_window_aggregation() -> WindowAggregation {
    WindowAggregation::Count
}

fn default_partitions() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    pub name: String,
    #[serde(default = "default_partitions")]
    pub partitions: u32,
    #[serde(default = "default_watermark_mode")]
    pub watermark_mode: WatermarkMode,
    #[serde(default)]
    pub max_out_of_orderness: f64,
    #[serde(default)]
    pub idle_timeout: Option<f64>,
    #[serde(default)]
    pub allowed_lateness: f64,
    #[serde(default)]
    pub alignment_max_drift: Option<f64>,
    #[serde(default = "default_late_policy")]
    pub late_policy: LatePolicy,
    pub created_at: f64,
}

fn default_late_policy() -> LatePolicy {
    LatePolicy::SideOutput
}

impl StreamConfig {
    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            bail!("stream name must not be empty");
        }
        if self.partitions == 0 {
            bail!("stream must have at least one partition");
        }
        if self.max_out_of_orderness < 0.0 || !self.max_out_of_orderness.is_finite() {
            bail!("max_out_of_orderness must be finite and non-negative");
        }
        if self.allowed_lateness < 0.0 || !self.allowed_lateness.is_finite() {
            bail!("allowed_lateness must be finite and non-negative");
        }
        if self
            .idle_timeout
            .is_some_and(|value| value <= 0.0 || !value.is_finite())
        {
            bail!("idle_timeout must be finite and positive");
        }
        if self
            .alignment_max_drift
            .is_some_and(|value| value < 0.0 || !value.is_finite())
        {
            bail!("alignment_max_drift must be finite and non-negative");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartitionState {
    pub partition: u32,
    pub next_offset: u64,
    pub max_event_time: Option<f64>,
    pub watermark: Option<f64>,
    pub last_activity_at: f64,
    pub idle: bool,
    pub sealed: bool,
}

impl PartitionState {
    pub fn new(partition: u32, now: f64) -> Self {
        Self {
            partition,
            next_offset: 0,
            max_event_time: None,
            watermark: None,
            last_activity_at: now,
            idle: false,
            sealed: false,
        }
    }

    pub fn observe(
        &mut self,
        event_time: f64,
        out_of_orderness: f64,
        generate_watermark: bool,
        now: f64,
    ) -> Result<u64> {
        if self.sealed {
            bail!("partition {} is sealed", self.partition);
        }
        if !event_time.is_finite() {
            bail!("event_time must be finite");
        }
        self.max_event_time = Some(
            self.max_event_time
                .map_or(event_time, |current| current.max(event_time)),
        );
        if generate_watermark {
            let candidate = event_time - out_of_orderness;
            self.watermark = Some(
                self.watermark
                    .map_or(candidate, |current| current.max(candidate)),
            );
        }
        self.last_activity_at = now;
        self.idle = false;
        let offset = self.next_offset;
        self.next_offset += 1;
        Ok(offset)
    }

    pub fn advance_watermark(&mut self, watermark: f64, now: f64) -> Result<()> {
        if self.sealed {
            bail!("partition {} is sealed", self.partition);
        }
        if !watermark.is_finite() {
            bail!("watermark must be finite");
        }
        if self.watermark.is_some_and(|current| watermark < current) {
            bail!("partition watermarks cannot move backwards");
        }
        self.watermark = Some(watermark);
        self.last_activity_at = now;
        self.idle = false;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamState {
    pub watermark: Option<f64>,
    pub finalized: bool,
    pub max_event_time: Option<f64>,
    pub updated_at: f64,
}

impl StreamState {
    pub fn new(now: f64) -> Self {
        Self {
            watermark: None,
            finalized: false,
            max_event_time: None,
            updated_at: now,
        }
    }

    pub fn refresh(
        &mut self,
        config: &StreamConfig,
        partitions: &mut [PartitionState],
        now: f64,
    ) -> bool {
        let previous = self.clone();
        if let Some(timeout) = config.idle_timeout {
            for partition in partitions.iter_mut().filter(|partition| !partition.sealed) {
                if !partition.idle && now - partition.last_activity_at >= timeout {
                    partition.idle = true;
                }
            }
        }
        self.max_event_time = partitions
            .iter()
            .filter_map(|partition| partition.max_event_time)
            .reduce(f64::max);
        self.finalized = partitions.iter().all(|partition| partition.sealed);
        let active: Vec<&PartitionState> = partitions
            .iter()
            .filter(|partition| !partition.sealed && !partition.idle)
            .collect();
        if !active.is_empty() && active.iter().all(|partition| partition.watermark.is_some()) {
            let candidate = active
                .iter()
                .filter_map(|partition| partition.watermark)
                .reduce(f64::min)
                .expect("active watermarks checked above");
            self.watermark = Some(
                self.watermark
                    .map_or(candidate, |current| current.max(candidate)),
            );
        }
        self.updated_at = now;
        self.watermark != previous.watermark
            || self.finalized != previous.finalized
            || self.max_event_time != previous.max_event_time
    }

    pub fn is_late(&self, event_time: f64) -> bool {
        self.watermark
            .is_some_and(|watermark| event_time <= watermark)
    }

    pub fn is_too_late(&self, event_time: f64, allowed_lateness: f64) -> bool {
        self.watermark
            .is_some_and(|watermark| event_time <= watermark - allowed_lateness)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamRecord {
    pub stream: String,
    pub partition: u32,
    pub offset: u64,
    pub sequence: u64,
    pub event_time: f64,
    pub ingestion_time: f64,
    pub key: Option<String>,
    pub value: Value,
    #[serde(default = "default_change_kind")]
    pub kind: ChangeKind,
    #[serde(default)]
    pub event_id: Option<String>,
    #[serde(default)]
    pub key_group: u32,
    #[serde(default)]
    pub owner_epoch: u64,
    #[serde(default)]
    pub source_id: Option<String>,
    #[serde(default)]
    pub source_partition: Option<u32>,
    #[serde(default)]
    pub source_offset: Option<u64>,
    pub late: bool,
    pub too_late: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalJoin {
    pub join_id: String,
    pub probe_stream: String,
    pub version_stream: String,
    pub workflow_type: String,
    pub task_queue: String,
    pub join_type: TemporalJoinType,
    pub status: String,
    pub created_at: f64,
    pub probes_received: u64,
    pub versions_received: u64,
    pub probes_emitted: u64,
    pub matches_emitted: u64,
}

impl TemporalJoin {
    pub fn validate(&self) -> Result<()> {
        if self.join_id.trim().is_empty() {
            bail!("join_id must not be empty");
        }
        if self.probe_stream.trim().is_empty() || self.version_stream.trim().is_empty() {
            bail!("temporal join streams must not be empty");
        }
        if self.probe_stream == self.version_stream {
            bail!("probe_stream and version_stream must be different");
        }
        if self.workflow_type.trim().is_empty() {
            bail!("workflow_type must not be empty");
        }
        Ok(())
    }

    pub fn has_same_spec(&self, other: &Self) -> bool {
        self.join_id == other.join_id
            && self.probe_stream == other.probe_stream
            && self.version_stream == other.version_stream
            && self.workflow_type == other.workflow_type
            && self.task_queue == other.task_queue
            && self.join_type == other.join_type
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TemporalJoinOutput {
    pub join_id: String,
    pub probe: StreamRecord,
    pub version: Option<StreamRecord>,
    pub as_of: f64,
    pub watermark: Option<f64>,
    pub workflow_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntervalJoin {
    pub join_id: String,
    pub left_stream: String,
    pub right_stream: String,
    pub workflow_type: String,
    pub task_queue: String,
    pub lower_bound: f64,
    pub upper_bound: f64,
    pub join_type: IntervalJoinType,
    pub status: String,
    pub created_at: f64,
    pub left_received: u64,
    pub right_received: u64,
    pub pairs_emitted: u64,
}

impl IntervalJoin {
    pub fn validate(&self) -> Result<()> {
        if self.join_id.trim().is_empty() {
            bail!("join_id must not be empty");
        }
        if self.left_stream.trim().is_empty() || self.right_stream.trim().is_empty() {
            bail!("interval join streams must not be empty");
        }
        if !self.lower_bound.is_finite()
            || !self.upper_bound.is_finite()
            || self.lower_bound > self.upper_bound
        {
            bail!("interval join bounds must be finite and lower_bound <= upper_bound");
        }
        if self.left_stream == self.right_stream && self.lower_bound < 0.0 {
            bail!("ordered self interval joins require a non-negative lower_bound");
        }
        if self.workflow_type.trim().is_empty() {
            bail!("workflow_type must not be empty");
        }
        Ok(())
    }

    pub fn has_same_spec(&self, other: &Self) -> bool {
        self.join_id == other.join_id
            && self.left_stream == other.left_stream
            && self.right_stream == other.right_stream
            && self.workflow_type == other.workflow_type
            && self.task_queue == other.task_queue
            && self.lower_bound == other.lower_bound
            && self.upper_bound == other.upper_bound
            && self.join_type == other.join_type
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IntervalJoinOutput {
    pub join_id: String,
    pub left: Option<StreamRecord>,
    pub right: Option<StreamRecord>,
    pub workflow_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Deduplicate {
    pub operator_id: String,
    pub stream: String,
    pub workflow_type: String,
    pub task_queue: String,
    pub status: String,
    pub created_at: f64,
    pub records_received: u64,
    pub records_emitted: u64,
    pub duplicates_suppressed: u64,
}

impl Deduplicate {
    pub fn validate(&self) -> Result<()> {
        if self.operator_id.trim().is_empty() || self.stream.trim().is_empty() {
            bail!("operator_id and stream must not be empty");
        }
        if self.workflow_type.trim().is_empty() {
            bail!("workflow_type must not be empty");
        }
        Ok(())
    }

    pub fn has_same_spec(&self, other: &Self) -> bool {
        self.operator_id == other.operator_id
            && self.stream == other.stream
            && self.workflow_type == other.workflow_type
            && self.task_queue == other.task_queue
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeduplicateOutput {
    pub operator_id: String,
    pub record: StreamRecord,
    pub canonical: bool,
    pub canonical_record: StreamRecord,
    pub workflow_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamFilter {
    pub operator_id: String,
    pub stream: String,
    pub workflow_type: String,
    pub task_queue: String,
    pub field: String,
    pub comparison: Comparison,
    pub operand: Value,
    pub status: String,
    pub created_at: f64,
    pub records_received: u64,
    pub records_emitted: u64,
}

impl StreamFilter {
    pub fn validate(&self) -> Result<()> {
        if self.operator_id.trim().is_empty()
            || self.stream.trim().is_empty()
            || self.workflow_type.trim().is_empty()
            || self.field.trim().is_empty()
        {
            bail!("operator_id, stream, workflow_type, and field must not be empty");
        }
        Ok(())
    }

    pub fn has_same_spec(&self, other: &Self) -> bool {
        self.operator_id == other.operator_id
            && self.stream == other.stream
            && self.workflow_type == other.workflow_type
            && self.task_queue == other.task_queue
            && self.field == other.field
            && self.comparison == other.comparison
            && self.operand == other.operand
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamFilterOutput {
    pub operator_id: String,
    pub record: StreamRecord,
    pub workflow_id: String,
}

pub fn completeness_frontier(config: &StreamConfig, state: &StreamState) -> Option<f64> {
    if state.finalized {
        Some(f64::MAX)
    } else {
        state
            .watermark
            .map(|watermark| watermark - config.allowed_lateness)
    }
}

pub fn temporal_join_frontier(
    probe_config: &StreamConfig,
    probe_state: &StreamState,
    version_config: &StreamConfig,
    version_state: &StreamState,
) -> Option<f64> {
    match (
        completeness_frontier(probe_config, probe_state),
        completeness_frontier(version_config, version_state),
    ) {
        (Some(probe), Some(version)) => Some(probe.min(version)),
        _ => None,
    }
}

pub fn latest_version_as_of<'a>(
    versions: impl IntoIterator<Item = &'a StreamRecord>,
    event_time: f64,
) -> Option<&'a StreamRecord> {
    versions
        .into_iter()
        .filter(|version| version.event_time <= event_time)
        .max_by(|left, right| {
            left.event_time
                .total_cmp(&right.event_time)
                .then(left.sequence.cmp(&right.sequence))
        })
        .filter(|version| version.kind.is_addition())
}

pub fn interval_contains(
    left_event_time: f64,
    right_event_time: f64,
    lower_bound: f64,
    upper_bound: f64,
) -> bool {
    right_event_time >= left_event_time + lower_bound
        && right_event_time <= left_event_time + upper_bound
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowSchedule {
    pub schedule_id: String,
    pub stream: String,
    pub workflow_type: String,
    pub task_queue: String,
    pub window_size: f64,
    #[serde(default)]
    pub slide: f64,
    #[serde(default)]
    pub start_at: f64,
    pub next_window_start: f64,
    pub emit_empty_windows: bool,
    #[serde(default = "default_window_aggregation")]
    pub aggregation: WindowAggregation,
    #[serde(default)]
    pub value_field: Option<String>,
    pub status: String,
    pub created_at: f64,
    pub windows_fired: u64,
}

impl WindowSchedule {
    pub fn validate(&self) -> Result<()> {
        if self.schedule_id.trim().is_empty() {
            bail!("schedule_id must not be empty");
        }
        if self.window_size <= 0.0 || !self.window_size.is_finite() {
            bail!("window_size must be finite and positive");
        }
        if !self.next_window_start.is_finite() {
            bail!("start_at must be finite");
        }
        if self.effective_slide() <= 0.0
            || !self.effective_slide().is_finite()
            || self.effective_slide() > self.window_size
        {
            bail!("slide must be finite, positive, and no larger than window_size");
        }
        Ok(())
    }

    pub fn effective_slide(&self) -> f64 {
        if self.slide == 0.0 {
            self.window_size
        } else {
            self.slide
        }
    }

    pub fn has_same_spec(&self, other: &Self) -> bool {
        self.schedule_id == other.schedule_id
            && self.stream == other.stream
            && self.workflow_type == other.workflow_type
            && self.task_queue == other.task_queue
            && self.window_size == other.window_size
            && self.effective_slide() == other.effective_slide()
            && self.start_at == other.start_at
            && self.emit_empty_windows == other.emit_empty_windows
            && self.aggregation == other.aggregation
            && self.value_field == other.value_field
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WindowAccumulator {
    pub schedule_id: String,
    pub stream: String,
    pub key: Option<String>,
    pub window_start: f64,
    pub window_end: f64,
    pub count: i64,
    pub sum: f64,
    #[serde(default)]
    pub max: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> StreamConfig {
        StreamConfig {
            name: "events".to_owned(),
            partitions: 2,
            watermark_mode: WatermarkMode::Bounded,
            max_out_of_orderness: 5.0,
            idle_timeout: Some(10.0),
            allowed_lateness: 2.0,
            alignment_max_drift: None,
            late_policy: LatePolicy::SideOutput,
            created_at: 0.0,
        }
    }

    #[test]
    fn combined_watermark_is_minimum_active_partition() {
        let mut partitions = vec![PartitionState::new(0, 0.0), PartitionState::new(1, 0.0)];
        partitions[0].observe(20.0, 5.0, true, 1.0).unwrap();
        partitions[1].observe(12.0, 5.0, true, 1.0).unwrap();
        let mut state = StreamState::new(0.0);

        state.refresh(&config(), &mut partitions, 1.0);

        assert_eq!(state.watermark, Some(7.0));
    }

    #[test]
    fn idle_partition_stops_holding_back_watermark_without_regression() {
        let mut partitions = vec![PartitionState::new(0, 0.0), PartitionState::new(1, 0.0)];
        partitions[0].observe(20.0, 5.0, true, 9.0).unwrap();
        partitions[1].observe(8.0, 5.0, true, 0.0).unwrap();
        let mut state = StreamState::new(0.0);
        state.refresh(&config(), &mut partitions, 9.0);
        assert_eq!(state.watermark, Some(3.0));

        state.refresh(&config(), &mut partitions, 11.0);

        assert!(partitions[1].idle);
        assert_eq!(state.watermark, Some(15.0));
    }

    #[test]
    fn lateness_uses_current_combined_watermark() {
        let state = StreamState {
            watermark: Some(10.0),
            finalized: false,
            max_event_time: Some(20.0),
            updated_at: 0.0,
        };

        assert!(state.is_late(10.0));
        assert!(!state.is_too_late(9.0, 2.0));
        assert!(state.is_too_late(8.0, 2.0));
    }

    #[test]
    fn partition_watermark_is_monotonic() {
        let mut partition = PartitionState::new(0, 0.0);
        partition.advance_watermark(10.0, 1.0).unwrap();

        assert!(partition.advance_watermark(9.0, 2.0).is_err());
    }

    #[test]
    fn source_managed_partition_waits_for_reported_watermark() {
        let mut partition = PartitionState::new(0, 0.0);
        partition.observe(20.0, 5.0, false, 1.0).unwrap();
        assert_eq!(partition.max_event_time, Some(20.0));
        assert_eq!(partition.watermark, None);
        partition.advance_watermark(15.0, 2.0).unwrap();
        assert_eq!(partition.watermark, Some(15.0));
    }

    #[test]
    fn temporal_join_frontier_accounts_for_lateness_and_finalization() {
        let mut probe_config = config();
        probe_config.allowed_lateness = 2.0;
        let mut version_config = config();
        version_config.allowed_lateness = 5.0;
        let probe_state = StreamState {
            watermark: Some(20.0),
            finalized: false,
            max_event_time: Some(18.0),
            updated_at: 0.0,
        };
        let version_state = StreamState {
            watermark: Some(22.0),
            finalized: false,
            max_event_time: Some(20.0),
            updated_at: 0.0,
        };

        assert_eq!(
            temporal_join_frontier(&probe_config, &probe_state, &version_config, &version_state,),
            Some(17.0),
        );

        let finalized = StreamState {
            finalized: true,
            ..probe_state.clone()
        };
        assert_eq!(
            temporal_join_frontier(&probe_config, &finalized, &version_config, &version_state,),
            Some(17.0),
        );
    }

    #[test]
    fn temporal_join_selects_latest_version_before_observing_tombstone() {
        let record = |sequence, event_time, kind| StreamRecord {
            stream: "versions".to_owned(),
            partition: 0,
            offset: sequence,
            sequence,
            event_time,
            ingestion_time: 0.0,
            key: Some("account".to_owned()),
            value: Value::Null,
            kind,
            event_id: None,
            key_group: 0,
            owner_epoch: 1,
            source_id: None,
            source_partition: None,
            source_offset: None,
            late: false,
            too_late: false,
        };
        let old = record(1, 5.0, ChangeKind::Upsert);
        let replacement = record(2, 8.0, ChangeKind::Upsert);
        let deletion = record(3, 10.0, ChangeKind::Delete);
        let versions = [&old, &replacement, &deletion];

        assert_eq!(
            latest_version_as_of(versions, 9.0).map(|version| version.sequence),
            Some(2),
        );
        assert_eq!(latest_version_as_of(versions, 10.0), None);
    }

    #[test]
    fn interval_join_bounds_are_inclusive() {
        assert!(interval_contains(10.0, 8.0, -2.0, 5.0));
        assert!(interval_contains(10.0, 15.0, -2.0, 5.0));
        assert!(!interval_contains(10.0, 7.9, -2.0, 5.0));
        assert!(!interval_contains(10.0, 15.1, -2.0, 5.0));
    }
}
