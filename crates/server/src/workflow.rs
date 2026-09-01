use crate::*;
pub(crate) async fn start_workflow(
    State(state): State<AppState>,
    Json(request): Json<StartRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let workflow_id = request
        .workflow_id
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let selected_id = workflow_id.clone();
    state.commit(|transaction| {
        if transaction
            .get::<WorkflowRecord>(&workflow_key(&selected_id))?
            .is_some()
        {
            bail!("workflow already exists: {selected_id}");
        }
        let timestamp = now();
        let workflow = WorkflowRecord {
            workflow_id: selected_id.clone(),
            workflow_type: request.workflow_type.clone(),
            status: "RUNNING".to_owned(),
            result: None,
            error: None,
            task_queue: request
                .options
                .get("task_queue")
                .and_then(Value::as_str)
                .unwrap_or("default")
                .to_owned(),
            build_id: None,
            run_number: 1,
            parent_id: None,
            parent_command_id: None,
            parent_close_policy: None,
            execution_deadline: request
                .options
                .get("execution_timeout")
                .and_then(Value::as_f64)
                .map(|timeout| timestamp + timeout),
            created_at: timestamp,
            updated_at: timestamp,
        };
        transaction.put(workflow_key(&selected_id), &workflow)?;
        if let Some(deadline) = workflow.execution_deadline {
            transaction.put(
                workflow_deadline_key(&selected_id),
                &WorkflowDeadline {
                    workflow_id: selected_id.clone(),
                    deadline,
                },
            )?;
        }
        append_event(
            transaction,
            &selected_id,
            "WORKFLOW_STARTED",
            json!({
                "workflow_type": request.workflow_type, "args": request.args, "run_number": 1,
            }),
        )?;
        enqueue_workflow(transaction, &workflow)
    })?;
    Ok((
        StatusCode::CREATED,
        Json(json!({"workflow_id": workflow_id})),
    ))
}

pub(crate) async fn get_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let workflow = state
        .store
        .get::<WorkflowRecord>(&workflow_key(&id))?
        .ok_or_else(|| anyhow!("workflow not found: {id}"))?;
    Ok(Json(json!({
        "workflow_id": workflow.workflow_id, "workflow_type": workflow.workflow_type,
        "status": workflow.status, "result": workflow.result, "error": workflow.error,
    })))
}

pub(crate) async fn history(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let events: Vec<Event> = state
        .store
        .scan(&event_prefix(&id))?
        .into_iter()
        .map(|(_, event)| event)
        .collect();
    Ok(Json(events))
}

pub(crate) async fn external_event(
    state: &AppState,
    id: &str,
    event_type: &str,
    data: Value,
) -> Result<(), ApiError> {
    state.commit(|transaction| {
        let key = workflow_key(id);
        let mut workflow = transaction
            .get::<WorkflowRecord>(&key)?
            .ok_or_else(|| anyhow!("workflow not found: {id}"))?;
        if workflow.status != "RUNNING" {
            bail!("workflow {id} is {}", workflow.status);
        }
        append_event(transaction, id, event_type, data)?;
        workflow.updated_at = now();
        transaction.put(key, &workflow)?;
        enqueue_workflow(transaction, &workflow)
    })?;
    Ok(())
}

pub(crate) async fn signal(
    State(state): State<AppState>,
    Path((id, name)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<impl IntoResponse, ApiError> {
    external_event(
        &state,
        &id,
        "SIGNAL_RECEIVED",
        json!({"name": name, "args": body.get("args").cloned().unwrap_or(json!([]))}),
    )
    .await?;
    Ok((StatusCode::ACCEPTED, Json(json!({}))))
}

pub(crate) async fn update(
    State(state): State<AppState>,
    Path((id, name)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<impl IntoResponse, ApiError> {
    let update_id = body
        .get("update_id")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    external_event(&state, &id, "UPDATE_REQUESTED", json!({
        "name": name, "args": body.get("args").cloned().unwrap_or(json!([])), "update_id": update_id,
    })).await?;
    Ok((StatusCode::ACCEPTED, Json(json!({"update_id": update_id}))))
}

pub(crate) async fn query_workflow(
    State(state): State<AppState>,
    Path((id, name)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<impl IntoResponse, ApiError> {
    let workflow = state
        .store
        .get::<WorkflowRecord>(&workflow_key(&id))?
        .ok_or_else(|| anyhow!("workflow not found: {id}"))?;
    let history = state
        .store
        .scan::<Event>(&event_prefix(&id))?
        .into_iter()
        .map(|(_, event)| event)
        .collect();
    let token = Uuid::new_v4().to_string();
    let task = QueryTask {
        protocol_version: PROTOCOL_VERSION,
        task_token: token.clone(),
        workflow_id: id,
        workflow_type: workflow.workflow_type,
        name,
        args: body
            .get("args")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
        history,
    };
    let (sender, receiver) = oneshot::channel();
    state
        .query_results
        .lock()
        .map_err(|_| anyhow!("query result lock poisoned"))?
        .insert(token.clone(), sender);
    state
        .query_queue
        .lock()
        .map_err(|_| anyhow!("query queue lock poisoned"))?
        .push_back((workflow.task_queue, task));
    let response = tokio::time::timeout(std::time::Duration::from_secs(10), receiver).await;
    match response {
        Ok(Ok(Ok(result))) => Ok(Json(json!({"result": result}))),
        Ok(Ok(Err(error))) => Err(ApiError(anyhow!(error))),
        Ok(Err(_)) => Err(ApiError(anyhow!("query worker disconnected"))),
        Err(_) => {
            state
                .query_results
                .lock()
                .map_err(|_| anyhow!("query result lock poisoned"))?
                .remove(&token);
            Err(ApiError(anyhow!("query timed out")))
        }
    }
}

pub(crate) async fn cancel(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    external_event(&state, &id, "CANCEL_REQUESTED", json!({})).await?;
    Ok((StatusCode::ACCEPTED, Json(json!({}))))
}

pub(crate) async fn terminate(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Result<impl IntoResponse, ApiError> {
    let reason = body
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or("terminated")
        .to_owned();
    state.commit(|transaction| {
        let workflow = transaction
            .get::<WorkflowRecord>(&workflow_key(&id))?
            .ok_or_else(|| anyhow!("workflow not found: {id}"))?;
        if workflow.status != "RUNNING" {
            bail!("workflow {id} is {}", workflow.status);
        }
        close_workflow(transaction, &id, "TERMINATED", None, Some(reason))?;
        transaction.delete(workflow_task_key(&id));
        Ok(())
    })?;
    Ok((StatusCode::ACCEPTED, Json(json!({}))))
}

pub(crate) fn fire_timers(transaction: &mut Transaction<'_>) -> Result<()> {
    let due: Vec<(String, TimerRecord)> = transaction
        .scan("timer/")?
        .into_iter()
        .filter(|(_, timer): &(String, TimerRecord)| timer.fire_at <= now())
        .take(100)
        .collect();
    for (key, timer) in due {
        transaction.delete(key);
        append_event(
            transaction,
            &timer.workflow_id,
            "TIMER_FIRED",
            json!({"command_id": timer.command_id}),
        )?;
        if let Some(workflow) =
            transaction.get::<WorkflowRecord>(&workflow_key(&timer.workflow_id))?
        {
            enqueue_workflow(transaction, &workflow)?;
        }
    }
    Ok(())
}

pub(crate) fn expire_workflows(transaction: &mut Transaction<'_>) -> Result<()> {
    let due: Vec<WorkflowDeadline> = transaction
        .scan::<WorkflowDeadline>("workflow-deadline/")?
        .into_iter()
        .map(|(_, deadline)| deadline)
        .filter(|deadline| deadline.deadline <= now())
        .collect();
    for deadline in due {
        close_workflow(
            transaction,
            &deadline.workflow_id,
            "TIMED_OUT",
            None,
            Some("workflow execution timeout".to_owned()),
        )?;
        transaction.delete(workflow_task_key(&deadline.workflow_id));
    }
    Ok(())
}

pub(crate) async fn poll_workflow(
    State(state): State<AppState>,
    Json(request): Json<PollRequest>,
) -> Result<Response, ApiError> {
    if request.protocol_version != PROTOCOL_VERSION {
        return Err(ApiError(anyhow!("unsupported protocol version")));
    }
    state.authorize_poll(&request)?;
    let mut activation = None;
    state.commit(|transaction| {
        fire_timers(transaction)?;
        expire_workflows(transaction)?;
        let timestamp = now();
        let mut candidates: Vec<(String, WorkflowTask)> = transaction
            .scan("workflow-task/")?
            .into_iter()
            .filter(|(_, task): &(String, WorkflowTask)| {
                request
                    .task_queue
                    .as_ref()
                    .is_none_or(|queue| queue == &task.task_queue)
                    && task
                        .build_id
                        .as_ref()
                        .is_none_or(|build_id| request.build_ids.contains(build_id))
                    && task.available_at <= timestamp
                    && task.lease_expires.is_none_or(|expiry| expiry <= timestamp)
            })
            .collect();
        candidates.sort_by(|left, right| left.1.available_at.total_cmp(&right.1.available_at));
        let Some((key, mut task)) = candidates.into_iter().next() else {
            return Ok(());
        };
        let token = Uuid::new_v4().to_string();
        if let Some(previous_token) = task.task_token.as_deref() {
            transaction.delete(workflow_task_token_key(previous_token));
        }
        task.attempt += 1;
        task.lease_owner = Some(request.worker_id.clone());
        task.lease_expires = Some(timestamp + request.lease_seconds);
        task.task_token = Some(token.clone());
        transaction.put(key, &task)?;
        transaction.put(workflow_task_token_key(&token), &task.workflow_id)?;
        let workflow = transaction
            .get::<WorkflowRecord>(&workflow_key(&task.workflow_id))?
            .ok_or_else(|| anyhow!("workflow record missing"))?;
        let history = transaction
            .scan::<Event>(&event_prefix(&task.workflow_id))?
            .into_iter()
            .map(|(_, event)| event)
            .collect();
        activation = Some(WorkflowActivation {
            protocol_version: PROTOCOL_VERSION,
            task_token: token,
            workflow_id: task.workflow_id,
            workflow_type: workflow.workflow_type,
            attempt: task.attempt,
            build_id: workflow.build_id,
            history,
        });
        Ok(())
    })?;
    Ok(match activation {
        Some(value) => Json(value).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    })
}

pub(crate) async fn poll_workflow_batch(
    State(state): State<AppState>,
    Json(request): Json<PollRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if request.protocol_version != PROTOCOL_VERSION {
        return Err(ApiError(anyhow!("unsupported protocol version")));
    }
    state.authorize_poll(&request)?;
    let mut activations = Vec::new();
    state.commit(|transaction| {
        fire_timers(transaction)?;
        expire_workflows(transaction)?;
        let timestamp = now();
        let mut candidates: Vec<(String, WorkflowTask)> = transaction
            .scan("workflow-task/")?
            .into_iter()
            .filter(|(_, task): &(String, WorkflowTask)| {
                request
                    .task_queue
                    .as_ref()
                    .is_none_or(|queue| queue == &task.task_queue)
                    && task
                        .build_id
                        .as_ref()
                        .is_none_or(|build_id| request.build_ids.contains(build_id))
                    && task.available_at <= timestamp
                    && task.lease_expires.is_none_or(|expiry| expiry <= timestamp)
            })
            .collect();
        candidates.sort_by(|left, right| left.1.available_at.total_cmp(&right.1.available_at));
        let Some((_, first)) = candidates.first() else {
            return Ok(());
        };
        let batch_group = first.batch_group.clone();
        let build_id = first.build_id.clone();
        let task_queue = first.task_queue.clone();
        let max_size = if batch_group.is_some() {
            first.batch_max_size.clamp(1, 1_024) as usize
        } else {
            1
        };
        let available = candidates
            .iter()
            .filter(|(_, task)| {
                task.batch_group == batch_group
                    && task.build_id == build_id
                    && task.task_queue == task_queue
            })
            .count();
        if batch_group.is_some()
            && available < max_size
            && timestamp < first.enqueued_at + first.batch_max_delay
        {
            return Ok(());
        }
        for (key, mut task) in candidates
            .into_iter()
            .filter(|(_, task)| {
                task.batch_group == batch_group
                    && task.build_id == build_id
                    && task.task_queue == task_queue
            })
            .take(max_size)
        {
            let token = Uuid::new_v4().to_string();
            if let Some(previous_token) = task.task_token.as_deref() {
                transaction.delete(workflow_task_token_key(previous_token));
            }
            task.attempt += 1;
            task.lease_owner = Some(request.worker_id.clone());
            task.lease_expires = Some(timestamp + request.lease_seconds);
            task.task_token = Some(token.clone());
            transaction.put(key, &task)?;
            transaction.put(workflow_task_token_key(&token), &task.workflow_id)?;
            let workflow = transaction
                .get::<WorkflowRecord>(&workflow_key(&task.workflow_id))?
                .ok_or_else(|| anyhow!("workflow record missing"))?;
            let history = transaction
                .scan::<Event>(&event_prefix(&task.workflow_id))?
                .into_iter()
                .map(|(_, event)| event)
                .collect();
            activations.push(WorkflowActivation {
                protocol_version: PROTOCOL_VERSION,
                task_token: token,
                workflow_id: task.workflow_id,
                workflow_type: workflow.workflow_type,
                attempt: task.attempt,
                build_id: workflow.build_id,
                history,
            });
        }
        Ok(())
    })?;
    Ok(Json(activations))
}

pub(crate) fn close_workflow(
    transaction: &mut Transaction<'_>,
    id: &str,
    status: &str,
    result: Option<Value>,
    error: Option<String>,
) -> Result<()> {
    let key = workflow_key(id);
    let mut workflow = transaction
        .get::<WorkflowRecord>(&key)?
        .ok_or_else(|| anyhow!("workflow record missing"))?;
    workflow.status = status.to_owned();
    workflow.result = result.clone();
    workflow.error = error.clone();
    workflow.updated_at = now();
    transaction.put(key, &workflow)?;
    transaction.delete(workflow_deadline_key(id));
    append_event(
        transaction,
        id,
        &format!("WORKFLOW_{status}"),
        json!({"result": result, "error": error}),
    )?;
    if status == "COMPLETED" {
        let message_id = format!("workflow:{id}:run:{}", workflow.run_number);
        let outbox_key = outbox_key("workflows", &message_id);
        if transaction.get::<OutboxMessage>(&outbox_key)?.is_none() {
            transaction.put(
                outbox_key,
                &OutboxMessage {
                    sink: "workflows".to_owned(),
                    message_id,
                    workflow_id: id.to_owned(),
                    payload: workflow.result.clone().unwrap_or(Value::Null),
                    created_at: now(),
                    lease_owner: None,
                    lease_expires: None,
                    delivery_attempt: 0,
                    acked_at: None,
                },
            )?;
        }
    }
    for (key, activity) in transaction.scan::<ActivityRecord>("activity/")? {
        if activity.workflow_id == id {
            transaction.delete(key);
        }
    }
    for (key, timer) in transaction.scan::<TimerRecord>("timer/")? {
        if timer.workflow_id == id {
            transaction.delete(key);
        }
    }
    for (key, timer) in transaction.scan::<WatermarkTimerRecord>("watermark-timer/")? {
        if timer.workflow_id == id {
            transaction.delete(key);
        }
    }
    let child_ids: Vec<String> = transaction
        .scan::<String>(&workflow_child_prefix(id))?
        .into_iter()
        .map(|(_, child_id)| child_id)
        .collect();
    let mut children = Vec::new();
    for child_id in child_ids {
        if let Some(child) = transaction.get::<WorkflowRecord>(&workflow_key(&child_id))?
            && child.status == "RUNNING"
        {
            children.push(child);
        }
    }
    for child in children {
        match child.parent_close_policy.as_deref() {
            Some("REQUEST_CANCEL") => {
                append_event(
                    transaction,
                    &child.workflow_id,
                    "CANCEL_REQUESTED",
                    json!({"parent_closed": true}),
                )?;
                enqueue_workflow(transaction, &child)?;
            }
            Some("TERMINATE") => {
                close_workflow(
                    transaction,
                    &child.workflow_id,
                    "TERMINATED",
                    None,
                    Some("parent closed".to_owned()),
                )?;
                transaction.delete(workflow_task_key(&child.workflow_id));
            }
            _ => {}
        }
    }
    if let Some(parent_id) = workflow.parent_id.as_deref() {
        transaction.delete(workflow_child_key(parent_id, id));
    }
    if let Some(parent_id) = workflow.parent_id
        && let Some(parent) = transaction.get::<WorkflowRecord>(&workflow_key(&parent_id))?
        && parent.status == "RUNNING"
    {
        let event_type = if status == "COMPLETED" {
            "CHILD_WORKFLOW_COMPLETED"
        } else {
            "CHILD_WORKFLOW_FAILED"
        };
        append_event(
            transaction,
            &parent_id,
            event_type,
            json!({
                "command_id": workflow.parent_command_id, "result": workflow.result, "error": workflow.error,
            }),
        )?;
        enqueue_workflow(transaction, &parent)?;
    }
    finish_process_execution(transaction, id, status, workflow.result.as_ref())?;
    Ok(())
}

pub(crate) fn apply_command(
    transaction: &mut Transaction<'_>,
    workflow_id: &str,
    command: Command,
) -> Result<()> {
    let attributes = command.attributes;
    match command.command_type.as_str() {
        "COMPLETE_UPDATE" => {
            append_event(transaction, workflow_id, "UPDATE_COMPLETED", attributes)?;
        }
        "FAIL_UPDATE" => {
            append_event(transaction, workflow_id, "UPDATE_FAILED", attributes)?;
        }
        "START_TIMER" => {
            append_event(
                transaction,
                workflow_id,
                "TIMER_STARTED",
                attributes.clone(),
            )?;
            let command_id = attributes["command_id"]
                .as_u64()
                .context("timer command_id")?;
            let seconds = attributes["seconds"].as_f64().context("timer seconds")?;
            transaction.put(
                timer_key(workflow_id, command_id),
                &TimerRecord {
                    workflow_id: workflow_id.to_owned(),
                    command_id,
                    fire_at: now() + seconds,
                },
            )?;
        }
        "START_WATERMARK_TIMER" => {
            append_event(
                transaction,
                workflow_id,
                "WATERMARK_TIMER_STARTED",
                attributes.clone(),
            )?;
            let stream = attributes["stream"]
                .as_str()
                .context("watermark timer stream")?;
            let event_time = attributes["event_time"]
                .as_f64()
                .context("watermark timer event_time")?;
            let command_id = attributes["command_id"]
                .as_u64()
                .context("watermark timer command_id")?;
            let state = transaction
                .get::<StreamState>(&stream_state_key(stream))?
                .ok_or_else(|| anyhow!("stream not found: {stream}"))?;
            if state.finalized
                || state
                    .watermark
                    .is_some_and(|watermark| watermark >= event_time)
            {
                append_event(
                    transaction,
                    workflow_id,
                    "WATERMARK_TIMER_FIRED",
                    json!({
                        "command_id": command_id,
                        "stream": stream,
                        "event_time": event_time,
                        "watermark": state.watermark,
                        "finalized": state.finalized,
                    }),
                )?;
                let workflow = transaction
                    .get::<WorkflowRecord>(&workflow_key(workflow_id))?
                    .context("workflow missing")?;
                enqueue_workflow(transaction, &workflow)?;
            } else {
                transaction.put(
                    watermark_timer_key(stream, workflow_id, command_id),
                    &WatermarkTimerRecord {
                        stream: stream.to_owned(),
                        workflow_id: workflow_id.to_owned(),
                        command_id,
                        event_time,
                    },
                )?;
            }
        }
        "SCHEDULE_ACTIVITY" => {
            let id = transaction
                .get::<u64>("meta/activity_sequence")?
                .unwrap_or(0)
                + 1;
            transaction.put("meta/activity_sequence", &id)?;
            append_event(
                transaction,
                workflow_id,
                "ACTIVITY_SCHEDULED",
                attributes.clone(),
            )?;
            let workflow = transaction
                .get::<WorkflowRecord>(&workflow_key(workflow_id))?
                .context("workflow missing")?;
            let retry = &attributes["options"]["retry_policy"];
            let task_queue = attributes["options"]["task_queue"]
                .as_str()
                .unwrap_or(&workflow.task_queue)
                .to_owned();
            transaction.put(
                activity_key(id),
                &ActivityRecord {
                    id,
                    workflow_id: workflow_id.to_owned(),
                    command_id: attributes["command_id"]
                        .as_u64()
                        .context("activity command_id")?,
                    name: attributes["name"]
                        .as_str()
                        .context("activity name")?
                        .to_owned(),
                    args: attributes["args"]
                        .as_array()
                        .context("activity args")?
                        .clone(),
                    task_queue,
                    attempt: 1,
                    max_attempts: retry["maximum_attempts"].as_u64().unwrap_or(3) as u32,
                    initial_interval: retry["initial_interval"].as_f64().unwrap_or(0.1),
                    backoff: retry["backoff_coefficient"].as_f64().unwrap_or(2.0),
                    max_interval: retry["maximum_interval"].as_f64().unwrap_or(30.0),
                    schedule_deadline: attributes["options"]["schedule_to_close_timeout"]
                        .as_f64()
                        .map(|timeout| now() + timeout),
                    start_to_close_timeout: attributes["options"]["start_to_close_timeout"]
                        .as_f64(),
                    heartbeat_timeout: attributes["options"]["heartbeat_timeout"].as_f64(),
                    available_at: now(),
                    lease_owner: None,
                    lease_expires: None,
                    task_token: None,
                },
            )?;
        }
        "RECORD_VERSION" => {
            append_event(
                transaction,
                workflow_id,
                "VERSION_MARKER",
                json!({
                    "change_id": attributes["change_id"], "version": attributes["version"],
                }),
            )?;
            let workflow = transaction
                .get::<WorkflowRecord>(&workflow_key(workflow_id))?
                .context("workflow missing")?;
            enqueue_workflow(transaction, &workflow)?;
        }
        "START_CHILD" => {
            append_event(
                transaction,
                workflow_id,
                "CHILD_WORKFLOW_SCHEDULED",
                attributes.clone(),
            )?;
            let child_id = attributes["workflow_id"]
                .as_str()
                .context("child workflow_id")?
                .to_owned();
            if transaction
                .get::<WorkflowRecord>(&workflow_key(&child_id))?
                .is_some()
            {
                bail!("child workflow already exists");
            }
            let parent = transaction
                .get::<WorkflowRecord>(&workflow_key(workflow_id))?
                .context("parent missing")?;
            let timestamp = now();
            let child = WorkflowRecord {
                workflow_id: child_id.clone(),
                workflow_type: attributes["name"]
                    .as_str()
                    .context("child name")?
                    .to_owned(),
                status: "RUNNING".to_owned(),
                result: None,
                error: None,
                task_queue: parent.task_queue,
                build_id: parent.build_id,
                run_number: 1,
                parent_id: Some(workflow_id.to_owned()),
                parent_command_id: attributes["command_id"].as_u64(),
                parent_close_policy: attributes["parent_close_policy"]
                    .as_str()
                    .map(str::to_owned),
                execution_deadline: None,
                created_at: timestamp,
                updated_at: timestamp,
            };
            transaction.put(workflow_key(&child_id), &child)?;
            transaction.put(workflow_child_key(workflow_id, &child_id), &child_id)?;
            append_event(
                transaction,
                &child_id,
                "WORKFLOW_STARTED",
                json!({"workflow_type": child.workflow_type, "args": attributes["args"], "run_number": 1}),
            )?;
            enqueue_workflow(transaction, &child)?;
        }
        "CONTINUE_AS_NEW" => {
            let key = workflow_key(workflow_id);
            let mut workflow = transaction
                .get::<WorkflowRecord>(&key)?
                .context("workflow missing")?;
            let history: Vec<Event> = transaction
                .scan::<Event>(&event_prefix(workflow_id))?
                .into_iter()
                .map(|(_, event)| event)
                .collect();
            append_event(
                transaction,
                workflow_id,
                "WORKFLOW_CONTINUED_AS_NEW",
                json!({"args": attributes["args"]}),
            )?;
            transaction.put(
                format!(
                    "archive/{}/{:020}",
                    encoded(workflow_id),
                    workflow.run_number
                ),
                &json!({
                    "workflow_id": workflow_id,
                    "run_number": workflow.run_number,
                    "workflow_type": workflow.workflow_type,
                    "history": history,
                    "closed_at": now(),
                }),
            )?;
            for (event_key, _) in transaction.scan::<Event>(&event_prefix(workflow_id))? {
                transaction.delete(event_key);
            }
            for (runtime_key, activity) in transaction.scan::<ActivityRecord>("activity/")? {
                if activity.workflow_id == workflow_id {
                    transaction.delete(runtime_key);
                }
            }
            for (runtime_key, timer) in transaction.scan::<TimerRecord>("timer/")? {
                if timer.workflow_id == workflow_id {
                    transaction.delete(runtime_key);
                }
            }
            for (runtime_key, timer) in
                transaction.scan::<WatermarkTimerRecord>("watermark-timer/")?
            {
                if timer.workflow_id == workflow_id {
                    transaction.delete(runtime_key);
                }
            }
            workflow.run_number += 1;
            workflow.updated_at = now();
            transaction.put(key, &workflow)?;
            append_event(
                transaction,
                workflow_id,
                "WORKFLOW_STARTED",
                json!({
                    "workflow_type": workflow.workflow_type,
                    "args": attributes["args"],
                    "run_number": workflow.run_number,
                }),
            )?;
            enqueue_workflow(transaction, &workflow)?;
        }
        "COMPLETE_WORKFLOW" => close_workflow(
            transaction,
            workflow_id,
            "COMPLETED",
            attributes.get("result").cloned(),
            None,
        )?,
        "FAIL_WORKFLOW" => close_workflow(
            transaction,
            workflow_id,
            "FAILED",
            None,
            attributes["error"].as_str().map(str::to_owned),
        )?,
        "CANCEL_WORKFLOW" => close_workflow(
            transaction,
            workflow_id,
            "CANCELLED",
            None,
            Some("cancelled".to_owned()),
        )?,
        other => bail!("unsupported activation command {other}"),
    }
    Ok(())
}

pub(crate) fn apply_workflow_completion(
    transaction: &mut Transaction<'_>,
    completion: WorkflowCompletion,
) -> Result<()> {
    if completion.protocol_version != PROTOCOL_VERSION {
        bail!("unsupported protocol version");
    }
    let workflow_id = transaction
        .get::<String>(&workflow_task_token_key(&completion.task_token))?
        .ok_or_else(|| anyhow!("workflow task lease lost"))?;
    let key = workflow_task_key(&workflow_id);
    let task = transaction
        .get::<WorkflowTask>(&key)?
        .filter(|task| task.task_token.as_deref() == Some(&completion.task_token))
        .ok_or_else(|| anyhow!("workflow task lease lost"))?;
    transaction.delete(workflow_task_token_key(&completion.task_token));
    let workflow_id = task.workflow_id.clone();
    if let Some(failure) = completion.failure {
        append_event(
            transaction,
            &workflow_id,
            "WORKFLOW_TASK_FAILED",
            json!({"error": failure, "attempt": task.attempt}),
        )?;
        if let Some(mut execution) =
            transaction.get::<ProcessExecution>(&process_execution_key(&workflow_id))?
        {
            let storage_key = process_key(&execution.process_id);
            let mut process = transaction
                .get::<DurableProcess>(&storage_key)?
                .ok_or_else(|| anyhow!("process missing: {}", execution.process_id))?;
            if execution.attempt == 0 {
                process.running = process.running.saturating_sub(1);
                process.retrying += 1;
            }
            execution.attempt = task.attempt;
            execution.last_failure = Some(failure.clone());
            transaction.put(process_execution_key(&workflow_id), &execution)?;
            if process_failure_disposition(task.attempt, process.max_attempts)
                == ProcessFailureDisposition::Quarantine
            {
                transaction.put(
                    process_quarantine_key(
                        &execution.process_id,
                        execution.shard as usize,
                        execution.record.sequence,
                    ),
                    &ProcessQuarantineRecord {
                        process_id: execution.process_id.clone(),
                        key: execution.key.clone(),
                        sequence: execution.record.sequence,
                        event_time: execution.event_time,
                        record: execution.record.clone(),
                        attempts: task.attempt,
                        failure: failure.clone(),
                        quarantined_at: now(),
                    },
                )?;
                process.quarantined += 1;
                transaction.put(&storage_key, &process)?;
                transaction.delete(key);
                close_workflow(transaction, &workflow_id, "FAILED", None, Some(failure))?;
                return Ok(());
            }
            transaction.put(storage_key, &process)?;
        }
        let mut retry = task;
        retry.lease_owner = None;
        retry.lease_expires = None;
        retry.task_token = None;
        retry.available_at =
            now() + (0.1 * 2f64.powi((retry.attempt.saturating_sub(1)) as i32)).min(5.0);
        retry.enqueued_at = retry.available_at;
        transaction.put(key, &retry)?;
        return Ok(());
    }
    let external_arrived = transaction
        .scan::<Event>(&event_prefix(&workflow_id))?
        .last()
        .is_some_and(|(_, event)| event.id > completion.history_event_id);
    transaction.delete(key);
    for command in completion.commands {
        apply_command(transaction, &workflow_id, command)?;
    }
    if external_arrived
        && let Some(workflow) = transaction.get::<WorkflowRecord>(&workflow_key(&workflow_id))?
        && workflow.status == "RUNNING"
    {
        enqueue_workflow(transaction, &workflow)?;
    }
    Ok(())
}

pub(crate) async fn complete_workflow(
    State(state): State<AppState>,
    Json(completion): Json<WorkflowCompletion>,
) -> Result<impl IntoResponse, ApiError> {
    state.commit(|transaction| apply_workflow_completion(transaction, completion))?;
    Ok(Json(json!({})))
}

pub(crate) async fn complete_workflow_batch(
    State(state): State<AppState>,
    Json(completions): Json<Vec<WorkflowCompletion>>,
) -> Result<impl IntoResponse, ApiError> {
    if completions.is_empty() || completions.len() > 1_024 {
        return Err(ApiError(anyhow!(
            "workflow completion batches must contain between 1 and 1024 items"
        )));
    }
    state.commit(|transaction| {
        transaction.defer_process_dispatch = true;
        for completion in completions {
            apply_workflow_completion(transaction, completion)?;
        }
        transaction.defer_process_dispatch = false;
        refresh_processes(transaction, None)
    })?;
    Ok(Json(json!({})))
}

pub(crate) async fn poll_activity(
    State(state): State<AppState>,
    Json(request): Json<PollRequest>,
) -> Result<Response, ApiError> {
    if request.protocol_version != PROTOCOL_VERSION {
        return Err(ApiError(anyhow!("unsupported protocol version")));
    }
    state.authorize_poll(&request)?;
    let mut selected = None;
    state.commit(|transaction| {
        let timestamp = now();
        let expired: Vec<(String, ActivityRecord)> = transaction
            .scan("activity/")?
            .into_iter()
            .filter(|(_, task): &(String, ActivityRecord)| {
                task.schedule_deadline
                    .is_some_and(|deadline| deadline <= timestamp)
            })
            .collect();
        for (key, task) in expired {
            append_event(
                transaction,
                &task.workflow_id,
                "ACTIVITY_FAILED",
                json!({"command_id": task.command_id, "result": null, "error": "schedule-to-close timeout"}),
            )?;
            transaction.delete(key);
            if let Some(workflow) =
                transaction.get::<WorkflowRecord>(&workflow_key(&task.workflow_id))?
            {
                enqueue_workflow(transaction, &workflow)?;
            }
        }
        let mut candidates: Vec<(String, ActivityRecord)> = transaction
            .scan("activity/")?
            .into_iter()
            .filter(|(_, task): &(String, ActivityRecord)| {
                request
                    .task_queue
                    .as_ref()
                    .is_none_or(|queue| queue == &task.task_queue)
                    && task.available_at <= timestamp
                    && task.lease_expires.is_none_or(|expiry| expiry <= timestamp)
            })
            .collect();
        candidates.sort_by(|left, right| left.1.available_at.total_cmp(&right.1.available_at));
        let Some((key, mut task)) = candidates.into_iter().next() else {
            return Ok(());
        };
        let token = Uuid::new_v4().to_string();
        let lease_seconds = task
            .heartbeat_timeout
            .map_or(request.lease_seconds, |timeout| {
                timeout.min(request.lease_seconds)
            });
        task.lease_owner = Some(request.worker_id.clone());
        task.lease_expires = Some(timestamp + lease_seconds);
        task.task_token = Some(token.clone());
        transaction.put(key, &task)?;
        selected = Some(ActivityTask {
            protocol_version: PROTOCOL_VERSION,
            task_token: token,
            id: task.id,
            workflow_id: task.workflow_id,
            name: task.name,
            args: task.args,
            attempt: task.attempt,
            lease_seconds,
            start_to_close_timeout: task.start_to_close_timeout,
        });
        Ok(())
    })?;
    Ok(match selected {
        Some(value) => Json(value).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    })
}

pub(crate) async fn complete_activity(
    State(state): State<AppState>,
    Json(completion): Json<ActivityCompletion>,
) -> Result<impl IntoResponse, ApiError> {
    state.commit(|transaction| {
        let tasks: Vec<(String, ActivityRecord)> = transaction.scan("activity/")?;
        let (key, mut task) = tasks
            .into_iter()
            .find(|(_, task)| task.task_token.as_deref() == Some(&completion.task_token))
            .ok_or_else(|| anyhow!("activity task lease lost"))?;
        if let Some(error) = completion.error {
            if !completion.non_retryable && task.attempt < task.max_attempts {
                let delay = (task.initial_interval * task.backoff.powi((task.attempt - 1) as i32))
                    .min(task.max_interval);
                if task
                    .schedule_deadline
                    .is_none_or(|deadline| now() + delay < deadline)
                {
                    append_event(
                        transaction,
                        &task.workflow_id,
                        "ACTIVITY_RETRY_SCHEDULED",
                        json!({
                            "command_id": task.command_id,
                            "failed_attempt": task.attempt,
                            "next_attempt": task.attempt + 1,
                            "delay_seconds": delay,
                            "error": error,
                        }),
                    )?;
                    task.attempt += 1;
                    task.available_at = now() + delay;
                    task.lease_owner = None;
                    task.lease_expires = None;
                    task.task_token = None;
                    transaction.put(key, &task)?;
                    return Ok(());
                }
            }
            append_event(
                transaction,
                &task.workflow_id,
                "ACTIVITY_FAILED",
                json!({"command_id": task.command_id, "result": null, "error": error}),
            )?;
        } else {
            append_event(
                transaction,
                &task.workflow_id,
                "ACTIVITY_COMPLETED",
                json!({"command_id": task.command_id, "result": completion.result, "error": null}),
            )?;
        }
        transaction.delete(key);
        if let Some(workflow) =
            transaction.get::<WorkflowRecord>(&workflow_key(&task.workflow_id))?
        {
            enqueue_workflow(transaction, &workflow)?;
        }
        Ok(())
    })?;
    Ok(Json(json!({})))
}

pub(crate) async fn poll_query(
    State(state): State<AppState>,
    Json(request): Json<PollRequest>,
) -> Result<Response, ApiError> {
    if request.protocol_version != PROTOCOL_VERSION {
        return Err(ApiError(anyhow!("unsupported protocol version")));
    }
    state.authorize_poll(&request)?;
    let mut queue = state
        .query_queue
        .lock()
        .map_err(|_| anyhow!("query queue lock poisoned"))?;
    let position = queue.iter().position(|(task_queue, _)| {
        request
            .task_queue
            .as_ref()
            .is_none_or(|selected| selected == task_queue)
    });
    Ok(match position.and_then(|position| queue.remove(position)) {
        Some((_, task)) => Json(task).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    })
}

pub(crate) async fn complete_query(
    State(state): State<AppState>,
    Json(completion): Json<QueryCompletion>,
) -> Result<impl IntoResponse, ApiError> {
    if completion.protocol_version != PROTOCOL_VERSION {
        return Err(ApiError(anyhow!("unsupported protocol version")));
    }
    if let Some(sender) = state
        .query_results
        .lock()
        .map_err(|_| anyhow!("query result lock poisoned"))?
        .remove(&completion.task_token)
    {
        let result = match completion.error {
            Some(error) => Err(error),
            None => Ok(completion.result.unwrap_or(Value::Null)),
        };
        let _ = sender.send(result);
    }
    Ok(Json(json!({})))
}

pub(crate) async fn heartbeat_activity(
    State(state): State<AppState>,
    Path(token): Path<String>,
    Json(_body): Json<Value>,
) -> Result<impl IntoResponse, ApiError> {
    state.commit(|transaction| {
        let tasks: Vec<(String, ActivityRecord)> = transaction.scan("activity/")?;
        let (key, mut task) = tasks
            .into_iter()
            .find(|(_, task)| task.task_token.as_deref() == Some(&token))
            .ok_or_else(|| anyhow!("activity task lease lost"))?;
        task.lease_expires = Some(now() + task.heartbeat_timeout.unwrap_or(30.0));
        transaction.put(key, &task)
    })?;
    Ok(Json(json!({"accepted": true})))
}
