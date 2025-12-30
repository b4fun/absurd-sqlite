use crate::retry;
use crate::sql;
use crate::validate;
use chrono::DateTime;
use serde_json::Value as JsonValue;
use sqlite3ext_sys::sqlite3;
use sqlite_loadable::prelude::*;
use sqlite_loadable::{api, Error, Result};
use uuid::Uuid;

fn parse_optional_time_ms(value: &*mut sqlite3_value, label: &str) -> Result<Option<i64>> {
    if api::value_is_null(value) {
        return Ok(None);
    }
    match api::value_type(value) {
        api::ValueType::Integer => Ok(Some(api::value_int64(value))),
        api::ValueType::Text => {
            let raw = api::value_text(value).unwrap_or("").trim();
            if raw.is_empty() {
                Ok(None)
            } else if let Ok(parsed) = raw.parse::<i64>() {
                Ok(Some(parsed))
            } else if let Ok(parsed) = DateTime::parse_from_rfc3339(raw) {
                Ok(Some(parsed.timestamp_millis()))
            } else {
                Err(Error::new_message(format!(
                    "{label} must be integer ms or RFC3339"
                )))
            }
        }
        _ => Err(Error::new_message(format!(
            "{label} must be integer ms or RFC3339"
        ))),
    }
}

fn parse_required_int(value: &*mut sqlite3_value, label: &str) -> Result<i64> {
    if api::value_is_null(value) {
        return Err(Error::new_message(format!("{label} must be provided")));
    }
    match api::value_type(value) {
        api::ValueType::Integer => Ok(api::value_int64(value)),
        api::ValueType::Float => Ok(api::value_double(value) as i64),
        api::ValueType::Text => {
            let raw = api::value_text(value).unwrap_or("").trim();
            if raw.is_empty() {
                Err(Error::new_message(format!("{label} must be provided")))
            } else {
                raw.parse::<i64>().map_err(|err| {
                    Error::new_message(format!("{label} must be integer: {:?}", err))
                })
            }
        }
        _ => Err(Error::new_message(format!("{label} must be integer",))),
    }
}

fn parse_required_ttl_secs(value: &*mut sqlite3_value, label: &str) -> Result<i64> {
    let ttl = parse_required_int(value, label)?;
    if ttl < 0 {
        return Err(Error::new_message(
            "TTL must be a non-negative number of seconds",
        ));
    }
    Ok(ttl)
}

fn parse_optional_limit(value: Option<*mut sqlite3_value>) -> Result<i64> {
    let value = match value {
        Some(value) => value,
        None => return Ok(1000),
    };
    if api::value_is_null(&value) {
        return Ok(1000);
    }
    let limit = parse_required_int(&value, "limit")?;
    if limit < 1 {
        return Err(Error::new_message("limit must be >= 1"));
    }
    Ok(limit)
}

fn fail_run_impl(
    db: *mut sqlite3,
    queue_name: &str,
    run_id: &str,
    reason: &str,
    retry_at: Option<i64>,
) -> Result<()> {
    validate::queue_name(queue_name)?;
    let _: JsonValue = serde_json::from_str(reason)
        .map_err(|err| Error::new_message(format!("reason must be valid JSON: {:?}", err)))?;

    let now = sql::now_ms_from_db(db);
    let now_value = now.to_string();
    let retry_at_value = retry_at.unwrap_or(0);

    sql::exec_with_bind_text(db, "begin immediate", &[])?;

    let result = (|| -> Result<()> {
        let mut stmt = sqlite_loadable::exec::Statement::prepare(
            db,
            "select task_id, attempt
               from absurd_runs
              where queue_name = ?1
                and run_id = ?2
                and state in ('running','sleeping')",
        )
        .map_err(|err| Error::new_message(format!("failed to prepare run lookup: {:?}", err)))?;
        stmt.bind_text(1, queue_name)
            .map_err(|err| Error::new_message(format!("failed to bind queue_name: {:?}", err)))?;
        stmt.bind_text(2, run_id)
            .map_err(|err| Error::new_message(format!("failed to bind run_id: {:?}", err)))?;
        let mut rows = stmt.execute();
        let row = rows
            .next()
            .ok_or_else(|| Error::new_message("run not found or not active"))?
            .map_err(|err| Error::new_message(format!("failed to read run row: {:?}", err)))?;
        let task_id = row
            .get::<String>(0)
            .map_err(|err| Error::new_message(format!("failed to read task_id: {:?}", err)))?;
        let attempt = row
            .get::<i64>(1)
            .map_err(|err| Error::new_message(format!("failed to read attempt: {:?}", err)))?;

        let mut task_stmt = sqlite_loadable::exec::Statement::prepare(
            db,
            "select coalesce(json(retry_strategy), '') as retry_strategy,
                    coalesce(max_attempts, 0) as max_attempts,
                    coalesce(first_started_at, 0) as first_started_at,
                    coalesce(json(cancellation), '') as cancellation
               from absurd_tasks
              where queue_name = ?1
                and task_id = ?2",
        )
        .map_err(|err| Error::new_message(format!("failed to prepare task lookup: {:?}", err)))?;
        task_stmt
            .bind_text(1, queue_name)
            .map_err(|err| Error::new_message(format!("failed to bind queue_name: {:?}", err)))?;
        task_stmt
            .bind_text(2, &task_id)
            .map_err(|err| Error::new_message(format!("failed to bind task_id: {:?}", err)))?;
        let mut task_rows = task_stmt.execute();
        let task_row = task_rows
            .next()
            .ok_or_else(|| Error::new_message("task not found"))?
            .map_err(|err| Error::new_message(format!("failed to read task row: {:?}", err)))?;
        let retry_strategy = task_row.get::<String>(0).map_err(|err| {
            Error::new_message(format!("failed to read retry_strategy: {:?}", err))
        })?;
        let max_attempts = task_row
            .get::<i64>(1)
            .map_err(|err| Error::new_message(format!("failed to read max_attempts: {:?}", err)))?;
        let first_started_at = task_row.get::<i64>(2).map_err(|err| {
            Error::new_message(format!("failed to read first_started_at: {:?}", err))
        })?;
        let cancellation = task_row
            .get::<String>(3)
            .map_err(|err| Error::new_message(format!("failed to read cancellation: {:?}", err)))?;

        sql::exec_with_bind_text(
            db,
            "update absurd_runs
                set state = 'failed',
                    wake_event = null,
                    failed_at = cast(?1 as integer),
                    failure_reason = jsonb(?2),
                    claimed_by = null,
                    claim_expires_at = null
              where queue_name = ?3
                and run_id = ?4",
            &[&now_value, reason, queue_name, run_id],
        )?;

        let next_attempt = attempt + 1;
        let max_attempts_opt = if max_attempts <= 0 {
            None
        } else {
            Some(max_attempts)
        };
        let allow_retry = max_attempts_opt.is_none_or(|max| next_attempt <= max);
        let mut task_state = "failed";
        let mut last_attempt_run = run_id.to_string();
        let mut cancelled_at = "";
        let mut recorded_attempt = attempt;

        if allow_retry {
            let mut next_available = if retry_at_value > 0 {
                retry_at_value
            } else {
                now + retry::parse_retry_strategy(&retry_strategy, attempt)?
            };
            if next_available < now {
                next_available = now;
            }

            let max_duration_ms = retry::parse_cancellation_max_duration(&cancellation)?;
            let cancel_task = if let Some(max_duration) = max_duration_ms {
                first_started_at > 0 && (next_available - first_started_at) >= max_duration
            } else {
                false
            };

            if cancel_task {
                task_state = "cancelled";
                cancelled_at = &now_value;
            } else {
                let new_run_id = Uuid::now_v7().to_string();
                let next_available_value = next_available.to_string();
                let next_attempt_value = next_attempt.to_string();
                let run_state = if next_available > now {
                    "sleeping"
                } else {
                    "pending"
                };
                sql::exec_with_bind_text(
                    db,
                    "insert into absurd_runs (
                        queue_name,
                        run_id,
                        task_id,
                        attempt,
                        state,
                        available_at,
                        wake_event,
                        event_payload,
                        result,
                        failure_reason
                     )
                     values (
                        ?1,
                        ?2,
                        ?3,
                        cast(?4 as integer),
                        ?5,
                        cast(?6 as integer),
                        null,
                        null,
                        null,
                        null
                     )",
                    &[
                        queue_name,
                        &new_run_id,
                        &task_id,
                        &next_attempt_value,
                        run_state,
                        &next_available_value,
                    ],
                )?;
                task_state = run_state;
                last_attempt_run = new_run_id;
                recorded_attempt = next_attempt;
            }
        }

        let attempt_value = recorded_attempt.to_string();
        sql::exec_with_bind_text(
            db,
            "update absurd_tasks
                set state = ?1,
                    attempts = case
                      when attempts < cast(?2 as integer) then cast(?2 as integer)
                      else attempts
                    end,
                    last_attempt_run = ?3,
                    cancelled_at = nullif(?4, '')
              where queue_name = ?5
                and task_id = ?6",
            &[
                task_state,
                &attempt_value,
                &last_attempt_run,
                cancelled_at,
                queue_name,
                &task_id,
            ],
        )?;

        sql::exec_with_bind_text(
            db,
            "delete from absurd_waits
              where queue_name = ?1
                and run_id = ?2",
            &[queue_name, run_id],
        )?;

        Ok(())
    })();

    match result {
        Ok(()) => {
            sql::exec_with_bind_text(db, "commit", &[])?;
            Ok(())
        }
        Err(err) => {
            let _ = sql::exec_with_bind_text(db, "rollback", &[]);
            Err(err)
        }
    }
}

pub fn absurd_complete_run(
    context: *mut sqlite3_context,
    values: &[*mut sqlite3_value],
) -> Result<()> {
    let queue_name = api::value_text_notnull(values.first().expect("queue_name"))?;
    let run_id = api::value_text_notnull(values.get(1).expect("run_id"))?;
    let state_value = values.get(2);
    let state_text = state_value
        .and_then(|value| {
            if api::value_is_null(value) {
                None
            } else {
                let raw = api::value_text(value).ok()?.trim().to_string();
                if raw.is_empty() {
                    None
                } else {
                    Some(raw)
                }
            }
        })
        .unwrap_or_default();

    if !state_text.is_empty() {
        let _: JsonValue = serde_json::from_str(&state_text)
            .map_err(|err| Error::new_message(format!("state must be valid JSON: {:?}", err)))?;
    }

    validate::queue_name(queue_name)?;
    let db = api::context_db_handle(context);
    let now_value = sql::now_ms_from_db(db).to_string();

    sql::exec_with_bind_text(db, "begin immediate", &[])?;

    let result = (|| -> Result<()> {
        let mut stmt = sqlite_loadable::exec::Statement::prepare(
            db,
            "select task_id, state
               from absurd_runs
              where queue_name = ?1
                and run_id = ?2",
        )
        .map_err(|err| Error::new_message(format!("failed to prepare run lookup: {:?}", err)))?;
        stmt.bind_text(1, queue_name)
            .map_err(|err| Error::new_message(format!("failed to bind queue_name: {:?}", err)))?;
        stmt.bind_text(2, run_id)
            .map_err(|err| Error::new_message(format!("failed to bind run_id: {:?}", err)))?;
        let mut rows = stmt.execute();
        let row = rows
            .next()
            .ok_or_else(|| Error::new_message("run not found"))?
            .map_err(|err| Error::new_message(format!("failed to read run row: {:?}", err)))?;
        let task_id = row
            .get::<String>(0)
            .map_err(|err| Error::new_message(format!("failed to read task_id: {:?}", err)))?;
        let run_state = row
            .get::<String>(1)
            .map_err(|err| Error::new_message(format!("failed to read state: {:?}", err)))?;

        if run_state != "running" {
            return Err(Error::new_message("run is not currently running"));
        }

        sql::exec_with_bind_text(
            db,
            "update absurd_runs
                set state = 'completed',
                    completed_at = cast(?1 as integer),
                    result = case when ?2 = '' then null else jsonb(?2) end
              where queue_name = ?3
                and run_id = ?4",
            &[&now_value, &state_text, queue_name, run_id],
        )?;

        sql::exec_with_bind_text(
            db,
            "update absurd_tasks
                set state = 'completed',
                    completed_payload = case when ?1 = '' then null else jsonb(?1) end,
                    last_attempt_run = ?2
              where queue_name = ?3
                and task_id = ?4",
            &[&state_text, run_id, queue_name, &task_id],
        )?;

        sql::exec_with_bind_text(
            db,
            "delete from absurd_waits
              where queue_name = ?1
                and run_id = ?2",
            &[queue_name, run_id],
        )?;

        Ok(())
    })();

    match result {
        Ok(()) => {
            sql::exec_with_bind_text(db, "commit", &[])?;
            Ok(())
        }
        Err(err) => {
            let _ = sql::exec_with_bind_text(db, "rollback", &[]);
            Err(err)
        }
    }
}

pub fn absurd_fail_run(context: *mut sqlite3_context, values: &[*mut sqlite3_value]) -> Result<()> {
    let queue_name = api::value_text_notnull(values.first().expect("queue_name"))?;
    let run_id = api::value_text_notnull(values.get(1).expect("run_id"))?;
    let reason = api::value_text_notnull(values.get(2).expect("reason"))?;
    let retry_at = values
        .get(3)
        .map(|value| parse_optional_time_ms(value, "retry_at"))
        .transpose()?
        .flatten();

    let db = api::context_db_handle(context);
    fail_run_impl(db, queue_name, run_id, reason, retry_at)?;
    Ok(())
}

pub fn absurd_fail_run_no_retry(
    context: *mut sqlite3_context,
    values: &[*mut sqlite3_value],
) -> Result<()> {
    let queue_name = api::value_text_notnull(values.first().expect("queue_name"))?;
    let run_id = api::value_text_notnull(values.get(1).expect("run_id"))?;
    let reason = api::value_text_notnull(values.get(2).expect("reason"))?;
    let db = api::context_db_handle(context);
    fail_run_impl(db, queue_name, run_id, reason, None)?;
    Ok(())
}

pub fn absurd_extend_claim(
    context: *mut sqlite3_context,
    values: &[*mut sqlite3_value],
) -> Result<()> {
    let queue_name = api::value_text_notnull(values.first().expect("queue_name"))?;
    let run_id = api::value_text_notnull(values.get(1).expect("run_id"))?;
    let extend_by = parse_required_int(values.get(2).expect("extend_by"), "extend_by")?;

    validate::queue_name(queue_name)?;
    let db = api::context_db_handle(context);
    let now = sql::now_ms_from_db(db);
    let claim_expires_at = now + extend_by.saturating_mul(1000);
    let claim_value = claim_expires_at.to_string();

    sql::exec_with_bind_text(db, "begin immediate", &[])?;

    let result = (|| -> Result<()> {
        let mut stmt = sqlite_loadable::exec::Statement::prepare(
            db,
            "select t.state
               from absurd_runs r
               join absurd_tasks t
                 on t.queue_name = r.queue_name
                and t.task_id = r.task_id
              where r.queue_name = ?1
                and r.run_id = ?2",
        )
        .map_err(|err| Error::new_message(format!("failed to prepare task lookup: {:?}", err)))?;
        stmt.bind_text(1, queue_name)
            .map_err(|err| Error::new_message(format!("failed to bind queue_name: {:?}", err)))?;
        stmt.bind_text(2, run_id)
            .map_err(|err| Error::new_message(format!("failed to bind run_id: {:?}", err)))?;
        let mut rows = stmt.execute();
        let row = rows
            .next()
            .ok_or_else(|| Error::new_message("run not found"))?
            .map_err(|err| Error::new_message(format!("failed to read task row: {:?}", err)))?;
        let task_state = row
            .get::<String>(0)
            .map_err(|err| Error::new_message(format!("failed to read state: {:?}", err)))?;

        if task_state == "cancelled" {
            return Err(Error::new_message("Task has been cancelled"));
        }

        sql::exec_with_bind_text(
            db,
            "update absurd_runs
                set claim_expires_at = cast(?1 as integer)
              where queue_name = ?2
                and run_id = ?3
                and state = 'running'
                and claim_expires_at is not null",
            &[&claim_value, queue_name, run_id],
        )?;

        let changes = sql::query_row_i64(db, "select changes()", &[])?;
        if changes == 0 {
            return Err(Error::new_message("run is not currently running"));
        }

        Ok(())
    })();

    match result {
        Ok(()) => {
            sql::exec_with_bind_text(db, "commit", &[])?;
            Ok(())
        }
        Err(err) => {
            let _ = sql::exec_with_bind_text(db, "rollback", &[]);
            Err(err)
        }
    }
}

pub fn absurd_schedule_run(
    context: *mut sqlite3_context,
    values: &[*mut sqlite3_value],
) -> Result<()> {
    let queue_name = api::value_text_notnull(values.first().expect("queue_name"))?;
    let run_id = api::value_text_notnull(values.get(1).expect("run_id"))?;
    let wake_at = parse_optional_time_ms(values.get(2).expect("wake_at"), "wake_at")?
        .ok_or_else(|| Error::new_message("wake_at must be provided"))?;
    validate::queue_name(queue_name)?;

    let db = api::context_db_handle(context);
    sql::exec_with_bind_text(db, "begin immediate", &[])?;

    let result = (|| -> Result<()> {
        let mut stmt = sqlite_loadable::exec::Statement::prepare(
            db,
            "select task_id
               from absurd_runs
              where queue_name = ?1
                and run_id = ?2
                and state = 'running'",
        )
        .map_err(|err| Error::new_message(format!("failed to prepare run lookup: {:?}", err)))?;
        stmt.bind_text(1, queue_name)
            .map_err(|err| Error::new_message(format!("failed to bind queue_name: {:?}", err)))?;
        stmt.bind_text(2, run_id)
            .map_err(|err| Error::new_message(format!("failed to bind run_id: {:?}", err)))?;
        let mut rows = stmt.execute();
        let row = rows
            .next()
            .ok_or_else(|| Error::new_message("run not running"))?
            .map_err(|err| Error::new_message(format!("failed to read run row: {:?}", err)))?;
        let task_id = row
            .get::<String>(0)
            .map_err(|err| Error::new_message(format!("failed to read task_id: {:?}", err)))?;

        let wake_at_value = wake_at.to_string();
        sql::exec_with_bind_text(
            db,
            "update absurd_runs
                set state = 'sleeping',
                    claimed_by = null,
                    claim_expires_at = null,
                    available_at = cast(?1 as integer),
                    wake_event = null
              where queue_name = ?2
                and run_id = ?3",
            &[&wake_at_value, queue_name, run_id],
        )?;

        sql::exec_with_bind_text(
            db,
            "update absurd_tasks
                set state = 'sleeping'
              where queue_name = ?1
                and task_id = ?2",
            &[queue_name, &task_id],
        )?;

        Ok(())
    })();

    match result {
        Ok(()) => {
            sql::exec_with_bind_text(db, "commit", &[])?;
            Ok(())
        }
        Err(err) => {
            let _ = sql::exec_with_bind_text(db, "rollback", &[]);
            Err(err)
        }
    }
}

pub fn absurd_cleanup_tasks(
    context: *mut sqlite3_context,
    values: &[*mut sqlite3_value],
) -> Result<()> {
    let queue_name = api::value_text_notnull(values.first().expect("queue_name"))?;
    let ttl_seconds = parse_required_ttl_secs(values.get(1).expect("ttl_seconds"), "ttl_seconds")?;
    let limit = parse_optional_limit(values.get(2).copied())?;

    validate::queue_name(queue_name)?;
    let db = api::context_db_handle(context);
    let now = sql::now_ms_from_db(db);
    let cutoff = now - ttl_seconds.saturating_mul(1000);
    let cutoff_value = cutoff.to_string();
    let limit_value = limit.to_string();

    sql::exec_with_bind_text(db, "begin immediate", &[])?;

    let deleted = (|| -> Result<i64> {
        let mut select_stmt = sqlite_loadable::exec::Statement::prepare(
            db,
            "select task_id
               from (
                 select t.task_id as task_id,
                        case
                          when t.state = 'completed' then r.completed_at
                          when t.state = 'failed' then r.failed_at
                          when t.state = 'cancelled' then t.cancelled_at
                          else null
                        end as terminal_at
                   from absurd_tasks t
                   left join absurd_runs r
                     on r.queue_name = t.queue_name
                    and r.run_id = t.last_attempt_run
                  where t.queue_name = ?1
                    and t.state in ('completed','failed','cancelled')
               )
              where terminal_at is not null
                and terminal_at < cast(?2 as integer)
              order by terminal_at
              limit cast(?3 as integer)",
        )
        .map_err(|err| {
            Error::new_message(format!("failed to prepare cleanup_tasks select: {:?}", err))
        })?;
        select_stmt
            .bind_text(1, queue_name)
            .map_err(|err| Error::new_message(format!("failed to bind queue_name: {:?}", err)))?;
        select_stmt
            .bind_text(2, &cutoff_value)
            .map_err(|err| Error::new_message(format!("failed to bind cutoff: {:?}", err)))?;
        select_stmt
            .bind_text(3, &limit_value)
            .map_err(|err| Error::new_message(format!("failed to bind limit: {:?}", err)))?;

        let mut task_ids = Vec::new();
        for row in select_stmt.execute() {
            let row = row
                .map_err(|err| Error::new_message(format!("failed to read task row: {:?}", err)))?;
            let task_id = row
                .get::<String>(0)
                .map_err(|err| Error::new_message(format!("failed to read task_id: {:?}", err)))?;
            task_ids.push(task_id);
        }
        drop(select_stmt);

        if task_ids.is_empty() {
            return Ok(0);
        }

        let mut deleted_count = 0;
        for task_id in task_ids {
            sql::exec_with_bind_text(
                db,
                "delete from absurd_tasks where queue_name = ?1 and task_id = ?2",
                &[queue_name, &task_id],
            )?;
            let changes = sql::query_row_i64(db, "select changes()", &[])?;
            if changes > 0 {
                deleted_count += 1;
            }
        }

        Ok(deleted_count)
    })();

    let deleted = match deleted {
        Ok(count) => {
            sql::exec_with_bind_text(db, "commit", &[])?;
            count
        }
        Err(err) => {
            let _ = sql::exec_with_bind_text(db, "rollback", &[]);
            return Err(err);
        }
    };

    api::result_int64(context, deleted);
    Ok(())
}

pub fn absurd_cleanup_events(
    context: *mut sqlite3_context,
    values: &[*mut sqlite3_value],
) -> Result<()> {
    let queue_name = api::value_text_notnull(values.first().expect("queue_name"))?;
    let ttl_seconds = parse_required_ttl_secs(values.get(1).expect("ttl_seconds"), "ttl_seconds")?;
    let limit = parse_optional_limit(values.get(2).copied())?;

    validate::queue_name(queue_name)?;
    let db = api::context_db_handle(context);
    let now = sql::now_ms_from_db(db);
    let cutoff = now - ttl_seconds.saturating_mul(1000);
    let cutoff_value = cutoff.to_string();
    let limit_value = limit.to_string();

    sql::exec_with_bind_text(db, "begin immediate", &[])?;

    let deleted = (|| -> Result<i64> {
        let mut select_stmt = sqlite_loadable::exec::Statement::prepare(
            db,
            "select event_name
               from absurd_events
              where queue_name = ?1
                and emitted_at < cast(?2 as integer)
              order by emitted_at
              limit cast(?3 as integer)",
        )
        .map_err(|err| {
            Error::new_message(format!(
                "failed to prepare cleanup_events select: {:?}",
                err
            ))
        })?;
        select_stmt
            .bind_text(1, queue_name)
            .map_err(|err| Error::new_message(format!("failed to bind queue_name: {:?}", err)))?;
        select_stmt
            .bind_text(2, &cutoff_value)
            .map_err(|err| Error::new_message(format!("failed to bind cutoff: {:?}", err)))?;
        select_stmt
            .bind_text(3, &limit_value)
            .map_err(|err| Error::new_message(format!("failed to bind limit: {:?}", err)))?;

        let mut deleted_count = 0;
        for row in select_stmt.execute() {
            let row = row.map_err(|err| {
                Error::new_message(format!("failed to read event row: {:?}", err))
            })?;
            let event_name = row.get::<String>(0).map_err(|err| {
                Error::new_message(format!("failed to read event_name: {:?}", err))
            })?;
            sql::exec_with_bind_text(
                db,
                "delete from absurd_events where queue_name = ?1 and event_name = ?2",
                &[queue_name, &event_name],
            )?;
            let changes = sql::query_row_i64(db, "select changes()", &[])?;
            if changes > 0 {
                deleted_count += 1;
            }
        }
        drop(select_stmt);

        Ok(deleted_count)
    })();

    let deleted = match deleted {
        Ok(count) => {
            sql::exec_with_bind_text(db, "commit", &[])?;
            count
        }
        Err(err) => {
            let _ = sql::exec_with_bind_text(db, "rollback", &[]);
            return Err(err);
        }
    };

    api::result_int64(context, deleted);
    Ok(())
}

pub fn absurd_cancel_task(
    context: *mut sqlite3_context,
    values: &[*mut sqlite3_value],
) -> Result<()> {
    let queue_name = api::value_text_notnull(values.first().expect("queue_name"))?;
    let task_id = api::value_text_notnull(values.get(1).expect("task_id"))?;

    validate::queue_name(queue_name)?;
    let db = api::context_db_handle(context);
    let now_value = sql::now_ms_from_db(db).to_string();

    sql::exec_with_bind_text(db, "begin immediate", &[])?;

    let result = (|| -> Result<()> {
        let mut stmt = sqlite_loadable::exec::Statement::prepare(
            db,
            "select state
               from absurd_tasks
              where queue_name = ?1
                and task_id = ?2",
        )
        .map_err(|err| Error::new_message(format!("failed to prepare task lookup: {:?}", err)))?;
        stmt.bind_text(1, queue_name)
            .map_err(|err| Error::new_message(format!("failed to bind queue_name: {:?}", err)))?;
        stmt.bind_text(2, task_id)
            .map_err(|err| Error::new_message(format!("failed to bind task_id: {:?}", err)))?;
        let mut rows = stmt.execute();
        let row = rows
            .next()
            .ok_or_else(|| Error::new_message("Task not found"))?
            .map_err(|err| Error::new_message(format!("failed to read task row: {:?}", err)))?;
        let task_state = row
            .get::<String>(0)
            .map_err(|err| Error::new_message(format!("failed to read task state: {:?}", err)))?;

        if task_state == "completed" || task_state == "failed" || task_state == "cancelled" {
            return Ok(());
        }

        sql::exec_with_bind_text(
            db,
            "update absurd_tasks
                set state = 'cancelled',
                    cancelled_at = coalesce(cancelled_at, cast(?1 as integer))
              where queue_name = ?2
                and task_id = ?3",
            &[&now_value, queue_name, task_id],
        )?;

        sql::exec_with_bind_text(
            db,
            "update absurd_runs
                set state = 'cancelled',
                    claimed_by = null,
                    claim_expires_at = null
              where queue_name = ?1
                and task_id = ?2
                and state not in ('completed','failed','cancelled')",
            &[queue_name, task_id],
        )?;

        sql::exec_with_bind_text(
            db,
            "delete from absurd_waits
              where queue_name = ?1
                and task_id = ?2",
            &[queue_name, task_id],
        )?;

        Ok(())
    })();

    match result {
        Ok(()) => {
            sql::exec_with_bind_text(db, "commit", &[])?;
            Ok(())
        }
        Err(err) => {
            let _ = sql::exec_with_bind_text(db, "rollback", &[]);
            Err(err)
        }
    }
}
