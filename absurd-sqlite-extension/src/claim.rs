use crate::retry;
use crate::sql;
use crate::validate;
use serde_json::Value as JsonValue;
use sqlite3ext_sys::sqlite3;
use sqlite_loadable::prelude::*;
use sqlite_loadable::{
    api,
    table::{BestIndexError, ConstraintOperator, IndexInfo, VTab, VTabArguments, VTabCursor},
    Error, Result,
};
use std::os::raw::c_int;
use uuid::Uuid;

struct ClaimArgs {
    queue_name: String,
    worker_id: String,
    claim_timeout_secs: i64,
    qty: i64,
}

struct ClaimResult {
    run_id: String,
    task_id: String,
    attempt: i64,
    run_state: String,
    task_name: String,
    params: String,
    retry_strategy: Option<String>,
    max_attempts: Option<i64>,
    headers: Option<String>,
    wake_event: Option<String>,
    event_payload: Option<String>,
}

fn parse_claim_args(values: &[*mut sqlite3_value]) -> Result<ClaimArgs> {
    let queue_name =
        api::value_text_notnull(values.get(0).expect("queue_name is required"))?.to_string();
    validate::queue_name(&queue_name)?;

    let worker_id = values
        .get(1)
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
        .unwrap_or_else(|| "worker".to_string());

    let claim_timeout_secs = values
        .get(2)
        .and_then(|value| {
            if api::value_is_null(value) {
                None
            } else {
                Some(api::value_int64(value))
            }
        })
        .unwrap_or(30)
        .max(0);

    let qty = values
        .get(3)
        .and_then(|value| {
            if api::value_is_null(value) {
                None
            } else {
                Some(api::value_int64(value))
            }
        })
        .unwrap_or(1)
        .max(1);

    Ok(ClaimArgs {
        queue_name,
        worker_id,
        claim_timeout_secs,
        qty,
    })
}

fn apply_cancellation_rules(db: *mut sqlite3, queue_name: &str, now: i64) -> Result<()> {
    let now_value = now.to_string();
    sql::exec_with_bind_text(
        db,
        "with limits as (
            select task_id,
                   cast(jsonb_extract(cancellation, '$.max_delay') as integer) as max_delay,
                   cast(jsonb_extract(cancellation, '$.max_duration') as integer) as max_duration,
                   enqueue_at,
                   first_started_at
              from absurd_tasks
             where queue_name = ?1
               and state in ('pending','sleeping','running')
         ),
         to_cancel as (
            select task_id
              from limits
             where
               (
                 max_delay is not null
                 and first_started_at is null
                 and (?2 - enqueue_at) >= max_delay * 1000
               )
               or
               (
                 max_duration is not null
                 and first_started_at is not null
                 and (?2 - first_started_at) >= max_duration * 1000
               )
         )
         update absurd_tasks
            set state = 'cancelled',
                cancelled_at = coalesce(cancelled_at, cast(?2 as integer))
          where queue_name = ?1
            and task_id in (select task_id from to_cancel)",
        &[queue_name, &now_value],
    )?;

    sql::exec_with_bind_text(
        db,
        "update absurd_runs
            set state = 'cancelled',
                claimed_by = null,
                claim_expires_at = null,
                available_at = cast(?2 as integer),
                wake_event = null
          where queue_name = ?1
            and task_id in (select task_id from absurd_tasks where queue_name = ?1 and state = 'cancelled')
            and state <> 'cancelled'",
        &[queue_name, &now_value],
    )?;

    Ok(())
}

fn expire_claims(db: *mut sqlite3, queue_name: &str, now: i64) -> Result<()> {
    let now_value = now.to_string();
    let mut stmt = sqlite_loadable::exec::Statement::prepare(
        db,
        "select r.run_id,
                r.task_id,
                r.attempt,
                coalesce(r.claimed_by, '') as claimed_by,
                coalesce(r.claim_expires_at, 0) as claim_expires_at,
                coalesce(json(t.retry_strategy), '') as retry_strategy,
                coalesce(t.max_attempts, 0) as max_attempts,
                coalesce(t.first_started_at, 0) as first_started_at,
                coalesce(json(t.cancellation), '') as cancellation
           from absurd_runs r
           join absurd_tasks t
             on t.queue_name = r.queue_name
            and t.task_id = r.task_id
          where r.queue_name = ?1
            and r.state = 'running'
            and r.claim_expires_at is not null
            and r.claim_expires_at <= cast(?2 as integer)",
    )
    .map_err(|err| {
        Error::new_message(&format!("failed to prepare expired claim query: {:?}", err))
    })?;
    stmt.bind_text(1, queue_name)
        .map_err(|err| Error::new_message(&format!("failed to bind queue_name: {:?}", err)))?;
    stmt.bind_text(2, &now_value)
        .map_err(|err| Error::new_message(&format!("failed to bind now: {:?}", err)))?;

    for row in stmt.execute() {
        let row = row.map_err(|err| {
            Error::new_message(&format!("failed to read expired claim row: {:?}", err))
        })?;
        let run_id = row
            .get::<String>(0)
            .map_err(|err| Error::new_message(&format!("failed to read run_id: {:?}", err)))?;
        let task_id = row
            .get::<String>(1)
            .map_err(|err| Error::new_message(&format!("failed to read task_id: {:?}", err)))?;
        let attempt = row
            .get::<i64>(2)
            .map_err(|err| Error::new_message(&format!("failed to read attempt: {:?}", err)))?;
        let claimed_by = row
            .get::<String>(3)
            .map_err(|err| Error::new_message(&format!("failed to read claimed_by: {:?}", err)))?;
        let claim_expires_at = row.get::<i64>(4).map_err(|err| {
            Error::new_message(&format!("failed to read claim_expires_at: {:?}", err))
        })?;
        let retry_strategy = row.get::<String>(5).map_err(|err| {
            Error::new_message(&format!("failed to read retry_strategy: {:?}", err))
        })?;
        let max_attempts = row.get::<i64>(6).map_err(|err| {
            Error::new_message(&format!("failed to read max_attempts: {:?}", err))
        })?;
        let first_started_at = row.get::<i64>(7).map_err(|err| {
            Error::new_message(&format!("failed to read first_started_at: {:?}", err))
        })?;
        let cancellation = row.get::<String>(8).map_err(|err| {
            Error::new_message(&format!("failed to read cancellation: {:?}", err))
        })?;

        let failure_reason = serde_json::json!({
            "name": "$ClaimTimeout",
            "message": "worker did not finish task within claim interval",
            "workerId": if claimed_by.is_empty() { JsonValue::Null } else { JsonValue::String(claimed_by.clone()) },
            "claimExpiredAt": if claim_expires_at == 0 { JsonValue::Null } else { JsonValue::from(claim_expires_at) },
            "attempt": attempt,
        })
        .to_string();

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
            &[&now_value, &failure_reason, queue_name, &run_id],
        )?;

        sql::exec_with_bind_text(
            db,
            "delete from absurd_waits
              where queue_name = ?1
                and run_id = ?2",
            &[queue_name, &run_id],
        )?;

        let next_attempt = attempt + 1;
        let max_attempts_opt = if max_attempts == 0 {
            None
        } else {
            Some(max_attempts)
        };
        let allow_retry = max_attempts_opt.map_or(true, |max| next_attempt <= max);
        let mut task_state = "failed";
        let mut last_attempt_run = run_id.clone();
        let mut cancelled_at = "";
        let mut recorded_attempt = attempt;

        if allow_retry {
            let delay_ms = retry::parse_retry_strategy(&retry_strategy, attempt)?;
            let mut next_available = now + delay_ms;
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
    }

    Ok(())
}

fn select_candidates(
    db: *mut sqlite3,
    queue_name: &str,
    now: i64,
    qty: i64,
) -> Result<Vec<ClaimResult>> {
    let now_value = now.to_string();
    let qty_value = qty.to_string();
    let mut stmt = sqlite_loadable::exec::Statement::prepare(
        db,
        "select r.run_id,
                r.task_id,
                r.attempt,
                r.state,
                t.task_name,
                json(t.params) as params,
                coalesce(json(t.retry_strategy), '') as retry_strategy,
                coalesce(t.max_attempts, 0) as max_attempts,
                coalesce(json(t.headers), '') as headers,
                coalesce(r.wake_event, '') as wake_event,
                coalesce(json(r.event_payload), '') as event_payload
           from absurd_runs r
           join absurd_tasks t
             on t.queue_name = r.queue_name
            and t.task_id = r.task_id
          where r.queue_name = ?1
            and r.state in ('pending','sleeping')
            and t.state in ('pending','sleeping','running')
            and r.available_at <= cast(?2 as integer)
          order by r.available_at, r.run_id
          limit cast(?3 as integer)",
    )
    .map_err(|err| Error::new_message(&format!("failed to prepare claim query: {:?}", err)))?;

    stmt.bind_text(1, queue_name)
        .map_err(|err| Error::new_message(&format!("failed to bind queue_name: {:?}", err)))?;
    stmt.bind_text(2, &now_value)
        .map_err(|err| Error::new_message(&format!("failed to bind now: {:?}", err)))?;
    stmt.bind_text(3, &qty_value)
        .map_err(|err| Error::new_message(&format!("failed to bind qty: {:?}", err)))?;

    let mut results = Vec::new();
    for row in stmt.execute() {
        let row =
            row.map_err(|err| Error::new_message(&format!("failed to read row: {:?}", err)))?;
        let run_id = row
            .get::<String>(0)
            .map_err(|err| Error::new_message(&format!("failed to read run_id: {:?}", err)))?;
        let task_id = row
            .get::<String>(1)
            .map_err(|err| Error::new_message(&format!("failed to read task_id: {:?}", err)))?;
        let attempt = row
            .get::<i64>(2)
            .map_err(|err| Error::new_message(&format!("failed to read attempt: {:?}", err)))?;
        let run_state = row
            .get::<String>(3)
            .map_err(|err| Error::new_message(&format!("failed to read run_state: {:?}", err)))?;
        let task_name = row
            .get::<String>(4)
            .map_err(|err| Error::new_message(&format!("failed to read task_name: {:?}", err)))?;
        let params = row
            .get::<String>(5)
            .map_err(|err| Error::new_message(&format!("failed to read params: {:?}", err)))?;
        let retry_strategy_raw = row.get::<String>(6).map_err(|err| {
            Error::new_message(&format!("failed to read retry_strategy: {:?}", err))
        })?;
        let max_attempts_raw = row.get::<i64>(7).map_err(|err| {
            Error::new_message(&format!("failed to read max_attempts: {:?}", err))
        })?;
        let headers_raw = row
            .get::<String>(8)
            .map_err(|err| Error::new_message(&format!("failed to read headers: {:?}", err)))?;
        let wake_event_raw = row
            .get::<String>(9)
            .map_err(|err| Error::new_message(&format!("failed to read wake_event: {:?}", err)))?;
        let event_payload_raw = row.get::<String>(10).map_err(|err| {
            Error::new_message(&format!("failed to read event_payload: {:?}", err))
        })?;

        results.push(ClaimResult {
            run_id,
            task_id,
            attempt,
            run_state,
            task_name,
            params,
            retry_strategy: if retry_strategy_raw.is_empty() {
                None
            } else {
                Some(retry_strategy_raw)
            },
            max_attempts: if max_attempts_raw == 0 {
                None
            } else {
                Some(max_attempts_raw)
            },
            headers: if headers_raw.is_empty() {
                None
            } else {
                Some(headers_raw)
            },
            wake_event: if wake_event_raw.is_empty() {
                None
            } else {
                Some(wake_event_raw)
            },
            event_payload: if event_payload_raw.is_empty() {
                None
            } else {
                Some(event_payload_raw)
            },
        });
    }

    Ok(results)
}

fn claim_task_impl(db: *mut sqlite3, args: ClaimArgs) -> Result<Vec<ClaimResult>> {
    let now = sql::now_ms_from_db(db);
    let claim_expires_at = if args.claim_timeout_secs > 0 {
        Some(now + args.claim_timeout_secs.saturating_mul(1000))
    } else {
        None
    };
    let claim_expires_value = claim_expires_at.map(|v| v.to_string()).unwrap_or_default();
    let now_value = now.to_string();

    sql::exec_with_bind_text(db, "begin immediate", &[])?;

    let result = (|| -> Result<Vec<ClaimResult>> {
        apply_cancellation_rules(db, &args.queue_name, now)?;
        expire_claims(db, &args.queue_name, now)?;

        let candidates = select_candidates(db, &args.queue_name, now, args.qty)?;
        for candidate in &candidates {
            sql::exec_with_bind_text(
                db,
                "update absurd_runs
                    set state = 'running',
                        claimed_by = ?1,
                        claim_expires_at = cast(nullif(?2, '') as integer),
                        started_at = case
                          when ?7 = 'sleeping' then cast(?3 as integer)
                          else coalesce(started_at, cast(?3 as integer))
                        end,
                        available_at = cast(?4 as integer)
                  where queue_name = ?5
                    and run_id = ?6",
                &[
                    &args.worker_id,
                    &claim_expires_value,
                    &now_value,
                    &now_value,
                    &args.queue_name,
                    &candidate.run_id,
                    &candidate.run_state,
                ],
            )?;

            let attempt_value = candidate.attempt.to_string();
            sql::exec_with_bind_text(
                db,
                "update absurd_tasks
                    set state = 'running',
                        attempts = case
                          when attempts < cast(?1 as integer) then cast(?1 as integer)
                          else attempts
                        end,
                        first_started_at = coalesce(first_started_at, cast(?2 as integer)),
                        last_attempt_run = ?3
                  where queue_name = ?4
                    and task_id = ?5",
                &[
                    &attempt_value,
                    &now_value,
                    &candidate.run_id,
                    &args.queue_name,
                    &candidate.task_id,
                ],
            )?;

            sql::exec_with_bind_text(
                db,
                "delete from absurd_waits
                  where queue_name = ?1
                    and run_id = ?2
                    and timeout_at is not null
                    and timeout_at <= cast(?3 as integer)",
                &[&args.queue_name, &candidate.run_id, &now_value],
            )?;
        }

        Ok(candidates)
    })();

    match result {
        Ok(results) => {
            sql::exec_with_bind_text(db, "commit", &[])?;
            Ok(results)
        }
        Err(err) => {
            let _ = sql::exec_with_bind_text(db, "rollback", &[]);
            Err(err)
        }
    }
}

static CLAIM_CREATE_SQL: &str = "CREATE TABLE x(run_id TEXT, task_id TEXT, attempt INTEGER, task_name TEXT, params BLOB, retry_strategy BLOB, max_attempts INTEGER, headers BLOB, wake_event TEXT, event_payload BLOB, queue_name TEXT hidden, worker_id TEXT hidden, claim_timeout INTEGER hidden, qty INTEGER hidden)";

enum ClaimColumns {
    RunId,
    TaskId,
    Attempt,
    TaskName,
    Params,
    RetryStrategy,
    MaxAttempts,
    Headers,
    WakeEvent,
    EventPayload,
    QueueName,
    WorkerId,
    ClaimTimeout,
    Qty,
}

fn claim_column(index: i32) -> Option<ClaimColumns> {
    match index {
        0 => Some(ClaimColumns::RunId),
        1 => Some(ClaimColumns::TaskId),
        2 => Some(ClaimColumns::Attempt),
        3 => Some(ClaimColumns::TaskName),
        4 => Some(ClaimColumns::Params),
        5 => Some(ClaimColumns::RetryStrategy),
        6 => Some(ClaimColumns::MaxAttempts),
        7 => Some(ClaimColumns::Headers),
        8 => Some(ClaimColumns::WakeEvent),
        9 => Some(ClaimColumns::EventPayload),
        10 => Some(ClaimColumns::QueueName),
        11 => Some(ClaimColumns::WorkerId),
        12 => Some(ClaimColumns::ClaimTimeout),
        13 => Some(ClaimColumns::Qty),
        _ => None,
    }
}

#[repr(C)]
pub struct ClaimTaskTable {
    base: sqlite3_vtab,
    db: *mut sqlite3,
}

impl<'vtab> VTab<'vtab> for ClaimTaskTable {
    type Aux = ();
    type Cursor = ClaimTaskCursor;

    fn connect(
        db: *mut sqlite3,
        _aux: Option<&Self::Aux>,
        _args: VTabArguments,
    ) -> Result<(String, ClaimTaskTable)> {
        let base: sqlite3_vtab = unsafe { std::mem::zeroed() };
        let vtab = ClaimTaskTable { base, db };
        Ok((CLAIM_CREATE_SQL.to_owned(), vtab))
    }

    fn destroy(&self) -> Result<()> {
        Ok(())
    }

    fn best_index(&self, mut info: IndexInfo) -> core::result::Result<(), BestIndexError> {
        let mut has_queue = false;

        for mut constraint in info.constraints() {
            match claim_column(constraint.column_idx()) {
                Some(ClaimColumns::QueueName) => {
                    if constraint.usable() && constraint.op() == Some(ConstraintOperator::EQ) {
                        constraint.set_omit(true);
                        constraint.set_argv_index(1);
                        has_queue = true;
                    } else {
                        return Err(BestIndexError::Constraint);
                    }
                }
                Some(ClaimColumns::WorkerId) => {
                    if constraint.usable() && constraint.op() == Some(ConstraintOperator::EQ) {
                        constraint.set_omit(true);
                        constraint.set_argv_index(2);
                    } else {
                        return Err(BestIndexError::Constraint);
                    }
                }
                Some(ClaimColumns::ClaimTimeout) => {
                    if constraint.usable() && constraint.op() == Some(ConstraintOperator::EQ) {
                        constraint.set_omit(true);
                        constraint.set_argv_index(3);
                    } else {
                        return Err(BestIndexError::Constraint);
                    }
                }
                Some(ClaimColumns::Qty) => {
                    if constraint.usable() && constraint.op() == Some(ConstraintOperator::EQ) {
                        constraint.set_omit(true);
                        constraint.set_argv_index(4);
                    } else {
                        return Err(BestIndexError::Constraint);
                    }
                }
                _ => {}
            }
        }

        if !has_queue {
            return Err(BestIndexError::Error);
        }

        info.set_estimated_cost(10.0);
        info.set_estimated_rows(10);
        info.set_idxnum(1);
        Ok(())
    }

    fn open(&mut self) -> Result<ClaimTaskCursor> {
        Ok(ClaimTaskCursor::new(self.db))
    }
}

#[repr(C)]
pub struct ClaimTaskCursor {
    base: sqlite3_vtab_cursor,
    db: *mut sqlite3,
    rowid: i64,
    results: Vec<ClaimResult>,
}

impl ClaimTaskCursor {
    fn new(db: *mut sqlite3) -> ClaimTaskCursor {
        let base: sqlite3_vtab_cursor = unsafe { std::mem::zeroed() };
        ClaimTaskCursor {
            base,
            db,
            rowid: 0,
            results: Vec::new(),
        }
    }
}

impl VTabCursor for ClaimTaskCursor {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        values: &[*mut sqlite3_value],
    ) -> Result<()> {
        let args = parse_claim_args(values)?;
        let results = claim_task_impl(self.db, args)?;
        self.results = results;
        self.rowid = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.rowid += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.rowid as usize >= self.results.len()
    }

    fn column(&self, context: *mut sqlite3_context, i: c_int) -> Result<()> {
        if self.rowid as usize >= self.results.len() {
            api::result_null(context);
            return Ok(());
        }
        let result = &self.results[self.rowid as usize];
        match claim_column(i) {
            Some(ClaimColumns::RunId) => api::result_text(context, &result.run_id)?,
            Some(ClaimColumns::TaskId) => api::result_text(context, &result.task_id)?,
            Some(ClaimColumns::Attempt) => api::result_int64(context, result.attempt),
            Some(ClaimColumns::TaskName) => api::result_text(context, &result.task_name)?,
            Some(ClaimColumns::Params) => sql::result_json_value(self.db, context, &result.params)?,
            Some(ClaimColumns::RetryStrategy) => match &result.retry_strategy {
                Some(value) => sql::result_json_value(self.db, context, value)?,
                None => api::result_null(context),
            },
            Some(ClaimColumns::MaxAttempts) => match result.max_attempts {
                Some(value) => api::result_int64(context, value),
                None => api::result_null(context),
            },
            Some(ClaimColumns::Headers) => match &result.headers {
                Some(value) => sql::result_json_value(self.db, context, value)?,
                None => api::result_null(context),
            },
            Some(ClaimColumns::WakeEvent) => match &result.wake_event {
                Some(value) => api::result_text(context, value)?,
                None => api::result_null(context),
            },
            Some(ClaimColumns::EventPayload) => match &result.event_payload {
                Some(value) => sql::result_json_value(self.db, context, value)?,
                None => api::result_null(context),
            },
            _ => {
                api::result_null(context);
            }
        }
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.rowid)
    }
}
