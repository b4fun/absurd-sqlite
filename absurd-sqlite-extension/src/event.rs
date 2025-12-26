use crate::sql;
use crate::validate;
use serde_json::Value as JsonValue;
use sqlite_loadable::prelude::*;
use sqlite_loadable::{
    api,
    table::{BestIndexError, ConstraintOperator, IndexInfo, VTab, VTabArguments, VTabCursor},
    Error, Result,
};
use sqlite3ext_sys::sqlite3;
use std::os::raw::c_int;

struct AwaitResult {
    should_suspend: i64,
    payload: Option<String>,
}

const FAR_FUTURE_MS: i64 = 9_223_372_036_854_775_000;



fn parse_optional_timeout_secs(value: Option<*mut sqlite3_value>) -> Result<Option<i64>> {
    let value = match value {
        Some(value) => value,
        None => return Ok(None),
    };
    if api::value_is_null(&value) {
        return Ok(None);
    }
    let timeout = match api::value_type(&value) {
        api::ValueType::Integer => api::value_int64(&value),
        api::ValueType::Text => {
            let raw = api::value_text(&value)
                .map_err(|err| Error::new_message(&format!("timeout must be non-negative integer: {:?}", err)))?
                .trim();
            raw.parse::<i64>()
                .map_err(|err| Error::new_message(&format!("timeout must be non-negative integer: {:?}", err)))?
        }
        _ => return Err(Error::new_message("timeout must be non-negative integer")),
    };
    if timeout < 0 {
        return Err(Error::new_message("timeout must be non-negative"));
    }
    Ok(Some(timeout))
}

fn await_event_impl(
    db: *mut sqlite3,
    queue_name: &str,
    task_id: &str,
    run_id: &str,
    step_name: &str,
    event_name: &str,
    timeout_secs: Option<i64>,
) -> Result<AwaitResult> {
    validate::queue_name(queue_name)?;
    validate::step_name(step_name)?;
    validate::event_name(event_name)?;

    let now = sql::now_ms_from_db(db);
    let now_value = now.to_string();
    let timeout_at = timeout_secs.map(|secs| now + secs.saturating_mul(1000));
    let timeout_at_value = timeout_at.map(|v| v.to_string()).unwrap_or_default();
    let available_at = timeout_at.unwrap_or(FAR_FUTURE_MS);
    let available_value = available_at.to_string();

    sql::exec_with_bind_text(db, "begin immediate", &[])?;

    let result = (|| -> Result<AwaitResult> {
        let mut checkpoint_stmt = sqlite_loadable::exec::Statement::prepare(
            db,
            "select coalesce(json(state), '') as state
               from absurd_checkpoints
              where queue_name = ?1
                and task_id = ?2
                and checkpoint_name = ?3",
        )
        .map_err(|err| Error::new_message(&format!("failed to prepare checkpoint lookup: {:?}", err)))?;
        checkpoint_stmt
            .bind_text(1, queue_name)
            .map_err(|err| Error::new_message(&format!("failed to bind queue_name: {:?}", err)))?;
        checkpoint_stmt
            .bind_text(2, task_id)
            .map_err(|err| Error::new_message(&format!("failed to bind task_id: {:?}", err)))?;
        checkpoint_stmt
            .bind_text(3, step_name)
            .map_err(|err| Error::new_message(&format!("failed to bind step_name: {:?}", err)))?;
        let mut checkpoint_rows = checkpoint_stmt.execute();
        if let Some(Ok(row)) = checkpoint_rows.next() {
            let checkpoint_payload = row
                .get::<String>(0)
                .map_err(|err| Error::new_message(&format!("failed to read checkpoint payload: {:?}", err)))?;
            if !checkpoint_payload.is_empty() {
                return Ok(AwaitResult {
                    should_suspend: 0,
                    payload: Some(checkpoint_payload),
                });
            }
        }

        sql::exec_with_bind_text(
            db,
            "insert into absurd_events (queue_name, event_name, payload, emitted_at)
             values (?1, ?2, null, 0)
             on conflict (queue_name, event_name) do nothing",
            &[queue_name, event_name],
        )?;

        let mut run_stmt = sqlite_loadable::exec::Statement::prepare(
            db,
            "select r.state,
                    coalesce(json(r.event_payload), '') as event_payload,
                    coalesce(r.wake_event, '') as wake_event,
                    t.state as task_state
               from absurd_runs r
               join absurd_tasks t
                 on t.queue_name = r.queue_name
                and t.task_id = r.task_id
              where r.queue_name = ?1
                and r.run_id = ?2",
        )
        .map_err(|err| Error::new_message(&format!("failed to prepare run lookup: {:?}", err)))?;
        run_stmt
            .bind_text(1, queue_name)
            .map_err(|err| Error::new_message(&format!("failed to bind queue_name: {:?}", err)))?;
        run_stmt
            .bind_text(2, run_id)
            .map_err(|err| Error::new_message(&format!("failed to bind run_id: {:?}", err)))?;
        let mut run_rows = run_stmt.execute();
        let run_row = run_rows
            .next()
            .ok_or_else(|| Error::new_message("Run not found while awaiting event"))?
            .map_err(|err| Error::new_message(&format!("failed to read run row: {:?}", err)))?;
        let run_state = run_row
            .get::<String>(0)
            .map_err(|err| Error::new_message(&format!("failed to read run state: {:?}", err)))?;
        let existing_payload = run_row
            .get::<String>(1)
            .map_err(|err| Error::new_message(&format!("failed to read event payload: {:?}", err)))?;
        let wake_event = run_row
            .get::<String>(2)
            .map_err(|err| Error::new_message(&format!("failed to read wake_event: {:?}", err)))?;
        let task_state = run_row
            .get::<String>(3)
            .map_err(|err| Error::new_message(&format!("failed to read task state: {:?}", err)))?;

        if task_state == "cancelled" {
            return Err(Error::new_message("Task has been cancelled"));
        }

        let mut event_stmt = sqlite_loadable::exec::Statement::prepare(
            db,
            "select coalesce(json(payload), '') as payload
               from absurd_events
              where queue_name = ?1
                and event_name = ?2",
        )
        .map_err(|err| Error::new_message(&format!("failed to prepare event lookup: {:?}", err)))?;
        event_stmt
            .bind_text(1, queue_name)
            .map_err(|err| Error::new_message(&format!("failed to bind queue_name: {:?}", err)))?;
        event_stmt
            .bind_text(2, event_name)
            .map_err(|err| Error::new_message(&format!("failed to bind event_name: {:?}", err)))?;
        let mut event_rows = event_stmt.execute();
        let event_row = event_rows
            .next()
            .ok_or_else(|| Error::new_message("event not found"))?
            .map_err(|err| Error::new_message(&format!("failed to read event row: {:?}", err)))?;
        let event_payload = event_row
            .get::<String>(0)
            .map_err(|err| Error::new_message(&format!("failed to read event payload: {:?}", err)))?;

        let mut resolved_payload: Option<String> = None;

        if !existing_payload.is_empty() {
            sql::exec_with_bind_text(
                db,
                "update absurd_runs
                    set event_payload = null
                  where queue_name = ?1
                    and run_id = ?2",
                &[queue_name, run_id],
            )?;

            if !event_payload.is_empty() && event_payload == existing_payload {
                resolved_payload = Some(existing_payload.clone());
            }
        }

        if run_state != "running" {
            return Err(Error::new_message(
                "Run must be running to await absurd_events",
            ));
        }

        if resolved_payload.is_none() && !event_payload.is_empty() {
            resolved_payload = Some(event_payload.clone());
        }

        if let Some(payload) = resolved_payload {
            sql::exec_with_bind_text(
                db,
                "insert into absurd_checkpoints (queue_name, task_id, checkpoint_name, state, status, owner_run_id, updated_at)
                 values (?1, ?2, ?3, jsonb(?4), 'committed', ?5, cast(?6 as integer))
                 on conflict (queue_name, task_id, checkpoint_name)
                 do update set state = excluded.state,
                               status = excluded.status,
                               owner_run_id = excluded.owner_run_id,
                               updated_at = excluded.updated_at",
                &[queue_name, task_id, step_name, &payload, run_id, &now_value],
            )?;
            return Ok(AwaitResult {
                should_suspend: 0,
                payload: Some(payload),
            });
        }

        if wake_event == event_name && existing_payload.is_empty() {
            sql::exec_with_bind_text(
                db,
                "update absurd_runs
                    set wake_event = null
                  where queue_name = ?1
                    and run_id = ?2",
                &[queue_name, run_id],
            )?;
            return Ok(AwaitResult {
                should_suspend: 0,
                payload: None,
            });
        }

        sql::exec_with_bind_text(
            db,
            "insert into absurd_waits (queue_name, task_id, run_id, step_name, event_name, timeout_at, created_at)
             values (?1, ?2, ?3, ?4, ?5, nullif(?6, ''), cast(?7 as integer))
             on conflict (queue_name, run_id, step_name)
             do update set event_name = excluded.event_name,
                           timeout_at = excluded.timeout_at,
                           created_at = excluded.created_at",
            &[
                queue_name,
                task_id,
                run_id,
                step_name,
                event_name,
                &timeout_at_value,
                &now_value,
            ],
        )?;

        sql::exec_with_bind_text(
            db,
            "update absurd_runs
                set state = 'sleeping',
                    claimed_by = null,
                    claim_expires_at = null,
                    available_at = cast(?1 as integer),
                    wake_event = ?2,
                    event_payload = null
              where queue_name = ?3
                and run_id = ?4",
            &[&available_value, event_name, queue_name, run_id],
        )?;

        sql::exec_with_bind_text(
            db,
            "update absurd_tasks
                set state = 'sleeping'
              where queue_name = ?1
                and task_id = ?2",
            &[queue_name, task_id],
        )?;

        Ok(AwaitResult {
            should_suspend: 1,
            payload: None,
        })
    })();

    match result {
        Ok(result) => {
            sql::exec_with_bind_text(db, "commit", &[])?;
            Ok(result)
        }
        Err(err) => {
            let _ = sql::exec_with_bind_text(db, "rollback", &[]);
            Err(err)
        }
    }
}

pub fn absurd_emit_event(context: *mut sqlite3_context, values: &[*mut sqlite3_value]) -> Result<()> {
    let queue_name = api::value_text_notnull(values.get(0).expect("queue_name"))?;
    let event_name = api::value_text_notnull(values.get(1).expect("event_name"))?;
    let payload_value = values.get(2);

    validate::queue_name(queue_name)?;
    validate::event_name(event_name)?;

    let payload_text = if let Some(value) = payload_value {
        if api::value_is_null(value) {
            "null".to_string()
        } else {
            let raw = api::value_text(value)
                .map_err(|err| Error::new_message(&format!("payload must be valid JSON: {:?}", err)))?
                .trim()
                .to_string();
            if raw.is_empty() {
                return Err(Error::new_message("payload must be valid JSON"));
            }
            let _: JsonValue = serde_json::from_str(&raw)
                .map_err(|err| Error::new_message(&format!("payload must be valid JSON: {:?}", err)))?;
            raw
        }
    } else {
        "null".to_string()
    };

    let db = api::context_db_handle(context);
    let now_value = sql::now_ms_from_db(db).to_string();

    sql::exec_with_bind_text(db, "begin immediate", &[])?;

    let result = (|| -> Result<()> {
        sql::exec_with_bind_text(
            db,
            "insert into absurd_events (queue_name, event_name, payload, emitted_at)
             values (?1, ?2, jsonb(?3), cast(?4 as integer))
             on conflict (queue_name, event_name)
             do update set payload = excluded.payload,
                           emitted_at = excluded.emitted_at",
            &[queue_name, event_name, &payload_text, &now_value],
        )?;

        sql::exec_with_bind_text(
            db,
            "delete from absurd_waits
              where queue_name = ?1
                and event_name = ?2
                and timeout_at is not null
                and timeout_at <= cast(?3 as integer)",
            &[queue_name, event_name, &now_value],
        )?;

        let mut waits_stmt = sqlite_loadable::exec::Statement::prepare(
            db,
            "select run_id, task_id, step_name
               from absurd_waits
              where queue_name = ?1
                and event_name = ?2
                and (timeout_at is null or timeout_at > cast(?3 as integer))",
        )
        .map_err(|err| Error::new_message(&format!("failed to prepare absurd_waits lookup: {:?}", err)))?;
        waits_stmt
            .bind_text(1, queue_name)
            .map_err(|err| Error::new_message(&format!("failed to bind queue_name: {:?}", err)))?;
        waits_stmt
            .bind_text(2, event_name)
            .map_err(|err| Error::new_message(&format!("failed to bind event_name: {:?}", err)))?;
        waits_stmt
            .bind_text(3, &now_value)
            .map_err(|err| Error::new_message(&format!("failed to bind now: {:?}", err)))?;

        for row in waits_stmt.execute() {
            let row = row.map_err(|err| Error::new_message(&format!("failed to read wait row: {:?}", err)))?;
            let run_id = row
                .get::<String>(0)
                .map_err(|err| Error::new_message(&format!("failed to read run_id: {:?}", err)))?;
            let task_id = row
                .get::<String>(1)
                .map_err(|err| Error::new_message(&format!("failed to read task_id: {:?}", err)))?;
            let step_name = row
                .get::<String>(2)
                .map_err(|err| Error::new_message(&format!("failed to read step_name: {:?}", err)))?;

            sql::exec_with_bind_text(
                db,
                "update absurd_runs
                    set state = 'pending',
                        available_at = cast(?1 as integer),
                        wake_event = null,
                        event_payload = jsonb(?2),
                        claimed_by = null,
                        claim_expires_at = null
                  where queue_name = ?3
                    and run_id = ?4
                    and state = 'sleeping'",
                &[&now_value, &payload_text, queue_name, &run_id],
            )?;

            let changes = sql::query_row_i64(db, "select changes()", &[])?;
            if changes == 0 {
                continue;
            }

            sql::exec_with_bind_text(
                db,
                "insert into absurd_checkpoints (queue_name, task_id, checkpoint_name, state, status, owner_run_id, updated_at)
                 values (?1, ?2, ?3, jsonb(?4), 'committed', ?5, cast(?6 as integer))
                 on conflict (queue_name, task_id, checkpoint_name)
                 do update set state = excluded.state,
                               status = excluded.status,
                               owner_run_id = excluded.owner_run_id,
                               updated_at = excluded.updated_at",
                &[queue_name, &task_id, &step_name, &payload_text, &run_id, &now_value],
            )?;

            sql::exec_with_bind_text(
                db,
                "update absurd_tasks
                    set state = 'pending'
                  where queue_name = ?1
                    and task_id = ?2",
                &[queue_name, &task_id],
            )?;

            sql::exec_with_bind_text(
                db,
                "delete from absurd_waits
                  where queue_name = ?1
                    and run_id = ?2
                    and event_name = ?3",
                &[queue_name, &run_id, event_name],
            )?;
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

static AWAIT_CREATE_SQL: &str = "CREATE TABLE x(should_suspend INTEGER, payload BLOB, queue_name TEXT hidden, task_id TEXT hidden, run_id TEXT hidden, step_name TEXT hidden, event_name TEXT hidden, timeout INTEGER hidden)";

enum AwaitColumns {
    ShouldSuspend,
    Payload,
    QueueName,
    TaskId,
    RunId,
    StepName,
    EventName,
    Timeout,
}

fn await_column(index: i32) -> Option<AwaitColumns> {
    match index {
        0 => Some(AwaitColumns::ShouldSuspend),
        1 => Some(AwaitColumns::Payload),
        2 => Some(AwaitColumns::QueueName),
        3 => Some(AwaitColumns::TaskId),
        4 => Some(AwaitColumns::RunId),
        5 => Some(AwaitColumns::StepName),
        6 => Some(AwaitColumns::EventName),
        7 => Some(AwaitColumns::Timeout),
        _ => None,
    }
}

#[repr(C)]
pub struct AwaitEventTable {
    base: sqlite3_vtab,
    db: *mut sqlite3,
}

impl<'vtab> VTab<'vtab> for AwaitEventTable {
    type Aux = ();
    type Cursor = AwaitCursor;

    fn connect(
        db: *mut sqlite3,
        _aux: Option<&Self::Aux>,
        _args: VTabArguments,
    ) -> Result<(String, AwaitEventTable)> {
        let base: sqlite3_vtab = unsafe { std::mem::zeroed() };
        let vtab = AwaitEventTable { base, db };
        Ok((AWAIT_CREATE_SQL.to_owned(), vtab))
    }

    fn destroy(&self) -> Result<()> {
        Ok(())
    }

    fn best_index(&self, mut info: IndexInfo) -> core::result::Result<(), BestIndexError> {
        let mut has_queue = false;
        let mut has_task = false;
        let mut has_run = false;
        let mut has_step = false;
        let mut has_event = false;

        for mut constraint in info.constraints() {
            match await_column(constraint.column_idx()) {
                Some(AwaitColumns::QueueName) => {
                    if constraint.usable() && constraint.op() == Some(ConstraintOperator::EQ) {
                        constraint.set_omit(true);
                        constraint.set_argv_index(1);
                        has_queue = true;
                    } else {
                        return Err(BestIndexError::Constraint);
                    }
                }
                Some(AwaitColumns::TaskId) => {
                    if constraint.usable() && constraint.op() == Some(ConstraintOperator::EQ) {
                        constraint.set_omit(true);
                        constraint.set_argv_index(2);
                        has_task = true;
                    } else {
                        return Err(BestIndexError::Constraint);
                    }
                }
                Some(AwaitColumns::RunId) => {
                    if constraint.usable() && constraint.op() == Some(ConstraintOperator::EQ) {
                        constraint.set_omit(true);
                        constraint.set_argv_index(3);
                        has_run = true;
                    } else {
                        return Err(BestIndexError::Constraint);
                    }
                }
                Some(AwaitColumns::StepName) => {
                    if constraint.usable() && constraint.op() == Some(ConstraintOperator::EQ) {
                        constraint.set_omit(true);
                        constraint.set_argv_index(4);
                        has_step = true;
                    } else {
                        return Err(BestIndexError::Constraint);
                    }
                }
                Some(AwaitColumns::EventName) => {
                    if constraint.usable() && constraint.op() == Some(ConstraintOperator::EQ) {
                        constraint.set_omit(true);
                        constraint.set_argv_index(5);
                        has_event = true;
                    } else {
                        return Err(BestIndexError::Constraint);
                    }
                }
                Some(AwaitColumns::Timeout) => {
                    if constraint.usable() && constraint.op() == Some(ConstraintOperator::EQ) {
                        constraint.set_omit(true);
                        constraint.set_argv_index(6);
                    } else {
                        return Err(BestIndexError::Constraint);
                    }
                }
                _ => {}
            }
        }

        if !has_queue || !has_task || !has_run || !has_step || !has_event {
            return Err(BestIndexError::Error);
        }

        info.set_estimated_cost(1.0);
        info.set_estimated_rows(1);
        info.set_idxnum(1);
        Ok(())
    }

    fn open(&mut self) -> Result<AwaitCursor> {
        Ok(AwaitCursor::new(self.db))
    }
}

#[repr(C)]
pub struct AwaitCursor {
    base: sqlite3_vtab_cursor,
    db: *mut sqlite3,
    rowid: i64,
    result: Option<AwaitResult>,
}

impl AwaitCursor {
    fn new(db: *mut sqlite3) -> AwaitCursor {
        let base: sqlite3_vtab_cursor = unsafe { std::mem::zeroed() };
        AwaitCursor {
            base,
            db,
            rowid: 0,
            result: None,
        }
    }
}

impl VTabCursor for AwaitCursor {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        values: &[*mut sqlite3_value],
    ) -> Result<()> {
        let queue_name = api::value_text_notnull(values.get(0).expect("queue_name"))?;
        let task_id = api::value_text_notnull(values.get(1).expect("task_id"))?;
        let run_id = api::value_text_notnull(values.get(2).expect("run_id"))?;
        let step_name = api::value_text_notnull(values.get(3).expect("step_name"))?;
        let event_name = api::value_text_notnull(values.get(4).expect("event_name"))?;
        let timeout = parse_optional_timeout_secs(values.get(5).copied())?;

        let result = await_event_impl(
            self.db,
            queue_name,
            task_id,
            run_id,
            step_name,
            event_name,
            timeout,
        )?;
        self.result = Some(result);
        self.rowid = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.rowid += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.result.is_none() || self.rowid > 0
    }

    fn column(&self, context: *mut sqlite3_context, i: c_int) -> Result<()> {
        let result = match &self.result {
            Some(result) => result,
            None => {
                api::result_null(context);
                return Ok(());
            }
        };
        match await_column(i) {
            Some(AwaitColumns::ShouldSuspend) => api::result_int64(context, result.should_suspend),
            Some(AwaitColumns::Payload) => match &result.payload {
                Some(value) => sql::result_json_value(self.db, context, value)?,
                None => api::result_null(context),
            },
            _ => api::result_null(context),
        }
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.rowid)
    }
}
