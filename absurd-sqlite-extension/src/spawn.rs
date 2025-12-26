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

#[derive(Default)]
struct SpawnOptions {
    headers: Option<String>,
    retry_strategy: Option<String>,
    max_attempts: Option<i64>,
    cancellation: Option<String>,
    idempotency_key: Option<String>,
}

struct SpawnResult {
    task_id: String,
    run_id: String,
    attempt: i64,
    created: i64,
}

fn parse_spawn_options(raw: &str) -> Result<SpawnOptions> {
    if raw.trim().is_empty() {
        return Ok(SpawnOptions::default());
    }
    let parsed: JsonValue =
        serde_json::from_str(raw).map_err(|err| Error::new_message(&format!("options must be valid JSON: {:?}", err)))?;
    if parsed.is_null() {
        return Ok(SpawnOptions::default());
    }
    let obj = parsed
        .as_object()
        .ok_or_else(|| Error::new_message("options must be a JSON object"))?;

    let headers = obj.get("headers").and_then(|v| {
        if v.is_null() {
            None
        } else {
            Some(v.to_string())
        }
    });
    let retry_strategy = obj.get("retry_strategy").and_then(|v| {
        if v.is_null() {
            None
        } else {
            Some(v.to_string())
        }
    });
    let cancellation = obj.get("cancellation").and_then(|v| {
        if v.is_null() {
            None
        } else {
            Some(v.to_string())
        }
    });
    let idempotency_key = obj
        .get("idempotency_key")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty());
    let max_attempts = obj.get("max_attempts").and_then(|v| v.as_i64());
    if let Some(max) = max_attempts {
        if max < 1 {
            return Err(Error::new_message("max_attempts must be >= 1"));
        }
    }

    Ok(SpawnOptions {
        headers,
        retry_strategy,
        max_attempts,
        cancellation,
        idempotency_key,
    })
}



fn spawn_task_impl(
    db: *mut sqlite3,
    queue_name: &str,
    task_name: &str,
    params: &str,
    options: &SpawnOptions,
) -> Result<SpawnResult> {
    validate::queue_name(queue_name)?;
    validate::task_name(task_name)?;

    let task_id = Uuid::now_v7().to_string();
    let run_id = Uuid::now_v7().to_string();
    let attempt = 1_i64;
    let now = sql::now_ms_from_db(db);

    let params_value = if params.trim().is_empty() {
        "null"
    } else {
        params
    };
    let headers_value = options.headers.as_deref().unwrap_or("");
    let retry_value = options.retry_strategy.as_deref().unwrap_or("");
    let cancellation_value = options.cancellation.as_deref().unwrap_or("");
    let idempotency_value = options.idempotency_key.as_deref().unwrap_or("");
    let max_attempts_value = options
        .max_attempts
        .map(|v| v.to_string())
        .unwrap_or_else(|| "0".to_string());

    let attempt_value = attempt.to_string();
    let now_value = now.to_string();

    if options.idempotency_key.is_some() {
        sql::exec_with_bind_text(
            db,
            "insert or ignore into absurd_tasks (queue_name, task_id, task_name, params, headers, retry_strategy, max_attempts, cancellation, enqueue_at, first_started_at, state, attempts, last_attempt_run, completed_payload, cancelled_at, idempotency_key)
             values (?1, ?2, ?3, jsonb(?4), case when ?5 = '' then null else jsonb(?5) end, case when ?6 = '' then null else jsonb(?6) end, nullif(cast(?7 as integer),0), case when ?8 = '' then null else jsonb(?8) end, cast(?9 as integer), null, 'pending', cast(?10 as integer), null, null, null, nullif(?11,''))",
            &[
                queue_name,
                &task_id,
                task_name,
                params_value,
                headers_value,
                retry_value,
                &max_attempts_value,
                cancellation_value,
                &now_value,
                &attempt_value,
                idempotency_value,
            ],
        )
        .map_err(|e| {
            Error::new_message(format!("task insert failed: {:?}", e))
        })?;

        let changes = sql::query_row_i64(db, "select changes()", &[])?;
        if changes == 0 {
            let (existing_task_id, existing_run_id, existing_attempt) = sql::query_row_strings(
                db,
                "select task_id, last_attempt_run, attempts from absurd_tasks where queue_name = ?1 and idempotency_key = ?2",
                &[queue_name, idempotency_value],
            )?;
            return Ok(SpawnResult {
                task_id: existing_task_id,
                run_id: existing_run_id,
                attempt: existing_attempt,
                created: 0,
            });
        }
    } else {
        sql::exec_with_bind_text(
            db,
            "insert into absurd_tasks (queue_name, task_id, task_name, params, headers, retry_strategy, max_attempts, cancellation, enqueue_at, first_started_at, state, attempts, last_attempt_run, completed_payload, cancelled_at, idempotency_key)
             values (?1, ?2, ?3, jsonb(?4), case when ?5 = '' then null else jsonb(?5) end, case when ?6 = '' then null else jsonb(?6) end, nullif(cast(?7 as integer),0), case when ?8 = '' then null else jsonb(?8) end, cast(?9 as integer), null, 'pending', cast(?10 as integer), null, null, null, null)",
            &[
                queue_name,
                &task_id,
                task_name,
                params_value,
                headers_value,
                retry_value,
                &max_attempts_value,
                cancellation_value,
                &now_value,
                &attempt_value,
            ],
        )
        .map_err(|e| Error::new_message(format!("task insert failed: {:?}", e)))?;
    }

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
            'pending',
            cast(?5 as integer),
            null,
            null,
            null,
            null
         )",
        &[
            queue_name,
            &run_id,
            &task_id,
            &attempt_value,
            &now_value,
        ],
    )?;

    sql::exec_with_bind_text(
        db,
        "update absurd_tasks set last_attempt_run = ?1 where queue_name = ?2 and task_id = ?3",
        &[&run_id, queue_name, &task_id],
    )
    .map_err(|err| Error::new_message(&format!("task update failed: {:?}", err)))?;

    Ok(SpawnResult {
        task_id,
        run_id,
        attempt,
        created: 1,
    })
}

static SPAWN_CREATE_SQL: &str = "CREATE TABLE x(task_id TEXT, run_id TEXT, attempt INTEGER, created INTEGER, queue_name TEXT hidden, task_name TEXT hidden, params TEXT hidden, options TEXT hidden)";

enum SpawnColumns {
    TaskId,
    RunId,
    Attempt,
    Created,
    QueueName,
    TaskName,
    Params,
    Options,
}

fn spawn_column(index: i32) -> Option<SpawnColumns> {
    match index {
        0 => Some(SpawnColumns::TaskId),
        1 => Some(SpawnColumns::RunId),
        2 => Some(SpawnColumns::Attempt),
        3 => Some(SpawnColumns::Created),
        4 => Some(SpawnColumns::QueueName),
        5 => Some(SpawnColumns::TaskName),
        6 => Some(SpawnColumns::Params),
        7 => Some(SpawnColumns::Options),
        _ => None,
    }
}

#[repr(C)]
pub struct SpawnTaskTable {
    base: sqlite3_vtab,
    db: *mut sqlite3,
}

impl<'vtab> VTab<'vtab> for SpawnTaskTable {
    type Aux = ();
    type Cursor = SpawnTaskCursor;

    fn connect(
        db: *mut sqlite3,
        _aux: Option<&Self::Aux>,
        _args: VTabArguments,
    ) -> Result<(String, SpawnTaskTable)> {
        let base: sqlite3_vtab = unsafe { std::mem::zeroed() };
        let vtab = SpawnTaskTable { base, db };
        Ok((SPAWN_CREATE_SQL.to_owned(), vtab))
    }

    fn destroy(&self) -> Result<()> {
        Ok(())
    }

    fn best_index(&self, mut info: IndexInfo) -> core::result::Result<(), BestIndexError> {
        let mut has_queue = false;
        let mut has_task = false;
        let mut has_params = false;

        for mut constraint in info.constraints() {
            match spawn_column(constraint.column_idx()) {
                Some(SpawnColumns::QueueName) => {
                    if constraint.usable() && constraint.op() == Some(ConstraintOperator::EQ) {
                        constraint.set_omit(true);
                        constraint.set_argv_index(1);
                        has_queue = true;
                    } else {
                        return Err(BestIndexError::Constraint);
                    }
                }
                Some(SpawnColumns::TaskName) => {
                    if constraint.usable() && constraint.op() == Some(ConstraintOperator::EQ) {
                        constraint.set_omit(true);
                        constraint.set_argv_index(2);
                        has_task = true;
                    } else {
                        return Err(BestIndexError::Constraint);
                    }
                }
                Some(SpawnColumns::Params) => {
                    if constraint.usable() && constraint.op() == Some(ConstraintOperator::EQ) {
                        constraint.set_omit(true);
                        constraint.set_argv_index(3);
                        has_params = true;
                    } else {
                        return Err(BestIndexError::Constraint);
                    }
                }
                Some(SpawnColumns::Options) => {
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

        if !has_queue || !has_task || !has_params {
            return Err(BestIndexError::Error);
        }

        info.set_estimated_cost(1.0);
        info.set_estimated_rows(1);
        info.set_idxnum(1);
        Ok(())
    }

    fn open(&mut self) -> Result<SpawnTaskCursor> {
        Ok(SpawnTaskCursor::new(self.db))
    }
}

#[repr(C)]
pub struct SpawnTaskCursor {
    base: sqlite3_vtab_cursor,
    db: *mut sqlite3,
    rowid: i64,
    result: Option<SpawnResult>,
}

impl SpawnTaskCursor {
    fn new(db: *mut sqlite3) -> SpawnTaskCursor {
        let base: sqlite3_vtab_cursor = unsafe { std::mem::zeroed() };
        SpawnTaskCursor {
            base,
            db,
            rowid: 0,
            result: None,
        }
    }
}

impl VTabCursor for SpawnTaskCursor {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        values: &[*mut sqlite3_value],
    ) -> Result<()> {
        let queue_name = api::value_text_notnull(values.get(0).expect("queue_name"))?;
        let task_name = api::value_text_notnull(values.get(1).expect("task_name"))?;
        let params = api::value_text(values.get(2).expect("params"))?;
        let options_raw = if values.len() > 3 {
            api::value_text(values.get(3).expect("options"))?
        } else {
            ""
        };

        let options = parse_spawn_options(options_raw)?;
        let result = spawn_task_impl(self.db, queue_name, task_name, params, &options)?;
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
        match spawn_column(i) {
            Some(SpawnColumns::TaskId) => api::result_text(context, &result.task_id)?,
            Some(SpawnColumns::RunId) => api::result_text(context, &result.run_id)?,
            Some(SpawnColumns::Attempt) => api::result_int64(context, result.attempt),
            Some(SpawnColumns::Created) => api::result_int64(context, result.created),
            _ => api::result_null(context),
        }
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.rowid)
    }
}
