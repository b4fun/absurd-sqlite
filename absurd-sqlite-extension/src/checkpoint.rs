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

struct CheckpointRow {
    checkpoint_name: String,
    state: Option<String>,
    status: String,
    owner_run_id: Option<String>,
    updated_at: i64,
}



fn parse_optional_int(value: &*mut sqlite3_value) -> Option<i64> {
    if api::value_is_null(value) {
        return None;
    }
    match api::value_type(value) {
        api::ValueType::Integer => Some(api::value_int64(value)),
        api::ValueType::Text => api::value_text(value).ok()?.trim().parse::<i64>().ok(),
        _ => None,
    }
}

pub fn absurd_set_task_checkpoint_state(
    context: *mut sqlite3_context,
    values: &[*mut sqlite3_value],
) -> Result<()> {
    let queue_name = api::value_text_notnull(values.get(0).expect("queue_name"))?;
    let task_id = api::value_text_notnull(values.get(1).expect("task_id"))?;
    let step_name = api::value_text_notnull(values.get(2).expect("step_name"))?;
    let state_value = values.get(3).expect("state");
    let owner_run = api::value_text_notnull(values.get(4).expect("owner_run"))?;
    let extend_claim_by = values
        .get(5)
        .and_then(|value| parse_optional_int(value))
        .unwrap_or(0);

    validate::queue_name(queue_name)?;
    validate::step_name(step_name)?;

    let state_text = if api::value_is_null(state_value) {
        "".to_string()
    } else {
        let raw = api::value_text(state_value)
            .map_err(|err| Error::new_message(&format!("state must be valid JSON: {:?}", err)))?
            .trim()
            .to_string();
        if raw.is_empty() {
            return Err(Error::new_message("state must be valid JSON"));
        }
        let _: JsonValue =
            serde_json::from_str(&raw).map_err(|err| Error::new_message(&format!("state must be valid JSON: {:?}", err)))?;
        raw
    };

    let db = api::context_db_handle(context);
    let now_value = sql::now_ms_from_db(db).to_string();

    sql::exec_with_bind_text(db, "begin immediate", &[])?;

    let result = (|| -> Result<()> {
        let mut stmt = sqlite_loadable::exec::Statement::prepare(
            db,
            "select r.attempt, t.state
               from absurd_runs r
               join absurd_tasks t
                 on t.queue_name = r.queue_name
                and t.task_id = r.task_id
              where r.queue_name = ?1
                and r.run_id = ?2",
        )
        .map_err(|err| Error::new_message(&format!("failed to prepare run lookup: {:?}", err)))?;
        stmt.bind_text(1, queue_name)
            .map_err(|err| Error::new_message(&format!("failed to bind queue_name: {:?}", err)))?;
        stmt.bind_text(2, owner_run)
            .map_err(|err| Error::new_message(&format!("failed to bind owner_run: {:?}", err)))?;
        let mut rows = stmt.execute();
        let row = rows
            .next()
            .ok_or_else(|| Error::new_message("run not found for checkpoint"))?
            .map_err(|err| Error::new_message(&format!("failed to read run row: {:?}", err)))?;
        let new_attempt = row
            .get::<i64>(0)
            .map_err(|err| Error::new_message(&format!("failed to read attempt: {:?}", err)))?;
        let task_state = row
            .get::<String>(1)
            .map_err(|err| Error::new_message(&format!("failed to read task state: {:?}", err)))?;

        if task_state == "cancelled" {
            return Err(Error::new_message("Task has been cancelled"));
        }

        if extend_claim_by > 0 {
            let extend_value = (sql::now_ms_from_db(db) + extend_claim_by.saturating_mul(1000)).to_string();
            sql::exec_with_bind_text(
                db,
                "update absurd_runs
                    set claim_expires_at = cast(?1 as integer)
                  where queue_name = ?2
                    and run_id = ?3
                    and state = 'running'
                    and claim_expires_at is not null",
                &[&extend_value, queue_name, owner_run],
            )?;
        }

        let mut existing_stmt = sqlite_loadable::exec::Statement::prepare(
            db,
            "select coalesce(c.owner_run_id, '') as owner_run_id,
                    coalesce(r.attempt, -1) as attempt
               from absurd_checkpoints c
               left join absurd_runs r
                 on r.queue_name = c.queue_name
                and r.run_id = c.owner_run_id
              where c.queue_name = ?1
                and c.task_id = ?2
                and c.checkpoint_name = ?3",
        )
        .map_err(|err| Error::new_message(&format!("failed to prepare checkpoint lookup: {:?}", err)))?;
        existing_stmt
            .bind_text(1, queue_name)
            .map_err(|err| Error::new_message(&format!("failed to bind queue_name: {:?}", err)))?;
        existing_stmt
            .bind_text(2, task_id)
            .map_err(|err| Error::new_message(&format!("failed to bind task_id: {:?}", err)))?;
        existing_stmt
            .bind_text(3, step_name)
            .map_err(|err| Error::new_message(&format!("failed to bind step_name: {:?}", err)))?;
        let mut existing_rows = existing_stmt.execute();
        let mut allow_update = true;
        if let Some(Ok(existing_row)) = existing_rows.next() {
            let existing_owner = existing_row
                .get::<String>(0)
                .map_err(|err| Error::new_message(&format!("failed to read owner_run_id: {:?}", err)))?;
            let existing_attempt = existing_row
                .get::<i64>(1)
                .map_err(|err| Error::new_message(&format!("failed to read attempt: {:?}", err)))?;
            if !existing_owner.is_empty() && existing_attempt >= 0 && new_attempt < existing_attempt {
                allow_update = false;
            }
        }

        if allow_update {
            sql::exec_with_bind_text(
                db,
                "insert into absurd_checkpoints (queue_name, task_id, checkpoint_name, state, status, owner_run_id, updated_at)
                 values (?1, ?2, ?3, case when ?4 = '' then null else jsonb(?4) end, 'committed', ?5, cast(?6 as integer))
                 on conflict (queue_name, task_id, checkpoint_name)
                 do update set state = excluded.state,
                               status = excluded.status,
                               owner_run_id = excluded.owner_run_id,
                               updated_at = excluded.updated_at",
                &[queue_name, task_id, step_name, &state_text, owner_run, &now_value],
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

static CHECKPOINT_ONE_SQL: &str = "CREATE TABLE x(checkpoint_name TEXT, state BLOB, status TEXT, owner_run_id TEXT, updated_at INTEGER, queue_name TEXT hidden, task_id TEXT hidden, step_name TEXT hidden, include_pending INTEGER hidden)";
static CHECKPOINT_ALL_SQL: &str = "CREATE TABLE x(checkpoint_name TEXT, state BLOB, status TEXT, owner_run_id TEXT, updated_at INTEGER, queue_name TEXT hidden, task_id TEXT hidden, run_id TEXT hidden)";

enum CheckpointColumns {
    CheckpointName,
    State,
    Status,
    OwnerRunId,
    UpdatedAt,
}

fn checkpoint_output_column(index: i32) -> Option<CheckpointColumns> {
    match index {
        0 => Some(CheckpointColumns::CheckpointName),
        1 => Some(CheckpointColumns::State),
        2 => Some(CheckpointColumns::Status),
        3 => Some(CheckpointColumns::OwnerRunId),
        4 => Some(CheckpointColumns::UpdatedAt),
        _ => None,
    }
}

#[repr(C)]
pub struct CheckpointStateTable {
    base: sqlite3_vtab,
    db: *mut sqlite3,
}

impl<'vtab> VTab<'vtab> for CheckpointStateTable {
    type Aux = ();
    type Cursor = CheckpointCursor;

    fn connect(
        db: *mut sqlite3,
        _aux: Option<&Self::Aux>,
        _args: VTabArguments,
    ) -> Result<(String, CheckpointStateTable)> {
        let base: sqlite3_vtab = unsafe { std::mem::zeroed() };
        let vtab = CheckpointStateTable { base, db };
        Ok((CHECKPOINT_ONE_SQL.to_owned(), vtab))
    }

    fn destroy(&self) -> Result<()> {
        Ok(())
    }

    fn best_index(&self, mut info: IndexInfo) -> core::result::Result<(), BestIndexError> {
        let mut has_queue = false;
        let mut has_task = false;
        let mut has_step = false;

        for mut constraint in info.constraints() {
            match constraint.column_idx() {
                5 => {
                    if constraint.usable() && constraint.op() == Some(ConstraintOperator::EQ) {
                        constraint.set_omit(true);
                        constraint.set_argv_index(1);
                        has_queue = true;
                    } else {
                        return Err(BestIndexError::Constraint);
                    }
                }
                6 => {
                    if constraint.usable() && constraint.op() == Some(ConstraintOperator::EQ) {
                        constraint.set_omit(true);
                        constraint.set_argv_index(2);
                        has_task = true;
                    } else {
                        return Err(BestIndexError::Constraint);
                    }
                }
                7 => {
                    if constraint.usable() && constraint.op() == Some(ConstraintOperator::EQ) {
                        constraint.set_omit(true);
                        constraint.set_argv_index(3);
                        has_step = true;
                    } else {
                        return Err(BestIndexError::Constraint);
                    }
                }
                8 => {
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

        if !has_queue || !has_task || !has_step {
            return Err(BestIndexError::Error);
        }

        info.set_estimated_cost(10.0);
        info.set_estimated_rows(10);
        info.set_idxnum(1);
        Ok(())
    }

    fn open(&mut self) -> Result<CheckpointCursor> {
        Ok(CheckpointCursor::new(self.db, false))
    }
}

#[repr(C)]
pub struct CheckpointStatesTable {
    base: sqlite3_vtab,
    db: *mut sqlite3,
}

impl<'vtab> VTab<'vtab> for CheckpointStatesTable {
    type Aux = ();
    type Cursor = CheckpointCursor;

    fn connect(
        db: *mut sqlite3,
        _aux: Option<&Self::Aux>,
        _args: VTabArguments,
    ) -> Result<(String, CheckpointStatesTable)> {
        let base: sqlite3_vtab = unsafe { std::mem::zeroed() };
        let vtab = CheckpointStatesTable { base, db };
        Ok((CHECKPOINT_ALL_SQL.to_owned(), vtab))
    }

    fn destroy(&self) -> Result<()> {
        Ok(())
    }

    fn best_index(&self, mut info: IndexInfo) -> core::result::Result<(), BestIndexError> {
        let mut has_queue = false;
        let mut has_task = false;

        for mut constraint in info.constraints() {
            match constraint.column_idx() {
                5 => {
                    if constraint.usable() && constraint.op() == Some(ConstraintOperator::EQ) {
                        constraint.set_omit(true);
                        constraint.set_argv_index(1);
                        has_queue = true;
                    } else {
                        return Err(BestIndexError::Constraint);
                    }
                }
                6 => {
                    if constraint.usable() && constraint.op() == Some(ConstraintOperator::EQ) {
                        constraint.set_omit(true);
                        constraint.set_argv_index(2);
                        has_task = true;
                    } else {
                        return Err(BestIndexError::Constraint);
                    }
                }
                7 => {
                    if constraint.usable() && constraint.op() == Some(ConstraintOperator::EQ) {
                        constraint.set_omit(true);
                        constraint.set_argv_index(3);
                    } else {
                        return Err(BestIndexError::Constraint);
                    }
                }
                _ => {}
            }
        }

        if !has_queue || !has_task {
            return Err(BestIndexError::Error);
        }

        info.set_estimated_cost(10.0);
        info.set_estimated_rows(10);
        info.set_idxnum(2);
        Ok(())
    }

    fn open(&mut self) -> Result<CheckpointCursor> {
        Ok(CheckpointCursor::new(self.db, true))
    }
}

#[repr(C)]
pub struct CheckpointCursor {
    base: sqlite3_vtab_cursor,
    db: *mut sqlite3,
    rowid: i64,
    results: Vec<CheckpointRow>,
    all: bool,
}

impl CheckpointCursor {
    fn new(db: *mut sqlite3, all: bool) -> CheckpointCursor {
        let base: sqlite3_vtab_cursor = unsafe { std::mem::zeroed() };
        CheckpointCursor {
            base,
            db,
            rowid: 0,
            results: Vec::new(),
            all,
        }
    }
}

impl VTabCursor for CheckpointCursor {
    fn filter(
        &mut self,
        _idx_num: c_int,
        idx_str: Option<&str>,
        values: &[*mut sqlite3_value],
    ) -> Result<()> {
        let queue_name = api::value_text_notnull(values.get(0).expect("queue_name"))?;
        let task_id = api::value_text_notnull(values.get(1).expect("task_id"))?;
        validate::queue_name(queue_name)?;

        let (sql_query, params): (&str, Vec<&str>) = if self.all || idx_str == Some("all") {
            (
                "select checkpoint_name,
                        coalesce(json(state), '') as state,
                        status,
                        coalesce(owner_run_id, '') as owner_run_id,
                        updated_at
                   from absurd_checkpoints
                  where queue_name = ?1
                    and task_id = ?2
                  order by updated_at asc",
                vec![queue_name, task_id],
            )
        } else {
            let step_name = api::value_text_notnull(values.get(2).expect("step_name"))?;
            validate::step_name(step_name)?;
            (
                "select checkpoint_name,
                        coalesce(json(state), '') as state,
                        status,
                        coalesce(owner_run_id, '') as owner_run_id,
                        updated_at
                   from absurd_checkpoints
                  where queue_name = ?1
                    and task_id = ?2
                    and checkpoint_name = ?3",
                vec![queue_name, task_id, step_name],
            )
        };

        let mut stmt = sqlite_loadable::exec::Statement::prepare(self.db, sql_query)
            .map_err(|err| Error::new_message(&format!("failed to prepare checkpoint query: {:?}", err)))?;
        for (idx, value) in params.iter().enumerate() {
            stmt.bind_text((idx + 1) as i32, value)
                .map_err(|err| Error::new_message(&format!("failed to bind parameter: {:?}", err)))?;
        }

        let mut results = Vec::new();
        for row in stmt.execute() {
            let row = row.map_err(|err| Error::new_message(&format!("failed to read row: {:?}", err)))?;
            let checkpoint_name = row
                .get::<String>(0)
                .map_err(|err| Error::new_message(&format!("failed to read checkpoint_name: {:?}", err)))?;
            let state_raw = row
                .get::<String>(1)
                .map_err(|err| Error::new_message(&format!("failed to read state: {:?}", err)))?;
            let status = row
                .get::<String>(2)
                .map_err(|err| Error::new_message(&format!("failed to read status: {:?}", err)))?;
            let owner_raw = row
                .get::<String>(3)
                .map_err(|err| Error::new_message(&format!("failed to read owner_run_id: {:?}", err)))?;
            let updated_at = row
                .get::<i64>(4)
                .map_err(|err| Error::new_message(&format!("failed to read updated_at: {:?}", err)))?;

            results.push(CheckpointRow {
                checkpoint_name,
                state: if state_raw.is_empty() { None } else { Some(state_raw) },
                status,
                owner_run_id: if owner_raw.is_empty() { None } else { Some(owner_raw) },
                updated_at,
            });
        }

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
        match checkpoint_output_column(i) {
            Some(CheckpointColumns::CheckpointName) => api::result_text(context, &result.checkpoint_name)?,
            Some(CheckpointColumns::State) => match &result.state {
                Some(value) => sql::result_json_value(self.db, context, value)?,
                None => api::result_null(context),
            },
            Some(CheckpointColumns::Status) => api::result_text(context, &result.status)?,
            Some(CheckpointColumns::OwnerRunId) => match &result.owner_run_id {
                Some(value) => api::result_text(context, value)?,
                None => api::result_null(context),
            },
            Some(CheckpointColumns::UpdatedAt) => api::result_int64(context, result.updated_at),
            _ => api::result_null(context),
        }
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.rowid)
    }
}
