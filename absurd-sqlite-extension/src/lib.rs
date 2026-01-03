use sqlite_loadable::exec::Statement;
use sqlite_loadable::prelude::*;
use sqlite_loadable::{
    define_scalar_function, define_table_function, Error, FunctionFlags, Result,
};

mod checkpoint;
mod claim;
mod event;
mod migrate;
mod migrations;
mod queue;
mod retry;
mod run;
mod settings;
mod spawn;
mod sql;
mod validate;

fn absurd_version(context: *mut sqlite3_context, _values: &[*mut sqlite3_value]) -> Result<()> {
    let version = env!("CARGO_PKG_VERSION");
    sqlite_loadable::api::result_text(context, format!("absurd-sqlite/{}", version))?;
    Ok(())
}

// NOTE: jsonb() is provided by SQLite 3.45+. No local shim needed.
fn absurd_create_queue(context: *mut sqlite3_context, values: &[*mut sqlite3_value]) -> Result<()> {
    let queue_name =
        sqlite_loadable::api::value_text_notnull(values.first().expect("queue_name is required"))?;
    validate::queue_name(queue_name)?;

    let db = sqlite_loadable::api::context_db_handle(context);
    sql::exec_with_bind_text(
        db,
        "insert or ignore into absurd_queues (queue_name) values (?1)",
        &[queue_name],
    )?;
    sqlite_loadable::api::result_int(context, 1);
    Ok(())
}

fn absurd_drop_queue(context: *mut sqlite3_context, values: &[*mut sqlite3_value]) -> Result<()> {
    let queue_name =
        sqlite_loadable::api::value_text_notnull(values.first().expect("queue_name is required"))?;
    validate::queue_name(queue_name)?;

    let db = sqlite_loadable::api::context_db_handle(context);
    sql::exec_with_bind_text(
        db,
        "delete from absurd_queues where queue_name = ?1",
        &[queue_name],
    )?;
    let changes = sql::query_row_i64(db, "select changes()", &[])?;
    sqlite_loadable::api::result_int64(context, changes);
    Ok(())
}

const MIN_JSONB_VERSION: (i32, i32, i32) = (3, 45, 0);

fn absurd_init(db: *mut sqlite3) -> Result<()> {
    let version = fetch_sqlite_version(db)?;
    if version < MIN_JSONB_VERSION {
        return Err(Error::new_message(
            "jsonb() requires SQLite 3.45.0+; please upgrade SQLite",
        ));
    }
    let flags = FunctionFlags::UTF8 | FunctionFlags::DETERMINISTIC;
    define_scalar_function(db, "absurd_version", 0, absurd_version, flags)?;
    define_scalar_function(db, "absurd_create_queue", 1, absurd_create_queue, flags)?;
    define_scalar_function(db, "absurd_drop_queue", 1, absurd_drop_queue, flags)?;
    define_scalar_function(
        db,
        "absurd_complete_run",
        3,
        run::absurd_complete_run,
        flags,
    )?;
    define_scalar_function(
        db,
        "absurd_schedule_run",
        3,
        run::absurd_schedule_run,
        flags,
    )?;
    define_scalar_function(
        db,
        "absurd_fail_run",
        3,
        run::absurd_fail_run_no_retry,
        flags,
    )?;
    define_scalar_function(db, "absurd_fail_run", 4, run::absurd_fail_run, flags)?;
    define_scalar_function(
        db,
        "absurd_extend_claim",
        3,
        run::absurd_extend_claim,
        flags,
    )?;
    define_scalar_function(
        db,
        "absurd_cleanup_tasks",
        2,
        run::absurd_cleanup_tasks,
        flags,
    )?;
    define_scalar_function(
        db,
        "absurd_cleanup_tasks",
        3,
        run::absurd_cleanup_tasks,
        flags,
    )?;
    define_scalar_function(
        db,
        "absurd_cleanup_events",
        2,
        run::absurd_cleanup_events,
        flags,
    )?;
    define_scalar_function(
        db,
        "absurd_cleanup_events",
        3,
        run::absurd_cleanup_events,
        flags,
    )?;
    define_scalar_function(db, "absurd_cancel_task", 2, run::absurd_cancel_task, flags)?;
    define_scalar_function(
        db,
        "absurd_set_task_checkpoint_state",
        5,
        checkpoint::absurd_set_task_checkpoint_state,
        flags,
    )?;
    define_scalar_function(
        db,
        "absurd_set_task_checkpoint_state",
        6,
        checkpoint::absurd_set_task_checkpoint_state,
        flags,
    )?;
    define_scalar_function(db, "absurd_emit_event", 2, event::absurd_emit_event, flags)?;
    define_scalar_function(db, "absurd_emit_event", 3, event::absurd_emit_event, flags)?;
    define_scalar_function(
        db,
        "absurd_set_fake_now",
        1,
        settings::absurd_set_fake_now,
        flags,
    )?;
    define_scalar_function(
        db,
        "absurd_apply_migrations",
        0,
        migrate::absurd_apply_migrations,
        flags,
    )?;
    define_scalar_function(
        db,
        "absurd_apply_migrations",
        1,
        migrate::absurd_apply_migrations,
        flags,
    )?;
    define_table_function::<spawn::SpawnTaskTable>(db, "absurd_spawn_task", None)?;
    define_table_function::<claim::ClaimTaskTable>(db, "absurd_claim_task", None)?;
    define_table_function::<checkpoint::CheckpointStateTable>(
        db,
        "absurd_get_task_checkpoint_state",
        None,
    )?;
    define_table_function::<checkpoint::CheckpointStatesTable>(
        db,
        "absurd_get_task_checkpoint_states",
        None,
    )?;
    define_table_function::<event::AwaitEventTable>(db, "absurd_await_event", None)?;
    define_table_function::<queue::ListQueuesTable>(db, "absurd_list_queues", None)?;
    define_table_function::<migrate::MigrationRecordsTable>(db, "absurd_migration_records", None)?;
    Ok(())
}

fn fetch_sqlite_version(db: *mut sqlite3) -> Result<(i32, i32, i32)> {
    let mut stmt = Statement::prepare(db, "select sqlite_version()")
        .map_err(|_| Error::new_message("failed to query sqlite_version()"))?;
    let mut rows = stmt.execute();
    let row = match rows.next() {
        Some(Ok(row)) => row,
        _ => return Err(Error::new_message("failed to read sqlite_version()")),
    };
    let version = row
        .get::<String>(0)
        .map_err(|_| Error::new_message("failed to parse sqlite_version()"))?;
    parse_sqlite_version(&version)
}

fn parse_sqlite_version(version: &str) -> Result<(i32, i32, i32)> {
    let mut parts = version.split('.');
    let major = parts
        .next()
        .ok_or_else(|| Error::new_message("invalid sqlite_version() format"))?
        .parse::<i32>()
        .map_err(|_| Error::new_message("invalid sqlite_version() major value"))?;
    let minor = parts
        .next()
        .ok_or_else(|| Error::new_message("invalid sqlite_version() format"))?
        .parse::<i32>()
        .map_err(|_| Error::new_message("invalid sqlite_version() minor value"))?;
    let patch = parts
        .next()
        .unwrap_or("0")
        .parse::<i32>()
        .map_err(|_| Error::new_message("invalid sqlite_version() patch value"))?;
    Ok((major, minor, patch))
}

#[sqlite_entrypoint]
pub fn sqlite3_absurd_init(db: *mut sqlite3) -> Result<()> {
    absurd_init(db)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{ffi::sqlite3_auto_extension, params, Connection};
    use std::collections::HashMap;

    #[test]
    fn test_absurd_version() {
        unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_absurd_init as *const ())));
        }

        let conn = Connection::open_in_memory().unwrap();
        let runtime_version: String = conn
            .query_row("select sqlite_version()", [], |row| row.get(0))
            .unwrap();
        eprintln!("sqlite_version={}", runtime_version);
        let result: String = conn
            .query_row("select absurd_version()", [], |row| row.get(0))
            .unwrap();

        assert_eq!(
            result,
            format!("absurd-sqlite/{}", env!("CARGO_PKG_VERSION"))
        );
    }

    #[test]
    fn test_apply_migrations_and_records() {
        unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_absurd_init as *const ())));
        }

        let conn = Connection::open_in_memory().unwrap();
        let applied: i64 = conn
            .query_row("select absurd_apply_migrations()", [], |row| row.get(0))
            .unwrap();
        assert!(applied >= 1);

        let count: i64 = conn
            .query_row(
                "select count(*) from absurd_migration_records()",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(count >= 1);

        let row: (i64, String, i64) = conn
            .query_row(
                "select id, introduced_version, applied_time from absurd_migration_records() order by id limit 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(row.0, 1);
        assert!(!row.1.trim().is_empty());
        assert!(row.2 > 0);

        let applied_again: i64 = conn
            .query_row("select absurd_apply_migrations()", [], |row| row.get(0))
            .unwrap();
        assert_eq!(applied_again, 0);

        let _: i64 = conn
            .query_row("select absurd_create_queue('alpha')", [], |r| r.get(0))
            .unwrap();
    }

    #[test]
    fn test_set_fake_now() {
        unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_absurd_init as *const ())));
        }

        let conn = Connection::open_in_memory().unwrap();
        let _: i64 = conn
            .query_row("select absurd_set_fake_now(12345)", [], |row| row.get(0))
            .unwrap();
        let _: i64 = conn
            .query_row("select absurd_apply_migrations()", [], |row| row.get(0))
            .unwrap();
        let applied_time: i64 = conn
            .query_row(
                "select applied_time from absurd_migrations where id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(applied_time, 12345);

        let _: i64 = conn
            .query_row("select absurd_set_fake_now(null)", [], |row| row.get(0))
            .unwrap();
    }

    #[test]
    fn test_apply_migrations_to_target() {
        unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_absurd_init as *const ())));
        }

        let conn = Connection::open_in_memory().unwrap();
        let applied: i64 = conn
            .query_row("select absurd_apply_migrations(1)", [], |row| row.get(0))
            .unwrap();
        assert_eq!(applied, 1);
    }

    #[test]
    fn test_list_queues() {
        unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_absurd_init as *const ())));
        }

        let conn = Connection::open_in_memory().unwrap();
        let _: i64 = conn
            .query_row("select absurd_apply_migrations()", [], |row| row.get(0))
            .unwrap();

        let _: i64 = conn
            .query_row("select absurd_create_queue('alpha')", [], |r| r.get(0))
            .unwrap();
        let _: i64 = conn
            .query_row("select absurd_create_queue('beta')", [], |r| r.get(0))
            .unwrap();

        let mut stmt = conn
            .prepare("select queue_name from absurd_list_queues() order by queue_name")
            .unwrap();
        let rows = stmt.query_map([], |row| row.get::<_, String>(0)).unwrap();
        let queues: Vec<String> = rows.map(|row| row.unwrap()).collect();
        assert_eq!(queues, vec!["alpha".to_string(), "beta".to_string()]);
    }

    #[test]
    fn test_drop_queue() {
        unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_absurd_init as *const ())));
        }

        let conn = Connection::open_in_memory().unwrap();
        let _: i64 = conn
            .query_row("select absurd_apply_migrations()", [], |row| row.get(0))
            .unwrap();

        let _: i64 = conn
            .query_row("select absurd_create_queue('alpha')", [], |r| r.get(0))
            .unwrap();

        let deleted: i64 = conn
            .query_row("select absurd_drop_queue('alpha')", [], |r| r.get(0))
            .unwrap();
        assert_eq!(deleted, 1);

        let count: i64 = conn
            .query_row("select count(*) from absurd_queues", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_spawn_task_basic() {
        unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_absurd_init as *const ())));
        }

        let conn = Connection::open_in_memory().unwrap();
        let _: i64 = conn
            .query_row("select absurd_apply_migrations()", [], |row| row.get(0))
            .unwrap();

        let _: i64 = conn
            .query_row("select absurd_create_queue('alpha')", [], |r| r.get(0))
            .unwrap();

        let row: (String, String, i64, i64) = conn
            .query_row(
                "select task_id, run_id, attempt, created from absurd_spawn_task('alpha','demo','{}','{}')",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();

        assert_eq!(row.2, 1);
        assert_eq!(row.3, 1);
    }

    #[test]
    fn test_claim_task_basic() {
        unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_absurd_init as *const ())));
        }

        let conn = Connection::open_in_memory().unwrap();
        let _: i64 = conn
            .query_row("select absurd_apply_migrations()", [], |row| row.get(0))
            .unwrap();

        let _: i64 = conn
            .query_row("select absurd_create_queue('alpha')", [], |r| r.get(0))
            .unwrap();

        let options = r#"{"headers":{"x":1}}"#;
        let row: (String, String, i64, i64) = conn
            .query_row(
                "select task_id, run_id, attempt, created from absurd_spawn_task('alpha','demo','{}',?1)",
                [options],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();

        let claim: (String, String, i64, String) = conn
            .query_row(
                "select run_id, task_id, attempt, task_name from absurd_claim_task('alpha','worker',30,1)",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();

        assert_eq!(claim.0, row.1);
        assert_eq!(claim.1, row.0);
        assert_eq!(claim.2, 1);
        assert_eq!(claim.3, "demo");

        let state: String = conn
            .query_row(
                "select state from absurd_runs where queue_name = 'alpha' and run_id = ?1",
                [row.1],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(state, "running");
    }

    #[test]
    fn test_claim_task_json_outputs() {
        unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_absurd_init as *const ())));
        }

        let conn = Connection::open_in_memory().unwrap();
        let _: i64 = conn
            .query_row("select absurd_apply_migrations()", [], |row| row.get(0))
            .unwrap();

        let _: i64 = conn
            .query_row("select absurd_create_queue('alpha')", [], |r| r.get(0))
            .unwrap();

        let options = r#"{"headers":{"x":1},"retry_strategy":{"kind":"fixed","base_seconds":1}}"#;
        let row: (String, String) = conn
            .query_row(
                "select task_id, run_id from absurd_spawn_task('alpha','demo','{}',?1)",
                [options],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();

        let json_flags: (i64, i64, i64, i64) = conn
            .query_row(
                "select json_valid(json(params)), json_valid(json(headers)), json_valid(json(retry_strategy)), coalesce(json_valid(json(event_payload)), 0)
                 from absurd_claim_task('alpha','worker',30,1)",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(json_flags.0, 1);
        assert_eq!(json_flags.1, 1);
        assert_eq!(json_flags.2, 1);
        assert_eq!(json_flags.3, 0);

        let _: i64 = conn
            .query_row(
                "select count(*) from absurd_runs where queue_name = 'alpha' and run_id = ?1",
                [&row.1],
                |r| r.get(0),
            )
            .unwrap();
    }

    #[test]
    fn test_claim_task_params_decode_client_side() {
        unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_absurd_init as *const ())));
        }

        let conn = Connection::open_in_memory().unwrap();
        let _: i64 = conn
            .query_row("select absurd_apply_migrations()", [], |row| row.get(0))
            .unwrap();

        let _: i64 = conn
            .query_row("select absurd_create_queue('alpha')", [], |r| r.get(0))
            .unwrap();

        let params = r#"{"x":1}"#;
        let _: (String, String) = conn
            .query_row(
                "select task_id, run_id from absurd_spawn_task('alpha','demo',?1,'{}')",
                [params],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();

        let claimed_params: String = conn
            .query_row(
                "select json(params) from absurd_claim_task('alpha','worker',30,1)",
                [],
                |r| r.get(0),
            )
            .unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&claimed_params).unwrap();
        assert_eq!(parsed.get("x").and_then(|v| v.as_i64()), Some(1));
    }

    fn table_column_types(conn: &Connection, table: &str) -> HashMap<String, String> {
        let pragma = format!("pragma table_xinfo({})", table);
        let mut stmt = conn.prepare(&pragma).expect("prepare table_xinfo");
        let rows = stmt
            .query_map([], |row| {
                let name: String = row.get(1)?;
                let ty: String = row.get(2)?;
                Ok((name, ty))
            })
            .expect("query table_xinfo");
        let mut map = HashMap::new();
        for row in rows {
            let (name, ty) = row.expect("read column type");
            map.insert(name, ty);
        }
        map
    }

    #[test]
    fn test_table_function_declared_types() {
        unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_absurd_init as *const ())));
        }

        let conn = Connection::open_in_memory().unwrap();

        let claim = table_column_types(&conn, "absurd_claim_task");
        assert_eq!(claim.get("run_id").unwrap(), "TEXT");
        assert_eq!(claim.get("task_id").unwrap(), "TEXT");
        assert_eq!(claim.get("attempt").unwrap(), "INTEGER");
        assert_eq!(claim.get("task_name").unwrap(), "TEXT");
        assert_eq!(claim.get("params").unwrap(), "BLOB");
        assert_eq!(claim.get("retry_strategy").unwrap(), "BLOB");
        assert_eq!(claim.get("max_attempts").unwrap(), "INTEGER");
        assert_eq!(claim.get("headers").unwrap(), "BLOB");
        assert_eq!(claim.get("wake_event").unwrap(), "TEXT");
        assert_eq!(claim.get("event_payload").unwrap(), "BLOB");
        assert_eq!(claim.get("queue_name").unwrap(), "TEXT");
        assert_eq!(claim.get("worker_id").unwrap(), "TEXT");
        assert_eq!(claim.get("claim_timeout").unwrap(), "INTEGER");
        assert_eq!(claim.get("qty").unwrap(), "INTEGER");

        let spawn = table_column_types(&conn, "absurd_spawn_task");
        assert_eq!(spawn.get("task_id").unwrap(), "TEXT");
        assert_eq!(spawn.get("run_id").unwrap(), "TEXT");
        assert_eq!(spawn.get("attempt").unwrap(), "INTEGER");
        assert_eq!(spawn.get("created").unwrap(), "INTEGER");
        assert_eq!(spawn.get("queue_name").unwrap(), "TEXT");
        assert_eq!(spawn.get("task_name").unwrap(), "TEXT");
        assert_eq!(spawn.get("params").unwrap(), "TEXT");
        assert_eq!(spawn.get("options").unwrap(), "TEXT");

        let await_event = table_column_types(&conn, "absurd_await_event");
        assert_eq!(await_event.get("should_suspend").unwrap(), "INTEGER");
        assert_eq!(await_event.get("payload").unwrap(), "BLOB");
        assert_eq!(await_event.get("queue_name").unwrap(), "TEXT");
        assert_eq!(await_event.get("task_id").unwrap(), "TEXT");
        assert_eq!(await_event.get("run_id").unwrap(), "TEXT");
        assert_eq!(await_event.get("step_name").unwrap(), "TEXT");
        assert_eq!(await_event.get("event_name").unwrap(), "TEXT");
        assert_eq!(await_event.get("timeout").unwrap(), "INTEGER");

        let queues = table_column_types(&conn, "absurd_list_queues");
        assert_eq!(queues.get("queue_name").unwrap(), "TEXT");
        assert_eq!(queues.get("created_at").unwrap(), "INTEGER");

        let checkpoint_one = table_column_types(&conn, "absurd_get_task_checkpoint_state");
        assert_eq!(checkpoint_one.get("checkpoint_name").unwrap(), "TEXT");
        assert_eq!(checkpoint_one.get("state").unwrap(), "BLOB");
        assert_eq!(checkpoint_one.get("status").unwrap(), "TEXT");
        assert_eq!(checkpoint_one.get("owner_run_id").unwrap(), "TEXT");
        assert_eq!(checkpoint_one.get("updated_at").unwrap(), "INTEGER");
        assert_eq!(checkpoint_one.get("queue_name").unwrap(), "TEXT");
        assert_eq!(checkpoint_one.get("task_id").unwrap(), "TEXT");
        assert_eq!(checkpoint_one.get("step_name").unwrap(), "TEXT");
        assert_eq!(checkpoint_one.get("include_pending").unwrap(), "INTEGER");

        let checkpoint_all = table_column_types(&conn, "absurd_get_task_checkpoint_states");
        assert_eq!(checkpoint_all.get("checkpoint_name").unwrap(), "TEXT");
        assert_eq!(checkpoint_all.get("state").unwrap(), "BLOB");
        assert_eq!(checkpoint_all.get("status").unwrap(), "TEXT");
        assert_eq!(checkpoint_all.get("owner_run_id").unwrap(), "TEXT");
        assert_eq!(checkpoint_all.get("updated_at").unwrap(), "INTEGER");
        assert_eq!(checkpoint_all.get("queue_name").unwrap(), "TEXT");
        assert_eq!(checkpoint_all.get("task_id").unwrap(), "TEXT");
        assert_eq!(checkpoint_all.get("run_id").unwrap(), "TEXT");
    }

    #[test]
    fn test_schedule_run_basic() {
        unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_absurd_init as *const ())));
        }

        let conn = Connection::open_in_memory().unwrap();
        let _: i64 = conn
            .query_row("select absurd_apply_migrations()", [], |row| row.get(0))
            .unwrap();

        let _: i64 = conn
            .query_row("select absurd_create_queue('alpha')", [], |r| r.get(0))
            .unwrap();

        let row: (String, String, i64, i64) = conn
            .query_row(
                "select task_id, run_id, attempt, created from absurd_spawn_task('alpha','demo','{}','{}')",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();

        let _ = conn
            .query_row(
                "select run_id from absurd_claim_task('alpha','worker',30,1)",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap();

        let wake_at = 123456_i64;
        let _: Option<i64> = conn
            .query_row(
                "select absurd_schedule_run('alpha', ?1, ?2)",
                (&row.1, wake_at),
                |r| r.get(0),
            )
            .unwrap();

        let run_state: (String, i64) = conn
            .query_row(
                "select state, available_at from absurd_runs where queue_name = 'alpha' and run_id = ?1",
                [&row.1],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(run_state.0, "sleeping");
        assert_eq!(run_state.1, wake_at);

        let task_state: String = conn
            .query_row(
                "select state from absurd_tasks where queue_name = 'alpha' and task_id = ?1",
                [&row.0],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(task_state, "sleeping");
    }

    #[test]
    fn test_fail_run_basic() {
        unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_absurd_init as *const ())));
        }

        let conn = Connection::open_in_memory().unwrap();
        let _: i64 = conn
            .query_row("select absurd_apply_migrations()", [], |row| row.get(0))
            .unwrap();

        let _: i64 = conn
            .query_row("select absurd_create_queue('alpha')", [], |r| r.get(0))
            .unwrap();

        let options = r#"{"retry_strategy":{"kind":"fixed","base_seconds":0},"max_attempts":2}"#;
        let row: (String, String, i64, i64) = conn
            .query_row(
                "select task_id, run_id, attempt, created from absurd_spawn_task('alpha','demo','{}',?1)",
                [options],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();

        let _ = conn
            .query_row(
                "select run_id from absurd_claim_task('alpha','worker',30,1)",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap();

        let reason = r#"{"name":"err","message":"boom"}"#;
        let _: Option<i64> = conn
            .query_row(
                "select absurd_fail_run('alpha', ?1, ?2, ?3)",
                (&row.1, reason, "1970-01-01T00:00:00Z"),
                |r| r.get(0),
            )
            .unwrap();

        let failed_state: String = conn
            .query_row(
                "select state from absurd_runs where queue_name = 'alpha' and run_id = ?1",
                [&row.1],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(failed_state, "failed");

        let next_run: (i64, String) = conn
            .query_row(
                "select attempt, state from absurd_runs where queue_name = 'alpha' and task_id = ?1 and attempt = 2",
                [&row.0],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(next_run.0, 2);
        assert_eq!(next_run.1, "pending");
    }

    #[test]
    fn test_complete_run_basic() {
        unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_absurd_init as *const ())));
        }

        let conn = Connection::open_in_memory().unwrap();
        let _: i64 = conn
            .query_row("select absurd_apply_migrations()", [], |row| row.get(0))
            .unwrap();

        let _: i64 = conn
            .query_row("select absurd_create_queue('alpha')", [], |r| r.get(0))
            .unwrap();

        let row: (String, String, i64, i64) = conn
            .query_row(
                "select task_id, run_id, attempt, created from absurd_spawn_task('alpha','demo','{}','{}')",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();

        let _: String = conn
            .query_row(
                "select run_id from absurd_claim_task('alpha','worker',30,1)",
                [],
                |r| r.get(0),
            )
            .unwrap();

        let state = r#"{"ok":true}"#;
        let _: Option<i64> = conn
            .query_row(
                "select absurd_complete_run('alpha', ?1, ?2)",
                (&row.1, state),
                |r| r.get(0),
            )
            .unwrap();

        let run_state: (String, String) = conn
            .query_row(
                "select state, json(result) from absurd_runs where queue_name = 'alpha' and run_id = ?1",
                [&row.1],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(run_state.0, "completed");
        assert_eq!(run_state.1, state);

        let task_state: (String, String) = conn
            .query_row(
                "select state, json(completed_payload) from absurd_tasks where queue_name = 'alpha' and task_id = ?1",
                [&row.0],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(task_state.0, "completed");
        assert_eq!(task_state.1, state);
    }

    #[test]
    fn test_extend_claim_basic() {
        unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_absurd_init as *const ())));
        }

        let conn = Connection::open_in_memory().unwrap();
        let _: i64 = conn
            .query_row("select absurd_apply_migrations()", [], |row| row.get(0))
            .unwrap();

        let _: i64 = conn
            .query_row("select absurd_create_queue('alpha')", [], |r| r.get(0))
            .unwrap();

        let row: (String, String, i64, i64) = conn
            .query_row(
                "select task_id, run_id, attempt, created from absurd_spawn_task('alpha','demo','{}','{}')",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();

        let _: String = conn
            .query_row(
                "select run_id from absurd_claim_task('alpha','worker',30,1)",
                [],
                |r| r.get(0),
            )
            .unwrap();

        let before: i64 = conn
            .query_row(
                "select coalesce(claim_expires_at, 0) from absurd_runs where queue_name = 'alpha' and run_id = ?1",
                [&row.1],
                |r| r.get(0),
            )
            .unwrap();

        let _: Option<i64> = conn
            .query_row(
                "select absurd_extend_claim('alpha', ?1, 60)",
                [&row.1],
                |r| r.get(0),
            )
            .unwrap();

        let after: i64 = conn
            .query_row(
                "select coalesce(claim_expires_at, 0) from absurd_runs where queue_name = 'alpha' and run_id = ?1",
                [&row.1],
                |r| r.get(0),
            )
            .unwrap();

        assert!(after >= before);
    }

    #[test]
    fn test_checkpoint_roundtrip() {
        unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_absurd_init as *const ())));
        }

        let conn = Connection::open_in_memory().unwrap();
        let _: i64 = conn
            .query_row("select absurd_apply_migrations()", [], |row| row.get(0))
            .unwrap();

        let _: i64 = conn
            .query_row("select absurd_create_queue('alpha')", [], |r| r.get(0))
            .unwrap();

        let row: (String, String, i64, i64) = conn
            .query_row(
                "select task_id, run_id, attempt, created from absurd_spawn_task('alpha','demo','{}','{}')",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();

        let _: String = conn
            .query_row(
                "select run_id from absurd_claim_task('alpha','worker',30,1)",
                [],
                |r| r.get(0),
            )
            .unwrap();

        let payload = r#"{"step":1}"#;
        let _: Option<i64> = conn
            .query_row(
                "select absurd_set_task_checkpoint_state('alpha', ?1, 'stepA', ?2, ?3, null)",
                (&row.0, payload, &row.1),
                |r| r.get(0),
            )
            .unwrap();

        let checkpoint: (String, String, String) = conn
            .query_row(
                "select checkpoint_name, json(state), status from absurd_get_task_checkpoint_state('alpha', ?1, 'stepA', 0)",
                [&row.0],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(checkpoint.0, "stepA");
        assert_eq!(checkpoint.1, payload);
        assert_eq!(checkpoint.2, "committed");

        let count: i64 = conn
            .query_row(
                "select count(*) from absurd_get_task_checkpoint_states('alpha', ?1, ?2)",
                (&row.0, &row.1),
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_await_and_emit_event() {
        unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_absurd_init as *const ())));
        }

        let conn = Connection::open_in_memory().unwrap();
        let _: i64 = conn
            .query_row("select absurd_apply_migrations()", [], |row| row.get(0))
            .unwrap();

        let _: i64 = conn
            .query_row("select absurd_create_queue('alpha')", [], |r| r.get(0))
            .unwrap();

        let row: (String, String, i64, i64) = conn
            .query_row(
                "select task_id, run_id, attempt, created from absurd_spawn_task('alpha','demo','{}','{}')",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();

        let _: String = conn
            .query_row(
                "select run_id from absurd_claim_task('alpha','worker',30,1)",
                [],
                |r| r.get(0),
            )
            .unwrap();

        let await_row: (i64, Option<String>) = conn
            .query_row(
                "select should_suspend, json(payload) from absurd_await_event('alpha', ?1, ?2, 'stepA', 'eventA', null)",
                (&row.0, &row.1),
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(await_row.0, 1);
        assert!(await_row.1.is_none());

        let payload = r#"{"ok":true}"#;
        let _: Option<i64> = conn
            .query_row(
                "select absurd_emit_event('alpha', 'eventA', ?1)",
                [payload],
                |r| r.get(0),
            )
            .unwrap();

        let _: String = conn
            .query_row(
                "select run_id from absurd_claim_task('alpha','worker',30,1)",
                [],
                |r| r.get(0),
            )
            .unwrap();

        let await_row2: (i64, Option<String>) = conn
            .query_row(
                "select should_suspend, json(payload) from absurd_await_event('alpha', ?1, ?2, 'stepA', 'eventA', null)",
                (&row.0, &row.1),
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(await_row2.0, 0);
        assert_eq!(await_row2.1.as_deref(), Some(payload));
    }

    #[test]
    fn test_cleanup_tasks_and_events() {
        unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_absurd_init as *const ())));
        }

        let conn = Connection::open_in_memory().unwrap();
        let _: i64 = conn
            .query_row("select absurd_apply_migrations()", [], |row| row.get(0))
            .unwrap();

        let _: i64 = conn
            .query_row("select absurd_create_queue('alpha')", [], |r| r.get(0))
            .unwrap();

        let row: (String, String, i64, i64) = conn
            .query_row(
                "select task_id, run_id, attempt, created from absurd_spawn_task('alpha','demo','{}','{}')",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();

        let _: String = conn
            .query_row(
                "select run_id from absurd_claim_task('alpha','worker',30,1)",
                [],
                |r| r.get(0),
            )
            .unwrap();

        let state = r#"{"ok":true}"#;
        let _: Option<i64> = conn
            .query_row(
                "select absurd_complete_run('alpha', ?1, ?2)",
                (&row.1, state),
                |r| r.get(0),
            )
            .unwrap();

        conn.execute(
            "update absurd_runs set completed_at = 1 where queue_name = 'alpha' and run_id = ?1",
            [&row.1],
        )
        .unwrap();

        let _: Option<i64> = conn
            .query_row("select absurd_emit_event('alpha', 'eventA')", [], |r| {
                r.get(0)
            })
            .unwrap();
        conn.execute(
            "update absurd_events set emitted_at = 1 where queue_name = 'alpha' and event_name = 'eventA'",
            [],
        )
        .unwrap();

        let deleted_tasks: i64 = conn
            .query_row("select absurd_cleanup_tasks('alpha', 1, 100)", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(deleted_tasks, 1);

        let deleted_events: i64 = conn
            .query_row("select absurd_cleanup_events('alpha', 1, 100)", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(deleted_events, 1);
    }

    #[test]
    fn test_cleanup_large_batches() {
        unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_absurd_init as *const ())));
        }

        let conn = Connection::open_in_memory().unwrap();
        let _: i64 = conn
            .query_row("select absurd_apply_migrations()", [], |row| row.get(0))
            .unwrap();
        let _: i64 = conn
            .query_row("select absurd_create_queue('alpha')", [], |r| r.get(0))
            .unwrap();

        let total_tasks = 12_000;
        let total_events = 12_000;

        conn.execute_batch("begin").unwrap();
        let mut insert_task = conn
            .prepare(
                "insert into absurd_tasks (queue_name, task_id, task_name, params, state)
                 values (?1, ?2, ?3, ?4, 'completed')",
            )
            .unwrap();
        let mut insert_run = conn
            .prepare(
                "insert into absurd_runs (queue_name, run_id, task_id, attempt, state, available_at, completed_at, created_at)
                 values (?1, ?2, ?3, 1, 'completed', 1, 1, 1)",
            )
            .unwrap();
        let mut update_task = conn
            .prepare(
                "update absurd_tasks set last_attempt_run = ?1 where queue_name = ?2 and task_id = ?3",
            )
            .unwrap();

        for index in 0..total_tasks {
            let task_id = format!("task_{}", index);
            let run_id = format!("run_{}", index);
            insert_task
                .execute(params!["alpha", task_id, "demo", "{}"])
                .unwrap();
            insert_run
                .execute(params!["alpha", run_id, task_id])
                .unwrap();
            update_task
                .execute(params![run_id, "alpha", task_id])
                .unwrap();
        }
        conn.execute_batch("commit").unwrap();

        conn.execute_batch("begin").unwrap();
        let mut insert_event = conn
            .prepare(
                "insert into absurd_events (queue_name, event_name, payload, emitted_at)
                 values (?1, ?2, ?3, 1)",
            )
            .unwrap();
        for index in 0..total_events {
            let event_name = format!("event_{}", index);
            insert_event
                .execute(params!["alpha", event_name, "{}"])
                .unwrap();
        }
        conn.execute_batch("commit").unwrap();

        let mut deleted_tasks = 0;
        loop {
            let deleted: i64 = conn
                .query_row("select absurd_cleanup_tasks('alpha', 1, 500)", [], |r| {
                    r.get(0)
                })
                .unwrap();
            deleted_tasks += deleted;
            if deleted == 0 {
                break;
            }
        }
        assert_eq!(deleted_tasks, total_tasks);
        let remaining_tasks: i64 = conn
            .query_row(
                "select count(*) from absurd_tasks where queue_name = 'alpha'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(remaining_tasks, 0);

        let mut deleted_events = 0;
        loop {
            let deleted: i64 = conn
                .query_row("select absurd_cleanup_events('alpha', 1, 500)", [], |r| {
                    r.get(0)
                })
                .unwrap();
            deleted_events += deleted;
            if deleted == 0 {
                break;
            }
        }
        assert_eq!(deleted_events, total_events);
        let remaining_events: i64 = conn
            .query_row(
                "select count(*) from absurd_events where queue_name = 'alpha'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(remaining_events, 0);
    }

    #[test]
    fn test_cancel_task_basic() {
        unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_absurd_init as *const ())));
        }

        let conn = Connection::open_in_memory().unwrap();
        let _: i64 = conn
            .query_row("select absurd_apply_migrations()", [], |row| row.get(0))
            .unwrap();

        let _: i64 = conn
            .query_row("select absurd_create_queue('alpha')", [], |r| r.get(0))
            .unwrap();

        let row: (String, String, i64, i64) = conn
            .query_row(
                "select task_id, run_id, attempt, created from absurd_spawn_task('alpha','demo','{}','{}')",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();

        let _: Option<i64> = conn
            .query_row("select absurd_cancel_task('alpha', ?1)", [&row.0], |r| {
                r.get(0)
            })
            .unwrap();

        let task_state: String = conn
            .query_row(
                "select state from absurd_tasks where queue_name = 'alpha' and task_id = ?1",
                [&row.0],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(task_state, "cancelled");

        let run_state: String = conn
            .query_row(
                "select state from absurd_runs where queue_name = 'alpha' and run_id = ?1",
                [&row.1],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(run_state, "cancelled");
    }
}
