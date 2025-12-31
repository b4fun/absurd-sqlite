use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::process::Command;
use uuid::Uuid;

fn extension_path() -> PathBuf {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir.parent().expect("workspace root not found");
    let lib_name = format!("libabsurd{}", std::env::consts::DLL_SUFFIX);
    workspace_root.join("target").join("debug").join(lib_name)
}

fn build_extension(_path: &Path) {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let status = Command::new(cargo)
        .args([
            "build",
            "--manifest-path",
            manifest_dir
                .join("Cargo.toml")
                .to_str()
                .expect("manifest path"),
        ])
        .status()
        .expect("run cargo build");
    if !status.success() {
        panic!("cargo build failed");
    }
}

fn apply_migrations(conn: &Connection) {
    let _: i64 = conn
        .query_row("select absurd_apply_migrations()", [], |r| r.get(0))
        .expect("apply migrations");
}

#[test]
fn test_extension_with_real_db() {
    let db_path = std::env::temp_dir().join(format!("absurd-test-{}.db", Uuid::new_v4()));
    let conn = Connection::open(&db_path).expect("open db");

    let ext_path = extension_path();
    build_extension(&ext_path);

    unsafe {
        conn.load_extension_enable().expect("enable load_extension");
        conn.load_extension(&ext_path, None::<&str>)
            .expect("load extension");
        conn.load_extension_disable()
            .expect("disable load_extension");
    }

    apply_migrations(&conn);

    let _: i64 = conn
        .query_row("select absurd_create_queue('alpha')", [], |r| r.get(0))
        .expect("create queue");

    let row: (String, String, i64, i64) = conn
        .query_row(
            "select task_id, run_id, attempt, created from absurd_spawn_task('alpha','demo','{}','{}')",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .expect("spawn task");

    assert_eq!(row.2, 1);
    assert_eq!(row.3, 1);

    let count: i64 = conn
        .query_row(
            "select count(*) from absurd_tasks where queue_name = 'alpha' and task_id = ?1",
            [&row.0],
            |r| r.get(0),
        )
        .expect("verify task row");
    assert_eq!(count, 1);

    let claim: (String, String, i64, String) = conn
        .query_row(
            "select run_id, task_id, attempt, task_name from absurd_claim_task('alpha','worker',30,1)",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .expect("claim task");

    assert_eq!(claim.0, row.1);
    assert_eq!(claim.1, row.0);
    assert_eq!(claim.2, 1);
    assert_eq!(claim.3, "demo");

    let claim_row: (String, i64) = conn
        .query_row(
            "select claimed_by, coalesce(claim_expires_at, 0) from absurd_runs where queue_name = 'alpha' and run_id = ?1",
            [row.1],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("verify claim");
    assert_eq!(claim_row.0, "worker");
    assert!(claim_row.1 > 0);

    let _ = std::fs::remove_file(db_path);
}

#[test]
fn test_claim_timeout_creates_retry_run() {
    let db_path = std::env::temp_dir().join(format!("absurd-test-{}.db", Uuid::new_v4()));
    let conn = Connection::open(&db_path).expect("open db");

    let ext_path = extension_path();
    build_extension(&ext_path);

    unsafe {
        conn.load_extension_enable().expect("enable load_extension");
        conn.load_extension(&ext_path, None::<&str>)
            .expect("load extension");
        conn.load_extension_disable()
            .expect("disable load_extension");
    }

    apply_migrations(&conn);

    let _: i64 = conn
        .query_row("select absurd_create_queue('alpha')", [], |r| r.get(0))
        .expect("create queue");

    let options = r#"{"retry_strategy":{"kind":"fixed","base_seconds":60},"max_attempts":2}"#;
    let row: (String, String, i64, i64) = conn
        .query_row(
            "select task_id, run_id, attempt, created from absurd_spawn_task('alpha','demo','{}',?1)",
            [options],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .expect("spawn task");

    conn.execute(
        "update absurd_runs set state = 'running', claimed_by = 'worker', claim_expires_at = 1, started_at = 1 where queue_name = 'alpha' and run_id = ?1",
        [&row.1],
    )
    .expect("mark run running");
    conn.execute(
        "update absurd_tasks set state = 'running', first_started_at = 1 where queue_name = 'alpha' and task_id = ?1",
        [&row.0],
    )
    .expect("mark task running");

    let _: i64 = conn
        .query_row(
            "select count(*) from absurd_claim_task('alpha','worker2',30,1)",
            [],
            |r| r.get(0),
        )
        .expect("claim to trigger timeout");

    let failed: (String, String) = conn
        .query_row(
            "select state, json(failure_reason) from absurd_runs where queue_name = 'alpha' and run_id = ?1",
            [&row.1],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("verify failed run");
    assert_eq!(failed.0, "failed");
    assert!(failed.1.contains("$ClaimTimeout"));

    let retry_run: (i64, String) = conn
        .query_row(
            "select attempt, state from absurd_runs where queue_name = 'alpha' and task_id = ?1 and attempt = 2",
            [&row.0],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("verify retry run");
    assert_eq!(retry_run.0, 2);
    assert_eq!(retry_run.1, "sleeping");

    let task_state: String = conn
        .query_row(
            "select state from absurd_tasks where queue_name = 'alpha' and task_id = ?1",
            [&row.0],
            |r| r.get(0),
        )
        .expect("verify task state");
    assert_eq!(task_state, "sleeping");

    let _ = std::fs::remove_file(db_path);
}

#[test]
fn test_schedule_and_fail_run_integration() {
    let db_path = std::env::temp_dir().join(format!("absurd-test-{}.db", Uuid::new_v4()));
    let conn = Connection::open(&db_path).expect("open db");

    let ext_path = extension_path();
    build_extension(&ext_path);

    unsafe {
        conn.load_extension_enable().expect("enable load_extension");
        conn.load_extension(&ext_path, None::<&str>)
            .expect("load extension");
        conn.load_extension_disable()
            .expect("disable load_extension");
    }

    apply_migrations(&conn);

    let _: i64 = conn
        .query_row("select absurd_create_queue('alpha')", [], |r| r.get(0))
        .expect("create queue");

    let options = r#"{"retry_strategy":{"kind":"fixed","base_seconds":0},"max_attempts":2}"#;
    let row: (String, String, i64, i64) = conn
        .query_row(
            "select task_id, run_id, attempt, created from absurd_spawn_task('alpha','demo','{}',?1)",
            [options],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .expect("spawn task");

    let claim1: String = conn
        .query_row(
            "select run_id from absurd_claim_task('alpha','worker',30,1)",
            [],
            |r| r.get(0),
        )
        .expect("claim run");
    assert_eq!(claim1, row.1);

    let reason = r#"{"name":"err","message":"boom"}"#;
    let _: Option<i64> = conn
        .query_row(
            "select absurd_fail_run('alpha', ?1, ?2)",
            (&row.1, reason),
            |r| r.get(0),
        )
        .expect("fail run");

    let failed_state: String = conn
        .query_row(
            "select state from absurd_runs where queue_name = 'alpha' and run_id = ?1",
            [&row.1],
            |r| r.get(0),
        )
        .expect("failed run state");
    assert_eq!(failed_state, "failed");

    let claim2: (String, i64) = conn
        .query_row(
            "select run_id, attempt from absurd_claim_task('alpha','worker',30,1)",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("claim retry run");
    assert_eq!(claim2.1, 2);

    let wake_at = "1970-01-01T00:01:27Z";
    let _: Option<i64> = conn
        .query_row(
            "select absurd_schedule_run('alpha', ?1, ?2)",
            (&claim2.0, wake_at),
            |r| r.get(0),
        )
        .expect("schedule run");

    let run_state: (String, i64) = conn
        .query_row(
            "select state, available_at from absurd_runs where queue_name = 'alpha' and run_id = ?1",
            [&claim2.0],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("run state");
    assert_eq!(run_state.0, "sleeping");
    assert_eq!(run_state.1, 87_000);

    let task_state: String = conn
        .query_row(
            "select state from absurd_tasks where queue_name = 'alpha' and task_id = ?1",
            [&row.0],
            |r| r.get(0),
        )
        .expect("task state");
    assert_eq!(task_state, "sleeping");

    let _ = std::fs::remove_file(db_path);
}

#[test]
fn test_complete_and_extend_claim_integration() {
    let db_path = std::env::temp_dir().join(format!("absurd-test-{}.db", Uuid::new_v4()));
    let conn = Connection::open(&db_path).expect("open db");

    let ext_path = extension_path();
    build_extension(&ext_path);

    unsafe {
        conn.load_extension_enable().expect("enable load_extension");
        conn.load_extension(&ext_path, None::<&str>)
            .expect("load extension");
        conn.load_extension_disable()
            .expect("disable load_extension");
    }

    apply_migrations(&conn);

    let _: i64 = conn
        .query_row("select absurd_create_queue('alpha')", [], |r| r.get(0))
        .expect("create queue");

    let row: (String, String, i64, i64) = conn
        .query_row(
            "select task_id, run_id, attempt, created from absurd_spawn_task('alpha','demo','{}','{}')",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .expect("spawn task");

    let _: String = conn
        .query_row(
            "select run_id from absurd_claim_task('alpha','worker',30,1)",
            [],
            |r| r.get(0),
        )
        .expect("claim run");

    let before: i64 = conn
        .query_row(
            "select coalesce(claim_expires_at, 0) from absurd_runs where queue_name = 'alpha' and run_id = ?1",
            [&row.1],
            |r| r.get(0),
        )
        .expect("claim expires before");

    let _: Option<i64> = conn
        .query_row(
            "select absurd_extend_claim('alpha', ?1, 60)",
            [&row.1],
            |r| r.get(0),
        )
        .expect("extend claim");

    let after: i64 = conn
        .query_row(
            "select coalesce(claim_expires_at, 0) from absurd_runs where queue_name = 'alpha' and run_id = ?1",
            [&row.1],
            |r| r.get(0),
        )
        .expect("claim expires after");
    assert!(after >= before);

    let state = r#"{"ok":true}"#;
    let _: Option<i64> = conn
        .query_row(
            "select absurd_complete_run('alpha', ?1, ?2)",
            (&row.1, state),
            |r| r.get(0),
        )
        .expect("complete run");

    let run_state: (String, String) = conn
        .query_row(
            "select state, json(result) from absurd_runs where queue_name = 'alpha' and run_id = ?1",
            [&row.1],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("run state");
    assert_eq!(run_state.0, "completed");
    assert_eq!(run_state.1, state);

    let task_state: (String, String) = conn
        .query_row(
            "select state, json(completed_payload) from absurd_tasks where queue_name = 'alpha' and task_id = ?1",
            [&row.0],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("task state");
    assert_eq!(task_state.0, "completed");
    assert_eq!(task_state.1, state);

    let _ = std::fs::remove_file(db_path);
}

#[test]
fn test_checkpoint_and_event_integration() {
    let db_path = std::env::temp_dir().join(format!("absurd-test-{}.db", Uuid::new_v4()));
    let conn = Connection::open(&db_path).expect("open db");

    let ext_path = extension_path();
    build_extension(&ext_path);

    unsafe {
        conn.load_extension_enable().expect("enable load_extension");
        conn.load_extension(&ext_path, None::<&str>)
            .expect("load extension");
        conn.load_extension_disable()
            .expect("disable load_extension");
    }

    apply_migrations(&conn);

    let _: i64 = conn
        .query_row("select absurd_create_queue('alpha')", [], |r| r.get(0))
        .expect("create queue");

    let row: (String, String, i64, i64) = conn
        .query_row(
            "select task_id, run_id, attempt, created from absurd_spawn_task('alpha','demo','{}','{}')",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .expect("spawn task");

    let _: String = conn
        .query_row(
            "select run_id from absurd_claim_task('alpha','worker',30,1)",
            [],
            |r| r.get(0),
        )
        .expect("claim run");

    let payload = r#"{"step":1}"#;
    let _: Option<i64> = conn
        .query_row(
            "select absurd_set_task_checkpoint_state('alpha', ?1, 'stepA', ?2, ?3)",
            (&row.0, payload, &row.1),
            |r| r.get(0),
        )
        .expect("set checkpoint");

    let checkpoint: (String, String) = conn
        .query_row(
            "select checkpoint_name, json(state) from absurd_get_task_checkpoint_state('alpha', ?1, 'stepA', 0)",
            [&row.0],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("checkpoint state");
    assert_eq!(checkpoint.0, "stepA");
    assert_eq!(checkpoint.1, payload);

    let await_row: (i64, Option<String>) = conn
        .query_row(
            "select should_suspend, json(payload) from absurd_await_event('alpha', ?1, ?2, 'stepB', 'eventB', null)",
            (&row.0, &row.1),
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("await event");
    assert_eq!(await_row.0, 1);
    assert!(await_row.1.is_none());

    let event_payload = r#"{"ok":true}"#;
    let _: Option<i64> = conn
        .query_row(
            "select absurd_emit_event('alpha', 'eventB', ?1)",
            [event_payload],
            |r| r.get(0),
        )
        .expect("emit event");

    let _: String = conn
        .query_row(
            "select run_id from absurd_claim_task('alpha','worker',30,1)",
            [],
            |r| r.get(0),
        )
        .expect("re-claim run");

    let await_row2: (i64, Option<String>) = conn
        .query_row(
            "select should_suspend, json(payload) from absurd_await_event('alpha', ?1, ?2, 'stepB', 'eventB', null)",
            (&row.0, &row.1),
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("await event after emit");
    assert_eq!(await_row2.0, 0);
    assert_eq!(await_row2.1.as_deref(), Some(event_payload));

    let _ = std::fs::remove_file(db_path);
}

#[test]
fn test_cleanup_integration() {
    let db_path = std::env::temp_dir().join(format!("absurd-test-{}.db", Uuid::new_v4()));
    let conn = Connection::open(&db_path).expect("open db");

    let ext_path = extension_path();
    build_extension(&ext_path);

    unsafe {
        conn.load_extension_enable().expect("enable load_extension");
        conn.load_extension(&ext_path, None::<&str>)
            .expect("load extension");
        conn.load_extension_disable()
            .expect("disable load_extension");
    }

    apply_migrations(&conn);

    let _: i64 = conn
        .query_row("select absurd_create_queue('alpha')", [], |r| r.get(0))
        .expect("create queue");

    let row: (String, String, i64, i64) = conn
        .query_row(
            "select task_id, run_id, attempt, created from absurd_spawn_task('alpha','demo','{}','{}')",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .expect("spawn task");

    let _: String = conn
        .query_row(
            "select run_id from absurd_claim_task('alpha','worker',30,1)",
            [],
            |r| r.get(0),
        )
        .expect("claim run");

    let state = r#"{"ok":true}"#;
    let _: Option<i64> = conn
        .query_row(
            "select absurd_complete_run('alpha', ?1, ?2)",
            (&row.1, state),
            |r| r.get(0),
        )
        .expect("complete run");

    conn.execute(
        "update absurd_runs set completed_at = 1 where queue_name = 'alpha' and run_id = ?1",
        [&row.1],
    )
    .expect("set completed_at");

    let _: Option<i64> = conn
        .query_row("select absurd_emit_event('alpha', 'eventA')", [], |r| {
            r.get(0)
        })
        .expect("emit event");
    conn.execute(
        "update absurd_events set emitted_at = 1 where queue_name = 'alpha' and event_name = 'eventA'",
        [],
    )
    .expect("set emitted_at");

    let deleted_tasks: i64 = conn
        .query_row("select absurd_cleanup_tasks('alpha', 1, 100)", [], |r| {
            r.get(0)
        })
        .expect("cleanup absurd_tasks");
    assert_eq!(deleted_tasks, 1);

    let deleted_events: i64 = conn
        .query_row("select absurd_cleanup_events('alpha', 1, 100)", [], |r| {
            r.get(0)
        })
        .expect("cleanup absurd_events");
    assert_eq!(deleted_events, 1);

    let _ = std::fs::remove_file(db_path);
}

#[test]
fn test_cancel_task_integration() {
    let db_path = std::env::temp_dir().join(format!("absurd-test-{}.db", Uuid::new_v4()));
    let conn = Connection::open(&db_path).expect("open db");

    let ext_path = extension_path();
    build_extension(&ext_path);

    unsafe {
        conn.load_extension_enable().expect("enable load_extension");
        conn.load_extension(&ext_path, None::<&str>)
            .expect("load extension");
        conn.load_extension_disable()
            .expect("disable load_extension");
    }

    apply_migrations(&conn);

    let _: i64 = conn
        .query_row("select absurd_create_queue('alpha')", [], |r| r.get(0))
        .expect("create queue");

    let row: (String, String, i64, i64) = conn
        .query_row(
            "select task_id, run_id, attempt, created from absurd_spawn_task('alpha','demo','{}','{}')",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .expect("spawn task");

    let _: Option<i64> = conn
        .query_row("select absurd_cancel_task('alpha', ?1)", [&row.0], |r| {
            r.get(0)
        })
        .expect("cancel task");

    let task_state: String = conn
        .query_row(
            "select state from absurd_tasks where queue_name = 'alpha' and task_id = ?1",
            [&row.0],
            |r| r.get(0),
        )
        .expect("task state");
    assert_eq!(task_state, "cancelled");

    let run_state: String = conn
        .query_row(
            "select state from absurd_runs where queue_name = 'alpha' and run_id = ?1",
            [&row.1],
            |r| r.get(0),
        )
        .expect("run state");
    assert_eq!(run_state, "cancelled");

    let _ = std::fs::remove_file(db_path);
}
