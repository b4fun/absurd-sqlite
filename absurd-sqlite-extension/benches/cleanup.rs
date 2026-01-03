use absurd::sqlite3_absurd_init;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use rusqlite::{ffi::sqlite3_auto_extension, params, Connection};

const QUEUE_NAME: &str = "alpha";
const BATCH_LIMIT: i64 = 1_000;
const BENCH_SIZES: [usize; 5] = [100, 500, 1_000, 5_000, 10_000];

fn setup_db(task_count: usize, event_count: usize) -> Connection {
    unsafe {
        sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_absurd_init as *const ())));
    }

    let conn = Connection::open_in_memory().unwrap();
    let _: i64 = conn
        .query_row("select absurd_apply_migrations()", [], |row| row.get(0))
        .unwrap();
    let _: i64 = conn
        .query_row("select absurd_create_queue(?1)", [QUEUE_NAME], |row| {
            row.get(0)
        })
        .unwrap();

    if task_count > 0 {
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

        for index in 0..task_count {
            let task_id = format!("task_{}", index);
            let run_id = format!("run_{}", index);
            insert_task
                .execute(params![QUEUE_NAME, task_id, "demo", "{}"])
                .unwrap();
            insert_run
                .execute(params![QUEUE_NAME, run_id, task_id])
                .unwrap();
            update_task
                .execute(params![run_id, QUEUE_NAME, task_id])
                .unwrap();
        }
        conn.execute_batch("commit").unwrap();
    }

    if event_count > 0 {
        conn.execute_batch("begin").unwrap();
        let mut insert_event = conn
            .prepare(
                "insert into absurd_events (queue_name, event_name, payload, emitted_at)
                 values (?1, ?2, ?3, 1)",
            )
            .unwrap();
        for index in 0..event_count {
            let event_name = format!("event_{}", index);
            insert_event
                .execute(params![QUEUE_NAME, event_name, "{}"])
                .unwrap();
        }
        conn.execute_batch("commit").unwrap();
    }

    conn
}

fn cleanup_tasks(conn: &Connection, expected: usize) {
    let mut deleted_total = 0;
    loop {
        let deleted: i64 = conn
            .query_row(
                "select absurd_cleanup_tasks(?1, 1, ?2)",
                params![QUEUE_NAME, BATCH_LIMIT],
                |row| row.get(0),
            )
            .unwrap();
        deleted_total += deleted;
        if deleted == 0 {
            break;
        }
    }
    assert_eq!(deleted_total as usize, expected);
}

fn cleanup_events(conn: &Connection, expected: usize) {
    let mut deleted_total = 0;
    loop {
        let deleted: i64 = conn
            .query_row(
                "select absurd_cleanup_events(?1, 1, ?2)",
                params![QUEUE_NAME, BATCH_LIMIT],
                |row| row.get(0),
            )
            .unwrap();
        deleted_total += deleted;
        if deleted == 0 {
            break;
        }
    }
    assert_eq!(deleted_total as usize, expected);
}

fn bench_cleanup_tasks(c: &mut Criterion) {
    let mut group = c.benchmark_group("cleanup_tasks");
    for size in BENCH_SIZES {
        group.bench_with_input(size.to_string(), &size, |b, &count| {
            b.iter_batched(
                || setup_db(count, 0),
                |conn| cleanup_tasks(&conn, count),
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

fn bench_cleanup_events(c: &mut Criterion) {
    let mut group = c.benchmark_group("cleanup_events");
    for size in BENCH_SIZES {
        group.bench_with_input(size.to_string(), &size, |b, &count| {
            b.iter_batched(
                || setup_db(0, count),
                |conn| cleanup_events(&conn, count),
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_cleanup_tasks, bench_cleanup_events);
criterion_main!(benches);
