use sqlite_loadable::ext::{
    sqlite3ext_bind_text, sqlite3ext_finalize, sqlite3ext_prepare_v2, sqlite3ext_step,
};
use sqlite_loadable::{Error, Result, SQLITE_DONE, SQLITE_OKAY, SQLITE_ROW};
use sqlite3ext_sys::{sqlite3, sqlite3_context};
use std::ffi::CString;
use std::os::raw::{c_char, c_void};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn exec_with_bind_text(db: *mut sqlite3, sql: &str, params: &[&str]) -> Result<()> {
    let mut stmt = std::ptr::null_mut();
    let sql_c = CString::new(sql).map_err(|err| Error::new_message(&format!("invalid sql: {:?}", err)))?;
    let rc = unsafe { sqlite3ext_prepare_v2(db, sql_c.as_ptr(), -1, &mut stmt, std::ptr::null_mut()) };
    if rc != SQLITE_OKAY {
        return Err(Error::new_message("failed to prepare statement"));
    }

    for (idx, value) in params.iter().enumerate() {
        bind_text(stmt, (idx + 1) as i32, value)?;
    }

    let step_rc = unsafe { sqlite3ext_step(stmt) };
    unsafe { sqlite3ext_finalize(stmt) };
    if step_rc != SQLITE_DONE && step_rc != SQLITE_ROW {
        return Err(Error::new_message(&format!(
            "statement execution failed (code {})",
            step_rc
        )));
    }
    Ok(())
}

pub fn exec_batch(db: *mut sqlite3, sql: &str) -> Result<()> {
    let sql_c = CString::new(sql).map_err(|err| Error::new_message(&format!("invalid sql: {:?}", err)))?;
    let mut tail = std::ptr::null();
    let mut current = sql_c.as_ptr();

    loop {
        let mut stmt = std::ptr::null_mut();
        let rc = unsafe { sqlite3ext_prepare_v2(db, current, -1, &mut stmt, &mut tail) };
        if rc != SQLITE_OKAY {
            return Err(Error::new_message("failed to prepare statement"));
        }
        if stmt.is_null() {
            break;
        }
        let step_rc = unsafe { sqlite3ext_step(stmt) };
        unsafe { sqlite3ext_finalize(stmt) };
        if step_rc != SQLITE_DONE && step_rc != SQLITE_ROW {
            return Err(Error::new_message(&format!(
                "statement execution failed (code {})",
                step_rc
            )));
        }
        if tail.is_null() || unsafe { *tail } == 0 {
            break;
        }
        current = tail;
    }

    Ok(())
}

pub fn query_row_i64(db: *mut sqlite3, sql: &str, params: &[&str]) -> Result<i64> {
    let mut stmt = sqlite_loadable::exec::Statement::prepare(db, sql)
        .map_err(|err| Error::new_message(&format!("failed to prepare statement: {:?}", err)))?;
    for (idx, value) in params.iter().enumerate() {
        stmt.bind_text((idx + 1) as i32, value)
            .map_err(|err| Error::new_message(&format!("failed to bind text: {:?}", err)))?;
    }
    let mut rows = stmt.execute();
    if let Some(Ok(row)) = rows.next() {
        row.get::<i64>(0)
            .map_err(|err| Error::new_message(&format!("failed to read row: {:?}", err)))
    } else {
        Err(Error::new_message("no rows returned"))
    }
}

pub fn query_row_strings(
    db: *mut sqlite3,
    sql: &str,
    params: &[&str],
) -> Result<(String, String, i64)> {
    let mut stmt = sqlite_loadable::exec::Statement::prepare(db, sql)
        .map_err(|err| Error::new_message(&format!("failed to prepare statement: {:?}", err)))?;
    for (idx, value) in params.iter().enumerate() {
        stmt.bind_text((idx + 1) as i32, value)
            .map_err(|err| Error::new_message(&format!("failed to bind text: {:?}", err)))?;
    }
    let mut rows = stmt.execute();
    if let Some(Ok(row)) = rows.next() {
        let task_id = row
            .get::<String>(0)
            .map_err(|err| Error::new_message(&format!("failed to read task_id: {:?}", err)))?;
        let run_id = row
            .get::<String>(1)
            .map_err(|err| Error::new_message(&format!("failed to read run_id: {:?}", err)))?;
        let attempt = row
            .get::<i64>(2)
            .map_err(|err| Error::new_message(&format!("failed to read attempt: {:?}", err)))?;
        Ok((task_id, run_id, attempt))
    } else {
        Err(Error::new_message("no rows returned"))
    }
}

fn bind_text(stmt: *mut sqlite3ext_sys::sqlite3_stmt, idx: i32, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    let cstr = CString::new(bytes).map_err(|err| Error::new_message(&format!("invalid bind text: {:?}", err)))?;
    unsafe {
        sqlite3ext_bind_text(
            stmt,
            idx,
            cstr.into_raw(),
            bytes.len() as i32,
            Some(cstring_destructor),
        );
    }
    Ok(())
}

unsafe extern "C" fn cstring_destructor(raw: *mut c_void) {
    drop(CString::from_raw(raw.cast::<c_char>()));
}

pub fn result_json_value(
    _db: *mut sqlite3,
    context: *mut sqlite3_context,
    raw: &str,
) -> Result<()> {
    if raw.trim().is_empty() {
        sqlite_loadable::api::result_null(context);
        return Ok(());
    }
    sqlite_loadable::api::result_text(context, raw)?;
    sqlite_loadable::api::result_subtype(context, b'J');
    Ok(())
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub fn now_ms_from_db(db: *mut sqlite3) -> i64 {
    let mut stmt = match sqlite_loadable::exec::Statement::prepare(
        db,
        "select value from absurd_settings where key = 'fake_now' limit 1",
    ) {
        Ok(stmt) => stmt,
        Err(_) => return now_ms(),
    };
    let mut rows = stmt.execute();
    let row = match rows.next() {
        Some(Ok(row)) => row,
        _ => return now_ms(),
    };

    if let Ok(value) = row.get::<i64>(0) {
        return value;
    }
    if let Ok(raw) = row.get::<String>(0) {
        if let Ok(parsed) = raw.trim().parse::<i64>() {
            return parsed;
        }
    }

    now_ms()
}
