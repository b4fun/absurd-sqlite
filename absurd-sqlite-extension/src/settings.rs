use crate::sql;
use sqlite_loadable::prelude::*;
use sqlite_loadable::{api, Error, Result};

const SETTINGS_TABLE_SQL: &str =
    "create table if not exists absurd_settings (key text primary key, value text)";

fn parse_optional_int(value: Option<*mut sqlite3_value>) -> Result<Option<i64>> {
    let value = match value {
        Some(value) => value,
        None => return Ok(None),
    };
    if api::value_is_null(&value) {
        return Ok(None);
    }
    match api::value_type(&value) {
        api::ValueType::Integer => Ok(Some(api::value_int64(&value))),
        api::ValueType::Text => {
            let raw = api::value_text(&value)
                .map_err(|err| Error::new_message(format!("fake_now must be integer: {:?}", err)))?
                .trim();
            if raw.is_empty() {
                return Ok(None);
            }
            raw.parse::<i64>()
                .map(Some)
                .map_err(|err| Error::new_message(format!("fake_now must be integer: {:?}", err)))
        }
        _ => Err(Error::new_message("fake_now must be integer")),
    }
}

/// SQL: absurd_set_fake_now(fake_now_ms_or_null)
/// Usage: override or clear the engine's time source for testing.
/// Section: Meta
pub fn absurd_set_fake_now(
    context: *mut sqlite3_context,
    values: &[*mut sqlite3_value],
) -> Result<()> {
    let db = api::context_db_handle(context);
    let fake_now = parse_optional_int(values.first().copied())?;

    sql::exec_with_bind_text(db, SETTINGS_TABLE_SQL, &[])?;

    match fake_now {
        Some(value) => {
            let value_str = value.to_string();
            sql::exec_with_bind_text(
                db,
                "insert into absurd_settings (key, value)
                 values ('fake_now', ?1)
                     on conflict(key) do update set value = excluded.value",
                &[&value_str],
            )?;
        }
        None => {
            sql::exec_with_bind_text(
                db,
                "delete from absurd_settings where key = 'fake_now'",
                &[],
            )?;
        }
    }

    api::result_int64(context, 1);
    Ok(())
}
