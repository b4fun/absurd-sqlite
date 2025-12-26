use crate::migrations;
use crate::sql;
use sqlite3ext_sys::sqlite3;
use sqlite_loadable::prelude::*;
use sqlite_loadable::{
    api,
    table::{BestIndexError, IndexInfo, VTab, VTabArguments, VTabCursor},
    Error, Result,
};
use std::collections::HashSet;
use std::os::raw::c_int;

const MIGRATIONS_TABLE_SQL: &str = "create table if not exists absurd_migrations (id integer primary key, introduced_version text not null, applied_time integer not null)";
const MIGRATION_RECORDS_SQL: &str =
    "CREATE TABLE x(id INTEGER, introduced_version TEXT, applied_time INTEGER)";

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
                .map_err(|err| Error::new_message(&format!("to must be integer: {:?}", err)))?
                .trim();
            if raw.is_empty() {
                return Ok(None);
            }
            raw.parse::<i64>()
                .map(Some)
                .map_err(|err| Error::new_message(&format!("to must be integer: {:?}", err)))
        }
        _ => Err(Error::new_message("to must be integer")),
    }
}

fn ensure_migrations_table(db: *mut sqlite3) -> Result<()> {
    sql::exec_with_bind_text(db, MIGRATIONS_TABLE_SQL, &[])
}

pub fn absurd_apply_migrations(
    context: *mut sqlite3_context,
    values: &[*mut sqlite3_value],
) -> Result<()> {
    let target = parse_optional_int(values.get(0).copied())?;
    let db = api::context_db_handle(context);

    sql::exec_with_bind_text(db, "begin immediate", &[])?;

    let result = (|| -> Result<i64> {
        ensure_migrations_table(db)?;

        let mut applied_ids: Vec<i64> = Vec::new();
        let mut stmt = sqlite_loadable::exec::Statement::prepare(
            db,
            "select id from absurd_migrations order by id",
        )
        .map_err(|err| Error::new_message(&format!("failed to prepare migrations lookup: {:?}", err)))?;
        for row in stmt.execute() {
            let row = row.map_err(|err| Error::new_message(&format!("failed to read migration row: {:?}", err)))?;
            let id = row
                .get::<i64>(0)
                .map_err(|err| Error::new_message(&format!("failed to read migration id: {:?}", err)))?;
            applied_ids.push(id);
        }
        let max_applied = applied_ids.iter().max().cloned().unwrap_or(0);
        let applied_set: HashSet<i64> = applied_ids.into_iter().collect();

        let migrations_list = migrations::MIGRATIONS;
        let max_known = migrations_list.last().map(|m| m.id).unwrap_or(0);
        let target = target.unwrap_or(max_known);
        if target < max_applied {
            return Err(Error::new_message(
                "target version is older than applied migrations",
            ));
        }
        if target > max_known {
            return Err(Error::new_message(
                "target version is newer than available migrations",
            ));
        }

        let mut applied_count = 0;
        for migration in migrations_list.iter().filter(|m| m.id <= target) {
            if applied_set.contains(&migration.id) {
                continue;
            }
            sql::exec_batch(db, migration.sql)?;
            let now_value = sql::now_ms_from_db(db).to_string();
            let id_value = migration.id.to_string();
            sql::exec_with_bind_text(
                db,
                "insert into absurd_migrations (id, introduced_version, applied_time) values (?1, ?2, cast(?3 as integer))",
                &[&id_value, migration.introduced_version, &now_value],
            )?;
            applied_count += 1;
        }

        Ok(applied_count)
    })();

    match result {
        Ok(count) => {
            sql::exec_with_bind_text(db, "commit", &[])?;
            api::result_int64(context, count);
            Ok(())
        }
        Err(err) => {
            let _ = sql::exec_with_bind_text(db, "rollback", &[]);
            Err(err)
        }
    }
}

struct MigrationRecord {
    id: i64,
    introduced_version: String,
    applied_time: i64,
}

#[repr(C)]
pub struct MigrationRecordsTable {
    base: sqlite3_vtab,
    db: *mut sqlite3,
}

impl<'vtab> VTab<'vtab> for MigrationRecordsTable {
    type Aux = ();
    type Cursor = MigrationRecordsCursor;

    fn connect(
        db: *mut sqlite3,
        _aux: Option<&Self::Aux>,
        _args: VTabArguments,
    ) -> Result<(String, MigrationRecordsTable)> {
        let base: sqlite3_vtab = unsafe { std::mem::zeroed() };
        let vtab = MigrationRecordsTable { base, db };
        Ok((MIGRATION_RECORDS_SQL.to_owned(), vtab))
    }

    fn destroy(&self) -> Result<()> {
        Ok(())
    }

    fn best_index(&self, mut info: IndexInfo) -> core::result::Result<(), BestIndexError> {
        info.set_estimated_cost(10.0);
        info.set_estimated_rows(10);
        info.set_idxnum(1);
        Ok(())
    }

    fn open(&mut self) -> Result<MigrationRecordsCursor> {
        Ok(MigrationRecordsCursor::new(self.db))
    }
}

#[repr(C)]
pub struct MigrationRecordsCursor {
    base: sqlite3_vtab_cursor,
    db: *mut sqlite3,
    rowid: i64,
    rows: Vec<MigrationRecord>,
}

impl MigrationRecordsCursor {
    fn new(db: *mut sqlite3) -> MigrationRecordsCursor {
        let base: sqlite3_vtab_cursor = unsafe { std::mem::zeroed() };
        MigrationRecordsCursor {
            base,
            db,
            rowid: 0,
            rows: Vec::new(),
        }
    }
}

impl VTabCursor for MigrationRecordsCursor {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        _values: &[*mut sqlite3_value],
    ) -> Result<()> {
        ensure_migrations_table(self.db)?;

        let mut stmt = sqlite_loadable::exec::Statement::prepare(
            self.db,
            "select id, introduced_version, applied_time from absurd_migrations order by id",
        )
        .map_err(|err| Error::new_message(&format!("failed to prepare migration records: {:?}", err)))?;
        let mut rows = Vec::new();
        for row in stmt.execute() {
            let row = row.map_err(|err| Error::new_message(&format!("failed to read migration record: {:?}", err)))?;
            let id = row
                .get::<i64>(0)
                .map_err(|err| Error::new_message(&format!("failed to read migration id: {:?}", err)))?;
            let introduced_version = row
                .get::<String>(1)
                .map_err(|err| Error::new_message(&format!("failed to read introduced_version: {:?}", err)))?;
            let applied_time = row
                .get::<i64>(2)
                .map_err(|err| Error::new_message(&format!("failed to read applied_time: {:?}", err)))?;
            rows.push(MigrationRecord {
                id,
                introduced_version,
                applied_time,
            });
        }
        self.rows = rows;
        self.rowid = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.rowid += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.rowid as usize >= self.rows.len()
    }

    fn column(&self, context: *mut sqlite3_context, i: c_int) -> Result<()> {
        let record = match self.rows.get(self.rowid as usize) {
            Some(record) => record,
            None => {
                api::result_null(context);
                return Ok(());
            }
        };
        match i {
            0 => api::result_int64(context, record.id),
            1 => api::result_text(context, &record.introduced_version)?,
            2 => api::result_int64(context, record.applied_time),
            _ => api::result_null(context),
        }
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.rowid)
    }
}
