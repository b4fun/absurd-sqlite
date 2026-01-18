use sqlite3ext_sys::sqlite3;
use sqlite_loadable::prelude::*;
use sqlite_loadable::{
    api,
    table::{BestIndexError, IndexInfo, VTab, VTabArguments, VTabCursor},
    Error, Result,
};
use std::os::raw::c_int;

const LIST_QUEUES_SQL: &str = "CREATE TABLE x(queue_name TEXT, created_at INTEGER)";

struct QueueRow {
    queue_name: String,
    created_at: i64,
}

/// SQL: absurd_list_queues()
/// Usage: list queues with creation timestamps.
/// Section: Durable
#[repr(C)]
pub struct ListQueuesTable {
    base: sqlite3_vtab,
    db: *mut sqlite3,
}

impl<'vtab> VTab<'vtab> for ListQueuesTable {
    type Aux = ();
    type Cursor = ListQueuesCursor;

    fn connect(
        db: *mut sqlite3,
        _aux: Option<&Self::Aux>,
        _args: VTabArguments,
    ) -> Result<(String, ListQueuesTable)> {
        let base: sqlite3_vtab = unsafe { std::mem::zeroed() };
        let vtab = ListQueuesTable { base, db };
        Ok((LIST_QUEUES_SQL.to_owned(), vtab))
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

    fn open(&mut self) -> Result<ListQueuesCursor> {
        Ok(ListQueuesCursor::new(self.db))
    }
}

#[repr(C)]
pub struct ListQueuesCursor {
    base: sqlite3_vtab_cursor,
    db: *mut sqlite3,
    rowid: i64,
    rows: Vec<QueueRow>,
}

impl ListQueuesCursor {
    fn new(db: *mut sqlite3) -> ListQueuesCursor {
        let base: sqlite3_vtab_cursor = unsafe { std::mem::zeroed() };
        ListQueuesCursor {
            base,
            db,
            rowid: 0,
            rows: Vec::new(),
        }
    }
}

impl VTabCursor for ListQueuesCursor {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        _values: &[*mut sqlite3_value],
    ) -> Result<()> {
        let mut stmt = sqlite_loadable::exec::Statement::prepare(
            self.db,
            "select queue_name, created_at from absurd_queues order by queue_name",
        )
        .map_err(|err| Error::new_message(format!("failed to prepare queue list: {:?}", err)))?;
        let mut rows = Vec::new();
        for row in stmt.execute() {
            let row = row.map_err(|err| {
                Error::new_message(format!("failed to read queue row: {:?}", err))
            })?;
            let queue_name = row.get::<String>(0).map_err(|err| {
                Error::new_message(format!("failed to read queue_name: {:?}", err))
            })?;
            let created_at = row.get::<i64>(1).map_err(|err| {
                Error::new_message(format!("failed to read created_at: {:?}", err))
            })?;
            rows.push(QueueRow {
                queue_name,
                created_at,
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
            0 => api::result_text(context, &record.queue_name)?,
            1 => api::result_int64(context, record.created_at),
            _ => api::result_null(context),
        }
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.rowid)
    }
}
