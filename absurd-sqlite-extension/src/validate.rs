use sqlite_loadable::Result;

pub fn queue_name(queue_name: &str) -> Result<()> {
    if queue_name.trim().is_empty() {
        return Err(sqlite_loadable::Error::new_message(
            "queue_name must be provided",
        ));
    }
    if queue_name.len() + 2 > 50 {
        return Err(sqlite_loadable::Error::new_message(
            "queue_name is too long",
        ));
    }
    Ok(())
}

pub fn task_name(task_name: &str) -> Result<()> {
    if task_name.trim().is_empty() {
        return Err(sqlite_loadable::Error::new_message(
            "task_name must be provided",
        ));
    }
    Ok(())
}

pub fn step_name(step_name: &str) -> Result<()> {
    if step_name.trim().is_empty() {
        return Err(sqlite_loadable::Error::new_message(
            "step_name must be provided",
        ));
    }
    Ok(())
}

pub fn event_name(event_name: &str) -> Result<()> {
    if event_name.trim().is_empty() {
        return Err(sqlite_loadable::Error::new_message(
            "event_name must be provided",
        ));
    }
    Ok(())
}
