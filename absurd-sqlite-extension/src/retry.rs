use serde_json::Value as JsonValue;
use sqlite_loadable::{Error, Result};

pub fn parse_retry_strategy(raw: &str, attempt: i64) -> Result<i64> {
    if raw.trim().is_empty() {
        return Ok(0);
    }
    let parsed: JsonValue =
        serde_json::from_str(raw).map_err(|err| Error::new_message(&format!("invalid retry_strategy JSON: {:?}", err)))?;
    let obj = match parsed.as_object() {
        Some(obj) => obj,
        None => return Ok(0),
    };
    let kind = obj
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or("none");
    let delay_seconds = match kind {
        "fixed" => obj
            .get("base_seconds")
            .and_then(|v| v.as_f64())
            .unwrap_or(60.0),
        "exponential" => {
            let base = obj
                .get("base_seconds")
                .and_then(|v| v.as_f64())
                .unwrap_or(30.0);
            let factor = obj
                .get("factor")
                .and_then(|v| v.as_f64())
                .unwrap_or(2.0);
            let mut delay = base * factor.powf((attempt.saturating_sub(1)) as f64);
            if let Some(max) = obj.get("max_seconds").and_then(|v| v.as_f64()) {
                if delay > max {
                    delay = max;
                }
            }
            delay
        }
        _ => 0.0,
    };
    let delay_ms = (delay_seconds * 1000.0).round() as i64;
    Ok(delay_ms.max(0))
}

pub fn parse_cancellation_max_duration(raw: &str) -> Result<Option<i64>> {
    if raw.trim().is_empty() {
        return Ok(None);
    }
    let parsed: JsonValue =
        serde_json::from_str(raw).map_err(|err| Error::new_message(&format!("invalid cancellation JSON: {:?}", err)))?;
    let obj = match parsed.as_object() {
        Some(obj) => obj,
        None => return Ok(None),
    };
    Ok(obj
        .get("max_duration")
        .and_then(|v| v.as_i64())
        .map(|v| v * 1000))
}
