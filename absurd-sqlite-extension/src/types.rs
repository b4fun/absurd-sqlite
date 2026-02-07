//! Public types for the Absurd SQLite extension.
//!
//! This module defines the core types that can be used by consumers of the library,
//! including task and run state enums.

use std::fmt;
use std::str::FromStr;

/// Represents the state of a task in the Absurd system.
///
/// Tasks transition through various states during their lifecycle:
/// - `Pending`: Task is waiting to be executed
/// - `Running`: Task is currently being executed
/// - `Sleeping`: Task is suspended, waiting for an event or timeout
/// - `Completed`: Task has successfully completed
/// - `Failed`: Task execution failed
/// - `Cancelled`: Task was cancelled
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskState {
    Pending,
    Running,
    Sleeping,
    Completed,
    Failed,
    Cancelled,
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            TaskState::Pending => "pending",
            TaskState::Running => "running",
            TaskState::Sleeping => "sleeping",
            TaskState::Completed => "completed",
            TaskState::Failed => "failed",
            TaskState::Cancelled => "cancelled",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for TaskState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(TaskState::Pending),
            "running" => Ok(TaskState::Running),
            "sleeping" => Ok(TaskState::Sleeping),
            "completed" => Ok(TaskState::Completed),
            "failed" => Ok(TaskState::Failed),
            "cancelled" => Ok(TaskState::Cancelled),
            _ => Err(format!("Invalid task state: {}", s)),
        }
    }
}

/// Represents the state of a run (task attempt) in the Absurd system.
///
/// Runs transition through various states during their execution:
/// - `Pending`: Run is waiting to be claimed and executed
/// - `Running`: Run is currently being executed
/// - `Sleeping`: Run is suspended, waiting for an event or timeout
/// - `Completed`: Run has successfully completed
/// - `Failed`: Run execution failed
/// - `Cancelled`: Run was cancelled
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RunState {
    Pending,
    Running,
    Sleeping,
    Completed,
    Failed,
    Cancelled,
}

impl fmt::Display for RunState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            RunState::Pending => "pending",
            RunState::Running => "running",
            RunState::Sleeping => "sleeping",
            RunState::Completed => "completed",
            RunState::Failed => "failed",
            RunState::Cancelled => "cancelled",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for RunState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(RunState::Pending),
            "running" => Ok(RunState::Running),
            "sleeping" => Ok(RunState::Sleeping),
            "completed" => Ok(RunState::Completed),
            "failed" => Ok(RunState::Failed),
            "cancelled" => Ok(RunState::Cancelled),
            _ => Err(format!("Invalid run state: {}", s)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_state_display() {
        assert_eq!(TaskState::Pending.to_string(), "pending");
        assert_eq!(TaskState::Running.to_string(), "running");
        assert_eq!(TaskState::Sleeping.to_string(), "sleeping");
        assert_eq!(TaskState::Completed.to_string(), "completed");
        assert_eq!(TaskState::Failed.to_string(), "failed");
        assert_eq!(TaskState::Cancelled.to_string(), "cancelled");
    }

    #[test]
    fn test_task_state_from_str() {
        assert_eq!("pending".parse::<TaskState>().unwrap(), TaskState::Pending);
        assert_eq!("running".parse::<TaskState>().unwrap(), TaskState::Running);
        assert_eq!(
            "sleeping".parse::<TaskState>().unwrap(),
            TaskState::Sleeping
        );
        assert_eq!(
            "completed".parse::<TaskState>().unwrap(),
            TaskState::Completed
        );
        assert_eq!("failed".parse::<TaskState>().unwrap(), TaskState::Failed);
        assert_eq!(
            "cancelled".parse::<TaskState>().unwrap(),
            TaskState::Cancelled
        );
        assert!("invalid".parse::<TaskState>().is_err());
    }

    #[test]
    fn test_run_state_display() {
        assert_eq!(RunState::Pending.to_string(), "pending");
        assert_eq!(RunState::Running.to_string(), "running");
        assert_eq!(RunState::Sleeping.to_string(), "sleeping");
        assert_eq!(RunState::Completed.to_string(), "completed");
        assert_eq!(RunState::Failed.to_string(), "failed");
        assert_eq!(RunState::Cancelled.to_string(), "cancelled");
    }

    #[test]
    fn test_run_state_from_str() {
        assert_eq!("pending".parse::<RunState>().unwrap(), RunState::Pending);
        assert_eq!("running".parse::<RunState>().unwrap(), RunState::Running);
        assert_eq!("sleeping".parse::<RunState>().unwrap(), RunState::Sleeping);
        assert_eq!(
            "completed".parse::<RunState>().unwrap(),
            RunState::Completed
        );
        assert_eq!("failed".parse::<RunState>().unwrap(), RunState::Failed);
        assert_eq!(
            "cancelled".parse::<RunState>().unwrap(),
            RunState::Cancelled
        );
        assert!("invalid".parse::<RunState>().is_err());
    }

    #[test]
    fn test_task_state_equality() {
        assert_eq!(TaskState::Pending, TaskState::Pending);
        assert_ne!(TaskState::Pending, TaskState::Running);
    }

    #[test]
    fn test_run_state_equality() {
        assert_eq!(RunState::Pending, RunState::Pending);
        assert_ne!(RunState::Pending, RunState::Running);
    }
}
