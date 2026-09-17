use std::{error::Error, fmt, future::Future, mem};

use tokio::{
    sync::watch,
    task::{JoinError, JoinHandle},
    time::{Instant, timeout_at},
};

const ABORT_JOIN_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(100);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TaskCommand {
    Run,
    Stop,
}

#[derive(Clone)]
pub(crate) struct StopToken {
    state: watch::Receiver<TaskCommand>,
}

impl StopToken {
    pub(crate) fn is_stopping(&self) -> bool {
        *self.state.borrow() == TaskCommand::Stop
    }

    pub(crate) async fn cancelled(&mut self) {
        while !self.is_stopping() {
            if self.state.changed().await.is_err() {
                return;
            }
        }
    }
}

struct NamedTask {
    name: String,
    handle: Option<JoinHandle<()>>,
}

impl NamedTask {
    fn is_finished(&self) -> bool {
        self.handle.as_ref().is_none_or(JoinHandle::is_finished)
    }

    fn abort(&self) {
        if let Some(handle) = &self.handle
            && !handle.is_finished()
        {
            handle.abort();
        }
    }
}

impl Drop for NamedTask {
    fn drop(&mut self) {
        self.abort();
    }
}

pub(crate) struct TaskGroup {
    stop: watch::Sender<TaskCommand>,
    tasks: Vec<NamedTask>,
}

impl TaskGroup {
    pub(crate) fn new() -> Self {
        let (stop, _) = watch::channel(TaskCommand::Run);
        Self {
            stop,
            tasks: Vec::new(),
        }
    }

    pub(crate) fn spawn<F, Fut>(&mut self, name: impl Into<String>, run: F)
    where
        F: FnOnce(StopToken) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let stop = StopToken {
            state: self.stop.subscribe(),
        };
        self.tasks.push(NamedTask {
            name: name.into(),
            handle: Some(tokio::spawn(run(stop))),
        });
    }

    pub(crate) async fn stop_and_join(mut self, deadline: Instant) -> Result<(), TaskGroupError> {
        let _ = self.stop.send(TaskCommand::Stop);
        let mut tasks = mem::take(&mut self.tasks).into_iter();
        let mut failures = Vec::new();

        while let Some(mut task) = tasks.next() {
            match timeout_at(
                deadline,
                task.handle.as_mut().expect("task handle is present"),
            )
            .await
            {
                Ok(result) => {
                    task.handle.take();
                    record_join_failure(&mut failures, mem::take(&mut task.name), result);
                }
                Err(_) => {
                    let remaining = std::iter::once(task)
                        .chain(tasks)
                        .map(|task| {
                            let deadline_expired = !task.is_finished();
                            if deadline_expired {
                                task.abort();
                            }
                            (task, deadline_expired)
                        })
                        .collect::<Vec<_>>();

                    for (mut task, deadline_expired) in remaining {
                        if deadline_expired {
                            let result = tokio::time::timeout(
                                ABORT_JOIN_TIMEOUT,
                                task.handle.as_mut().expect("task handle is present"),
                            )
                            .await;
                            task.handle.take();
                            failures.push(TaskFailure {
                                task: mem::take(&mut task.name),
                                kind: if result.is_ok() {
                                    TaskFailureKind::StopDeadline
                                } else {
                                    TaskFailureKind::AbortDeadline
                                },
                            });
                        } else {
                            let result =
                                task.handle.as_mut().expect("task handle is present").await;
                            task.handle.take();
                            record_join_failure(&mut failures, mem::take(&mut task.name), result);
                        }
                    }
                    break;
                }
            }
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(TaskGroupError { failures })
        }
    }
}

impl Drop for TaskGroup {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

#[derive(Debug)]
pub(crate) struct TaskGroupError {
    failures: Vec<TaskFailure>,
}

impl fmt::Display for TaskGroupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} task failure(s)", self.failures.len())?;
        for failure in &self.failures {
            write!(
                formatter,
                "; {}: {}",
                failure.task,
                failure.kind.description()
            )?;
        }
        Ok(())
    }
}

impl Error for TaskGroupError {}

impl TaskGroupError {
    pub(crate) fn all_joined(&self) -> bool {
        self.failures
            .iter()
            .all(|failure| failure.kind != TaskFailureKind::AbortDeadline)
    }
}

#[derive(Debug)]
struct TaskFailure {
    task: String,
    kind: TaskFailureKind,
}

#[derive(Debug, Eq, PartialEq)]
enum TaskFailureKind {
    Panicked,
    JoinCancelled,
    StopDeadline,
    AbortDeadline,
}

impl TaskFailureKind {
    fn description(&self) -> &'static str {
        match self {
            Self::Panicked => "panicked",
            Self::JoinCancelled => "join cancelled",
            Self::StopDeadline => "stop deadline expired",
            Self::AbortDeadline => "did not stop after abort",
        }
    }
}

fn record_join_failure(
    failures: &mut Vec<TaskFailure>,
    task: String,
    result: Result<(), JoinError>,
) {
    if let Err(error) = result {
        failures.push(TaskFailure {
            task,
            kind: if error.is_panic() {
                TaskFailureKind::Panicked
            } else {
                TaskFailureKind::JoinCancelled
            },
        });
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use tokio::sync::{mpsc, oneshot};

    use super::*;

    struct DropSignal(Arc<AtomicBool>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[tokio::test]
    async fn cooperative_stop_joins_every_task() {
        let mut group = TaskGroup::new();
        let completed = Arc::new(AtomicUsize::new(0));
        let (started, mut starts) = mpsc::unbounded_channel();

        for name in ["first", "second"] {
            let completed = completed.clone();
            let started = started.clone();
            group.spawn(name, move |mut stop| async move {
                assert!(!stop.is_stopping());
                started.send(()).unwrap();
                stop.cancelled().await;
                assert!(stop.is_stopping());
                completed.fetch_add(1, Ordering::AcqRel);
            });
        }
        drop(started);

        starts.recv().await.unwrap();
        starts.recv().await.unwrap();

        group
            .stop_and_join(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();

        assert_eq!(completed.load(Ordering::Acquire), 2);
        assert!(starts.recv().await.is_none());
    }

    #[tokio::test]
    async fn stop_deadline_aborts_only_unfinished_tasks_and_joins_all() {
        let mut group = TaskGroup::new();
        let dropped = Arc::new(AtomicBool::new(false));
        let (stuck_started, stuck_ready) = oneshot::channel();
        let stuck_dropped = dropped.clone();
        group.spawn("stuck", move |_stop| async move {
            let _drop_signal = DropSignal(stuck_dropped);
            stuck_started.send(()).unwrap();
            future::pending::<()>().await;
        });

        let (cooperative_started, cooperative_ready) = oneshot::channel();
        group.spawn("cooperative", move |mut stop| async move {
            cooperative_started.send(()).unwrap();
            stop.cancelled().await;
        });

        let (panicked_started, panicked_ready) = oneshot::channel();
        group.spawn("panicked", move |_stop| async move {
            panicked_started.send(()).unwrap();
            panic!("task failure");
        });

        stuck_ready.await.unwrap();
        cooperative_ready.await.unwrap();
        let _ = panicked_ready.await;

        let error = tokio::time::timeout(
            Duration::from_secs(1),
            group.stop_and_join(Instant::now() + Duration::from_millis(25)),
        )
        .await
        .expect("task group stop must remain bounded")
        .expect_err("unfinished and panicked tasks must fail the join");

        for _ in 0..10 {
            if dropped.load(Ordering::Acquire) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(error.failures.len(), 2);
        assert_eq!(error.failures[0].task, "stuck");
        assert_eq!(error.failures[0].kind, TaskFailureKind::StopDeadline);
        assert_eq!(error.failures[1].task, "panicked");
        assert_eq!(error.failures[1].kind, TaskFailureKind::Panicked);
    }

    #[tokio::test]
    async fn cancelling_stop_and_join_aborts_owned_tasks() {
        let mut group = TaskGroup::new();
        let dropped = Arc::new(AtomicBool::new(false));
        let (started, ready) = oneshot::channel();
        let task_dropped = dropped.clone();
        group.spawn("stuck", move |_stop| async move {
            let _drop_signal = DropSignal(task_dropped);
            started.send(()).unwrap();
            future::pending::<()>().await;
        });
        ready.await.unwrap();

        let stopping = tokio::spawn(group.stop_and_join(Instant::now() + Duration::from_secs(60)));
        tokio::task::yield_now().await;
        stopping.abort();
        let _ = stopping.await;
        for _ in 0..10 {
            if dropped.load(Ordering::Acquire) {
                return;
            }
            tokio::task::yield_now().await;
        }
        assert!(dropped.load(Ordering::Acquire));
    }
}
