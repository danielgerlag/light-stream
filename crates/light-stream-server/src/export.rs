use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    future::Future,
    io::{Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
    thread::JoinHandle as ThreadJoinHandle,
};

use light_stream_core::{AmbiguousRequest, ArtifactIdentity, DomainError, ExportId};
use light_stream_export::{ExportLimits, ExportWriteError, VerifyError};
use light_stream_storage::{
    ApplyResult, CommittedStateReader, LogicalExportCancellation, LogicalExportError,
    PreparedLogicalExportV1,
};
use tokio::{
    sync::Mutex,
    task::JoinHandle,
    time::{Instant, timeout_at},
};

#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};

pub(crate) struct ExportCoordinator {
    directory: PathBuf,
    limits: ExportLimits,
    materialization: Mutex<Option<OwnedMaterialization>>,
    proposal: Mutex<Option<OwnedProposal>>,
    stopping: AtomicBool,
    release_cursor: Mutex<(Option<ExportId>, usize)>,
}

struct OwnedMaterialization {
    export: ExportId,
    cancellation: LogicalExportCancellation,
    handle: ThreadJoinHandle<Result<ArtifactIdentity, MaterializationError>>,
}

struct OwnedProposal {
    request: Option<AmbiguousRequest>,
    handle: JoinHandle<Result<ApplyResult, DomainError>>,
}

pub(crate) enum ProposalPoll {
    Idle,
    Pending(Option<AmbiguousRequest>),
    Resolved(Box<Result<ApplyResult, DomainError>>),
}

pub(crate) enum ProposalStart {
    Started,
    Busy(Option<AmbiguousRequest>),
}

#[derive(Debug)]
pub(crate) enum ReadyArtifactError {
    Missing,
    Retryable(String),
    Deterministic(String),
}

#[derive(Debug)]
pub(crate) enum MaterializationError {
    Cancelled,
    Limit(String),
    Retryable(String),
    Deterministic(String),
}

#[derive(Debug)]
pub(crate) enum CoordinatorShutdownError {
    Materialization(String),
    Proposal(String),
    Deadline,
}

impl CoordinatorShutdownError {
    pub(crate) const fn teardown_safe(&self) -> bool {
        !matches!(self, Self::Deadline)
    }
}

impl std::fmt::Display for CoordinatorShutdownError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Materialization(reason) => {
                write!(
                    formatter,
                    "export materialization shutdown failed: {reason}"
                )
            }
            Self::Proposal(reason) => {
                write!(formatter, "export proposal shutdown failed: {reason}")
            }
            Self::Deadline => formatter.write_str("export work exceeded the shutdown deadline"),
        }
    }
}

impl std::error::Error for CoordinatorShutdownError {}

impl ExportCoordinator {
    pub(crate) fn open(data_dir: &Path, limits: ExportLimits) -> Result<Self, DomainError> {
        let directory = data_dir.join("exports");
        prepare_directory(&directory)?;
        cleanup_building_files(&directory)?;
        Ok(Self {
            directory,
            limits,
            materialization: Mutex::new(None),
            proposal: Mutex::new(None),
            stopping: AtomicBool::new(false),
            release_cursor: Mutex::new((None, 0)),
        })
    }

    #[cfg(test)]
    pub(crate) fn open_ready(
        &self,
        export: ExportId,
        expected: ArtifactIdentity,
    ) -> Result<File, ReadyArtifactError> {
        let (mut file, actual) = self.verified_ready(export)?;
        if actual != expected {
            return Err(ReadyArtifactError::Deterministic(
                "local export artifact identity does not match committed identity".to_owned(),
            ));
        }
        file.seek(SeekFrom::Start(0))
            .map_err(|error| ReadyArtifactError::Retryable(error.to_string()))?;
        Ok(file)
    }

    pub(crate) fn verified_ready(
        &self,
        export: ExportId,
    ) -> Result<(File, ArtifactIdentity), ReadyArtifactError> {
        let path = self.ready_path(export);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(ReadyArtifactError::Missing);
            }
            Err(error) => return Err(ReadyArtifactError::Retryable(error.to_string())),
        };
        if metadata.file_type().is_symlink() {
            return Err(ReadyArtifactError::Deterministic(format!(
                "export artifact {} is a symlink",
                path.display()
            )));
        }
        let file = open_existing_nofollow(&path)
            .map_err(|error| ReadyArtifactError::Retryable(error.to_string()))?;
        let opened_metadata = file
            .metadata()
            .map_err(|error| ReadyArtifactError::Retryable(error.to_string()))?;
        validate_regular_private_file(&path, &opened_metadata)
            .map_err(ReadyArtifactError::Deterministic)?;
        let verified =
            light_stream_export::verify(file, &self.limits).map_err(|error| match error {
                VerifyError::Io(error) => ReadyArtifactError::Retryable(error.to_string()),
                other => ReadyArtifactError::Deterministic(other.to_string()),
            })?;
        let artifact = verified.artifact();
        let mut file = verified.into_reader();
        file.seek(SeekFrom::Start(0))
            .map_err(|error| ReadyArtifactError::Retryable(error.to_string()))?;
        Ok((file, artifact))
    }

    pub(crate) fn recover_ready(
        &self,
        export: ExportId,
        expected: Option<ArtifactIdentity>,
    ) -> Result<Option<ArtifactIdentity>, ReadyArtifactError> {
        let path = self.ready_path(export);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(ReadyArtifactError::Retryable(error.to_string())),
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(ReadyArtifactError::Deterministic(format!(
                "export artifact {} is not a regular file",
                path.display()
            )));
        }
        match self.verified_ready(export) {
            Ok((_, artifact)) if expected.is_none_or(|expected| expected == artifact) => {
                Ok(Some(artifact))
            }
            Ok(_) | Err(ReadyArtifactError::Deterministic(_)) => {
                self.remove_regular_ready(&path)?;
                Ok(None)
            }
            Err(ReadyArtifactError::Missing) => Ok(None),
            Err(ReadyArtifactError::Retryable(reason)) => {
                Err(ReadyArtifactError::Retryable(reason))
            }
        }
    }

    pub(crate) async fn poll_materialization(
        &self,
        export: ExportId,
        control: CommittedStateReader,
        data: Vec<CommittedStateReader>,
    ) -> Result<Option<ArtifactIdentity>, MaterializationError> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(MaterializationError::Cancelled);
        }
        loop {
            let stale = {
                let mut owned = self.materialization.lock().await;
                if let Some(task) = owned.as_mut() {
                    if task.export != export {
                        task.cancellation.cancel();
                        owned.take()
                    } else if !task.handle.is_finished() {
                        return Ok(None);
                    } else {
                        let task = owned.take().expect("materialization task is present");
                        return join_materialization(task)?.map(Some);
                    }
                } else {
                    None
                }
            };
            if let Some(task) = stale {
                reap_cancelled_materialization(task).await?;
                continue;
            }
            let mut owned = self.materialization.lock().await;
            if owned.is_none() {
                let directory = self.directory.clone();
                let limits = self.limits;
                let cancellation = LogicalExportCancellation::new();
                let task_cancellation = cancellation.clone();
                let handle = std::thread::Builder::new()
                    .name(format!("light-stream-export-{export}"))
                    .spawn(move || {
                        let prepared = control
                            .prepare_logical_export_v1_cancellable(
                                &data,
                                limits,
                                task_cancellation.clone(),
                            )
                            .map_err(classify_source_error)?;
                        materialize_sync(&directory, export, prepared, &task_cancellation)
                    })
                    .map_err(|error| MaterializationError::Retryable(error.to_string()))?;
                *owned = Some(OwnedMaterialization {
                    export,
                    cancellation,
                    handle,
                });
                return Ok(None);
            }
        }
    }

    pub(crate) async fn cancel_materialization(&self) {
        if let Some(task) = self.materialization.lock().await.as_ref() {
            task.cancellation.cancel();
        }
    }

    pub(crate) async fn cancel_and_join_materialization(&self) -> Result<(), MaterializationError> {
        let task = {
            let mut owned = self.materialization.lock().await;
            if let Some(task) = owned.as_ref() {
                task.cancellation.cancel();
            }
            owned.take()
        };
        match task {
            Some(task) => reap_cancelled_materialization(task).await,
            None => Ok(()),
        }
    }

    pub(crate) async fn owns_materialization(&self, export: ExportId) -> bool {
        self.materialization
            .lock()
            .await
            .as_ref()
            .is_some_and(|task| task.export == export)
    }

    pub(crate) async fn start_proposal<F>(
        &self,
        request: Option<AmbiguousRequest>,
        proposal: F,
    ) -> Result<ProposalStart, DomainError>
    where
        F: Future<Output = Result<ApplyResult, DomainError>> + Send + 'static,
    {
        if self.stopping.load(Ordering::Acquire) {
            return Err(shutting_down());
        }
        let mut owned = self.proposal.lock().await;
        if self.stopping.load(Ordering::Acquire) {
            return Err(shutting_down());
        }
        if let Some(proposal) = owned.as_ref() {
            return Ok(ProposalStart::Busy(proposal.request.clone()));
        }
        *owned = Some(OwnedProposal {
            request,
            handle: tokio::spawn(proposal),
        });
        Ok(ProposalStart::Started)
    }

    pub(crate) async fn poll_proposal(&self) -> Result<ProposalPoll, DomainError> {
        let mut owned = self.proposal.lock().await;
        let Some(proposal) = owned.as_mut() else {
            return Ok(ProposalPoll::Idle);
        };
        if !proposal.handle.is_finished() {
            return Ok(ProposalPoll::Pending(proposal.request.clone()));
        }
        let proposal = owned.take().expect("proposal task is present");
        resolve_proposal(proposal)
            .await
            .map(|result| ProposalPoll::Resolved(Box::new(result)))
    }

    pub(crate) async fn wait_proposal_until(
        &self,
        deadline: Instant,
    ) -> Result<ProposalPoll, DomainError> {
        let mut owned = self.proposal.lock().await;
        let Some(proposal) = owned.as_mut() else {
            return Ok(ProposalPoll::Idle);
        };
        let joined = match timeout_at(deadline, &mut proposal.handle).await {
            Ok(joined) => joined,
            Err(_) => return Ok(ProposalPoll::Pending(proposal.request.clone())),
        };
        let _ = owned.take().expect("proposal task is present");
        match joined {
            Ok(result) => Ok(ProposalPoll::Resolved(Box::new(result))),
            Err(error) => Err(storage_error(format!(
                "export proposal task failed: {error}"
            ))),
        }
    }

    pub(crate) async fn stop_and_join(
        &self,
        deadline: Instant,
    ) -> Result<(), CoordinatorShutdownError> {
        self.stopping.store(true, Ordering::Release);
        let materialization = {
            let mut owned = self.materialization.lock().await;
            if let Some(task) = owned.as_ref() {
                task.cancellation.cancel();
            }
            owned.take()
        };
        let materialization_result = match materialization {
            Some(task) => join_materialization_for_shutdown(task, deadline).await,
            None => Ok(()),
        };
        let proposal = self.proposal.lock().await.take();
        let proposal_result = match proposal {
            Some(proposal) => join_proposal_for_shutdown(proposal, deadline).await,
            None => Ok(()),
        };
        materialization_result?;
        proposal_result
    }

    pub(crate) fn cleanup_spool_files(&self, keep: Option<ExportId>) -> Result<(), DomainError> {
        let keep = keep.map(|export| self.ready_path(export));
        for entry in fs::read_dir(&self.directory).map_err(|error| {
            storage_error(format!(
                "failed to scan export spool {}: {error}",
                self.directory.display()
            ))
        })? {
            let entry = entry.map_err(|error| {
                storage_error(format!(
                    "failed to read export spool {}: {error}",
                    self.directory.display()
                ))
            })?;
            let path = entry.path();
            let extension = path.extension();
            let is_ready = extension.is_some_and(|value| value == "ready");
            let is_building = extension.is_some_and(|value| value == "building");
            if (!is_ready && !is_building) || (is_ready && keep.as_ref() == Some(&path)) {
                continue;
            }
            let metadata = fs::symlink_metadata(&path).map_err(|error| {
                storage_error(format!(
                    "failed to inspect export spool entry {}: {error}",
                    path.display()
                ))
            })?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                continue;
            }
            fs::remove_file(&path).map_err(|error| {
                storage_error(format!(
                    "failed to remove stale export artifact {}: {error}",
                    path.display()
                ))
            })?;
        }
        sync_directory(&self.directory).map_err(|error| {
            storage_error(format!(
                "failed to sync export spool {}: {error}",
                self.directory.display()
            ))
        })
    }

    pub(crate) fn remove_ready(&self, export: ExportId) -> Result<(), DomainError> {
        let path = self.ready_path(export);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                fs::remove_file(&path).map_err(|error| {
                    storage_error(format!(
                        "failed to remove terminal export artifact {}: {error}",
                        path.display()
                    ))
                })?;
                sync_directory(&self.directory).map_err(|error| {
                    storage_error(format!(
                        "failed to sync export spool {}: {error}",
                        self.directory.display()
                    ))
                })
            }
            Ok(_) => Err(storage_error(format!(
                "export artifact {} is not a regular file",
                path.display()
            ))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(storage_error(format!(
                "failed to inspect terminal export artifact {}: {error}",
                path.display()
            ))),
        }
    }

    pub(crate) async fn next_release_candidate(
        &self,
        export: ExportId,
        groups: &BTreeSet<light_stream_core::GroupId>,
    ) -> Option<light_stream_core::GroupId> {
        if groups.is_empty() {
            return None;
        }
        let mut cursor = self.release_cursor.lock().await;
        if cursor.0 != Some(export) {
            *cursor = (Some(export), 0);
        }
        let candidate = groups.iter().copied().nth(cursor.1 % groups.len());
        cursor.1 = (cursor.1 + 1) % groups.len();
        candidate
    }

    pub(crate) fn ready_path(&self, export: ExportId) -> PathBuf {
        ready_path(&self.directory, export)
    }

    fn remove_regular_ready(&self, path: &Path) -> Result<(), ReadyArtifactError> {
        fs::remove_file(path).map_err(|error| ReadyArtifactError::Retryable(error.to_string()))?;
        sync_directory(&self.directory)
            .map_err(|error| ReadyArtifactError::Retryable(error.to_string()))
    }

    #[cfg(test)]
    pub(crate) async fn start_test_materialization(
        &self,
        export: ExportId,
        exited: std::sync::Arc<AtomicBool>,
    ) -> Result<(), MaterializationError> {
        let mut owned = self.materialization.lock().await;
        if owned.is_some() {
            return Err(MaterializationError::Deterministic(
                "materialization is already active".to_owned(),
            ));
        }
        let cancellation = LogicalExportCancellation::new();
        let task_cancellation = cancellation.clone();
        let handle = std::thread::spawn(move || {
            while !task_cancellation.is_cancelled() {
                std::thread::yield_now();
            }
            exited.store(true, Ordering::Release);
            Err(MaterializationError::Cancelled)
        });
        *owned = Some(OwnedMaterialization {
            export,
            cancellation,
            handle,
        });
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn materialization_is_running(&self) -> bool {
        self.materialization.lock().await.is_some()
    }
}

async fn reap_cancelled_materialization(
    task: OwnedMaterialization,
) -> Result<(), MaterializationError> {
    match join_materialization(task)? {
        Ok(_) | Err(MaterializationError::Cancelled) => Ok(()),
        Err(error) => Err(error),
    }
}

fn join_materialization(
    task: OwnedMaterialization,
) -> Result<Result<ArtifactIdentity, MaterializationError>, MaterializationError> {
    task.handle
        .join()
        .map_err(|_| MaterializationError::Retryable("materialization thread panicked".to_owned()))
}

async fn resolve_proposal(
    proposal: OwnedProposal,
) -> Result<Result<ApplyResult, DomainError>, DomainError> {
    proposal
        .handle
        .await
        .map_err(|error| storage_error(format!("export proposal task failed: {error}")))
}

async fn join_materialization_for_shutdown(
    task: OwnedMaterialization,
    deadline: Instant,
) -> Result<(), CoordinatorShutdownError> {
    while !task.handle.is_finished() {
        if Instant::now() >= deadline {
            return Err(CoordinatorShutdownError::Deadline);
        }
        tokio::task::yield_now().await;
    }
    match join_materialization(task)
        .map_err(|error| CoordinatorShutdownError::Materialization(format!("{error:?}")))?
    {
        Ok(_) | Err(MaterializationError::Cancelled) => Ok(()),
        Err(error) => Err(CoordinatorShutdownError::Materialization(format!(
            "{error:?}"
        ))),
    }
}

async fn join_proposal_for_shutdown(
    mut proposal: OwnedProposal,
    deadline: Instant,
) -> Result<(), CoordinatorShutdownError> {
    match timeout_at(deadline, &mut proposal.handle).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => Err(CoordinatorShutdownError::Proposal(error.to_string())),
        Err(_) => Err(CoordinatorShutdownError::Deadline),
    }
}

fn shutting_down() -> DomainError {
    DomainError::ShuttingDown {
        outcome: light_stream_core::RequestOutcome::DefiniteNoCommit,
    }
}

fn prepare_directory(directory: &Path) -> Result<(), DomainError> {
    match fs::symlink_metadata(directory) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(storage_error(format!(
                    "export spool {} is not a real directory",
                    directory.display()
                )));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut builder = fs::DirBuilder::new();
            builder.recursive(false);
            #[cfg(unix)]
            builder.mode(0o700);
            builder.create(directory).map_err(|error| {
                storage_error(format!(
                    "failed to create export spool {}: {error}",
                    directory.display()
                ))
            })?;
        }
        Err(error) => {
            return Err(storage_error(format!(
                "failed to inspect export spool {}: {error}",
                directory.display()
            )));
        }
    }
    Ok(())
}

fn cleanup_building_files(directory: &Path) -> Result<(), DomainError> {
    let entries = fs::read_dir(directory).map_err(|error| {
        storage_error(format!(
            "failed to scan export spool {}: {error}",
            directory.display()
        ))
    })?;
    for entry in entries {
        let entry = entry.map_err(|error| {
            storage_error(format!(
                "failed to read export spool {}: {error}",
                directory.display()
            ))
        })?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(|error| {
            storage_error(format!(
                "failed to inspect export spool entry {}: {error}",
                path.display()
            ))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(storage_error(format!(
                "export spool entry {} is a symlink",
                path.display()
            )));
        }
        if path
            .extension()
            .is_some_and(|extension| extension == "building")
        {
            if !metadata.is_file() {
                return Err(storage_error(format!(
                    "export building path {} is not a regular file",
                    path.display()
                )));
            }
            fs::remove_file(&path).map_err(|error| {
                storage_error(format!(
                    "failed to remove incomplete export {}: {error}",
                    path.display()
                ))
            })?;
        } else if path
            .extension()
            .is_some_and(|extension| extension == "ready")
            && !metadata.is_file()
        {
            return Err(storage_error(format!(
                "export artifact {} is not a regular file",
                path.display()
            )));
        }
    }
    sync_directory(directory).map_err(|error| {
        storage_error(format!(
            "failed to sync export spool {}: {error}",
            directory.display()
        ))
    })
}

fn materialize_sync(
    directory: &Path,
    export: ExportId,
    mut prepared: PreparedLogicalExportV1,
    cancellation: &LogicalExportCancellation,
) -> Result<ArtifactIdentity, MaterializationError> {
    let building = building_path(directory, export);
    let ready = ready_path(directory, export);
    prepare_building_path(&building)?;
    refuse_existing_path(&ready)?;

    let result = (|| {
        let mut file = create_private_file(&building)
            .map_err(|error| MaterializationError::Retryable(error.to_string()))?;
        let document = prepared.document();
        let limits = prepared.limits();
        let identity =
            light_stream_export::write_v1(&mut file, &document, prepared.source_mut(), &limits)
                .map_err(classify_write_error)?;
        if cancellation.is_cancelled() {
            return Err(MaterializationError::Cancelled);
        }
        file.sync_all()
            .map_err(|error| MaterializationError::Retryable(error.to_string()))?;
        file.seek(SeekFrom::Start(0))
            .map_err(|error| MaterializationError::Retryable(error.to_string()))?;
        let verified =
            light_stream_export::verify_cancellable(file, &limits, || cancellation.is_cancelled())
                .map_err(classify_verify_error)?;
        if verified.artifact() != identity {
            return Err(MaterializationError::Deterministic(
                "written export identity changed during verification".to_owned(),
            ));
        }
        drop(verified);
        if cancellation.is_cancelled() {
            return Err(MaterializationError::Cancelled);
        }
        fs::rename(&building, &ready)
            .map_err(|error| MaterializationError::Retryable(error.to_string()))?;
        sync_directory(directory)
            .map_err(|error| MaterializationError::Retryable(error.to_string()))?;
        Ok(identity)
    })();

    if result.is_err() {
        remove_regular_file_if_present(&building);
    }
    result
}

fn classify_source_error(error: LogicalExportError) -> MaterializationError {
    match error {
        LogicalExportError::Cancelled => MaterializationError::Cancelled,
        LogicalExportError::Limit { .. } => MaterializationError::Limit(error.to_string()),
        LogicalExportError::Storage { .. } => MaterializationError::Retryable(error.to_string()),
        other => MaterializationError::Deterministic(other.to_string()),
    }
}

fn classify_write_error(error: ExportWriteError<LogicalExportError>) -> MaterializationError {
    match error {
        ExportWriteError::Io(error) => MaterializationError::Retryable(error.to_string()),
        ExportWriteError::Source(error) => classify_source_error(error),
        ExportWriteError::Limit { .. } => MaterializationError::Limit(error.to_string()),
        ExportWriteError::NonCanonical { .. } | ExportWriteError::Inconsistent { .. } => {
            MaterializationError::Deterministic(error.to_string())
        }
    }
}

fn classify_verify_error(error: VerifyError) -> MaterializationError {
    match error {
        VerifyError::Cancelled => MaterializationError::Cancelled,
        VerifyError::Io(error) => MaterializationError::Retryable(error.to_string()),
        VerifyError::Limit { .. } => MaterializationError::Limit(error.to_string()),
        other => MaterializationError::Deterministic(other.to_string()),
    }
}

fn prepare_building_path(path: &Path) -> Result<(), MaterializationError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            fs::remove_file(path)
                .map_err(|error| MaterializationError::Retryable(error.to_string()))
        }
        Ok(_) => Err(MaterializationError::Deterministic(format!(
            "export path {} is not a regular file",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(MaterializationError::Retryable(error.to_string())),
    }
}

fn refuse_existing_path(path: &Path) -> Result<(), MaterializationError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(MaterializationError::Deterministic(format!(
            "export path {} already exists",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(MaterializationError::Retryable(error.to_string())),
    }
}

fn building_path(directory: &Path, export: ExportId) -> PathBuf {
    directory.join(format!("{export}.building"))
}

fn ready_path(directory: &Path, export: ExportId) -> PathBuf {
    directory.join(format!("{export}.ready"))
}

fn validate_regular_private_file(path: &Path, metadata: &fs::Metadata) -> Result<(), String> {
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!(
            "export artifact {} is not a regular file",
            path.display()
        ));
    }
    #[cfg(unix)]
    if metadata.permissions().mode() & 0o777 != 0o600 {
        return Err(format!(
            "export artifact {} does not have mode 0600",
            path.display()
        ));
    }
    Ok(())
}

fn create_private_file(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    let file = options.open(path)?;
    #[cfg(unix)]
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    Ok(file)
}

fn open_existing_nofollow(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW);
    options.open(path)
}

fn remove_regular_file_if_present(path: &Path) {
    if fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_file()) {
        let _ = fs::remove_file(path);
    }
}

fn sync_directory(directory: &Path) -> std::io::Result<()> {
    File::open(directory)?.sync_all()
}

fn storage_error(reason: String) -> DomainError {
    DomainError::Storage { reason }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering as AtomicOrdering},
        },
        time::Duration,
    };

    use super::*;
    use light_stream_core::AmbiguousRequest;
    use light_stream_storage::ApplyResult;
    use tokio::sync::oneshot;
    use uuid::Uuid;

    fn root(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/test-data/light-stream-server")
            .join(name)
    }

    fn export_id() -> ExportId {
        ExportId::from_uuid(Uuid::from_u128(1))
    }

    #[test]
    fn startup_cleans_building_files_without_removing_ready_files() {
        let root = root("export-spool-cleanup");
        let _ = fs::remove_dir_all(&root);
        let exports = root.join("exports");
        fs::create_dir_all(&exports).unwrap();
        fs::write(
            exports.join(format!("{}.building", export_id())),
            b"partial",
        )
        .unwrap();
        let ready = exports.join(format!("{}.ready", export_id()));
        fs::write(&ready, b"ready").unwrap();
        #[cfg(unix)]
        fs::set_permissions(&ready, fs::Permissions::from_mode(0o600)).unwrap();

        let coordinator = ExportCoordinator::open(&root, ExportLimits::default()).unwrap();

        assert!(!exports.join(format!("{}.building", export_id())).exists());
        assert!(coordinator.ready_path(export_id()).exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn invalid_regular_ready_file_is_removed_for_rebuild() {
        let root = root("export-invalid-ready-rebuild");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let coordinator = ExportCoordinator::open(&root, ExportLimits::default()).unwrap();
        let ready = coordinator.ready_path(export_id());
        fs::write(&ready, b"invalid").unwrap();
        #[cfg(unix)]
        fs::set_permissions(&ready, fs::Permissions::from_mode(0o600)).unwrap();

        assert!(matches!(
            coordinator.recover_ready(export_id(), None),
            Ok(None)
        ));
        assert!(!ready.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn startup_cleanup_removes_only_unreferenced_ready_files() {
        let root = root("export-ready-startup-cleanup");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let coordinator = ExportCoordinator::open(&root, ExportLimits::default()).unwrap();
        let active = export_id();
        let stale = ExportId::from_uuid(Uuid::from_u128(2));
        for export in [active, stale] {
            let ready = coordinator.ready_path(export);
            fs::write(&ready, b"ready").unwrap();
            #[cfg(unix)]
            fs::set_permissions(&ready, fs::Permissions::from_mode(0o600)).unwrap();
        }

        coordinator.cleanup_spool_files(Some(active)).unwrap();

        assert!(coordinator.ready_path(active).exists());
        assert!(!coordinator.ready_path(stale).exists());
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn stopping_coordinator_cancels_and_joins_owned_materialization() {
        let root = root("export-owned-materialization-stop");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let coordinator = ExportCoordinator::open(&root, ExportLimits::default()).unwrap();
        let exited = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        coordinator
            .start_test_materialization(export_id(), exited.clone())
            .await
            .unwrap();

        coordinator
            .stop_and_join(tokio::time::Instant::now() + std::time::Duration::from_secs(1))
            .await
            .unwrap();

        assert!(exited.load(std::sync::atomic::Ordering::Acquire));
        assert!(!coordinator.materialization_is_running().await);
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn coordinator_never_overlaps_materialization_or_proposals() {
        let root = root("export-no-overlap");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let coordinator = ExportCoordinator::open(&root, ExportLimits::default()).unwrap();
        let exited = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        coordinator
            .start_test_materialization(export_id(), exited)
            .await
            .unwrap();

        assert!(
            coordinator
                .start_test_materialization(
                    ExportId::from_uuid(Uuid::from_u128(2)),
                    std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                )
                .await
                .is_err()
        );

        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let (release, blocked) = oneshot::channel();
        let running = active.clone();
        let observed_maximum = maximum.clone();
        let request = light_stream_core::MutationRequestId::new(
            light_stream_core::PrincipalId::parse("export-operator").unwrap(),
            light_stream_core::MutationSessionId::from_uuid(Uuid::from_u128(2)),
            light_stream_core::RequestSequence::new(1),
        );
        assert!(matches!(
            coordinator
                .start_proposal(
                    Some(AmbiguousRequest::Mutation {
                        request: request.clone(),
                    }),
                    async move {
                        let now = running.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                        observed_maximum.fetch_max(now, AtomicOrdering::SeqCst);
                        let _ = blocked.await;
                        running.fetch_sub(1, AtomicOrdering::SeqCst);
                        Ok(ApplyResult::Rejected(
                            light_stream_core::DomainError::ExportConflict,
                        ))
                    },
                )
                .await
                .unwrap(),
            ProposalStart::Started
        ));
        assert!(matches!(
            coordinator
                .wait_proposal_until(Instant::now() + Duration::from_millis(10))
                .await
                .unwrap(),
            ProposalPoll::Pending(Some(AmbiguousRequest::Mutation {
                request: actual
            })) if actual == request
        ));
        assert!(matches!(
            coordinator
                .start_proposal(None, async {
                    Ok(ApplyResult::Rejected(
                        light_stream_core::DomainError::ExportConflict,
                    ))
                })
                .await
                .unwrap(),
            ProposalStart::Busy(Some(AmbiguousRequest::Mutation {
                request: actual
            })) if actual == request
        ));
        assert_eq!(maximum.load(AtomicOrdering::SeqCst), 1);

        release.send(()).unwrap();
        let resolved = loop {
            match coordinator.poll_proposal().await.unwrap() {
                ProposalPoll::Pending(_) => tokio::task::yield_now().await,
                result => break result,
            }
        };
        let ProposalPoll::Resolved(resolved) = resolved else {
            panic!("expected resolved proposal");
        };
        assert!(matches!(
            *resolved,
            Ok(ApplyResult::Rejected(
                light_stream_core::DomainError::ExportConflict
            ))
        ));
        assert!(matches!(
            coordinator.poll_proposal().await.unwrap(),
            ProposalPoll::Idle
        ));
        assert_eq!(active.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(maximum.load(AtomicOrdering::SeqCst), 1);

        coordinator
            .stop_and_join(tokio::time::Instant::now() + std::time::Duration::from_secs(1))
            .await
            .unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn shutdown_deadline_returns_unsafe_without_waiting_for_proposal() {
        let root = root("export-proposal-shutdown");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let coordinator =
            Arc::new(ExportCoordinator::open(&root, ExportLimits::default()).unwrap());
        let (release, blocked) = oneshot::channel();
        let (entered, started) = oneshot::channel();
        assert!(matches!(
            coordinator
                .start_proposal(None, async move {
                    let _ = entered.send(());
                    let _ = blocked.await;
                    Ok(ApplyResult::Rejected(
                        light_stream_core::DomainError::ExportConflict,
                    ))
                })
                .await
                .unwrap(),
            ProposalStart::Started
        ));
        started.await.unwrap();
        let expired = tokio::time::Instant::now() - Duration::from_secs(1);
        let error = coordinator.stop_and_join(expired).await.unwrap_err();
        assert!(matches!(error, CoordinatorShutdownError::Deadline));
        assert!(!error.teardown_safe());
        release.send(()).unwrap();
        tokio::task::yield_now().await;
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn startup_refuses_symlinked_spool_entries() {
        use std::os::unix::fs::symlink;

        let root = root("export-spool-symlink");
        let _ = fs::remove_dir_all(&root);
        let exports = root.join("exports");
        fs::create_dir_all(&exports).unwrap();
        let target = root.join("target");
        fs::write(&target, b"do not follow").unwrap();
        symlink(&target, exports.join(format!("{}.building", export_id()))).unwrap();

        assert!(ExportCoordinator::open(&root, ExportLimits::default()).is_err());
        assert_eq!(fs::read(target).unwrap(), b"do not follow");
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn startup_refuses_a_symlinked_spool_directory() {
        use std::os::unix::fs::symlink;

        let root = root("export-spool-directory-symlink");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let target = root.join("target");
        fs::create_dir_all(&target).unwrap();
        symlink(&target, root.join("exports")).unwrap();

        assert!(ExportCoordinator::open(&root, ExportLimits::default()).is_err());
        let _ = fs::remove_dir_all(root);
    }
}
