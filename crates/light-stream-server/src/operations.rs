use std::{fmt::Display, sync::Arc};

use axum::{
    Json, Router,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use light_stream_core::{
    ConsensusGroup, ExportStatusPhase, GroupId, NodePhase, ReadinessReason, WriteReadiness,
};
use serde::Serialize;
use tokio::{net::TcpListener, sync::watch};

use crate::{
    lifecycle::{AdmissionDrainSnapshot, LifecycleController, OperationalState},
    publish_scheduler::PublishQueueSnapshot,
    runtime::{ClusterManager, GroupDiagnostic, NodeDiagnostic},
};

#[derive(Clone)]
struct OperationsState {
    lifecycle: LifecycleController,
    cluster: Arc<ClusterManager>,
}

struct OperationalSnapshot {
    lifecycle: OperationalState,
    admission_drain: AdmissionDrainSnapshot,
    diagnostics: NodeDiagnostic,
    export_state: Option<ExportStatusPhase>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ConsensusKindLabel {
    Control,
    Data,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ReadinessGroupLabel {
    None,
    Configured(GroupId),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublishQueueResource {
    Requests,
    Records,
    ResidentBytes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublishRejectionReason {
    Overloaded,
}

impl ConsensusKindLabel {
    const fn from_group(group: ConsensusGroup) -> Self {
        match group {
            ConsensusGroup::Control => Self::Control,
            ConsensusGroup::Data => Self::Data,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Control => "control",
            Self::Data => "data",
        }
    }
}

impl ReadinessGroupLabel {
    fn value(self) -> String {
        match self {
            Self::None => "none".to_owned(),
            Self::Configured(group) => group.get().to_string(),
        }
    }
}

impl PublishQueueResource {
    const ALL: [Self; 3] = [Self::Requests, Self::Records, Self::ResidentBytes];

    const fn label(self) -> &'static str {
        match self {
            Self::Requests => "requests",
            Self::Records => "records",
            Self::ResidentBytes => "resident_bytes",
        }
    }

    const fn limit(self, queue: PublishQueueSnapshot) -> usize {
        match self {
            Self::Requests => queue.request_limit,
            Self::Records => queue.record_limit,
            Self::ResidentBytes => queue.resident_byte_limit,
        }
    }
}

impl PublishRejectionReason {
    const fn label(self) -> &'static str {
        match self {
            Self::Overloaded => "overloaded",
        }
    }
}

#[derive(Serialize)]
struct ProbeResponse {
    live: bool,
    write_ready: bool,
    lifecycle: NodePhase,
    lifecycle_generation: u64,
    readiness: WriteReadiness,
}

pub(crate) async fn serve(
    listener: TcpListener,
    lifecycle: LifecycleController,
    cluster: Arc<ClusterManager>,
    shutdown: watch::Receiver<bool>,
) -> Result<(), std::io::Error> {
    let state = OperationsState { lifecycle, cluster };
    let app = Router::new()
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(state);
    axum::serve(listener, app)
        .with_graceful_shutdown(wait_for_shutdown(shutdown))
        .await
}

async fn livez(State(state): State<OperationsState>) -> impl IntoResponse {
    let snapshot = state.lifecycle.snapshot();
    let live = is_live(snapshot.phase);
    (
        if live {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(probe_response(snapshot)),
    )
}

async fn readyz(State(state): State<OperationsState>) -> impl IntoResponse {
    let snapshot = state.lifecycle.snapshot();
    (
        if snapshot.readiness.is_ready() {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(probe_response(snapshot)),
    )
}

async fn metrics(State(state): State<OperationsState>) -> Response {
    let snapshot = collect_snapshot(&state).await;
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        encode_metrics(&snapshot),
    )
        .into_response()
}

async fn collect_snapshot(state: &OperationsState) -> OperationalSnapshot {
    let lifecycle = state.lifecycle.snapshot();
    let admission_drain = state.lifecycle.admission_drain_snapshot();
    let diagnostics = state.cluster.diagnostics().await;
    let export_state = state.cluster.export_state_snapshot().await;
    OperationalSnapshot {
        lifecycle,
        admission_drain,
        diagnostics,
        export_state,
    }
}

fn probe_response(state: OperationalState) -> ProbeResponse {
    ProbeResponse {
        live: is_live(state.phase),
        write_ready: state.readiness.is_ready(),
        lifecycle: state.phase,
        lifecycle_generation: state.generation,
        readiness: state.readiness,
    }
}

fn is_live(phase: NodePhase) -> bool {
    matches!(
        phase,
        NodePhase::Starting | NodePhase::Running | NodePhase::Draining
    )
}

fn encode_metrics(snapshot: &OperationalSnapshot) -> String {
    let mut output = String::new();
    metric(
        &mut output,
        "light_stream_live",
        u8::from(is_live(snapshot.lifecycle.phase)),
    );
    metric(
        &mut output,
        "light_stream_write_ready",
        u8::from(snapshot.lifecycle.readiness.is_ready()),
    );
    output.push_str(&format!(
        "light_stream_lifecycle_state{{state=\"{}\"}} 1\n",
        phase_name(snapshot.lifecycle.phase)
    ));

    let mut readiness_reasons = match &snapshot.lifecycle.readiness {
        WriteReadiness::Ready => &[][..],
        WriteReadiness::NotReady { reasons } => reasons,
    }
    .iter()
    .collect::<Vec<_>>();
    readiness_reasons.sort_by_key(|reason| (reason.code(), readiness_group_label(reason)));
    for reason in readiness_reasons {
        let group_id = readiness_group_label(reason).value();
        output.push_str(&format!(
            "light_stream_readiness_reason{{reason=\"{}\",group_id=\"{group_id}\"}} 1\n",
            reason.code(),
        ));
    }

    metric(
        &mut output,
        "light_stream_mutation_admission_open",
        u8::from(snapshot.admission_drain.admission.is_open()),
    );
    metric(
        &mut output,
        "light_stream_mutations_in_flight",
        snapshot.admission_drain.mutations_in_flight,
    );
    if let Some(state) = snapshot.export_state {
        output.push_str(&format!(
            "light_stream_export_state{{state=\"{}\"}} 1\n",
            export_state_label(state)
        ));
    }

    let mut groups = snapshot.diagnostics.groups.iter().collect::<Vec<_>>();
    groups.sort_by_key(|group| (ConsensusKindLabel::from_group(group.group), group.group_id));
    for group in groups {
        let kind = ConsensusKindLabel::from_group(group.group).label();
        let labels = format!("kind=\"{kind}\",group_id=\"{}\"", group.group_id);
        output.push_str(&format!(
            "light_stream_group_has_leader{{{labels}}} {}\n",
            u8::from(group.current_leader.is_some())
        ));
        for (name, value) in [
            ("local_commit_index", group.local_committed_index),
            ("cluster_commit_index", group.cluster_committed_index),
            ("applied_index", group.last_applied_index),
        ] {
            if let Some(value) = value {
                output.push_str(&format!("light_stream_group_{name}{{{labels}}} {value}\n"));
            }
        }
        encode_replication_lag(&mut output, &snapshot.diagnostics, group, kind);
        if let Some(queue) = group.publish_queue {
            for (name, value) in [
                ("requests", queue.requests),
                ("records", queue.records),
                ("resident_bytes", queue.resident_bytes),
            ] {
                output.push_str(&format!(
                    "light_stream_publish_queue_{name}{{group_id=\"{}\"}} {value}\n",
                    group.group_id
                ));
            }
            for resource in PublishQueueResource::ALL {
                output.push_str(&format!(
                    "light_stream_publish_queue_limit{{group_id=\"{}\",resource=\"{}\"}} {}\n",
                    group.group_id,
                    resource.label(),
                    resource.limit(queue)
                ));
            }
            output.push_str(&format!(
                "light_stream_publish_rejections_total{{group_id=\"{}\",reason=\"{}\"}} {}\n",
                group.group_id,
                PublishRejectionReason::Overloaded.label(),
                queue.rejected_total
            ));
        }
    }
    output
}

fn encode_replication_lag(
    output: &mut String,
    diagnostics: &NodeDiagnostic,
    group: &GroupDiagnostic,
    kind: &str,
) {
    if group.current_leader != Some(diagnostics.node_id) {
        return;
    }
    let mut replication = group.replication.iter().collect::<Vec<_>>();
    replication.sort_by_key(|target| target.target_node_id);
    for target in replication {
        if !diagnostics
            .peers
            .iter()
            .any(|peer| peer.node_id().get() == target.target_node_id)
        {
            continue;
        }
        let Some(lag) = group
            .last_log_index
            .zip(target.matched_log_index)
            .and_then(|(leader_last, matched)| leader_last.checked_sub(matched))
        else {
            continue;
        };
        output.push_str(&format!(
            "light_stream_group_replication_lag_entries{{kind=\"{kind}\",group_id=\"{}\",target_node_id=\"{}\"}} {lag}\n",
            group.group_id, target.target_node_id
        ));
    }
}

fn readiness_group_label(reason: &ReadinessReason) -> ReadinessGroupLabel {
    match reason {
        ReadinessReason::Starting
        | ReadinessReason::NotBootstrapped
        | ReadinessReason::Forming
        | ReadinessReason::Retired
        | ReadinessReason::SecurityPolicyStale
        | ReadinessReason::ExportInProgress
        | ReadinessReason::Draining
        | ReadinessReason::StorageFailure => ReadinessGroupLabel::None,
        ReadinessReason::GroupLeaderUnknown { group }
        | ReadinessReason::GroupAuthorityStale { group }
        | ReadinessReason::ProbeUnsupported { group } => ReadinessGroupLabel::Configured(*group),
    }
}

fn metric(output: &mut String, name: &str, value: impl Display) {
    output.push_str(&format!("{name} {value}\n"));
}

fn phase_name(phase: NodePhase) -> &'static str {
    match phase {
        NodePhase::Starting => "starting",
        NodePhase::Running => "running",
        NodePhase::Draining => "draining",
        NodePhase::Stopping => "stopping",
        NodePhase::Failed => "failed",
    }
}

fn export_state_label(phase: ExportStatusPhase) -> &'static str {
    match phase {
        ExportStatusPhase::Preparing => "preparing",
        ExportStatusPhase::Frozen => "frozen",
        ExportStatusPhase::Materializing => "materializing",
        ExportStatusPhase::Available => "available",
        ExportStatusPhase::Releasing => "releasing",
        ExportStatusPhase::Aborting => "aborting",
    }
}

async fn wait_for_shutdown(mut receiver: watch::Receiver<bool>) {
    while !*receiver.borrow() {
        if receiver.changed().await.is_err() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{lifecycle::MutationAdmission, runtime::ReplicationDiagnostic};
    use light_stream_core::{ConsensusGroup, GroupId, NodeDescriptor, NodeId, ReadinessReason};

    #[test]
    fn metrics_encode_the_non_export_contract_deterministically() {
        let snapshot = OperationalSnapshot {
            lifecycle: OperationalState {
                generation: 2,
                phase: NodePhase::Running,
                readiness: WriteReadiness::Ready,
            },
            admission_drain: AdmissionDrainSnapshot {
                admission: MutationAdmission::Open,
                mutations_in_flight: 2,
            },
            diagnostics: NodeDiagnostic {
                node_id: 1,
                lifecycle: "application-id-canary customer-stream customer-principal".to_owned(),
                peers: vec![
                    node(5, "peer-token-canary"),
                    node(3, "peer-certificate-canary"),
                    node(4, "peer-request-canary"),
                ],
                groups: vec![
                    group(
                        ConsensusGroup::Data,
                        2,
                        Some(1),
                        Some(10),
                        vec![
                            ReplicationDiagnostic {
                                target_node_id: 5,
                                matched_log_index: Some(11),
                            },
                            ReplicationDiagnostic {
                                target_node_id: 99,
                                matched_log_index: Some(8),
                            },
                            ReplicationDiagnostic {
                                target_node_id: 4,
                                matched_log_index: None,
                            },
                            ReplicationDiagnostic {
                                target_node_id: 3,
                                matched_log_index: Some(7),
                            },
                        ],
                        Some(PublishQueueSnapshot {
                            accepting: true,
                            requests: 4,
                            records: 5,
                            resident_bytes: 6,
                            request_limit: 10,
                            record_limit: 20,
                            resident_byte_limit: 30,
                            rejected_total: 7,
                        }),
                    ),
                    group(
                        ConsensusGroup::Control,
                        1,
                        Some(1),
                        Some(12),
                        vec![ReplicationDiagnostic {
                            target_node_id: 3,
                            matched_log_index: Some(9),
                        }],
                        None,
                    ),
                    group(
                        ConsensusGroup::Data,
                        3,
                        Some(2),
                        Some(12),
                        vec![ReplicationDiagnostic {
                            target_node_id: 3,
                            matched_log_index: Some(9),
                        }],
                        None,
                    ),
                    group(
                        ConsensusGroup::Data,
                        4,
                        Some(1),
                        None,
                        vec![ReplicationDiagnostic {
                            target_node_id: 3,
                            matched_log_index: Some(9),
                        }],
                        None,
                    ),
                ],
                data_group_slots: 1,
                data_group_count: 1,
                rocksdb_cache_budget_bytes: 0,
                rocksdb_write_buffer_budget_bytes: 0,
                per_group_cache_bytes: 0,
                per_group_write_buffer_bytes: 0,
                unsupported_claims: vec![
                    "endpoint=https://sensitive.invalid/private-path".to_owned(),
                    "request=request-canary token=secret-token-canary".to_owned(),
                    "certificate_subject=CN=certificate-subject-canary".to_owned(),
                ],
            },
            export_state: None,
        };

        let encoded = encode_metrics(&snapshot);

        assert_eq!(
            encoded,
            concat!(
                "light_stream_live 1\n",
                "light_stream_write_ready 1\n",
                "light_stream_lifecycle_state{state=\"running\"} 1\n",
                "light_stream_mutation_admission_open 1\n",
                "light_stream_mutations_in_flight 2\n",
                "light_stream_group_has_leader{kind=\"control\",group_id=\"1\"} 1\n",
                "light_stream_group_replication_lag_entries{kind=\"control\",group_id=\"1\",target_node_id=\"3\"} 3\n",
                "light_stream_group_has_leader{kind=\"data\",group_id=\"2\"} 1\n",
                "light_stream_group_replication_lag_entries{kind=\"data\",group_id=\"2\",target_node_id=\"3\"} 3\n",
                "light_stream_publish_queue_requests{group_id=\"2\"} 4\n",
                "light_stream_publish_queue_records{group_id=\"2\"} 5\n",
                "light_stream_publish_queue_resident_bytes{group_id=\"2\"} 6\n",
                "light_stream_publish_queue_limit{group_id=\"2\",resource=\"requests\"} 10\n",
                "light_stream_publish_queue_limit{group_id=\"2\",resource=\"records\"} 20\n",
                "light_stream_publish_queue_limit{group_id=\"2\",resource=\"resident_bytes\"} 30\n",
                "light_stream_publish_rejections_total{group_id=\"2\",reason=\"overloaded\"} 7\n",
                "light_stream_group_has_leader{kind=\"data\",group_id=\"3\"} 1\n",
                "light_stream_group_has_leader{kind=\"data\",group_id=\"4\"} 1\n",
            )
        );
        for canary in [
            "application-id-canary",
            "customer-stream",
            "customer-principal",
            "peer-token-canary",
            "peer-certificate-canary",
            "peer-request-canary",
            "sensitive.invalid",
            "private-path",
            "request-canary",
            "secret-token-canary",
            "certificate-subject-canary",
            "leader-role-canary",
            "group-application-id-canary",
            "group-endpoint.invalid",
            "private-group-path",
            "group-request-canary",
            "group-token-canary",
            "group-certificate-canary",
        ] {
            assert!(!encoded.contains(canary), "leaked canary: {canary}");
        }
        assert!(!encoded.contains("target_node_id=\"4\""));
        assert!(!encoded.contains("target_node_id=\"5\""));
        assert!(!encoded.contains("target_node_id=\"99\""));
        assert!(!encoded.contains("kind=\"data\",group_id=\"3\",target_node_id"));
        assert!(!encoded.contains("kind=\"data\",group_id=\"4\",target_node_id"));
        assert!(!encoded.contains("light_stream_export_state"));
    }

    #[test]
    fn readiness_group_labels_and_closed_admission_are_bounded() {
        let snapshot = OperationalSnapshot {
            lifecycle: OperationalState {
                generation: 2,
                phase: NodePhase::Running,
                readiness: WriteReadiness::NotReady {
                    reasons: vec![
                        ReadinessReason::SecurityPolicyStale,
                        ReadinessReason::GroupAuthorityStale {
                            group: GroupId::new(3).unwrap(),
                        },
                    ],
                },
            },
            admission_drain: AdmissionDrainSnapshot {
                admission: MutationAdmission::Closed,
                mutations_in_flight: 0,
            },
            diagnostics: NodeDiagnostic::empty_for_test(),
            export_state: Some(ExportStatusPhase::Available),
        };

        let encoded = encode_metrics(&snapshot);

        assert!(encoded.contains(
            "light_stream_readiness_reason{reason=\"group_authority_stale\",group_id=\"3\"} 1\n"
        ));
        assert!(encoded.contains(
            "light_stream_readiness_reason{reason=\"security_policy_stale\",group_id=\"none\"} 1\n"
        ));
        assert!(encoded.contains("light_stream_mutation_admission_open 0\n"));
        assert!(encoded.contains("light_stream_export_state{state=\"available\"} 1\n"));
        assert!(!encoded.contains("group_id=\"0\""));
    }

    fn node(node_id: u64, uri_canary: &str) -> NodeDescriptor {
        NodeDescriptor::new(
            NodeId::new(node_id).unwrap(),
            format!("http://{uri_canary}.public.invalid"),
            format!("http://{uri_canary}.peer.invalid"),
        )
    }

    fn group(
        group: ConsensusGroup,
        group_id: u64,
        current_leader: Option<u64>,
        last_log_index: Option<u64>,
        replication: Vec<ReplicationDiagnostic>,
        publish_queue: Option<PublishQueueSnapshot>,
    ) -> GroupDiagnostic {
        GroupDiagnostic {
            group,
            group_id,
            local_role: concat!(
                "group-application-id-canary leader-role-canary ",
                "endpoint=https://group-endpoint.invalid/private-group-path ",
                "request=group-request-canary token=group-token-canary ",
                "certificate_subject=CN=group-certificate-canary"
            )
            .to_owned(),
            current_leader,
            effective_uniform: false,
            effective_voters: Vec::new(),
            effective_learners: Vec::new(),
            committed_uniform: false,
            committed_voters: Vec::new(),
            committed_learners: Vec::new(),
            last_log_index,
            local_committed_index: None,
            cluster_committed_index: None,
            last_applied_index: None,
            replication,
            snapshot_index: None,
            purged_index: None,
            slot: None,
            cache_budget_bytes: None,
            write_buffer_budget_bytes: None,
            publish_queue,
        }
    }
}
