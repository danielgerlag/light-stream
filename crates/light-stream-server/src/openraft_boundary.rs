use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct BoundaryCommand {
    operation: String,
}

impl fmt::Display for BoundaryCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.operation)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct BoundaryResponse {
    accepted: bool,
}

openraft::declare_raft_types!(
    pub(crate) BoundaryConfig:
        D = BoundaryCommand,
        R = BoundaryResponse,
        NodeId = u64,
        Node = BasicNode,
);

pub(crate) fn assert_compile_boundary() {
    fn assert_config<T: openraft::RaftTypeConfig>() {}
    assert_config::<BoundaryConfig>();
    let _ = std::any::type_name::<openraft::type_config::alias::VoteOf<BoundaryConfig>>();
    let _ = std::any::type_name::<openraft::storage::LogState<BoundaryConfig>>();
    let _ = std::any::type_name::<openraft::type_config::alias::SnapshotMetaOf<BoundaryConfig>>();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fmt::Debug, future::Future, io, ops::RangeBounds};

    use futures_util::Stream;
    use openraft::{
        OptionalSend,
        errors::{RPCError, ReplicationClosed, StreamingError},
        network::{RPCOption, RaftNetworkFactory, RaftNetworkV2},
        raft::{
            AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest,
            VoteResponse,
        },
        storage::{
            EntryResponder, IOFlushed, LogState, RaftLogReader, RaftLogStorage,
            RaftSnapshotBuilder, RaftStateMachine,
        },
        type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf},
    };

    struct CompileLogReader;
    struct CompileLogStore;
    struct CompileSnapshotBuilder;
    struct CompileStateMachine;
    struct CompileNetwork;
    struct CompileNetworkFactory;

    impl RaftLogReader<BoundaryConfig> for CompileLogReader {
        async fn try_get_log_entries<RB>(
            &mut self,
            _range: RB,
        ) -> Result<Vec<<BoundaryConfig as openraft::RaftTypeConfig>::Entry>, io::Error>
        where
            RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
        {
            panic!("compile-only Openraft log reader")
        }

        async fn read_vote(&mut self) -> Result<Option<VoteOf<BoundaryConfig>>, io::Error> {
            panic!("compile-only Openraft vote reader")
        }
    }

    impl RaftLogStorage<BoundaryConfig> for CompileLogStore {
        type LogReader = CompileLogReader;

        async fn get_log_state(&mut self) -> Result<LogState<BoundaryConfig>, io::Error> {
            panic!("compile-only Openraft log store")
        }

        async fn get_log_reader(&mut self) -> Self::LogReader {
            panic!("compile-only Openraft log store")
        }

        async fn save_vote(&mut self, _vote: &VoteOf<BoundaryConfig>) -> Result<(), io::Error> {
            panic!("compile-only Openraft log store")
        }

        async fn save_committed(
            &mut self,
            _committed: Option<LogIdOf<BoundaryConfig>>,
        ) -> Result<(), io::Error> {
            panic!("compile-only Openraft log store")
        }

        async fn read_committed(&mut self) -> Result<Option<LogIdOf<BoundaryConfig>>, io::Error> {
            panic!("compile-only Openraft log store")
        }

        async fn append<I>(
            &mut self,
            _entries: I,
            _callback: IOFlushed<BoundaryConfig>,
        ) -> Result<(), io::Error>
        where
            I: IntoIterator<Item = <BoundaryConfig as openraft::RaftTypeConfig>::Entry>
                + OptionalSend,
            I::IntoIter: OptionalSend,
        {
            panic!("compile-only Openraft log store")
        }

        async fn truncate_after(
            &mut self,
            _last_log_id: Option<LogIdOf<BoundaryConfig>>,
        ) -> Result<(), io::Error> {
            panic!("compile-only Openraft log store")
        }

        async fn purge(&mut self, _log_id: LogIdOf<BoundaryConfig>) -> Result<(), io::Error> {
            panic!("compile-only Openraft log store")
        }
    }

    impl RaftSnapshotBuilder<BoundaryConfig> for CompileSnapshotBuilder {
        type SnapshotData = Vec<u8>;

        async fn build_snapshot(
            &mut self,
        ) -> Result<SnapshotOf<BoundaryConfig, Self::SnapshotData>, io::Error> {
            panic!("compile-only Openraft snapshot builder")
        }
    }

    impl RaftStateMachine<BoundaryConfig> for CompileStateMachine {
        type SnapshotData = Vec<u8>;
        type SnapshotBuilder = CompileSnapshotBuilder;

        async fn applied_state(
            &mut self,
        ) -> Result<
            (
                Option<LogIdOf<BoundaryConfig>>,
                StoredMembershipOf<BoundaryConfig>,
            ),
            io::Error,
        > {
            panic!("compile-only Openraft state machine")
        }

        async fn apply<Strm>(&mut self, _entries: Strm) -> Result<(), io::Error>
        where
            Strm: Stream<Item = Result<EntryResponder<BoundaryConfig>, io::Error>>
                + Unpin
                + OptionalSend,
        {
            panic!("compile-only Openraft state machine")
        }

        async fn try_create_snapshot_builder(
            &mut self,
            _force: bool,
        ) -> Option<Self::SnapshotBuilder> {
            panic!("compile-only Openraft state machine")
        }

        async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
            panic!("compile-only Openraft state machine")
        }

        async fn install_snapshot(
            &mut self,
            _meta: &SnapshotMetaOf<BoundaryConfig>,
            _snapshot: Self::SnapshotData,
        ) -> Result<(), io::Error> {
            panic!("compile-only Openraft state machine")
        }

        async fn get_current_snapshot(
            &mut self,
        ) -> Result<Option<SnapshotOf<BoundaryConfig, Self::SnapshotData>>, io::Error> {
            panic!("compile-only Openraft state machine")
        }
    }

    impl RaftNetworkV2<BoundaryConfig> for CompileNetwork {
        type SnapshotData = Vec<u8>;

        async fn append_entries(
            &mut self,
            _rpc: AppendEntriesRequest<BoundaryConfig>,
            _option: RPCOption,
        ) -> Result<AppendEntriesResponse<BoundaryConfig>, RPCError<BoundaryConfig>> {
            panic!("compile-only Openraft network")
        }

        async fn vote(
            &mut self,
            _rpc: VoteRequest<BoundaryConfig>,
            _option: RPCOption,
        ) -> Result<VoteResponse<BoundaryConfig>, RPCError<BoundaryConfig>> {
            panic!("compile-only Openraft network")
        }

        async fn full_snapshot(
            &mut self,
            _vote: VoteOf<BoundaryConfig>,
            _snapshot: SnapshotOf<BoundaryConfig, Self::SnapshotData>,
            _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
            _option: RPCOption,
        ) -> Result<SnapshotResponse<BoundaryConfig>, StreamingError<BoundaryConfig>> {
            panic!("compile-only Openraft network")
        }
    }

    impl RaftNetworkFactory<BoundaryConfig> for CompileNetworkFactory {
        type Network = CompileNetwork;

        async fn new_client(&mut self, _target: u64, _node: &BasicNode) -> Self::Network {
            CompileNetwork
        }

        async fn new_heartbeat_client(&mut self, _target: u64, _node: &BasicNode) -> Self::Network {
            CompileNetwork
        }

        async fn new_snapshot_client(&mut self, _target: u64, _node: &BasicNode) -> Self::Network {
            CompileNetwork
        }
    }

    #[test]
    fn exact_openraft_type_mapping_compiles() {
        assert_compile_boundary();
        fn assert_storage<T: RaftLogStorage<BoundaryConfig>>() {}
        fn assert_state_machine<T: RaftStateMachine<BoundaryConfig>>() {}
        fn assert_network<T: RaftNetworkFactory<BoundaryConfig>>() {}
        assert_storage::<CompileLogStore>();
        assert_state_machine::<CompileStateMachine>();
        assert_network::<CompileNetworkFactory>();
    }
}
