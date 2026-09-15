use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::PathBuf,
    str::FromStr,
    time::Duration,
};

use clap::Parser;
use light_stream_core::{
    DEFAULT_MAX_DATA_GROUPS, DEFAULT_MAX_PARTITIONS_PER_STREAM, DEFAULT_MAX_STREAMS, DomainError,
    MAX_DATA_GROUPS, MIN_DATA_GROUPS, NodeDescriptor, NodeId, SecurityMode,
};
use tonic::transport::{Endpoint, Uri};

use crate::publish_scheduler::PublishSchedulerConfig;
use crate::{StartupError, manifest::GroupPoolConfig};

const DEFAULT_ROCKSDB_CACHE_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_ROCKSDB_WRITE_BUFFER_BYTES: usize = 128 * 1024 * 1024;
const DEFAULT_PUBLISH_QUEUE_REQUESTS: usize = 512;
const DEFAULT_PUBLISH_QUEUE_RECORDS: usize = 8_192;
const DEFAULT_PUBLISH_QUEUE_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_PUBLISH_BATCH_REQUESTS: usize = 64;
const DEFAULT_PUBLISH_BATCH_RECORDS: usize = 512;
const DEFAULT_PUBLISH_BATCH_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_PUBLISH_COALESCE_US: u64 = 200;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PeerRoutes(BTreeMap<u64, String>);

impl PeerRoutes {
    pub(crate) fn parse(values: Vec<String>, local_node_id: u64) -> Result<Self, StartupError> {
        let mut routes = BTreeMap::new();
        let mut route_identities = BTreeSet::new();
        for value in values {
            let (target, uri) = value.split_once('=').ok_or_else(|| {
                StartupError::InvalidConfig(
                    "peer route must use NODE_ID=HTTP_URI syntax".to_owned(),
                )
            })?;
            let target = target.parse::<u64>().map_err(|error| {
                StartupError::InvalidConfig(format!("peer route node ID is invalid: {error}"))
            })?;
            if target == 0 {
                return Err(StartupError::InvalidConfig(
                    "peer route node ID zero is reserved".to_owned(),
                ));
            }
            if target == local_node_id {
                return Err(StartupError::InvalidConfig(
                    "peer route cannot target the local node".to_owned(),
                ));
            }
            validate_http_uri("peer route", uri)?;
            let identity = route_identity(uri).map_err(StartupError::InvalidConfig)?;
            if !route_identities.insert(identity) {
                return Err(StartupError::InvalidConfig(
                    "peer route URI is duplicated".to_owned(),
                ));
            }
            if routes.insert(target, uri.to_owned()).is_some() {
                return Err(StartupError::InvalidConfig(format!(
                    "peer route for node {target} is duplicated"
                )));
            }
        }
        Ok(Self(routes))
    }

    pub(crate) fn validate_topology(
        &self,
        local_node_id: NodeId,
        members: &[NodeDescriptor],
    ) -> Result<(), DomainError> {
        let member_ids = members
            .iter()
            .map(|member| member.node_id().get())
            .collect::<BTreeSet<_>>();
        let public_routes = members
            .iter()
            .map(|member| route_identity(member.public_uri()))
            .collect::<Result<BTreeSet<_>, _>>()
            .map_err(|reason| DomainError::InvalidName {
                kind: "public URI".to_owned(),
                reason,
            })?;
        for (target, uri) in &self.0 {
            if *target == local_node_id.get() {
                return Err(DomainError::InvalidName {
                    kind: "peer route".to_owned(),
                    reason: "peer route cannot target the local node".to_owned(),
                });
            }
            if !member_ids.contains(target) {
                return Err(DomainError::InvalidName {
                    kind: "peer route".to_owned(),
                    reason: format!("peer route target node {target} is not in the topology"),
                });
            }
            let identity = route_identity(uri).map_err(|reason| DomainError::InvalidName {
                kind: "peer route".to_owned(),
                reason,
            })?;
            if public_routes.contains(&identity) {
                return Err(DomainError::InvalidName {
                    kind: "peer route".to_owned(),
                    reason: format!("peer route for node {target} aliases a public route"),
                });
            }
        }
        Ok(())
    }

    pub(crate) fn get(&self, target: u64) -> Option<&str> {
        self.0.get(&target).map(String::as_str)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Debug, Parser)]
#[command(name = "light-streamd", version, about = "Light Stream server")]
pub struct ServerArgs {
    #[arg(long, default_value = "127.0.0.1:7101")]
    pub public_listen: SocketAddr,
    #[arg(long, default_value = "127.0.0.1:7201")]
    pub peer_listen: SocketAddr,
    #[arg(long, default_value = ".light-stream")]
    pub data_dir: PathBuf,
    #[arg(long, default_value = "local-insecure")]
    pub security_mode: String,
    #[arg(long)]
    pub allow_insecure_non_loopback: bool,
    #[arg(long, default_value_t = light_stream_storage::DEFAULT_RECEIPT_WINDOW)]
    pub receipt_window: usize,
    #[arg(long, default_value_t = 1)]
    pub node_id: u64,
    #[arg(long)]
    pub advertise_public_uri: Option<String>,
    #[arg(long)]
    pub advertise_peer_uri: Option<String>,
    #[arg(long = "peer-route")]
    pub peer_routes: Vec<String>,
    #[arg(long, default_value_t = DEFAULT_MAX_DATA_GROUPS)]
    pub max_data_groups: u16,
    #[arg(long, default_value_t = DEFAULT_MAX_STREAMS)]
    pub max_streams: u32,
    #[arg(long, default_value_t = DEFAULT_MAX_PARTITIONS_PER_STREAM)]
    pub max_partitions_per_stream: u32,
    #[arg(long, default_value_t = DEFAULT_ROCKSDB_CACHE_BYTES)]
    pub rocksdb_cache_bytes: usize,
    #[arg(long, default_value_t = DEFAULT_ROCKSDB_WRITE_BUFFER_BYTES)]
    pub rocksdb_write_buffer_bytes: usize,
    #[arg(long, default_value_t = DEFAULT_PUBLISH_QUEUE_REQUESTS)]
    pub publish_queue_requests: usize,
    #[arg(long, default_value_t = DEFAULT_PUBLISH_QUEUE_RECORDS)]
    pub publish_queue_records: usize,
    #[arg(long, default_value_t = DEFAULT_PUBLISH_QUEUE_BYTES)]
    pub publish_queue_bytes: usize,
    #[arg(long, default_value_t = DEFAULT_PUBLISH_BATCH_REQUESTS)]
    pub publish_batch_requests: usize,
    #[arg(long, default_value_t = DEFAULT_PUBLISH_BATCH_RECORDS)]
    pub publish_batch_records: usize,
    #[arg(long, default_value_t = DEFAULT_PUBLISH_BATCH_BYTES)]
    pub publish_batch_bytes: usize,
    #[arg(long, default_value_t = DEFAULT_PUBLISH_COALESCE_US)]
    pub publish_coalesce_us: u64,
    #[arg(long)]
    pub verification_enable_fault_hooks: bool,
    #[arg(long)]
    pub verification_delay_group_id: Option<u64>,
    #[arg(long, default_value_t = 0)]
    pub verification_delay_ms: u64,
    #[arg(long)]
    pub verification_response_delay_group_id: Option<u64>,
    #[arg(long, default_value_t = 0)]
    pub verification_response_delay_ms: u64,
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    public_listen: SocketAddr,
    peer_listen: SocketAddr,
    data_dir: PathBuf,
    security_mode: SecurityMode,
    receipt_window: usize,
    node_id: u64,
    advertise_public_uri: Option<String>,
    advertise_peer_uri: Option<String>,
    peer_routes: PeerRoutes,
    group_pool: GroupPoolConfig,
    publish_scheduler: PublishSchedulerConfig,
    verification_delay: Option<(u64, Duration)>,
    verification_response_delay: Option<(u64, Duration)>,
}

impl TryFrom<ServerArgs> for ServerConfig {
    type Error = StartupError;

    fn try_from(args: ServerArgs) -> Result<Self, Self::Error> {
        let security_mode = SecurityMode::from_str(&args.security_mode)
            .map_err(|error| StartupError::InvalidConfig(error.to_string()))?;
        if security_mode == SecurityMode::Secured {
            return Err(StartupError::SecuredModeUnsupported);
        }
        if args.public_listen == args.peer_listen && args.public_listen.port() != 0 {
            return Err(StartupError::InvalidConfig(
                "public and peer listeners must use different addresses".to_owned(),
            ));
        }
        if !args.allow_insecure_non_loopback
            && (!args.public_listen.ip().is_loopback() || !args.peer_listen.ip().is_loopback())
        {
            return Err(StartupError::InvalidConfig(
                "local-insecure listeners must bind loopback unless --allow-insecure-non-loopback is set"
                    .to_owned(),
            ));
        }
        if args.receipt_window == 0 {
            return Err(StartupError::InvalidConfig(
                "receipt window must be greater than zero".to_owned(),
            ));
        }
        if args.node_id == 0 {
            return Err(StartupError::InvalidConfig(
                "node ID zero is reserved".to_owned(),
            ));
        }
        for (kind, value) in [
            ("public", args.advertise_public_uri.as_ref()),
            ("peer", args.advertise_peer_uri.as_ref()),
        ] {
            if let Some(value) = value {
                validate_http_uri(&format!("advertised {kind} URI"), value)?;
            }
        }
        if args.advertise_public_uri == args.advertise_peer_uri
            && args.advertise_public_uri.is_some()
        {
            return Err(StartupError::InvalidConfig(
                "advertised public and peer URIs must differ".to_owned(),
            ));
        }
        let peer_routes = PeerRoutes::parse(args.peer_routes, args.node_id)?;
        if !(MIN_DATA_GROUPS..=MAX_DATA_GROUPS).contains(&args.max_data_groups)
            || args.max_streams == 0
            || args.max_partitions_per_stream == 0
        {
            return Err(StartupError::InvalidConfig(
                "invalid bounded group or catalog limit".to_owned(),
            ));
        }
        let group_pool = GroupPoolConfig::try_new(
            args.max_data_groups,
            args.max_streams,
            args.max_partitions_per_stream,
            args.rocksdb_cache_bytes,
            args.rocksdb_write_buffer_bytes,
        )
        .map_err(|error| StartupError::InvalidConfig(error.to_string()))?;
        let publish_scheduler = PublishSchedulerConfig::try_new(
            args.publish_queue_requests,
            args.publish_queue_records,
            args.publish_queue_bytes,
            args.publish_batch_requests,
            args.publish_batch_records,
            args.publish_batch_bytes,
            Duration::from_micros(args.publish_coalesce_us),
        )
        .map_err(StartupError::InvalidConfig)?;
        let verification_delay = parse_verification_delay(
            args.verification_enable_fault_hooks,
            args.verification_delay_group_id,
            args.verification_delay_ms,
            "verification delay",
        )?;
        let verification_response_delay = parse_verification_delay(
            args.verification_enable_fault_hooks,
            args.verification_response_delay_group_id,
            args.verification_response_delay_ms,
            "verification response delay",
        )?;
        Ok(Self {
            public_listen: args.public_listen,
            peer_listen: args.peer_listen,
            data_dir: args.data_dir,
            security_mode,
            receipt_window: args.receipt_window,
            node_id: args.node_id,
            advertise_public_uri: args.advertise_public_uri,
            advertise_peer_uri: args.advertise_peer_uri,
            peer_routes,
            group_pool,
            publish_scheduler,
            verification_delay,
            verification_response_delay,
        })
    }
}

impl ServerConfig {
    pub const fn public_listen(&self) -> SocketAddr {
        self.public_listen
    }

    pub const fn peer_listen(&self) -> SocketAddr {
        self.peer_listen
    }

    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    pub const fn security_mode(&self) -> SecurityMode {
        self.security_mode
    }

    pub const fn receipt_window(&self) -> usize {
        self.receipt_window
    }

    pub const fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn advertise_public_uri(&self) -> Option<&str> {
        self.advertise_public_uri.as_deref()
    }

    pub fn advertise_peer_uri(&self) -> Option<&str> {
        self.advertise_peer_uri.as_deref()
    }

    pub const fn peer_routes(&self) -> &PeerRoutes {
        &self.peer_routes
    }

    pub const fn group_pool(&self) -> &GroupPoolConfig {
        &self.group_pool
    }

    pub(crate) const fn publish_scheduler(&self) -> PublishSchedulerConfig {
        self.publish_scheduler
    }

    pub const fn verification_delay(&self) -> Option<(u64, Duration)> {
        self.verification_delay
    }

    pub const fn verification_response_delay(&self) -> Option<(u64, Duration)> {
        self.verification_response_delay
    }
}

fn parse_verification_delay(
    enabled: bool,
    group: Option<u64>,
    milliseconds: u64,
    name: &str,
) -> Result<Option<(u64, Duration)>, StartupError> {
    match (enabled, group, milliseconds) {
        (_, None, 0) => Ok(None),
        (true, Some(group), milliseconds @ 1..=5000) if group > 1 => {
            Ok(Some((group, Duration::from_millis(milliseconds))))
        }
        _ => Err(StartupError::InvalidConfig(format!(
            "{name} requires its explicit gate, a data group ID, and 1..=5000 ms"
        ))),
    }
}

fn validate_http_uri(kind: &str, value: &str) -> Result<(), StartupError> {
    if !value.starts_with("http://") || Endpoint::from_shared(value.to_owned()).is_err() {
        return Err(StartupError::InvalidConfig(format!(
            "{kind} must be a valid http:// URI"
        )));
    }
    Ok(())
}

fn route_identity(value: &str) -> Result<String, String> {
    let uri = value
        .parse::<Uri>()
        .map_err(|error| format!("invalid HTTP URI: {error}"))?;
    let scheme = uri
        .scheme_str()
        .ok_or_else(|| "HTTP URI is missing a scheme".to_owned())?;
    let authority = uri
        .authority()
        .ok_or_else(|| "HTTP URI is missing an authority".to_owned())?;
    Ok(format!("{scheme}://{authority}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> ServerArgs {
        ServerArgs {
            public_listen: "127.0.0.1:7101".parse().unwrap(),
            peer_listen: "127.0.0.1:7201".parse().unwrap(),
            data_dir: PathBuf::from("data"),
            security_mode: "local-insecure".to_owned(),
            allow_insecure_non_loopback: false,
            receipt_window: light_stream_storage::DEFAULT_RECEIPT_WINDOW,
            node_id: 1,
            advertise_public_uri: None,
            advertise_peer_uri: None,
            peer_routes: Vec::new(),
            max_data_groups: DEFAULT_MAX_DATA_GROUPS,
            max_streams: DEFAULT_MAX_STREAMS,
            max_partitions_per_stream: DEFAULT_MAX_PARTITIONS_PER_STREAM,
            rocksdb_cache_bytes: DEFAULT_ROCKSDB_CACHE_BYTES,
            rocksdb_write_buffer_bytes: DEFAULT_ROCKSDB_WRITE_BUFFER_BYTES,
            publish_queue_requests: DEFAULT_PUBLISH_QUEUE_REQUESTS,
            publish_queue_records: DEFAULT_PUBLISH_QUEUE_RECORDS,
            publish_queue_bytes: DEFAULT_PUBLISH_QUEUE_BYTES,
            publish_batch_requests: DEFAULT_PUBLISH_BATCH_REQUESTS,
            publish_batch_records: DEFAULT_PUBLISH_BATCH_RECORDS,
            publish_batch_bytes: DEFAULT_PUBLISH_BATCH_BYTES,
            publish_coalesce_us: DEFAULT_PUBLISH_COALESCE_US,
            verification_enable_fault_hooks: false,
            verification_delay_group_id: None,
            verification_delay_ms: 0,
            verification_response_delay_group_id: None,
            verification_response_delay_ms: 0,
        }
    }

    #[test]
    fn local_insecure_defaults_require_loopback() {
        let mut value = args();
        value.public_listen = "0.0.0.0:7101".parse().unwrap();
        assert!(ServerConfig::try_from(value).is_err());
    }

    #[test]
    fn secured_mode_is_explicitly_unsupported() {
        let mut value = args();
        value.security_mode = "secured".to_owned();
        assert!(matches!(
            ServerConfig::try_from(value),
            Err(StartupError::SecuredModeUnsupported)
        ));
    }

    #[test]
    fn peer_routes_reject_local_duplicate_and_invalid_values() {
        for routes in [
            vec!["1=http://127.0.0.1:7301".to_owned()],
            vec![
                "2=http://127.0.0.1:7302".to_owned(),
                "2=http://127.0.0.1:7303".to_owned(),
            ],
            vec![
                "2=http://127.0.0.1:7302".to_owned(),
                "3=http://127.0.0.1:7302/".to_owned(),
            ],
            vec!["2=not-a-uri".to_owned()],
        ] {
            let mut value = args();
            value.peer_routes = routes;
            assert!(ServerConfig::try_from(value).is_err());
        }
    }

    #[test]
    fn peer_routes_reject_unknown_nodes_and_public_aliases() {
        let routes = PeerRoutes::parse(vec!["2=http://127.0.0.1:7103".to_owned()], 1).unwrap();
        let members = vec![
            NodeDescriptor::new(
                NodeId::new(1).unwrap(),
                "http://127.0.0.1:7101",
                "http://127.0.0.1:7201",
            ),
            NodeDescriptor::new(
                NodeId::new(2).unwrap(),
                "http://127.0.0.1:7102",
                "http://127.0.0.1:7202",
            ),
            NodeDescriptor::new(
                NodeId::new(3).unwrap(),
                "http://127.0.0.1:7103",
                "http://127.0.0.1:7203",
            ),
        ];
        assert!(
            routes
                .validate_topology(NodeId::new(1).unwrap(), &members)
                .is_err()
        );

        let unknown = PeerRoutes::parse(vec!["4=http://127.0.0.1:7304".to_owned()], 1).unwrap();
        assert!(
            unknown
                .validate_topology(NodeId::new(1).unwrap(), &members)
                .is_err()
        );
    }
}
