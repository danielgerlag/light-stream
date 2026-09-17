use std::{
    cmp::Ordering,
    fmt, fs,
    io::Cursor,
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use light_stream_core::{
    AuthenticatedPrincipal, CertificateFingerprint, ClusterId, CredentialGeneration, CredentialId,
    CredentialRef, DomainError, NodeId, Permission, PrincipalId, ResourceScope, SecurityMode,
    SecurityPolicy, TokenVerifierDigest,
};
use rustls::{
    CertificateError, DigitallySignedStruct, DistinguishedName, Error as RustlsError,
    RootCertStore, SignatureScheme,
    client::{
        WebPkiServerVerifier,
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    },
    pki_types::{CertificateDer, ServerName, UnixTime, pem::PemObject},
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tonic::{
    Request, Status,
    metadata::MetadataMap,
    transport::{
        Certificate, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig,
        server::{TcpConnectInfo, TlsConnectInfo},
    },
};
use x509_parser::{extensions::GeneralName, parse_x509_certificate, pem::parse_x509_pem};
use zeroize::Zeroizing;

use crate::{StartupError, manifest::DurableSecurityProfile};

const MAX_SECURITY_FILE_BYTES: u64 = 8 * 1024 * 1024;
const MAX_BEARER_BYTES: usize = 1024;
const MIN_TOKEN_SECRET_BYTES: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PeerOperation {
    AppendEntries,
    Vote,
    PreVote,
    PrepareJoin,
    Activate,
    PrepareReplacement,
    ActivateReplacement,
    RetireReplacement,
    TransferLeader,
    ProbeWriteAuthority,
    SnapshotBegin,
    SnapshotChunk,
    SnapshotFinish,
}

impl PeerOperation {
    const fn permits_stale_recovery(self) -> bool {
        matches!(
            self,
            Self::AppendEntries
                | Self::Vote
                | Self::PreVote
                | Self::SnapshotBegin
                | Self::SnapshotChunk
                | Self::SnapshotFinish
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PeerRecoveryScope {
    ControlGroup,
    None,
}

#[derive(Clone)]
pub(crate) enum RuntimeSecurityConfig {
    LocalInsecure,
    Secured(Arc<SecuredServerSecurity>),
}

impl fmt::Debug for RuntimeSecurityConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeSecurityConfig")
            .field("mode", &self.mode())
            .finish()
    }
}

impl RuntimeSecurityConfig {
    pub(crate) fn load(
        mode: SecurityMode,
        config_path: Option<&Path>,
        local_node: NodeId,
    ) -> Result<Self, StartupError> {
        match (mode, config_path) {
            (SecurityMode::LocalInsecure, None) => Ok(Self::LocalInsecure),
            (SecurityMode::LocalInsecure, Some(_)) => Err(StartupError::InvalidConfig(
                "local-insecure mode does not accept --security-config".to_owned(),
            )),
            (SecurityMode::Secured, Some(path)) => Ok(Self::Secured(Arc::new(
                SecuredServerSecurity::load(path, local_node)?,
            ))),
            (SecurityMode::Secured, None) => Err(StartupError::InvalidConfig(
                "secured mode requires --security-config".to_owned(),
            )),
        }
    }

    pub(crate) const fn mode(&self) -> SecurityMode {
        match self {
            Self::LocalInsecure => SecurityMode::LocalInsecure,
            Self::Secured(_) => SecurityMode::Secured,
        }
    }

    pub(crate) fn configured_cluster(&self) -> Option<ClusterId> {
        match self {
            Self::LocalInsecure => None,
            Self::Secured(value) => Some(value.cluster),
        }
    }

    pub(crate) fn bootstrap_policy(&self) -> Option<&SecurityPolicy> {
        match self {
            Self::LocalInsecure => None,
            Self::Secured(value) => Some(&value.bootstrap_policy),
        }
    }

    pub(crate) fn durable_profile(&self) -> DurableSecurityProfile {
        match self {
            Self::LocalInsecure => DurableSecurityProfile::LocalInsecure,
            Self::Secured(value) => DurableSecurityProfile::Secured {
                bootstrap_policy_digest: value.bootstrap_policy_digest,
                minimum_policy_revision: value.bootstrap_policy.revision(),
            },
        }
    }

    pub(crate) fn current_policy(&self) -> Result<Option<SecurityPolicy>, DomainError> {
        self.current_policy_at(Instant::now())
    }

    fn current_policy_at(&self, now: Instant) -> Result<Option<SecurityPolicy>, DomainError> {
        match self {
            Self::LocalInsecure => Ok(None),
            Self::Secured(value) => {
                let lease = value
                    .policy_lease
                    .read()
                    .map_err(|_| DomainError::Storage {
                        reason: "security policy lease lock is poisoned".to_owned(),
                    })?;
                Ok(Some(
                    lease.current(now, value.maximum_policy_staleness)?.clone(),
                ))
            }
        }
    }

    pub(crate) fn renew_policy(&self, policy: SecurityPolicy) -> Result<(), DomainError> {
        self.renew_policy_at(policy, Instant::now())
    }

    fn renew_policy_at(&self, policy: SecurityPolicy, now: Instant) -> Result<(), DomainError> {
        let Self::Secured(value) = self else {
            return Ok(());
        };
        if policy.cluster() != value.cluster {
            return Err(DomainError::IdentityMismatch {
                reason: "refreshed security policy belongs to another cluster".to_owned(),
            });
        }
        value
            .policy_lease
            .write()
            .map_err(|_| DomainError::Storage {
                reason: "security policy lease lock is poisoned".to_owned(),
            })?
            .renew(policy, now)
    }

    pub(crate) fn validate_local_peer_policy(
        &self,
        policy: &SecurityPolicy,
    ) -> Result<(), DomainError> {
        let Self::Secured(value) = self else {
            return Ok(());
        };
        policy
            .authenticate_peer(
                value.cluster,
                value.local_node,
                value.peer_certificate_fingerprint,
            )
            .map(|_| ())
    }

    pub(crate) fn validate_durable_profile(
        &self,
        profile: &DurableSecurityProfile,
    ) -> Result<(), DomainError> {
        if &self.durable_profile() == profile {
            Ok(())
        } else {
            Err(DomainError::IdentityMismatch {
                reason: "configured security mode conflicts with the durable manifest".to_owned(),
            })
        }
    }

    pub(crate) fn can_transition_from(&self, profile: &DurableSecurityProfile) -> bool {
        matches!(
            (self, profile),
            (Self::Secured(_), DurableSecurityProfile::LocalInsecure)
        )
    }

    pub(crate) fn validate_members(
        &self,
        members: &[light_stream_core::NodeDescriptor],
    ) -> Result<(), DomainError> {
        match self {
            Self::LocalInsecure => {
                if members.iter().any(|member| {
                    !member.public_uri().starts_with("http://")
                        || !member.peer_uri().starts_with("http://")
                }) {
                    return Err(DomainError::InvalidName {
                        kind: "node security profile".to_owned(),
                        reason: "local-insecure members must use http:// endpoints".to_owned(),
                    });
                }
            }
            Self::Secured(value) => {
                if members.iter().any(|member| {
                    !member.public_uri().starts_with("https://")
                        || !member.peer_uri().starts_with("https://")
                        || !value.bootstrap_policy.has_active_peer(member.node_id())
                }) {
                    return Err(DomainError::InvalidName {
                        kind: "node security profile".to_owned(),
                        reason: "secured members require https:// endpoints and active peer certificate bindings".to_owned(),
                    });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn validate_advertised_uris(
        &self,
        public_uri: &str,
        peer_uri: &str,
    ) -> Result<(), StartupError> {
        let Self::Secured(value) = self else {
            return Ok(());
        };
        certificate_covers_uri(&value.public_certificate, public_uri, "public")?;
        certificate_covers_uri(&value.peer_certificate, peer_uri, "peer")
    }

    pub(crate) fn public_tls(&self) -> Option<ServerTlsConfig> {
        match self {
            Self::LocalInsecure => None,
            Self::Secured(value) => Some(ServerTlsConfig::new().identity(Identity::from_pem(
                value.public_certificate.clone(),
                &value.public_private_key,
            ))),
        }
    }

    pub(crate) fn peer_tls(&self) -> Option<ServerTlsConfig> {
        match self {
            Self::LocalInsecure => None,
            Self::Secured(value) => Some(
                ServerTlsConfig::new()
                    .identity(Identity::from_pem(
                        value.peer_certificate.clone(),
                        &value.peer_private_key,
                    ))
                    .client_ca_root(Certificate::from_pem(value.peer_trust_roots.clone())),
            ),
        }
    }

    pub(crate) fn configure_peer_endpoint(
        &self,
        endpoint: Endpoint,
        expected_peer_uri: &str,
        target_node: NodeId,
        operation: PeerOperation,
        recovery_scope: PeerRecoveryScope,
    ) -> Result<Endpoint, String> {
        match self {
            Self::LocalInsecure => Ok(endpoint),
            Self::Secured(value) => {
                let expected = expected_peer_uri
                    .parse::<http::Uri>()
                    .map_err(|error| error.to_string())?;
                let domain = expected
                    .host()
                    .ok_or_else(|| "peer URI has no host".to_owned())?;
                let webpki = peer_webpki(&value.peer_trust_roots)?;
                endpoint
                    .tls_config_with_verifier(
                        ClientTlsConfig::new()
                            .domain_name(domain.to_owned())
                            .identity(Identity::from_pem(
                                value.peer_certificate.clone(),
                                &value.peer_private_key,
                            )),
                        Arc::new(ExactPeerServerVerifier {
                            webpki,
                            secured: value.clone(),
                            target_node,
                            operation,
                            recovery_scope,
                            expected_spiffe_uri: format!(
                                "spiffe://light-stream/cluster/{}/node/{}",
                                value.cluster,
                                target_node.get()
                            ),
                        }),
                    )
                    .map_err(|error| error.to_string())
            }
        }
    }

    pub(crate) fn authorize<A>(
        &self,
        metadata: &MetadataMap,
        policy: Option<&SecurityPolicy>,
        permission: Permission,
        resource: &ResourceScope,
        claimed_principal: Option<&PrincipalId>,
    ) -> Result<Permit<A>, Status> {
        match self {
            Self::LocalInsecure => Ok(Permit {
                principal: None,
                marker: PhantomData,
            }),
            Self::Secured(_) => {
                let policy =
                    policy.ok_or_else(|| Status::unavailable("security policy is unavailable"))?;
                let (credential, digest) = match bearer_evidence(metadata) {
                    Ok(evidence) => evidence,
                    Err(error) => {
                        security_event(
                            None,
                            None,
                            permission,
                            resource,
                            "authentication_denied",
                            policy,
                        );
                        return Err(error);
                    }
                };
                let principal = match policy.authenticate(&credential, digest) {
                    Ok(principal) => principal,
                    Err(error) => {
                        security_event(
                            None,
                            Some(&credential),
                            permission,
                            resource,
                            "authentication_denied",
                            policy,
                        );
                        return Err(authentication_status(error));
                    }
                };
                if let Err(error) = policy.authorize(principal.principal(), permission, resource) {
                    security_event(
                        Some(principal.principal()),
                        Some(&credential),
                        permission,
                        resource,
                        "authorization_denied",
                        policy,
                    );
                    return Err(authorization_status(error));
                }
                if claimed_principal.is_some_and(|claimed| claimed != principal.principal()) {
                    security_event(
                        Some(principal.principal()),
                        Some(&credential),
                        permission,
                        resource,
                        "principal_mismatch",
                        policy,
                    );
                    return Err(Status::permission_denied(
                        "request principal does not match the authenticated principal",
                    ));
                }
                security_event(
                    Some(principal.principal()),
                    Some(&credential),
                    permission,
                    resource,
                    "allowed",
                    policy,
                );
                Ok(Permit {
                    principal: Some(principal),
                    marker: PhantomData,
                })
            }
        }
    }

    pub(crate) fn reauthorize<A, B>(
        &self,
        permit: &Permit<A>,
        policy: Option<&SecurityPolicy>,
        permission: Permission,
        resource: &ResourceScope,
    ) -> Result<Permit<B>, Status> {
        match self {
            Self::LocalInsecure => Ok(Permit {
                principal: None,
                marker: PhantomData,
            }),
            Self::Secured(_) => {
                let policy =
                    policy.ok_or_else(|| Status::unavailable("security policy is unavailable"))?;
                let principal = permit
                    .principal
                    .as_ref()
                    .ok_or_else(|| Status::unauthenticated("client authentication failed"))?;
                policy
                    .validate_active_credential_binding(principal)
                    .map_err(authentication_status)?;
                policy
                    .authorize(principal.principal(), permission, resource)
                    .map_err(authorization_status)?;
                security_event(
                    Some(principal.principal()),
                    Some(principal.credential()),
                    permission,
                    resource,
                    "allowed",
                    policy,
                );
                Ok(Permit {
                    principal: Some(principal.clone()),
                    marker: PhantomData,
                })
            }
        }
    }

    pub(crate) fn authenticate_peer<T>(
        &self,
        request: &Request<T>,
        claimed_cluster: ClusterId,
        claimed_node: NodeId,
        operation: PeerOperation,
        recovery_scope: PeerRecoveryScope,
    ) -> Result<(), Status> {
        let Self::Secured(value) = self else {
            return Ok(());
        };
        let policy = value
            .policy_lease
            .read()
            .map_err(|_| Status::unavailable("security policy lease lock is poisoned"))?
            .for_peer(
                Instant::now(),
                value.maximum_policy_staleness,
                operation,
                recovery_scope,
            )
            .map_err(|error| Status::unavailable(error.to_string()))?
            .clone();
        let connect = request
            .extensions()
            .get::<TlsConnectInfo<TcpConnectInfo>>()
            .ok_or_else(|| Status::unauthenticated("peer authentication failed"))?;
        let certificates = connect
            .peer_certs()
            .ok_or_else(|| Status::unauthenticated("peer authentication failed"))?;
        let certificate = certificates
            .first()
            .ok_or_else(|| Status::unauthenticated("peer authentication failed"))?;
        let (_, parsed) = parse_x509_certificate(certificate.as_ref())
            .map_err(|_| Status::unauthenticated("peer authentication failed"))?;
        let subject = parsed
            .subject_alternative_name()
            .map_err(|_| Status::unauthenticated("peer authentication failed"))?
            .and_then(|extension| {
                extension
                    .value
                    .general_names
                    .iter()
                    .find_map(|name| match name {
                        GeneralName::URI(value)
                            if value.starts_with("spiffe://light-stream/cluster/") =>
                        {
                            Some(*value)
                        }
                        _ => None,
                    })
            })
            .ok_or_else(|| Status::unauthenticated("peer authentication failed"))?;
        let (certificate_cluster, certificate_node) = parse_peer_subject(subject)?;
        if certificate_cluster != claimed_cluster || certificate_node != claimed_node {
            return Err(Status::permission_denied(
                "peer certificate identity does not match the envelope",
            ));
        }
        policy
            .authenticate_peer(
                certificate_cluster,
                certificate_node,
                CertificateFingerprint::from_der(certificate.as_ref()),
            )
            .map_err(authentication_status)
    }
}

#[derive(Clone)]
pub(crate) struct Permit<A> {
    principal: Option<AuthenticatedPrincipal>,
    marker: PhantomData<A>,
}

impl<A> Permit<A> {
    pub(crate) fn principal(&self) -> Option<&PrincipalId> {
        self.principal
            .as_ref()
            .map(AuthenticatedPrincipal::principal)
    }
}

pub(crate) mod action {
    pub struct ClusterObserve;
    pub struct ClusterBootstrap;
    pub struct StreamDiscover;
    pub struct StreamCreate;
    pub struct StreamDescribe;
    pub struct StreamDelete;
    pub struct RouteResolve;
    pub struct Publish;
    pub struct Fetch;
    pub struct ReceiptRead;
    pub struct BookmarkRead;
    pub struct BookmarkManage;
    pub struct RetentionRead;
    pub struct RetentionManage;
    pub struct ReplayRead;
    pub struct ReplayManage;
    pub struct CheckpointRead;
    pub struct CheckpointManage;
    pub struct SnapshotManage;
    pub struct ClusterAdmin;
    pub struct SecurityObserve;
    pub struct SecurityAdmin;
}

#[cfg(test)]
const PUBLIC_RPC_NAMES: &[&str] = &[
    "Health",
    "Capabilities",
    "Bootstrap",
    "CreateStream",
    "DescribeStream",
    "ListStreams",
    "DeleteStream",
    "ResolveRoute",
    "Publish",
    "CommitPublish",
    "Fetch",
    "GetReceipt",
    "CreateBookmark",
    "ResolveBookmark",
    "DeleteBookmark",
    "ListBookmarks",
    "CreateStreamBookmark",
    "ResolveStreamBookmark",
    "DeleteStreamBookmark",
    "ListStreamBookmarks",
    "AdvanceRetention",
    "GetRetentionStatus",
    "AdmitReplayLease",
    "RenewReplayLease",
    "ReleaseReplayLease",
    "GetReplayLease",
    "FetchProtected",
    "GetCheckpoint",
    "CompareAndSetCheckpoint",
    "GetSecurityPolicy",
    "ApplySecurityMutation",
    "ActivateSecuredTransport",
    "Diagnostics",
    "SnapshotGroup",
    "ReplaceVoter",
    "TransferLeadership",
    "GetAdministration",
    "AbortAdministration",
];

#[cfg(test)]
fn rpc_handler_name(name: &str) -> String {
    let mut result = String::new();
    for (index, character) in name.chars().enumerate() {
        if character.is_ascii_uppercase() {
            if index != 0 {
                result.push('_');
            }
            result.push(character.to_ascii_lowercase());
        } else {
            result.push(character);
        }
    }
    result
}

#[cfg(test)]
fn masked_rust_source(source: &str) -> String {
    let bytes = source.as_bytes();
    let mut masked = bytes.to_vec();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index..].starts_with(b"//") {
            let start = index;
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            masked[start..index].fill(b' ');
        } else if bytes[index..].starts_with(b"/*") {
            let start = index;
            index += 2;
            let mut depth = 1_u32;
            while index < bytes.len() && depth != 0 {
                if bytes[index..].starts_with(b"/*") {
                    depth += 1;
                    index += 2;
                } else if bytes[index..].starts_with(b"*/") {
                    depth -= 1;
                    index += 2;
                } else {
                    index += 1;
                }
            }
            for byte in &mut masked[start..index] {
                if *byte != b'\n' {
                    *byte = b' ';
                }
            }
        } else if bytes[index] == b'"' {
            let start = index;
            index += 1;
            while index < bytes.len() {
                match bytes[index] {
                    b'\\' => index = (index + 2).min(bytes.len()),
                    b'"' => {
                        index += 1;
                        break;
                    }
                    _ => index += 1,
                }
            }
            for byte in &mut masked[start..index] {
                if *byte != b'\n' {
                    *byte = b' ';
                }
            }
        } else if bytes[index] == b'r' {
            let mut delimiter = index + 1;
            while delimiter < bytes.len() && bytes[delimiter] == b'#' {
                delimiter += 1;
            }
            if delimiter < bytes.len() && bytes[delimiter] == b'"' {
                let hashes = delimiter - index - 1;
                let start = index;
                index = delimiter + 1;
                while index < bytes.len() {
                    if bytes[index] == b'"'
                        && index + 1 + hashes <= bytes.len()
                        && bytes[index + 1..index + 1 + hashes]
                            .iter()
                            .all(|byte| *byte == b'#')
                    {
                        index += hashes + 1;
                        break;
                    }
                    index += 1;
                }
                for byte in &mut masked[start..index] {
                    if *byte != b'\n' {
                        *byte = b' ';
                    }
                }
            } else {
                index += 1;
            }
        } else {
            index += 1;
        }
    }
    String::from_utf8(masked).expect("Rust source is UTF-8")
}

#[cfg(test)]
fn braced_body<'a>(source: &'a str, masked: &str, open: usize) -> Option<&'a str> {
    let mut depth = 0_u32;
    for (offset, byte) in masked.as_bytes()[open..].iter().enumerate() {
        match byte {
            b'{' => depth += 1,
            b'}' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(&source[open + 1..open + offset]);
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
fn public_handler_body<'a>(source: &'a str, handler: &str) -> Option<&'a str> {
    let masked = masked_rust_source(source);
    let implementation = masked.find("impl LightStream for PublicApi")?;
    let implementation_open = masked[implementation..].find('{')? + implementation;
    let implementation_body = braced_body(source, &masked, implementation_open)?;
    let body_start = implementation_open + 1;
    let implementation_masked = &masked[body_start..body_start + implementation_body.len()];
    let signature = format!("async fn {handler}(");
    let method = implementation_masked.find(&signature)? + body_start;
    let method_open = masked[method..].find('{')? + method;
    braced_body(source, &masked, method_open)
}

#[cfg(test)]
fn has_direct_admission(body: &str) -> bool {
    let compact = masked_rust_source(body)
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect::<Vec<_>>();
    compact
        .windows(b"self.admit::<action::".len())
        .any(|window| window == b"self.admit::<action::")
}

pub(crate) struct SecuredServerSecurity {
    cluster: ClusterId,
    local_node: NodeId,
    public_certificate: Vec<u8>,
    public_private_key: Zeroizing<Vec<u8>>,
    peer_certificate: Vec<u8>,
    peer_certificate_fingerprint: CertificateFingerprint,
    peer_private_key: Zeroizing<Vec<u8>>,
    peer_trust_roots: Vec<u8>,
    bootstrap_policy: SecurityPolicy,
    bootstrap_policy_digest: [u8; 32],
    maximum_policy_staleness: Duration,
    policy_lease: RwLock<PolicyLease>,
}

struct PolicyLease {
    policy: SecurityPolicy,
    confirmed_at: Instant,
}

impl PolicyLease {
    fn current(
        &self,
        now: Instant,
        maximum_policy_staleness: Duration,
    ) -> Result<&SecurityPolicy, DomainError> {
        let fresh_until = policy_deadline(self.confirmed_at, maximum_policy_staleness)?;
        if now < fresh_until {
            Ok(&self.policy)
        } else {
            Err(DomainError::SecurityPolicyStale)
        }
    }

    fn for_peer(
        &self,
        now: Instant,
        maximum_policy_staleness: Duration,
        operation: PeerOperation,
        recovery_scope: PeerRecoveryScope,
    ) -> Result<&SecurityPolicy, DomainError> {
        let fresh_until = policy_deadline(self.confirmed_at, maximum_policy_staleness)?;
        if now < fresh_until {
            return Ok(&self.policy);
        }
        let recovery_until = policy_deadline(fresh_until, maximum_policy_staleness)?;
        if now < recovery_until
            && operation.permits_stale_recovery()
            && recovery_scope == PeerRecoveryScope::ControlGroup
        {
            Ok(&self.policy)
        } else {
            Err(DomainError::SecurityPolicyStale)
        }
    }

    fn renew(&mut self, policy: SecurityPolicy, now: Instant) -> Result<(), DomainError> {
        match policy.revision().cmp(&self.policy.revision()) {
            Ordering::Less => return Err(DomainError::SecurityPolicyConflict),
            Ordering::Equal if policy != self.policy => {
                return Err(DomainError::SecurityPolicyConflict);
            }
            Ordering::Equal | Ordering::Greater => {}
        }
        self.policy = policy;
        self.confirmed_at = self.confirmed_at.max(now);
        Ok(())
    }
}

fn policy_deadline(confirmed_at: Instant, interval: Duration) -> Result<Instant, DomainError> {
    confirmed_at
        .checked_add(interval)
        .ok_or_else(|| DomainError::Storage {
            reason: "security policy lease deadline overflow".to_owned(),
        })
}

fn peer_webpki(peer_trust_roots: &[u8]) -> Result<Arc<WebPkiServerVerifier>, String> {
    let mut root_store = RootCertStore::empty();
    let mut root_count = 0;
    for root in CertificateDer::pem_reader_iter(Cursor::new(peer_trust_roots)) {
        root_store
            .add(root.map_err(|error| format!("peer trust roots contain invalid PEM: {error}"))?)
            .map_err(|error| format!("peer trust roots contain an invalid certificate: {error}"))?;
        root_count += 1;
    }
    if root_count == 0 {
        return Err("peer trust roots contain no certificates".to_owned());
    }
    WebPkiServerVerifier::builder_with_provider(
        Arc::new(root_store),
        Arc::new(rustls::crypto::ring::default_provider()),
    )
    .build()
    .map_err(|error| format!("peer trust roots are invalid: {error}"))
}

struct ExactPeerServerVerifier {
    webpki: Arc<WebPkiServerVerifier>,
    secured: Arc<SecuredServerSecurity>,
    target_node: NodeId,
    operation: PeerOperation,
    recovery_scope: PeerRecoveryScope,
    expected_spiffe_uri: String,
}

impl fmt::Debug for ExactPeerServerVerifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExactPeerServerVerifier")
            .field("cluster", &self.secured.cluster)
            .field("target_node", &self.target_node)
            .finish()
    }
}

impl ServerCertVerifier for ExactPeerServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        self.webpki.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        )?;
        let (_, certificate) = parse_x509_certificate(end_entity.as_ref())
            .map_err(|_| RustlsError::InvalidCertificate(CertificateError::BadEncoding))?;
        let names = certificate
            .subject_alternative_name()
            .map_err(|_| RustlsError::InvalidCertificate(CertificateError::BadEncoding))?
            .ok_or_else(peer_certificate_rejected)?;
        let light_stream_uris = names
            .value
            .general_names
            .iter()
            .filter_map(|name| match name {
                GeneralName::URI(value) if value.starts_with("spiffe://light-stream/cluster/") => {
                    Some(*value)
                }
                _ => None,
            });
        let policy = self
            .secured
            .policy_lease
            .read()
            .map_err(|_| peer_certificate_rejected())?;
        verify_peer_leaf_binding(
            policy
                .for_peer(
                    Instant::now(),
                    self.secured.maximum_policy_staleness,
                    self.operation,
                    self.recovery_scope,
                )
                .map_err(|_| peer_certificate_rejected())?,
            self.secured.cluster,
            self.target_node,
            &self.expected_spiffe_uri,
            light_stream_uris,
            end_entity.as_ref(),
        )?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        self.webpki.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        self.webpki.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.webpki.supported_verify_schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        self.webpki.requires_raw_public_keys()
    }

    fn root_hint_subjects(&self) -> Option<&[DistinguishedName]> {
        self.webpki.root_hint_subjects()
    }
}

fn peer_certificate_rejected() -> RustlsError {
    RustlsError::InvalidCertificate(CertificateError::ApplicationVerificationFailure)
}

fn verify_peer_leaf_binding<'a>(
    policy: &SecurityPolicy,
    cluster: ClusterId,
    target_node: NodeId,
    expected_spiffe_uri: &str,
    light_stream_uris: impl IntoIterator<Item = &'a str>,
    leaf_der: &[u8],
) -> Result<(), RustlsError> {
    let mut light_stream_uris = light_stream_uris.into_iter();
    if light_stream_uris.next() != Some(expected_spiffe_uri) || light_stream_uris.next().is_some() {
        return Err(peer_certificate_rejected());
    }
    policy
        .authenticate_peer(
            cluster,
            target_node,
            CertificateFingerprint::from_der(leaf_der),
        )
        .map_err(|_| peer_certificate_rejected())?;
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SecurityConfigFile {
    version: u32,
    cluster_id: ClusterId,
    public_tls: ServerIdentityPaths,
    peer_tls: PeerIdentityPaths,
    bootstrap_policy_file: PathBuf,
    maximum_policy_staleness_ms: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerIdentityPaths {
    certificate_chain_file: PathBuf,
    private_key_file: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PeerIdentityPaths {
    certificate_chain_file: PathBuf,
    private_key_file: PathBuf,
    trust_roots_file: PathBuf,
}

impl SecuredServerSecurity {
    fn load(path: &Path, local_node: NodeId) -> Result<Self, StartupError> {
        let config_bytes = read_file(path, false)?;
        let config: SecurityConfigFile =
            serde_json::from_slice(&config_bytes).map_err(|error| {
                StartupError::InvalidConfig(format!(
                    "security config {} is invalid: {error}",
                    path.display()
                ))
            })?;
        if config.version != 1 {
            return Err(StartupError::InvalidConfig(format!(
                "security config version {} is unsupported",
                config.version
            )));
        }
        if config.maximum_policy_staleness_ms == 0 || config.maximum_policy_staleness_ms > 5_000 {
            return Err(StartupError::InvalidConfig(
                "maximum policy staleness must be between 1 and 5000 ms".to_owned(),
            ));
        }
        for path in [
            &config.public_tls.certificate_chain_file,
            &config.public_tls.private_key_file,
            &config.peer_tls.certificate_chain_file,
            &config.peer_tls.private_key_file,
            &config.peer_tls.trust_roots_file,
            &config.bootstrap_policy_file,
        ] {
            if !path.is_absolute() {
                return Err(StartupError::InvalidConfig(format!(
                    "security material path {} must be absolute",
                    path.display()
                )));
            }
        }
        let bootstrap_bytes = read_file(&config.bootstrap_policy_file, false)?;
        let bootstrap_policy: SecurityPolicy =
            serde_json::from_slice(&bootstrap_bytes).map_err(|error| {
                StartupError::InvalidConfig(format!(
                    "bootstrap security policy {} is invalid: {error}",
                    config.bootstrap_policy_file.display()
                ))
            })?;
        if bootstrap_policy.cluster() != config.cluster_id {
            return Err(StartupError::InvalidConfig(
                "bootstrap security policy cluster does not match security config".to_owned(),
            ));
        }
        let bootstrap_policy_digest = Sha256::digest(&bootstrap_bytes).into();
        let maximum_policy_staleness = Duration::from_millis(config.maximum_policy_staleness_ms);
        let peer_certificate = read_file(&config.peer_tls.certificate_chain_file, false)?;
        let (_, peer_pem) = parse_x509_pem(&peer_certificate).map_err(|_| {
            StartupError::InvalidConfig(
                "peer certificate does not contain a valid PEM certificate".to_owned(),
            )
        })?;
        let (_, peer_x509) = parse_x509_certificate(&peer_pem.contents).map_err(|_| {
            StartupError::InvalidConfig("peer certificate is not valid X.509".to_owned())
        })?;
        let peer_subject = peer_x509
            .subject_alternative_name()
            .map_err(|_| {
                StartupError::InvalidConfig(
                    "peer certificate subject alternative name is invalid".to_owned(),
                )
            })?
            .and_then(|extension| {
                extension
                    .value
                    .general_names
                    .iter()
                    .find_map(|name| match name {
                        GeneralName::URI(value)
                            if value.starts_with("spiffe://light-stream/cluster/") =>
                        {
                            Some(*value)
                        }
                        _ => None,
                    })
            })
            .ok_or_else(|| {
                StartupError::InvalidConfig(
                    "peer certificate lacks a Light Stream URI identity".to_owned(),
                )
            })?;
        let (certificate_cluster, certificate_node) =
            parse_peer_subject(peer_subject).map_err(|_| {
                StartupError::InvalidConfig(
                    "peer certificate has an invalid Light Stream URI identity".to_owned(),
                )
            })?;
        if certificate_cluster != config.cluster_id || certificate_node != local_node {
            return Err(StartupError::InvalidConfig(
                "peer certificate identity conflicts with configured cluster or node".to_owned(),
            ));
        }
        let peer_certificate_fingerprint = CertificateFingerprint::from_der(&peer_pem.contents);
        let peer_trust_roots = read_file(&config.peer_tls.trust_roots_file, false)?;
        peer_webpki(&peer_trust_roots).map_err(StartupError::InvalidConfig)?;
        let has_bootstrap_administrator = bootstrap_policy
            .token_verifiers()
            .iter()
            .filter(|verifier| verifier.status() == light_stream_core::CredentialStatus::Active)
            .any(|verifier| {
                [
                    Permission::ClusterBootstrap,
                    Permission::ClusterAdmin,
                    Permission::SecurityAdmin,
                ]
                .into_iter()
                .all(|permission| {
                    bootstrap_policy
                        .authorize(
                            verifier.principal(),
                            permission,
                            &ResourceScope::Cluster {
                                cluster: config.cluster_id,
                            },
                        )
                        .is_ok()
                })
            });
        if !has_bootstrap_administrator {
            return Err(StartupError::InvalidConfig(
                "bootstrap policy requires one active cluster and security administrator"
                    .to_owned(),
            ));
        }
        Ok(Self {
            cluster: config.cluster_id,
            local_node,
            public_certificate: read_file(&config.public_tls.certificate_chain_file, false)?,
            public_private_key: Zeroizing::new(read_file(
                &config.public_tls.private_key_file,
                true,
            )?),
            peer_certificate,
            peer_certificate_fingerprint,
            peer_private_key: Zeroizing::new(read_file(&config.peer_tls.private_key_file, true)?),
            peer_trust_roots,
            policy_lease: RwLock::new(PolicyLease {
                policy: bootstrap_policy.clone(),
                confirmed_at: Instant::now(),
            }),
            bootstrap_policy,
            bootstrap_policy_digest,
            maximum_policy_staleness,
        })
    }
}

fn read_file(path: &Path, secret: bool) -> Result<Vec<u8>, StartupError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        StartupError::InvalidConfig(format!(
            "security material {} is unavailable: {error}",
            path.display()
        ))
    })?;
    if !metadata.file_type().is_file() {
        return Err(StartupError::InvalidConfig(format!(
            "security material {} must be a regular file",
            path.display()
        )));
    }

    if metadata.len() > MAX_SECURITY_FILE_BYTES {
        return Err(StartupError::InvalidConfig(format!(
            "security material {} exceeds {} bytes",
            path.display(),
            MAX_SECURITY_FILE_BYTES
        )));
    }
    #[cfg(unix)]
    {
        // SAFETY: geteuid has no preconditions and does not dereference pointers.
        let current_user = unsafe { libc::geteuid() };
        if metadata.uid() != current_user {
            return Err(StartupError::InvalidConfig(format!(
                "security material {} must be owned by the service user",
                path.display()
            )));
        }
        if metadata.mode() & 0o022 != 0 {
            return Err(StartupError::InvalidConfig(format!(
                "security material {} must not be group or world writable",
                path.display()
            )));
        }
        if secret && metadata.mode() & 0o077 != 0 {
            return Err(StartupError::InvalidConfig(format!(
                "secret file {} must not grant group or other permissions",
                path.display()
            )));
        }
    }
    fs::read(path).map_err(|error| {
        StartupError::InvalidConfig(format!(
            "security material {} could not be read: {error}",
            path.display()
        ))
    })
}

fn certificate_covers_uri(
    certificate_pem: &[u8],
    uri: &str,
    role: &str,
) -> Result<(), StartupError> {
    let uri = uri.parse::<http::Uri>().map_err(|error| {
        StartupError::InvalidConfig(format!("advertised {role} URI is invalid: {error}"))
    })?;
    let host = uri
        .host()
        .ok_or_else(|| StartupError::InvalidConfig(format!("advertised {role} URI has no host")))?;
    let (_, pem) = parse_x509_pem(certificate_pem)
        .map_err(|_| StartupError::InvalidConfig(format!("{role} certificate is not valid PEM")))?;
    let (_, certificate) = parse_x509_certificate(&pem.contents).map_err(|_| {
        StartupError::InvalidConfig(format!("{role} certificate is not valid X.509"))
    })?;
    let names = certificate
        .subject_alternative_name()
        .map_err(|_| {
            StartupError::InvalidConfig(format!(
                "{role} certificate subject alternative name is invalid"
            ))
        })?
        .ok_or_else(|| {
            StartupError::InvalidConfig(format!(
                "{role} certificate has no subject alternative name"
            ))
        })?;
    let matches = names.value.general_names.iter().any(|name| match name {
        GeneralName::DNSName(value) => value.eq_ignore_ascii_case(host),
        GeneralName::IPAddress(bytes) => {
            host.parse::<std::net::IpAddr>()
                .ok()
                .is_some_and(|address| match address {
                    std::net::IpAddr::V4(address) => address.octets().as_slice() == *bytes,
                    std::net::IpAddr::V6(address) => address.octets().as_slice() == *bytes,
                })
        }
        _ => false,
    });
    if matches {
        Ok(())
    } else {
        Err(StartupError::InvalidConfig(format!(
            "{role} certificate does not cover advertised host {host}"
        )))
    }
}

fn bearer_evidence(metadata: &MetadataMap) -> Result<(CredentialRef, TokenVerifierDigest), Status> {
    let header = metadata
        .get("authorization")
        .ok_or_else(|| Status::unauthenticated("client authentication failed"))?
        .to_str()
        .map_err(|_| Status::unauthenticated("client authentication failed"))?;
    let token = header
        .strip_prefix("Bearer ")
        .ok_or_else(|| Status::unauthenticated("client authentication failed"))?;
    if token.len() > MAX_BEARER_BYTES {
        return Err(Status::unauthenticated("client authentication failed"));
    }
    let mut parts = token.splitn(4, '.');
    let prefix = parts.next();
    let id = parts.next();
    let generation = parts.next();
    let secret = parts.next();
    let decoded_secret = secret
        .and_then(|value| URL_SAFE_NO_PAD.decode(value).ok())
        .filter(|value| value.len() >= MIN_TOKEN_SECRET_BYTES);
    if prefix != Some("ls1") || decoded_secret.is_none() {
        return Err(Status::unauthenticated("client authentication failed"));
    }
    let credential = CredentialRef::new(
        CredentialId::parse(
            id.ok_or_else(|| Status::unauthenticated("client authentication failed"))?,
        )
        .map_err(authentication_status)?,
        CredentialGeneration::new(
            generation
                .ok_or_else(|| Status::unauthenticated("client authentication failed"))?
                .parse()
                .map_err(|_| Status::unauthenticated("client authentication failed"))?,
        )
        .map_err(authentication_status)?,
    );
    Ok((
        credential,
        TokenVerifierDigest::from_token_bytes(token.as_bytes()),
    ))
}

fn authentication_status(_: DomainError) -> Status {
    Status::unauthenticated("client authentication failed")
}

fn authorization_status(error: DomainError) -> Status {
    match error {
        DomainError::SecurityPolicyStale => Status::unavailable(error.to_string()),
        _ => Status::permission_denied("client is not authorized for this operation"),
    }
}

fn security_event(
    principal: Option<&PrincipalId>,
    credential: Option<&CredentialRef>,
    permission: Permission,
    resource: &ResourceScope,
    result: &str,
    policy: &SecurityPolicy,
) {
    eprintln!(
        "{}",
        serde_json::json!({
            "event": "security_decision",
            "principal": principal.map(ToString::to_string),
            "credential_id": credential.map(|value| value.id().to_string()),
            "credential_generation": credential.map(|value| value.generation().get()),
            "permission": permission,
            "resource": resource,
            "result": result,
            "policy_revision": policy.revision().get(),
            "revocation_revision": policy.revocation_revision().get(),
        })
    );
}

fn parse_peer_subject(value: &str) -> Result<(ClusterId, NodeId), Status> {
    let value = value
        .strip_prefix("spiffe://light-stream/cluster/")
        .ok_or_else(|| Status::unauthenticated("peer authentication failed"))?;
    let (cluster, node) = value
        .split_once("/node/")
        .ok_or_else(|| Status::unauthenticated("peer authentication failed"))?;
    Ok((
        cluster
            .parse()
            .map_err(|_| Status::unauthenticated("peer authentication failed"))?,
        NodeId::new(
            node.parse()
                .map_err(|_| Status::unauthenticated("peer authentication failed"))?,
        )
        .map_err(authentication_status)?,
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use light_stream_core::{
        CredentialStatus, Grant, PeerCertificateBinding, Permission, PolicyRevision, ResourceScope,
        RevocationRevision, TokenVerifier,
    };
    use tonic::metadata::MetadataValue;
    use uuid::Uuid;

    use super::*;

    fn policy_with_revision(
        cluster: ClusterId,
        revision: u64,
        principal_name: &str,
        extra_grant: bool,
    ) -> SecurityPolicy {
        let principal = PrincipalId::parse(principal_name).unwrap();
        let administrator = PrincipalId::parse("lease-admin").unwrap();
        let credential = CredentialRef::new(
            CredentialId::parse("writer").unwrap(),
            CredentialGeneration::initial(),
        );
        let mut grants = BTreeSet::from([
            Grant::new(Permission::Publish, ResourceScope::AllStreams { cluster }),
            Grant::new(
                Permission::SecurityAdmin,
                ResourceScope::Cluster { cluster },
            ),
        ]);
        if extra_grant {
            grants.insert(Grant::new(
                Permission::ClusterObserve,
                ResourceScope::Cluster { cluster },
            ));
        }
        SecurityPolicy::try_new(
            cluster,
            PolicyRevision::new(revision).unwrap(),
            RevocationRevision::default(),
            BTreeMap::from([
                (principal.clone(), grants),
                (
                    administrator.clone(),
                    BTreeSet::from([Grant::new(
                        Permission::SecurityAdmin,
                        ResourceScope::Cluster { cluster },
                    )]),
                ),
            ]),
            vec![
                TokenVerifier::new(
                    credential,
                    principal,
                    TokenVerifierDigest::from_token_bytes(
                        b"ls1.writer.1.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                    ),
                ),
                TokenVerifier::new(
                    CredentialRef::new(
                        CredentialId::parse("lease-admin").unwrap(),
                        CredentialGeneration::initial(),
                    ),
                    administrator,
                    TokenVerifierDigest::from_token_bytes(
                        b"ls1.lease-admin.1.BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
                    ),
                ),
            ],
            BTreeMap::new(),
        )
        .unwrap()
    }

    fn secured_runtime(
        policy: SecurityPolicy,
        confirmed_at: Instant,
        maximum_policy_staleness: Duration,
    ) -> RuntimeSecurityConfig {
        RuntimeSecurityConfig::Secured(Arc::new(SecuredServerSecurity {
            cluster: policy.cluster(),
            local_node: NodeId::new(1).unwrap(),
            public_certificate: Vec::new(),
            public_private_key: Zeroizing::new(Vec::new()),
            peer_certificate: Vec::new(),
            peer_certificate_fingerprint: CertificateFingerprint::from_der(&[]),
            peer_private_key: Zeroizing::new(Vec::new()),
            peer_trust_roots: Vec::new(),
            bootstrap_policy: policy.clone(),
            bootstrap_policy_digest: Sha256::digest(serde_json::to_vec(&policy).unwrap()).into(),
            maximum_policy_staleness,
            policy_lease: RwLock::new(PolicyLease {
                policy,
                confirmed_at,
            }),
        }))
    }

    #[test]
    fn bearer_authentication_never_returns_secret_details() {
        let cluster = ClusterId::from_uuid(Uuid::new_v4());
        let principal = PrincipalId::parse("writer").unwrap();
        let token = "ls1.writer.1.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        let credential = CredentialRef::new(
            CredentialId::parse("writer").unwrap(),
            CredentialGeneration::initial(),
        );
        let policy = SecurityPolicy::try_new(
            cluster,
            PolicyRevision::initial(),
            RevocationRevision::default(),
            BTreeMap::from([(
                principal.clone(),
                BTreeSet::from([
                    Grant::new(Permission::Publish, ResourceScope::AllStreams { cluster }),
                    Grant::new(
                        Permission::SecurityAdmin,
                        ResourceScope::Cluster { cluster },
                    ),
                ]),
            )]),
            vec![TokenVerifier::new(
                credential,
                principal.clone(),
                TokenVerifierDigest::from_token_bytes(token.as_bytes()),
            )],
            BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(
            policy.token_verifiers()[0].status(),
            CredentialStatus::Active
        );
        let runtime = secured_runtime(policy.clone(), Instant::now(), Duration::from_secs(5));
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
        );
        let permit = runtime
            .authorize::<action::Publish>(
                &metadata,
                Some(&policy),
                Permission::Publish,
                &ResourceScope::Stream {
                    cluster,
                    stream: light_stream_core::StreamId::from_uuid(Uuid::new_v4()),
                },
                Some(&principal),
            )
            .unwrap();
        assert_eq!(permit.principal(), Some(&principal));

        metadata.insert(
            "authorization",
            MetadataValue::try_from("Bearer ls1.writer.1.XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
                .unwrap(),
        );
        let error = runtime
            .authorize::<action::Publish>(
                &metadata,
                Some(&policy),
                Permission::Publish,
                &ResourceScope::AllStreams { cluster },
                None,
            )
            .err()
            .unwrap();
        assert_eq!(error.code(), tonic::Code::Unauthenticated);
        assert!(!error.message().contains("XXXXX"));
    }

    #[test]
    fn peer_recovery_is_operation_scope_and_time_bounded() {
        let confirmed_at = Instant::now();
        let maximum = Duration::from_secs(5);
        let lease = PolicyLease {
            policy: policy_with_revision(ClusterId::from_uuid(Uuid::new_v4()), 1, "writer", false),
            confirmed_at,
        };
        let fresh_until = confirmed_at + maximum;
        let recovery_until = fresh_until + maximum;
        let operations = [
            (PeerOperation::AppendEntries, true),
            (PeerOperation::Vote, true),
            (PeerOperation::PreVote, true),
            (PeerOperation::PrepareJoin, false),
            (PeerOperation::Activate, false),
            (PeerOperation::PrepareReplacement, false),
            (PeerOperation::ActivateReplacement, false),
            (PeerOperation::RetireReplacement, false),
            (PeerOperation::TransferLeader, false),
            (PeerOperation::ProbeWriteAuthority, false),
            (PeerOperation::SnapshotBegin, true),
            (PeerOperation::SnapshotChunk, true),
            (PeerOperation::SnapshotFinish, true),
        ];

        for (operation, recoverable) in operations {
            for scope in [PeerRecoveryScope::ControlGroup, PeerRecoveryScope::None] {
                assert!(
                    lease
                        .for_peer(
                            fresh_until - Duration::from_nanos(1),
                            maximum,
                            operation,
                            scope,
                        )
                        .is_ok()
                );
                assert_eq!(
                    lease
                        .for_peer(fresh_until, maximum, operation, scope)
                        .is_ok(),
                    recoverable && scope == PeerRecoveryScope::ControlGroup
                );
                assert_eq!(
                    lease
                        .for_peer(
                            recovery_until - Duration::from_nanos(1),
                            maximum,
                            operation,
                            scope,
                        )
                        .is_ok(),
                    recoverable && scope == PeerRecoveryScope::ControlGroup
                );
                assert!(matches!(
                    lease.for_peer(recovery_until, maximum, operation, scope),
                    Err(DomainError::SecurityPolicyStale)
                ));
            }
        }
        assert!(matches!(
            lease.current(fresh_until, maximum),
            Err(DomainError::SecurityPolicyStale)
        ));
    }

    #[test]
    fn peer_leaf_binding_requires_exact_target_identity_and_active_fingerprint() {
        let cluster = ClusterId::from_uuid(Uuid::new_v4());
        let target = NodeId::new(2).unwrap();
        let leaf_der = b"target-node-leaf";
        let base = policy_with_revision(cluster, 1, "writer", false);
        let policy = SecurityPolicy::try_new(
            cluster,
            base.revision(),
            base.revocation_revision(),
            base.grants().clone(),
            base.token_verifiers().to_vec(),
            BTreeMap::from([(
                target,
                BTreeSet::from([PeerCertificateBinding::new(
                    cluster,
                    target,
                    CredentialGeneration::initial(),
                    CertificateFingerprint::from_der(leaf_der),
                )]),
            )]),
        )
        .unwrap();
        let expected = format!(
            "spiffe://light-stream/cluster/{cluster}/node/{}",
            target.get()
        );

        verify_peer_leaf_binding(
            &policy,
            cluster,
            target,
            &expected,
            [expected.as_str()],
            leaf_der,
        )
        .unwrap();
        assert!(
            verify_peer_leaf_binding(
                &policy,
                cluster,
                target,
                &expected,
                ["spiffe://light-stream/cluster/00000000-0000-0000-0000-000000000000/node/2"],
                leaf_der,
            )
            .is_err()
        );
        assert!(
            verify_peer_leaf_binding(
                &policy,
                cluster,
                target,
                &expected,
                [expected.as_str(), expected.as_str()],
                leaf_der,
            )
            .is_err()
        );
        assert!(
            verify_peer_leaf_binding(
                &policy,
                cluster,
                target,
                &expected,
                [expected.as_str()],
                b"other-leaf",
            )
            .is_err()
        );
    }

    #[test]
    fn policy_renewal_is_monotone_and_rejections_do_not_extend_freshness() {
        let cluster = ClusterId::from_uuid(Uuid::new_v4());
        let initial = policy_with_revision(cluster, 2, "writer", false);
        let confirmed_at = Instant::now();
        let runtime = secured_runtime(initial.clone(), confirmed_at, Duration::from_secs(5));

        assert_eq!(
            runtime
                .renew_policy_at(
                    policy_with_revision(cluster, 1, "writer", false),
                    confirmed_at + Duration::from_secs(1),
                )
                .unwrap_err(),
            DomainError::SecurityPolicyConflict
        );
        assert_eq!(
            runtime
                .renew_policy_at(
                    policy_with_revision(cluster, 2, "writer", true),
                    confirmed_at + Duration::from_secs(2),
                )
                .unwrap_err(),
            DomainError::SecurityPolicyConflict
        );
        let RuntimeSecurityConfig::Secured(secured) = &runtime else {
            unreachable!()
        };
        {
            let lease = secured.policy_lease.read().unwrap();
            assert_eq!(lease.policy, initial);
            assert_eq!(lease.confirmed_at, confirmed_at);
        }

        let identical_at = confirmed_at + Duration::from_secs(3);
        runtime
            .renew_policy_at(initial.clone(), identical_at)
            .unwrap();
        {
            let lease = secured.policy_lease.read().unwrap();
            assert_eq!(lease.policy, initial);
            assert_eq!(lease.confirmed_at, identical_at);
        }

        let newer = policy_with_revision(cluster, 3, "writer", true);
        let newer_at = confirmed_at + Duration::from_secs(4);
        runtime.renew_policy_at(newer.clone(), newer_at).unwrap();
        let delayed = policy_with_revision(cluster, 4, "writer", true);
        runtime
            .renew_policy_at(delayed.clone(), confirmed_at + Duration::from_secs(2))
            .unwrap();
        let lease = secured.policy_lease.read().unwrap();
        assert_eq!(lease.policy, delayed);
        assert_eq!(lease.confirmed_at, newer_at);
    }

    #[test]
    fn reauthorization_rejects_revoked_or_rebound_credential_generation() {
        let cluster = ClusterId::from_uuid(Uuid::new_v4());
        let token = b"ls1.writer.1.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        let policy = policy_with_revision(cluster, 1, "writer", false);
        let credential = CredentialRef::new(
            CredentialId::parse("writer").unwrap(),
            CredentialGeneration::initial(),
        );
        let authenticated = policy
            .authenticate(&credential, TokenVerifierDigest::from_token_bytes(token))
            .unwrap();
        let runtime = secured_runtime(policy.clone(), Instant::now(), Duration::from_secs(5));
        let permit = Permit::<action::StreamDiscover> {
            principal: Some(authenticated),
            marker: PhantomData,
        };
        let principal = PrincipalId::parse("writer").unwrap();
        let revoked = policy
            .apply(light_stream_core::SecurityMutation::new(
                light_stream_core::MutationRequestId::new(
                    principal,
                    light_stream_core::MutationSessionId::from_uuid(Uuid::new_v4()),
                    light_stream_core::RequestSequence::new(1),
                ),
                policy.revision(),
                light_stream_core::SecurityChange::RevokeTokenGeneration {
                    credential: credential.clone(),
                },
            ))
            .unwrap();
        let resource = ResourceScope::AllStreams { cluster };
        assert_eq!(
            runtime
                .reauthorize::<action::StreamDiscover, action::StreamDescribe>(
                    &permit,
                    Some(&revoked),
                    Permission::Publish,
                    &resource,
                )
                .err()
                .unwrap()
                .code(),
            tonic::Code::Unauthenticated
        );

        let replacement = PrincipalId::parse("replacement").unwrap();
        let rebound = SecurityPolicy::try_new(
            cluster,
            policy.revision(),
            policy.revocation_revision(),
            BTreeMap::from([(
                replacement.clone(),
                BTreeSet::from([
                    Grant::new(Permission::Publish, ResourceScope::AllStreams { cluster }),
                    Grant::new(
                        Permission::SecurityAdmin,
                        ResourceScope::Cluster { cluster },
                    ),
                ]),
            )]),
            vec![TokenVerifier::new(
                credential,
                replacement,
                TokenVerifierDigest::from_token_bytes(token),
            )],
            BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(
            runtime
                .reauthorize::<action::StreamDiscover, action::StreamDescribe>(
                    &permit,
                    Some(&rebound),
                    Permission::Publish,
                    &resource,
                )
                .err()
                .unwrap()
                .code(),
            tonic::Code::Unauthenticated
        );
    }

    #[test]
    fn every_public_rpc_has_an_authorization_policy() {
        let proto = include_str!("../../../proto/lightstream/v1/public.proto");
        let source = include_str!("service.rs");
        let service = proto
            .split_once("service LightStream {")
            .expect("public service exists")
            .1
            .split_once('}')
            .expect("public service closes")
            .0;
        let declared = service
            .lines()
            .filter_map(|line| {
                line.trim()
                    .strip_prefix("rpc ")
                    .and_then(|line| line.split_once('('))
                    .map(|(name, _)| name)
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            declared,
            PUBLIC_RPC_NAMES.iter().copied().collect::<BTreeSet<_>>()
        );
        for rpc in PUBLIC_RPC_NAMES {
            let handler = rpc_handler_name(rpc);
            let body = public_handler_body(source, &handler)
                .unwrap_or_else(|| panic!("public handler {handler} is missing"));
            assert!(
                has_direct_admission(body),
                "public handler {handler} must directly call self.admit::<action::...>"
            );
        }
    }

    #[test]
    fn handler_scanner_ignores_admission_text_in_comments_and_strings() {
        let source = r#"
            impl LightStream for PublicApi {
                async fn health(&self) {
                    let example = "} self.admit::<action::ClusterObserve, _>(";
                    // } self.admit::<action::ClusterObserve, _>(
                    helper();
                }
            }
        "#;
        let body = public_handler_body(source, "health").unwrap();
        assert!(!has_direct_admission(body));
    }
}
