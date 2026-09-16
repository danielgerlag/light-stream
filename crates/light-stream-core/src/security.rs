use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::NonZeroU64,
    str::FromStr,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::{
    ClusterId, DomainError, MutationRequestId, NodeId, PrincipalId, ProducerRequestId, StreamId,
};

const MAX_CREDENTIAL_ID_BYTES: usize = 128;
const MAX_SECURITY_PRINCIPALS: usize = 4_096;
const MAX_SECURITY_GRANTS: usize = 65_536;
const MAX_TOKEN_VERIFIERS: usize = 16_384;
const MAX_PEER_CERTIFICATES: usize = 4_096;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SecurityMode {
    LocalInsecure,
    Secured,
}

impl FromStr for SecurityMode {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "local-insecure" => Ok(Self::LocalInsecure),
            "secured" => Ok(Self::Secured),
            _ => Err(DomainError::InvalidName {
                kind: "security mode".to_owned(),
                reason: format!("unknown value {value:?}"),
            }),
        }
    }
}

impl fmt::Display for SecurityMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocalInsecure => formatter.write_str("local-insecure"),
            Self::Secured => formatter.write_str("secured"),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PolicyRevision(NonZeroU64);

impl PolicyRevision {
    pub fn new(value: u64) -> Result<Self, DomainError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or_else(|| DomainError::InvalidRange {
                reason: "security policy revision must be greater than zero".to_owned(),
            })
    }

    pub const fn initial() -> Self {
        Self(NonZeroU64::MIN)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }

    pub fn checked_next(self) -> Result<Self, DomainError> {
        self.get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .map(Self)
            .ok_or_else(|| DomainError::InvalidRange {
                reason: "security policy revision overflow".to_owned(),
            })
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct RevocationRevision(u64);

impl RevocationRevision {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn checked_next(self) -> Result<Self, DomainError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or_else(|| DomainError::InvalidRange {
                reason: "security revocation revision overflow".to_owned(),
            })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct CredentialId(String);

impl CredentialId {
    pub fn parse(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        if value.is_empty() {
            return Err(DomainError::InvalidName {
                kind: "credential ID".to_owned(),
                reason: "value is empty".to_owned(),
            });
        }
        if value.len() > MAX_CREDENTIAL_ID_BYTES {
            return Err(DomainError::InvalidName {
                kind: "credential ID".to_owned(),
                reason: format!("value exceeds {MAX_CREDENTIAL_ID_BYTES} UTF-8 bytes"),
            });
        }
        if value.chars().any(char::is_control) {
            return Err(DomainError::InvalidName {
                kind: "credential ID".to_owned(),
                reason: "control characters are not allowed".to_owned(),
            });
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(DomainError::InvalidName {
                kind: "credential ID".to_owned(),
                reason: "only ASCII letters, digits, '-' and '_' are allowed".to_owned(),
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CredentialId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct CredentialGeneration(NonZeroU64);

impl CredentialGeneration {
    pub fn new(value: u64) -> Result<Self, DomainError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or_else(|| DomainError::InvalidRange {
                reason: "credential generation must be greater than zero".to_owned(),
            })
    }

    pub const fn initial() -> Self {
        Self(NonZeroU64::MIN)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct CredentialRef {
    id: CredentialId,
    generation: CredentialGeneration,
}

impl CredentialRef {
    pub const fn new(id: CredentialId, generation: CredentialGeneration) -> Self {
        Self { id, generation }
    }

    pub fn id(&self) -> &CredentialId {
        &self.id
    }

    pub const fn generation(&self) -> CredentialGeneration {
        self.generation
    }
}

#[derive(Clone, Copy, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TokenVerifierDigest([u8; 32]);

impl TokenVerifierDigest {
    pub fn from_token_bytes(token: &[u8]) -> Self {
        Self(Sha256::digest(token).into())
    }

    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn matches(&self, presented: &Self) -> bool {
        bool::from(self.0.ct_eq(&presented.0))
    }
}

impl fmt::Debug for TokenVerifierDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TokenVerifierDigest([REDACTED])")
    }
}

#[derive(Clone, Copy, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct CertificateFingerprint([u8; 32]);

impl CertificateFingerprint {
    pub fn from_der(certificate: &[u8]) -> Self {
        Self(Sha256::digest(certificate).into())
    }

    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for CertificateFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CertificateFingerprint([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialStatus {
    Active,
    Revoked { at: RevocationRevision },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TokenVerifier {
    credential: CredentialRef,
    principal: PrincipalId,
    digest: TokenVerifierDigest,
    status: CredentialStatus,
}

impl TokenVerifier {
    pub const fn new(
        credential: CredentialRef,
        principal: PrincipalId,
        digest: TokenVerifierDigest,
    ) -> Self {
        Self {
            credential,
            principal,
            digest,
            status: CredentialStatus::Active,
        }
    }

    pub fn credential(&self) -> &CredentialRef {
        &self.credential
    }

    pub fn principal(&self) -> &PrincipalId {
        &self.principal
    }

    pub const fn digest(&self) -> TokenVerifierDigest {
        self.digest
    }

    pub const fn status(&self) -> CredentialStatus {
        self.status
    }

    fn revoke(&mut self, revision: RevocationRevision) {
        self.status = CredentialStatus::Revoked { at: revision };
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PeerCertificateBinding {
    cluster: ClusterId,
    node: NodeId,
    generation: CredentialGeneration,
    fingerprint: CertificateFingerprint,
    status: CredentialStatus,
}

impl PeerCertificateBinding {
    pub const fn new(
        cluster: ClusterId,
        node: NodeId,
        generation: CredentialGeneration,
        fingerprint: CertificateFingerprint,
    ) -> Self {
        Self {
            cluster,
            node,
            generation,
            fingerprint,
            status: CredentialStatus::Active,
        }
    }

    pub const fn cluster(&self) -> ClusterId {
        self.cluster
    }

    pub const fn node(&self) -> NodeId {
        self.node
    }

    pub const fn generation(&self) -> CredentialGeneration {
        self.generation
    }

    pub const fn fingerprint(&self) -> CertificateFingerprint {
        self.fingerprint
    }

    pub const fn status(&self) -> CredentialStatus {
        self.status
    }

    fn revoke(&mut self, revision: RevocationRevision) {
        self.status = CredentialStatus::Revoked { at: revision };
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    ClusterObserve,
    ClusterBootstrap,
    StreamDiscover,
    StreamCreate,
    StreamDescribe,
    StreamDelete,
    RouteResolve,
    Publish,
    Fetch,
    ReceiptRead,
    BookmarkRead,
    BookmarkManage,
    RetentionRead,
    RetentionManage,
    ReplayRead,
    ReplayManage,
    CheckpointRead,
    CheckpointManage,
    SnapshotManage,
    ClusterAdmin,
    SecurityObserve,
    SecurityAdmin,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResourceScope {
    Cluster {
        cluster: ClusterId,
    },
    AllStreams {
        cluster: ClusterId,
    },
    Stream {
        cluster: ClusterId,
        stream: StreamId,
    },
}

impl ResourceScope {
    pub const fn cluster(&self) -> ClusterId {
        match self {
            Self::Cluster { cluster }
            | Self::AllStreams { cluster }
            | Self::Stream { cluster, .. } => *cluster,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Grant {
    permission: Permission,
    scope: ResourceScope,
}

impl Grant {
    pub const fn new(permission: Permission, scope: ResourceScope) -> Self {
        Self { permission, scope }
    }

    pub const fn permission(&self) -> Permission {
        self.permission
    }

    pub const fn scope(&self) -> &ResourceScope {
        &self.scope
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedPrincipal {
    principal: PrincipalId,
    credential: CredentialRef,
    policy_revision: PolicyRevision,
    revocation_revision: RevocationRevision,
}

impl AuthenticatedPrincipal {
    pub fn principal(&self) -> &PrincipalId {
        &self.principal
    }

    pub fn credential(&self) -> &CredentialRef {
        &self.credential
    }

    pub const fn policy_revision(&self) -> PolicyRevision {
        self.policy_revision
    }

    pub const fn revocation_revision(&self) -> RevocationRevision {
        self.revocation_revision
    }

    pub fn bind_producer(
        &self,
        request: &ProducerRequestId,
    ) -> Result<ProducerRequestId, DomainError> {
        if request.principal() != &self.principal {
            return Err(DomainError::SecurityPermissionDenied);
        }
        Ok(ProducerRequestId::new(
            self.principal.clone(),
            request.session(),
            request.sequence(),
        ))
    }

    pub fn bind_mutation(
        &self,
        request: &MutationRequestId,
    ) -> Result<MutationRequestId, DomainError> {
        if request.principal() != &self.principal {
            return Err(DomainError::SecurityPermissionDenied);
        }
        Ok(MutationRequestId::new(
            self.principal.clone(),
            request.session(),
            request.sequence(),
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SecurityPolicy {
    cluster: ClusterId,
    revision: PolicyRevision,
    revocation_revision: RevocationRevision,
    grants: BTreeMap<PrincipalId, BTreeSet<Grant>>,
    token_verifiers: Vec<TokenVerifier>,
    peer_certificates: BTreeMap<NodeId, BTreeSet<PeerCertificateBinding>>,
}

#[derive(Deserialize)]
struct SecurityPolicyWire {
    cluster: ClusterId,
    revision: PolicyRevision,
    revocation_revision: RevocationRevision,
    grants: BTreeMap<PrincipalId, BTreeSet<Grant>>,
    token_verifiers: Vec<TokenVerifier>,
    peer_certificates: BTreeMap<NodeId, BTreeSet<PeerCertificateBinding>>,
}

impl<'de> Deserialize<'de> for SecurityPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = SecurityPolicyWire::deserialize(deserializer)?;
        Self::try_new(
            wire.cluster,
            wire.revision,
            wire.revocation_revision,
            wire.grants,
            wire.token_verifiers,
            wire.peer_certificates,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl SecurityPolicy {
    pub fn try_new(
        cluster: ClusterId,
        revision: PolicyRevision,
        revocation_revision: RevocationRevision,
        grants: BTreeMap<PrincipalId, BTreeSet<Grant>>,
        token_verifiers: Vec<TokenVerifier>,
        peer_certificates: BTreeMap<NodeId, BTreeSet<PeerCertificateBinding>>,
    ) -> Result<Self, DomainError> {
        let policy = Self {
            cluster,
            revision,
            revocation_revision,
            grants,
            token_verifiers,
            peer_certificates,
        };
        policy.validate()?;
        Ok(policy)
    }

    pub const fn cluster(&self) -> ClusterId {
        self.cluster
    }

    pub const fn revision(&self) -> PolicyRevision {
        self.revision
    }

    pub const fn revocation_revision(&self) -> RevocationRevision {
        self.revocation_revision
    }

    pub fn grants(&self) -> &BTreeMap<PrincipalId, BTreeSet<Grant>> {
        &self.grants
    }

    pub fn token_verifiers(&self) -> &[TokenVerifier] {
        &self.token_verifiers
    }

    pub fn peer_certificates(&self) -> &BTreeMap<NodeId, BTreeSet<PeerCertificateBinding>> {
        &self.peer_certificates
    }

    pub fn authenticate(
        &self,
        credential: &CredentialRef,
        presented: TokenVerifierDigest,
    ) -> Result<AuthenticatedPrincipal, DomainError> {
        let verifier = self
            .token_verifiers
            .iter()
            .find(|verifier| verifier.credential() == credential)
            .ok_or(DomainError::SecurityAuthenticationFailed)?;
        if verifier.status != CredentialStatus::Active || !verifier.digest.matches(&presented) {
            return Err(DomainError::SecurityAuthenticationFailed);
        }
        Ok(AuthenticatedPrincipal {
            principal: verifier.principal.clone(),
            credential: credential.clone(),
            policy_revision: self.revision,
            revocation_revision: self.revocation_revision,
        })
    }

    pub fn authorize(
        &self,
        principal: &PrincipalId,
        permission: Permission,
        resource: &ResourceScope,
    ) -> Result<(), DomainError> {
        let allowed = self.grants.get(principal).is_some_and(|grants| {
            grants
                .iter()
                .any(|grant| grant.permission == permission && scope_covers(&grant.scope, resource))
        });
        if allowed {
            Ok(())
        } else {
            Err(DomainError::SecurityPermissionDenied)
        }
    }

    pub fn validate_active_credential_binding(
        &self,
        authenticated: &AuthenticatedPrincipal,
    ) -> Result<(), DomainError> {
        let active = self.token_verifiers.iter().any(|verifier| {
            verifier.status == CredentialStatus::Active
                && verifier.credential == authenticated.credential
                && verifier.principal == authenticated.principal
        });
        if active {
            Ok(())
        } else {
            Err(DomainError::SecurityAuthenticationFailed)
        }
    }

    pub fn authenticate_peer(
        &self,
        cluster: ClusterId,
        node: NodeId,
        fingerprint: CertificateFingerprint,
    ) -> Result<(), DomainError> {
        if cluster != self.cluster {
            return Err(DomainError::SecurityAuthenticationFailed);
        }

        let allowed = self.peer_certificates.get(&node).is_some_and(|bindings| {
            bindings.iter().any(|binding| {
                binding.status == CredentialStatus::Active
                    && binding.cluster == cluster
                    && binding.node == node
                    && binding.fingerprint == fingerprint
            })
        });
        if allowed {
            Ok(())
        } else {
            Err(DomainError::SecurityAuthenticationFailed)
        }
    }

    pub fn has_active_peer(&self, node: NodeId) -> bool {
        self.peer_certificates.get(&node).is_some_and(|bindings| {
            bindings
                .iter()
                .any(|binding| binding.status == CredentialStatus::Active)
        })
    }

    pub fn apply(&self, mutation: SecurityMutation) -> Result<Self, DomainError> {
        if mutation.expected_revision != self.revision {
            return Err(DomainError::SecurityPolicyConflict);
        }
        let mut next = self.clone();
        next.revision = self.revision.checked_next()?;
        match mutation.change {
            SecurityChange::ReplacePrincipalGrants { principal, grants } => {
                next.grants.insert(principal, grants);
            }
            SecurityChange::AddTokenGeneration { verifier } => {
                let newest = next
                    .token_verifiers
                    .iter()
                    .filter(|existing| existing.credential.id == verifier.credential.id)
                    .map(|existing| existing.credential.generation)
                    .max();
                if newest.is_some_and(|newest| verifier.credential.generation <= newest) {
                    return Err(DomainError::SecurityPolicyConflict);
                }
                if next
                    .token_verifiers
                    .iter()
                    .any(|existing| existing.digest == verifier.digest)
                    || next
                        .token_verifiers
                        .iter()
                        .any(|existing| existing.credential == verifier.credential)
                {
                    return Err(DomainError::SecurityPolicyConflict);
                }
                next.token_verifiers.push(verifier);
            }
            SecurityChange::RevokeTokenGeneration { credential } => {
                let revocation = next.revocation_revision.checked_next()?;
                next.token_verifiers
                    .iter_mut()
                    .find(|verifier| verifier.credential == credential)
                    .ok_or(DomainError::SecurityPolicyConflict)?
                    .revoke(revocation);
                next.revocation_revision = revocation;
            }
            SecurityChange::AddPeerCertificate { binding } => {
                if binding.cluster != self.cluster {
                    return Err(DomainError::SecurityPolicyConflict);
                }
                let bindings = next.peer_certificates.entry(binding.node).or_default();
                if bindings
                    .iter()
                    .map(|existing| existing.generation)
                    .max()
                    .is_some_and(|newest| binding.generation <= newest)
                {
                    return Err(DomainError::SecurityPolicyConflict);
                }
                bindings.insert(binding);
            }
            SecurityChange::RevokePeerCertificate { node, generation } => {
                let revocation = next.revocation_revision.checked_next()?;
                let binding = next
                    .peer_certificates
                    .get_mut(&node)
                    .and_then(|bindings| {
                        bindings
                            .iter()
                            .find(|binding| binding.generation == generation)
                            .cloned()
                    })
                    .ok_or(DomainError::SecurityPolicyConflict)?;
                next.peer_certificates
                    .get_mut(&node)
                    .expect("node binding exists")
                    .take(&binding);
                let mut revoked = binding;
                revoked.revoke(revocation);
                next.peer_certificates
                    .get_mut(&node)
                    .expect("node binding exists")
                    .insert(revoked);
                next.revocation_revision = revocation;
            }
        }
        next.validate()?;
        Ok(next)
    }

    fn validate(&self) -> Result<(), DomainError> {
        let grant_count = self.grants.values().map(BTreeSet::len).sum::<usize>();
        let peer_certificate_count = self
            .peer_certificates
            .values()
            .map(BTreeSet::len)
            .sum::<usize>();
        if self.grants.len() > MAX_SECURITY_PRINCIPALS
            || grant_count > MAX_SECURITY_GRANTS
            || self.token_verifiers.len() > MAX_TOKEN_VERIFIERS
            || peer_certificate_count > MAX_PEER_CERTIFICATES
        {
            return Err(DomainError::ResourceLimit {
                resource: "security policy entries".to_owned(),
                limit: MAX_SECURITY_GRANTS as u64,
            });
        }
        if self
            .grants
            .values()
            .flatten()
            .any(|grant| grant.scope.cluster() != self.cluster)
        {
            return Err(DomainError::SecurityPolicyConflict);
        }
        let mut credentials = BTreeSet::new();
        let mut digests = BTreeSet::new();
        for verifier in &self.token_verifiers {
            if !credentials.insert(verifier.credential.clone()) || !digests.insert(verifier.digest)
            {
                return Err(DomainError::SecurityPolicyConflict);
            }
        }
        for (node, bindings) in &self.peer_certificates {
            if bindings
                .iter()
                .any(|binding| binding.node != *node || binding.cluster != self.cluster)
            {
                return Err(DomainError::SecurityPolicyConflict);
            }
        }
        let has_security_admin = self.token_verifiers.iter().any(|verifier| {
            verifier.status == CredentialStatus::Active
                && self.grants.get(&verifier.principal).is_some_and(|grants| {
                    grants.contains(&Grant::new(
                        Permission::SecurityAdmin,
                        ResourceScope::Cluster {
                            cluster: self.cluster,
                        },
                    ))
                })
        });
        if !has_security_admin {
            return Err(DomainError::SecurityPolicyConflict);
        }
        Ok(())
    }
}

fn scope_covers(grant: &ResourceScope, requested: &ResourceScope) -> bool {
    match (grant, requested) {
        (
            ResourceScope::Cluster { cluster: grant },
            ResourceScope::Cluster { cluster: requested },
        ) => grant == requested,
        (
            ResourceScope::AllStreams { cluster: grant },
            ResourceScope::AllStreams { cluster: requested }
            | ResourceScope::Stream {
                cluster: requested, ..
            },
        ) => grant == requested,
        (
            ResourceScope::Stream {
                cluster: grant_cluster,
                stream: grant_stream,
            },
            ResourceScope::Stream {
                cluster: requested_cluster,
                stream: requested_stream,
            },
        ) => grant_cluster == requested_cluster && grant_stream == requested_stream,
        _ => false,
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SecurityMutation {
    request: MutationRequestId,
    expected_revision: PolicyRevision,
    change: SecurityChange,
}

impl SecurityMutation {
    pub const fn new(
        request: MutationRequestId,
        expected_revision: PolicyRevision,
        change: SecurityChange,
    ) -> Self {
        Self {
            request,
            expected_revision,
            change,
        }
    }

    pub fn request(&self) -> &MutationRequestId {
        &self.request
    }

    pub const fn expected_revision(&self) -> PolicyRevision {
        self.expected_revision
    }

    pub const fn change(&self) -> &SecurityChange {
        &self.change
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SecurityChange {
    ReplacePrincipalGrants {
        principal: PrincipalId,
        grants: BTreeSet<Grant>,
    },
    AddTokenGeneration {
        verifier: TokenVerifier,
    },
    RevokeTokenGeneration {
        credential: CredentialRef,
    },
    AddPeerCertificate {
        binding: PeerCertificateBinding,
    },
    RevokePeerCertificate {
        node: NodeId,
        generation: CredentialGeneration,
    },
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::{MutationSessionId, ProducerSessionId, RequestSequence};

    fn cluster() -> ClusterId {
        ClusterId::from_uuid(Uuid::new_v4())
    }

    fn principal(value: &str) -> PrincipalId {
        PrincipalId::parse(value).unwrap()
    }

    fn credential(value: &str, generation: u64) -> CredentialRef {
        CredentialRef::new(
            CredentialId::parse(value).unwrap(),
            CredentialGeneration::new(generation).unwrap(),
        )
    }

    fn policy(raw_token: &[u8]) -> (SecurityPolicy, CredentialRef, PrincipalId) {
        let cluster = cluster();
        let principal = principal("security-admin");
        let credential = credential("admin", 1);
        let grants = BTreeMap::from([(
            principal.clone(),
            BTreeSet::from([
                Grant::new(
                    Permission::SecurityAdmin,
                    ResourceScope::Cluster { cluster },
                ),
                Grant::new(Permission::Publish, ResourceScope::AllStreams { cluster }),
            ]),
        )]);
        let verifiers = vec![TokenVerifier::new(
            credential.clone(),
            principal.clone(),
            TokenVerifierDigest::from_token_bytes(raw_token),
        )];
        (
            SecurityPolicy::try_new(
                cluster,
                PolicyRevision::initial(),
                RevocationRevision::default(),
                grants,
                verifiers,
                BTreeMap::new(),
            )
            .unwrap(),
            credential,
            principal,
        )
    }

    #[test]
    fn policy_authenticates_and_binds_one_principal() {
        let token = b"ls1.admin.1.synthetic-secret-canary";
        let (policy, credential, authenticated_principal) = policy(token);
        let authenticated = policy
            .authenticate(&credential, TokenVerifierDigest::from_token_bytes(token))
            .unwrap();
        policy
            .authorize(
                authenticated.principal(),
                Permission::Publish,
                &ResourceScope::Stream {
                    cluster: policy.cluster(),
                    stream: StreamId::from_uuid(Uuid::new_v4()),
                },
            )
            .unwrap();
        let producer = ProducerRequestId::new(
            authenticated_principal,
            ProducerSessionId::from_uuid(Uuid::new_v4()),
            RequestSequence::new(1),
        );
        assert_eq!(authenticated.bind_producer(&producer).unwrap(), producer);
        let other =
            ProducerRequestId::new(principal("other"), producer.session(), producer.sequence());
        assert_eq!(
            authenticated.bind_producer(&other).unwrap_err(),
            DomainError::SecurityPermissionDenied
        );
    }

    #[test]
    fn token_rotation_preserves_principal_and_revocation_is_immediate_in_policy() {
        let old_token = b"ls1.admin.1.old-synthetic-secret";
        let new_token = b"ls1.admin.2.new-synthetic-secret";
        let (policy, old_credential, principal) = policy(old_token);
        let authenticated = policy
            .authenticate(
                &old_credential,
                TokenVerifierDigest::from_token_bytes(old_token),
            )
            .unwrap();
        let new_credential = credential("admin", 2);
        let request = MutationRequestId::new(
            principal.clone(),
            MutationSessionId::from_uuid(Uuid::new_v4()),
            RequestSequence::new(1),
        );
        let policy = policy
            .apply(SecurityMutation::new(
                request.clone(),
                policy.revision(),
                SecurityChange::AddTokenGeneration {
                    verifier: TokenVerifier::new(
                        new_credential.clone(),
                        principal.clone(),
                        TokenVerifierDigest::from_token_bytes(new_token),
                    ),
                },
            ))
            .unwrap();
        assert_eq!(
            policy
                .authenticate(
                    &new_credential,
                    TokenVerifierDigest::from_token_bytes(new_token),
                )
                .unwrap()
                .principal(),
            &principal
        );
        policy
            .validate_active_credential_binding(&authenticated)
            .unwrap();
        let policy = policy
            .apply(SecurityMutation::new(
                MutationRequestId::new(principal, request.session(), RequestSequence::new(2)),
                policy.revision(),
                SecurityChange::RevokeTokenGeneration {
                    credential: old_credential.clone(),
                },
            ))
            .unwrap();
        assert_eq!(policy.revocation_revision().get(), 1);
        assert_eq!(
            policy
                .authenticate(
                    &old_credential,
                    TokenVerifierDigest::from_token_bytes(old_token),
                )
                .unwrap_err(),
            DomainError::SecurityAuthenticationFailed
        );
        assert_eq!(
            policy
                .validate_active_credential_binding(&authenticated)
                .unwrap_err(),
            DomainError::SecurityAuthenticationFailed
        );
    }

    #[test]
    fn active_credential_binding_rejects_principal_rebinding() {
        let token = b"ls1.admin.1.synthetic-secret";
        let (policy, credential, _) = policy(token);
        let authenticated = policy
            .authenticate(&credential, TokenVerifierDigest::from_token_bytes(token))
            .unwrap();
        let rebound_principal = principal("replacement-admin");
        let rebound = SecurityPolicy::try_new(
            policy.cluster(),
            policy.revision(),
            policy.revocation_revision(),
            BTreeMap::from([(
                rebound_principal.clone(),
                BTreeSet::from([Grant::new(
                    Permission::SecurityAdmin,
                    ResourceScope::Cluster {
                        cluster: policy.cluster(),
                    },
                )]),
            )]),
            vec![TokenVerifier::new(
                credential,
                rebound_principal,
                TokenVerifierDigest::from_token_bytes(token),
            )],
            BTreeMap::new(),
        )
        .unwrap();

        assert_eq!(
            rebound
                .validate_active_credential_binding(&authenticated)
                .unwrap_err(),
            DomainError::SecurityAuthenticationFailed
        );
    }

    #[test]
    fn serialized_policy_never_contains_raw_secret_material() {
        let raw_token = b"raw-token-secret-canary-4j6Hf3";
        let (policy, _, _) = policy(raw_token);
        let encoded = serde_json::to_vec(&policy).unwrap();
        assert!(
            !encoded
                .windows(raw_token.len())
                .any(|window| window == raw_token)
        );
        assert!(!String::from_utf8_lossy(&encoded).contains("private-key-canary"));
        assert_eq!(
            format!("{:?}", TokenVerifierDigest::from_token_bytes(raw_token)),
            "TokenVerifierDigest([REDACTED])"
        );
    }
}
