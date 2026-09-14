mod convert;

pub mod v1 {
    tonic::include_proto!("lightstream.v1");
}

pub use convert::{
    admit_replay_lease_from_wire, advance_retention_from_wire, bookmark_from_wire,
    bookmark_page_to_wire, bookmark_to_wire, bootstrap_from_wire, bootstrap_to_wire,
    capabilities_to_wire, create_bookmark_from_wire, create_stream_bookmark_from_wire,
    create_stream_from_wire, delete_bookmark_from_wire, delete_stream_bookmark_from_wire,
    domain_error_from_wire, domain_error_to_wire, fetch_from_wire, fetch_protected_from_wire,
    fetch_to_wire, get_replay_lease_from_wire, health_to_wire, list_bookmarks_from_wire,
    list_stream_bookmarks_from_wire, mutation_request_id_from_wire, mutation_request_id_to_wire,
    publish_batch_and_route_from_wire, publish_batch_from_wire, publish_probe_from_wire,
    publish_receipt_to_wire, receipt_from_wire, release_replay_lease_from_wire,
    renew_replay_lease_from_wire, replay_lease_from_wire, replay_lease_to_wire,
    resolve_bookmark_from_wire, resolve_stream_bookmark_from_wire, retention_result_from_wire,
    retention_result_to_wire, retention_status_from_response, retention_status_from_wire,
    retention_status_to_wire, route_from_wire, route_to_wire, security_mode_from_wire,
    stream_bookmark_from_wire, stream_bookmark_page_to_wire, stream_bookmark_to_wire,
    stream_from_wire, stream_selector_from_wire, stream_to_wire, unsupported_publish_to_wire,
};
