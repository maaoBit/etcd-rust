// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Benjamin Chess
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Find protobuf well-known types include path
    let proto_includes: Vec<&str> = ["/usr/local/include/include", "/usr/local/include", "/opt/homebrew/include"]
        .iter()
        .filter(|p| std::path::Path::new(p).join("google/protobuf/descriptor.proto").exists())
        .copied()
        .collect();

    let mut includes: Vec<std::path::PathBuf> = vec!["extern".into(), "extern/etcd/api".into(), "proto".into()];
    for inc in &proto_includes {
        includes.push((*inc).into());
    }

    // Build etcd API protos (server only) + election + lock
    tonic_build::configure()
        .build_client(false)
        .build_server(true)
        .bytes(&["KeyValue.value"])
        .compile_protos(&[
            "extern/etcd/api/rpc.proto",
            "proto/election.proto",
            "proto/lock.proto",
        ], &includes)?;

    // Build Raft transport proto (both client and server)
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/raft_transport.proto"], &["proto"])?;

    Ok(())
}
