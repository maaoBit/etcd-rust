fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_includes: Vec<&str> = ["/usr/local/include/include", "/usr/local/include", "/opt/homebrew/include"]
        .iter()
        .filter(|p| std::path::Path::new(p).join("google/protobuf/descriptor.proto").exists())
        .copied()
        .collect();

    let mut includes: Vec<std::path::PathBuf> = vec!["../extern".into(), "../extern/etcd/api".into()];
    for inc in &proto_includes {
        includes.push((*inc).into());
    }

    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .bytes(&["KeyValue.value", "PutRequest.value"])
        .compile_protos(&["../extern/etcd/api/rpc.proto"], &includes)?;
    Ok(())
}
