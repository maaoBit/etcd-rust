// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Benjamin Chess
use std::net::{SocketAddr, ToSocketAddrs};
use url::Url;

/// Parse and validate peer URLs.
/// Each URL must have a valid scheme (http or https) and a non-empty host.
pub fn parse_peer_urls(urls: &[String]) -> Result<Vec<Url>, String> {
    let mut result = Vec::with_capacity(urls.len());
    for u in urls {
        let parsed = Url::parse(u).map_err(|e| format!("invalid peer URL '{}': {}", u, e))?;
        if parsed.host().is_none() {
            return Err(format!("peer URL '{}' has no host", u));
        }
        match parsed.scheme() {
            "http" | "https" => {}
            scheme => return Err(format!("peer URL '{}' has unsupported scheme '{}'", u, scheme)),
        }
        result.push(parsed);
    }
    Ok(result)
}

/// Resolve a `host:port` address string into a `SocketAddr`.
/// Uses the standard library's `ToSocketAddrs` trait.
pub fn resolve_addr(addr: &str) -> Result<SocketAddr, String> {
    addr.to_socket_addrs()
        .map_err(|e| format!("failed to resolve address '{}': {}", addr, e))?
        .next()
        .ok_or_else(|| format!("no addresses resolved for '{}'", addr))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_peer_urls() {
        let urls = vec![
            "http://127.0.0.1:2379".to_string(),
            "https://peer.example.com:2380".to_string(),
        ];
        let parsed = parse_peer_urls(&urls).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].scheme(), "http");
        assert_eq!(parsed[1].scheme(), "https");
    }

    #[test]
    fn test_parse_peer_urls_invalid() {
        let urls = vec!["not-a-url".to_string()];
        assert!(parse_peer_urls(&urls).is_err());

        let urls = vec!["ftp://host:1234".to_string()];
        assert!(parse_peer_urls(&urls).is_err());
    }

    #[test]
    fn test_resolve_addr() {
        // localhost should always resolve
        let addr = resolve_addr("127.0.0.1:2379").unwrap();
        assert_eq!(addr.port(), 2379);
    }
}
