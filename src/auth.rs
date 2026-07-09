// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Benjamin Chess
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ── Types ───────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Debug)]
pub enum PermissionType {
    Read = 0,
    Write = 1,
    ReadWrite = 2,
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
pub struct AuthPermission {
    pub perm_type: PermissionType,
    pub key: Vec<u8>,
    pub range_end: Vec<u8>,
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
pub struct AuthRole {
    pub name: String,
    pub permissions: Vec<AuthPermission>,
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
pub struct AuthUser {
    pub name: String,
    pub password_hash: Vec<u8>,
    pub roles: Vec<String>,
    pub no_password: bool,
}

#[derive(Clone, Debug)]
pub struct TokenInfo {
    pub token: String,
    pub username: String,
    pub expiry: SystemTime,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AuthError {
    RootUserNotExist,
    RootRoleNotExist,
    UserAlreadyExist,
    UserEmpty,
    UserNotFound,
    RoleAlreadyExist,
    RoleNotFound,
    RoleEmpty,
    AuthFailed,
    PermissionNotGiven,
    PermissionDenied,
    RoleNotGranted,
    PermissionNotGranted,
    AuthNotEnabled,
    InvalidAuthToken,
    InvalidAuthMgmt,
    AuthOldRevision,
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::RootUserNotExist => write!(f, "root user does not exist"),
            AuthError::RootRoleNotExist => write!(f, "root role does not exist"),
            AuthError::UserAlreadyExist => write!(f, "user already exists"),
            AuthError::UserEmpty => write!(f, "user name is empty"),
            AuthError::UserNotFound => write!(f, "user not found"),
            AuthError::RoleAlreadyExist => write!(f, "role already exists"),
            AuthError::RoleNotFound => write!(f, "role not found"),
            AuthError::RoleEmpty => write!(f, "role name is empty"),
            AuthError::AuthFailed => write!(f, "authentication failed"),
            AuthError::PermissionNotGiven => write!(f, "permission not given"),
            AuthError::PermissionDenied => write!(f, "permission denied"),
            AuthError::RoleNotGranted => write!(f, "role not granted"),
            AuthError::PermissionNotGranted => write!(f, "permission not granted"),
            AuthError::AuthNotEnabled => write!(f, "authentication is not enabled"),
            AuthError::InvalidAuthToken => write!(f, "invalid authentication token"),
            AuthError::InvalidAuthMgmt => write!(f, "invalid authentication management operation"),
            AuthError::AuthOldRevision => write!(f, "authentication old revision"),
        }
    }
}

// ── Password Hashing ────────────────────────────────────────────────────────

/// Hash a password using SHA-256 for demo purposes.
///
/// # Note
/// This is a placeholder implementation. In production, use a proper key-derivation
/// function such as bcrypt, Argon2, or PBKDF2. The SHA-256 approach here is suitable
/// for development and testing only.
fn hash_password(password: &str) -> Vec<u8> {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    password.hash(&mut hasher);
    hasher.finish().to_le_bytes().to_vec()
}

/// Verify a password against a stored hash.
fn verify_password(password: &str, hash: &[u8]) -> bool {
    hash_password(password).as_slice() == hash
}

/// Generate a random hex token string for authentication sessions.
fn generate_token() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mixed = nanos
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    format!("v1.{:016x}{:016x}", mixed, mixed ^ (mixed >> 32))
}

// ── AuthStore ───────────────────────────────────────────────────────────────

const TOKEN_TTL_SECS: u64 = 300; // 5 minutes
const TOKEN_CLEANUP_INTERVAL: u64 = 60; // clean expired tokens every 60 writes

pub struct AuthStore {
    users: RwLock<BTreeMap<String, AuthUser>>,
    roles: RwLock<BTreeMap<String, AuthRole>>,
    enabled: AtomicBool,
    revision: AtomicI64,
    token_store: RwLock<HashMap<String, TokenInfo>>,
    write_count: AtomicI64,
}

impl AuthStore {
    pub fn new() -> Self {
        AuthStore {
            users: RwLock::new(BTreeMap::new()),
            roles: RwLock::new(BTreeMap::new()),
            enabled: AtomicBool::new(false),
            revision: AtomicI64::new(0),
            token_store: RwLock::new(HashMap::new()),
            write_count: AtomicI64::new(0),
        }
    }

    // ── Auth Enable / Disable ───────────────────────────────────────────────

    pub fn auth_enable(&self) -> Result<(), AuthError> {
        {
            let users = self.users.read().unwrap();
            if !users.contains_key("root") {
                return Err(AuthError::RootUserNotExist);
            }
        }
        {
            let roles = self.roles.read().unwrap();
            if !roles.contains_key("root") {
                return Err(AuthError::RootRoleNotExist);
            }
        }
        self.enabled.store(true, Ordering::SeqCst);
        self.revision.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    pub fn auth_disable(&self) -> Result<(), AuthError> {
        self.enabled.store(false, Ordering::SeqCst);
        self.revision.fetch_add(1, Ordering::SeqCst);
        // Clear all tokens on disable
        self.token_store.write().unwrap().clear();
        Ok(())
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }

    pub fn revision(&self) -> i64 {
        self.revision.load(Ordering::SeqCst)
    }

    // ── User Management ─────────────────────────────────────────────────────

    pub fn user_add(
        &self,
        name: &str,
        password: &str,
        no_password: bool,
    ) -> Result<(), AuthError> {
        if name.is_empty() {
            return Err(AuthError::UserEmpty);
        }
        let mut users = self.users.write().unwrap();
        if users.contains_key(name) {
            return Err(AuthError::UserAlreadyExist);
        }
        let password_hash = if no_password {
            Vec::new()
        } else {
            hash_password(password)
        };
        users.insert(
            name.to_string(),
            AuthUser {
                name: name.to_string(),
                password_hash,
                roles: Vec::new(),
                no_password,
            },
        );
        self.revision.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    pub fn user_delete(&self, name: &str) -> Result<(), AuthError> {
        if name.is_empty() {
            return Err(AuthError::UserEmpty);
        }
        let mut users = self.users.write().unwrap();
        if users.remove(name).is_none() {
            return Err(AuthError::UserNotFound);
        }
        self.revision.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    pub fn user_change_password(&self, name: &str, password: &str) -> Result<(), AuthError> {
        let mut users = self.users.write().unwrap();
        let user = users
            .get_mut(name)
            .ok_or(AuthError::UserNotFound)?;
        user.password_hash = hash_password(password);
        user.no_password = false;
        self.revision.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    pub fn user_get(&self, name: &str) -> Result<AuthUser, AuthError> {
        let users = self.users.read().unwrap();
        users.get(name).cloned().ok_or(AuthError::UserNotFound)
    }

    pub fn user_list(&self) -> Result<Vec<String>, AuthError> {
        let users = self.users.read().unwrap();
        Ok(users.keys().cloned().collect())
    }

    pub fn user_grant_role(&self, user: &str, role: &str) -> Result<(), AuthError> {
        let mut users = self.users.write().unwrap();
        let user = users.get_mut(user).ok_or(AuthError::UserNotFound)?;
        if user.roles.contains(&role.to_string()) {
            return Err(AuthError::RoleNotGranted);
        }
        // Verify the role exists
        {
            let roles = self.roles.read().unwrap();
            if !roles.contains_key(role) {
                return Err(AuthError::RoleNotFound);
            }
        }
        user.roles.push(role.to_string());
        self.revision.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    pub fn user_revoke_role(&self, user: &str, role: &str) -> Result<(), AuthError> {
        let mut users = self.users.write().unwrap();
        let user = users.get_mut(user).ok_or(AuthError::UserNotFound)?;
        let pos = user
            .roles
            .iter()
            .position(|r| r == role)
            .ok_or(AuthError::RoleNotGranted)?;
        user.roles.remove(pos);
        self.revision.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    // ── Role Management ─────────────────────────────────────────────────────

    pub fn role_add(&self, name: &str) -> Result<(), AuthError> {
        if name.is_empty() {
            return Err(AuthError::RoleEmpty);
        }
        let mut roles = self.roles.write().unwrap();
        if roles.contains_key(name) {
            return Err(AuthError::RoleAlreadyExist);
        }
        roles.insert(
            name.to_string(),
            AuthRole {
                name: name.to_string(),
                permissions: Vec::new(),
            },
        );
        self.revision.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    pub fn role_delete(&self, name: &str) -> Result<(), AuthError> {
        if name.is_empty() {
            return Err(AuthError::RoleEmpty);
        }
        let mut roles = self.roles.write().unwrap();
        if roles.remove(name).is_none() {
            return Err(AuthError::RoleNotFound);
        }
        self.revision.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    pub fn role_get(&self, name: &str) -> Result<AuthRole, AuthError> {
        let roles = self.roles.read().unwrap();
        roles.get(name).cloned().ok_or(AuthError::RoleNotFound)
    }

    pub fn role_list(&self) -> Result<Vec<String>, AuthError> {
        let roles = self.roles.read().unwrap();
        Ok(roles.keys().cloned().collect())
    }

    pub fn role_grant_permission(
        &self,
        name: &str,
        perm: AuthPermission,
    ) -> Result<(), AuthError> {
        let mut roles = self.roles.write().unwrap();
        let role = roles.get_mut(name).ok_or(AuthError::RoleNotFound)?;
        // Check if permission already exists (same key + range_end)
        if role
            .permissions
            .iter()
            .any(|p| p.key == perm.key && p.range_end == perm.range_end)
        {
            return Err(AuthError::PermissionNotGranted);
        }
        role.permissions.push(perm);
        self.revision.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    pub fn role_revoke_permission(
        &self,
        name: &str,
        key: &[u8],
        range_end: &[u8],
    ) -> Result<(), AuthError> {
        let mut roles = self.roles.write().unwrap();
        let role = roles.get_mut(name).ok_or(AuthError::RoleNotFound)?;
        let pos = role
            .permissions
            .iter()
            .position(|p| p.key == key && p.range_end == range_end)
            .ok_or(AuthError::PermissionNotGiven)?;
        role.permissions.remove(pos);
        self.revision.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    // ── Authentication ──────────────────────────────────────────────────────

    pub fn authenticate(&self, username: &str, password: &str) -> Result<String, AuthError> {
        if !self.is_enabled() {
            return Err(AuthError::AuthNotEnabled);
        }
        self.check_password(username, password)?;

        let token = generate_token();
        let info = TokenInfo {
            token: token.clone(),
            username: username.to_string(),
            expiry: SystemTime::now() + Duration::from_secs(TOKEN_TTL_SECS),
        };

        let mut token_store = self.token_store.write().unwrap();
        token_store.insert(token.clone(), info);

        // Periodically clean expired tokens
        let count = self.write_count.fetch_add(1, Ordering::SeqCst);
        if count % TOKEN_CLEANUP_INTERVAL as i64 == 0 {
            let now = SystemTime::now();
            token_store.retain(|_, info| info.expiry > now);
        }

        Ok(token)
    }

    pub fn check_password(&self, username: &str, password: &str) -> Result<(), AuthError> {
        let users = self.users.read().unwrap();
        let user = users.get(username).ok_or(AuthError::AuthFailed)?;
        if user.no_password {
            return Ok(());
        }
        if verify_password(password, &user.password_hash) {
            Ok(())
        } else {
            Err(AuthError::AuthFailed)
        }
    }

    pub fn validate_token(&self, token: &str) -> Result<String, AuthError> {
        if !self.is_enabled() {
            return Err(AuthError::AuthNotEnabled);
        }
        let token_store = self.token_store.read().unwrap();
        let info = token_store
            .get(token)
            .ok_or(AuthError::InvalidAuthToken)?;
        let now = SystemTime::now();
        if info.expiry <= now {
            return Err(AuthError::InvalidAuthToken);
        }
        Ok(info.username.clone())
    }

    // ── Authorization ───────────────────────────────────────────────────────

    pub fn check_perm(
        &self,
        username: &str,
        key: &[u8],
        range_end: &[u8],
        perm_type: PermissionType,
    ) -> Result<(), AuthError> {
        if !self.is_enabled() {
            return Ok(()); // Auth disabled: all operations allowed
        }

        let users = self.users.read().unwrap();
        let user = users.get(username).ok_or(AuthError::UserNotFound)?;

        let roles = self.roles.read().unwrap();

        // Check each role the user has
        for role_name in &user.roles {
            if let Some(role) = roles.get(role_name) {
                if self.has_permission(&role.permissions, key, range_end, perm_type) {
                    return Ok(());
                }
            }
        }

        // "root" user bypasses permission checks if root role exists
        if username == "root" && roles.contains_key("root") {
            return Ok(());
        }

        Err(AuthError::PermissionDenied)
    }

    /// Check whether any permission in the list matches the given key and perm_type.
    fn has_permission(
        &self,
        permissions: &[AuthPermission],
        key: &[u8],
        range_end: &[u8],
        perm_type: PermissionType,
    ) -> bool {
        permissions.iter().any(|p| {
            // Permission type must match
            if p.perm_type != perm_type && p.perm_type != PermissionType::ReadWrite {
                return false;
            }
            // Key range check
            if range_end.is_empty() {
                // Single key request: only check key
                self.key_in_range(key, &p.key, &p.range_end)
            } else {
                // Range request: check both key and range_end fall within permission
                self.key_in_range(key, &p.key, &p.range_end)
                    && self.key_in_range(range_end, &p.key, &p.range_end)
            }
        })
    }

    /// Check if a key falls within [range_start, range_end).
    /// An empty range_end means exact match (single key).
    fn key_in_range(&self, key: &[u8], range_start: &[u8], range_end: &[u8]) -> bool {
        if key.is_empty() && range_end.is_empty() {
            // Both empty keys match
            return true;
        }
        if key < range_start {
            return false;
        }
        if range_end.is_empty() {
            // Single key: exact match
            return key == range_start;
        }
        key < range_end
    }

    /// Refresh an existing token, extending its expiry.
    pub fn refresh_token(&self, token: &str) -> Result<String, AuthError> {
        let mut token_store = self.token_store.write().unwrap();
        let info = token_store
            .get_mut(token)
            .ok_or(AuthError::InvalidAuthToken)?;
        let now = SystemTime::now();
        if info.expiry <= now {
            token_store.remove(token);
            return Err(AuthError::InvalidAuthToken);
        }
        info.expiry = now + Duration::from_secs(TOKEN_TTL_SECS);
        Ok(info.username.clone())
    }

    /// List all active tokens (for debugging/admin purposes).
    pub fn list_tokens(&self) -> Vec<String> {
        let token_store = self.token_store.read().unwrap();
        let now = SystemTime::now();
        token_store
            .iter()
            .filter(|(_, info)| info.expiry > now)
            .map(|(token, _)| token.clone())
            .collect()
    }

    /// Remove a specific token (logout).
    pub fn revoke_token(&self, token: &str) {
        let mut token_store = self.token_store.write().unwrap();
        token_store.remove(token);
    }

    /// Get the internal stores for snapshot/restore purposes.
    pub fn get_users(&self) -> BTreeMap<String, AuthUser> {
        self.users.read().unwrap().clone()
    }

    pub fn get_roles(&self) -> BTreeMap<String, AuthRole> {
        self.roles.read().unwrap().clone()
    }

    pub fn set_users(&self, users: BTreeMap<String, AuthUser>) {
        *self.users.write().unwrap() = users;
    }

    pub fn set_roles(&self, roles: BTreeMap<String, AuthRole>) {
        *self.roles.write().unwrap() = roles;
    }
}

impl Default for AuthStore {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_auth_store() {
        let store = AuthStore::new();
        assert!(!store.is_enabled());
        assert_eq!(store.revision(), 0);
    }

    #[test]
    fn test_auth_enable_fails_without_root() {
        let store = AuthStore::new();
        // No root user or role exists yet
        assert_eq!(store.auth_enable(), Err(AuthError::RootUserNotExist));
    }

    #[test]
    fn test_auth_enable_success() {
        let store = AuthStore::new();
        store.user_add("root", "root123", false).unwrap();
        store.role_add("root").unwrap();
        store.user_grant_role("root", "root").unwrap();
        assert!(store.auth_enable().is_ok());
        assert!(store.is_enabled());
    }

    #[test]
    fn test_auth_disable() {
        let store = AuthStore::new();
        store.user_add("root", "root123", false).unwrap();
        store.role_add("root").unwrap();
        store.user_grant_role("root", "root").unwrap();
        store.auth_enable().unwrap();
        assert!(store.is_enabled());

        store.auth_disable().unwrap();
        assert!(!store.is_enabled());
    }

    #[test]
    fn test_user_add() {
        let store = AuthStore::new();
        assert!(store.user_add("alice", "pass123", false).is_ok());
        let user = store.user_get("alice").unwrap();
        assert_eq!(user.name, "alice");
        assert!(!user.no_password);
        assert!(!user.password_hash.is_empty());
    }

    #[test]
    fn test_user_add_empty_name() {
        let store = AuthStore::new();
        assert_eq!(
            store.user_add("", "pass123", false),
            Err(AuthError::UserEmpty)
        );
    }

    #[test]
    fn test_user_add_duplicate() {
        let store = AuthStore::new();
        store.user_add("alice", "pass123", false).unwrap();
        assert_eq!(
            store.user_add("alice", "other", false),
            Err(AuthError::UserAlreadyExist)
        );
    }

    #[test]
    fn test_user_add_no_password() {
        let store = AuthStore::new();
        assert!(store.user_add("guest", "", true).is_ok());
        let user = store.user_get("guest").unwrap();
        assert!(user.no_password);
        assert!(user.password_hash.is_empty());
    }

    #[test]
    fn test_user_delete() {
        let store = AuthStore::new();
        store.user_add("alice", "pass123", false).unwrap();
        assert!(store.user_delete("alice").is_ok());
        assert_eq!(store.user_get("alice"), Err(AuthError::UserNotFound));
    }

    #[test]
    fn test_user_delete_not_found() {
        let store = AuthStore::new();
        assert_eq!(
            store.user_delete("nonexistent"),
            Err(AuthError::UserNotFound)
        );
    }

    #[test]
    fn test_user_change_password() {
        let store = AuthStore::new();
        store.user_add("alice", "oldpass", false).unwrap();
        assert!(store.check_password("alice", "oldpass").is_ok());
        assert!(store.check_password("alice", "wrong").is_err());

        store.user_change_password("alice", "newpass").unwrap();
        // Old password no longer works
        assert!(store.check_password("alice", "oldpass").is_err());
        // New password works
        assert!(store.check_password("alice", "newpass").is_ok());
    }

    #[test]
    fn test_user_list() {
        let store = AuthStore::new();
        store.user_add("alice", "pass", false).unwrap();
        store.user_add("bob", "pass", false).unwrap();
        let users = store.user_list().unwrap();
        assert!(users.contains(&"alice".to_string()));
        assert!(users.contains(&"bob".to_string()));
        assert_eq!(users.len(), 2);
    }

    #[test]
    fn test_role_add() {
        let store = AuthStore::new();
        assert!(store.role_add("admin").is_ok());
        let role = store.role_get("admin").unwrap();
        assert_eq!(role.name, "admin");
        assert!(role.permissions.is_empty());
    }

    #[test]
    fn test_role_add_empty_name() {
        let store = AuthStore::new();
        assert_eq!(store.role_add(""), Err(AuthError::RoleEmpty));
    }

    #[test]
    fn test_role_add_duplicate() {
        let store = AuthStore::new();
        store.role_add("admin").unwrap();
        assert_eq!(
            store.role_add("admin"),
            Err(AuthError::RoleAlreadyExist)
        );
    }

    #[test]
    fn test_role_delete() {
        let store = AuthStore::new();
        store.role_add("admin").unwrap();
        assert!(store.role_delete("admin").is_ok());
        assert_eq!(store.role_get("admin"), Err(AuthError::RoleNotFound));
    }

    #[test]
    fn test_role_delete_not_found() {
        let store = AuthStore::new();
        assert_eq!(
            store.role_delete("nonexistent"),
            Err(AuthError::RoleNotFound)
        );
    }

    #[test]
    fn test_role_list() {
        let store = AuthStore::new();
        store.role_add("admin").unwrap();
        store.role_add("reader").unwrap();
        let roles = store.role_list().unwrap();
        assert!(roles.contains(&"admin".to_string()));
        assert!(roles.contains(&"reader".to_string()));
        assert_eq!(roles.len(), 2);
    }

    #[test]
    fn test_user_grant_and_revoke_role() {
        let store = AuthStore::new();
        store.user_add("alice", "pass", false).unwrap();
        store.role_add("admin").unwrap();

        store.user_grant_role("alice", "admin").unwrap();
        let user = store.user_get("alice").unwrap();
        assert!(user.roles.contains(&"admin".to_string()));

        store.user_revoke_role("alice", "admin").unwrap();
        let user = store.user_get("alice").unwrap();
        assert!(!user.roles.contains(&"admin".to_string()));
    }

    #[test]
    fn test_user_grant_role_not_found() {
        let store = AuthStore::new();
        store.user_add("alice", "pass", false).unwrap();
        assert_eq!(
            store.user_grant_role("alice", "nonexistent"),
            Err(AuthError::RoleNotFound)
        );
    }

    #[test]
    fn test_role_grant_permission() {
        let store = AuthStore::new();
        store.role_add("admin").unwrap();

        let perm = AuthPermission {
            perm_type: PermissionType::Read,
            key: b"/foo".to_vec(),
            range_end: Vec::new(),
        };
        assert!(store.role_grant_permission("admin", perm).is_ok());

        let role = store.role_get("admin").unwrap();
        assert_eq!(role.permissions.len(), 1);
        assert_eq!(role.permissions[0].perm_type, PermissionType::Read);
        assert_eq!(role.permissions[0].key, b"/foo");
    }

    #[test]
    fn test_role_revoke_permission() {
        let store = AuthStore::new();
        store.role_add("admin").unwrap();

        let perm = AuthPermission {
            perm_type: PermissionType::Read,
            key: b"/foo".to_vec(),
            range_end: Vec::new(),
        };
        store.role_grant_permission("admin", perm).unwrap();
        assert_eq!(store.role_get("admin").unwrap().permissions.len(), 1);

        store
            .role_revoke_permission("admin", b"/foo", b"")
            .unwrap();
        assert!(store.role_get("admin").unwrap().permissions.is_empty());
    }

    #[test]
    fn test_role_revoke_permission_not_found() {
        let store = AuthStore::new();
        store.role_add("admin").unwrap();
        assert_eq!(
            store.role_revoke_permission("admin", b"/foo", b""),
            Err(AuthError::PermissionNotGiven)
        );
    }

    #[test]
    fn test_authenticate() {
        let store = AuthStore::new();
        store.user_add("root", "root123", false).unwrap();
        store.role_add("root").unwrap();
        store.user_grant_role("root", "root").unwrap();
        store.auth_enable().unwrap();

        let token = store.authenticate("root", "root123").unwrap();
        assert!(token.starts_with("v1."));
        assert!(token.len() > 10);
    }

    #[test]
    fn test_authenticate_wrong_password() {
        let store = AuthStore::new();
        store.user_add("root", "root", false).unwrap();
        store.role_add("root").unwrap();
        store.user_grant_role("root", "root").unwrap();
        store.user_add("alice", "pass", false).unwrap();
        store.auth_enable().unwrap();
        assert_eq!(
            store.check_password("alice", "wrong"),
            Err(AuthError::AuthFailed)
        );
    }

    #[test]
    fn test_authenticate_disabled() {
        let store = AuthStore::new();
        store.user_add("root", "root123", false).unwrap();
        // Auth is not enabled
        assert!(store.authenticate("root", "root123").is_err());
    }

    #[test]
    fn test_authenticate_no_password_user() {
        let store = AuthStore::new();
        store.user_add("root", "root", false).unwrap();
        store.role_add("root").unwrap();
        store.user_grant_role("root", "root").unwrap();
        store.user_add("guest", "", true).unwrap();
        store.user_grant_role("guest", "root").unwrap();
        store.auth_enable().unwrap();

        // Check password for no-password user
        assert!(store.check_password("guest", "").is_ok());
    }

    #[test]
    fn test_validate_token() {
        let store = AuthStore::new();
        store.user_add("root", "root123", false).unwrap();
        store.role_add("root").unwrap();
        store.user_grant_role("root", "root").unwrap();
        store.auth_enable().unwrap();

        let token = store.authenticate("root", "root123").unwrap();
        let username = store.validate_token(&token).unwrap();
        assert_eq!(username, "root");
    }

    #[test]
    fn test_validate_token_invalid() {
        let store = AuthStore::new();
        store.user_add("root", "root123", false).unwrap();
        store.role_add("root").unwrap();
        store.user_grant_role("root", "root").unwrap();
        store.auth_enable().unwrap();

        assert_eq!(
            store.validate_token("invalid-token"),
            Err(AuthError::InvalidAuthToken)
        );
    }

    #[test]
    fn test_check_perm_disabled_auth() {
        let store = AuthStore::new();
        // Auth disabled: all permissions granted
        assert!(store
            .check_perm("anyone", b"/key", b"", PermissionType::Read)
            .is_ok());
    }

    #[test]
    fn test_check_perm_granted() {
        let store = AuthStore::new();
        store.user_add("alice", "pass", false).unwrap();
        store.role_add("reader").unwrap();

        let perm = AuthPermission {
            perm_type: PermissionType::Read,
            key: b"/foo".to_vec(),
            range_end: Vec::new(),
        };
        store.role_grant_permission("reader", perm).unwrap();
        store.user_grant_role("alice", "reader").unwrap();

        store.role_add("root").unwrap();
        store.user_add("root", "root123", false).unwrap();
        store.user_grant_role("root", "root").unwrap();
        store.auth_enable().unwrap();

        assert!(store
            .check_perm("alice", b"/foo", b"", PermissionType::Read)
            .is_ok());
    }

    #[test]
    fn test_check_perm_denied() {
        let store = AuthStore::new();
        store.user_add("alice", "pass", false).unwrap();
        store.role_add("reader").unwrap();

        let perm = AuthPermission {
            perm_type: PermissionType::Read,
            key: b"/foo".to_vec(),
            range_end: Vec::new(),
        };
        store.role_grant_permission("reader", perm).unwrap();
        store.user_grant_role("alice", "reader").unwrap();

        store.role_add("root").unwrap();
        store.user_add("root", "root123", false).unwrap();
        store.user_grant_role("root", "root").unwrap();
        store.auth_enable().unwrap();

        // No permission for /bar
        assert!(store
            .check_perm("alice", b"/bar", b"", PermissionType::Read)
            .is_err());
    }

    #[test]
    fn test_root_bypasses_permission_check() {
        let store = AuthStore::new();
        store.user_add("root", "root123", false).unwrap();
        store.role_add("root").unwrap();
        store.user_grant_role("root", "root").unwrap();
        store.auth_enable().unwrap();

        // Root has implicit full access
        assert!(store
            .check_perm("root", b"/any/key", b"", PermissionType::Read)
            .is_ok());
        assert!(store
            .check_perm("root", b"/any/key", b"", PermissionType::Write)
            .is_ok());
    }

    #[test]
    fn test_check_perm_range() {
        let store = AuthStore::new();
        store.user_add("alice", "pass", false).unwrap();
        store.role_add("prefix_reader").unwrap();

        let perm = AuthPermission {
            perm_type: PermissionType::Read,
            key: b"/foo/".to_vec(),
            range_end: b"/foo0".to_vec(),
        };
        store.role_grant_permission("prefix_reader", perm).unwrap();
        store.user_grant_role("alice", "prefix_reader").unwrap();

        store.role_add("root").unwrap();
        store.user_add("root", "root123", false).unwrap();
        store.user_grant_role("root", "root").unwrap();
        store.auth_enable().unwrap();

        // Key within range
        assert!(store
            .check_perm("alice", b"/foo/bar", b"", PermissionType::Read)
            .is_ok());
        // Key outside range
        assert!(store
            .check_perm("alice", b"/foobar", b"", PermissionType::Read)
            .is_err());
    }

    #[test]
    fn test_auth_error_display() {
        assert_eq!(
            format!("{}", AuthError::UserNotFound),
            "user not found"
        );
        assert_eq!(
            format!("{}", AuthError::RoleAlreadyExist),
            "role already exists"
        );
        assert_eq!(
            format!("{}", AuthError::AuthFailed),
            "authentication failed"
        );
        assert_eq!(
            format!("{}", AuthError::PermissionDenied),
            "permission denied"
        );
    }

    #[test]
    fn test_token_revocation() {
        let store = AuthStore::new();
        store.user_add("root", "root123", false).unwrap();
        store.role_add("root").unwrap();
        store.user_grant_role("root", "root").unwrap();
        store.auth_enable().unwrap();

        let token = store.authenticate("root", "root123").unwrap();
        assert!(store.validate_token(&token).is_ok());

        store.revoke_token(&token);
        assert_eq!(
            store.validate_token(&token),
            Err(AuthError::InvalidAuthToken)
        );
    }

    #[test]
    fn test_password_change_clears_no_password() {
        let store = AuthStore::new();
        store.user_add("guest", "", true).unwrap();
        let user = store.user_get("guest").unwrap();
        assert!(user.no_password);

        store.user_change_password("guest", "newpass").unwrap();
        let user = store.user_get("guest").unwrap();
        assert!(!user.no_password);
    }

    #[test]
    fn test_check_password_after_change() {
        let store = AuthStore::new();
        store.user_add("alice", "oldpass", false).unwrap();
        assert!(store.check_password("alice", "oldpass").is_ok());

        store.user_change_password("alice", "newpass").unwrap();
        assert!(store.check_password("alice", "newpass").is_ok());
        assert!(store.check_password("alice", "oldpass").is_err());
    }

    #[test]
    fn test_get_set_users_roles() {
        let store = AuthStore::new();
        store.user_add("alice", "pass", false).unwrap();
        store.role_add("admin").unwrap();

        let users = store.get_users();
        let roles = store.get_roles();

        assert_eq!(users.len(), 1);
        assert_eq!(roles.len(), 1);

        // Create a new store and restore
        let store2 = AuthStore::new();
        store2.set_users(users);
        store2.set_roles(roles);

        assert!(store2.user_get("alice").is_ok());
        assert!(store2.role_get("admin").is_ok());
    }

    #[test]
    fn test_revision_increments() {
        let store = AuthStore::new();
        let rev0 = store.revision();

        store.user_add("alice", "pass", false).unwrap();
        assert!(store.revision() > rev0);

        store.role_add("admin").unwrap();
        assert!(store.revision() > rev0 + 1);
    }

    #[test]
    fn test_list_tokens() {
        let store = AuthStore::new();
        store.user_add("root", "root123", false).unwrap();
        store.role_add("root").unwrap();
        store.user_grant_role("root", "root").unwrap();
        store.auth_enable().unwrap();

        let token = store.authenticate("root", "root123").unwrap();
        let tokens = store.list_tokens();
        assert!(tokens.contains(&token));
    }

    #[test]
    fn test_auth_not_enabled_authenticate() {
        let store = AuthStore::new();
        store.user_add("root", "root123", false).unwrap();
        // Auth is not enabled
        assert_eq!(
            store.authenticate("root", "root123"),
            Err(AuthError::AuthNotEnabled)
        );
    }
}
