# Changelog

All notable changes to FlowWarden Redis will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

### Removed

### Fixed

### Deprecated

### Security

## [1.0.0-rc.2] — 2026-08-28

### Added
- `saveProcessed(stream, token, timestamp, heartbeatTimestamp)` overridden in both stores as a single multi-field `HSET` (processed token + timestamp + heartbeat in one write) — the atomic form of the partial write stream-core 1.0.0-rc.4 issues on every settled-anchor persistence.
- Checkpoint hashes carry the new `lastHeartbeatTimestamp` field (round-tripped by `save`/`findByStreamName` in both stores) — the resume-point health signal introduced by stream-core's idle heartbeat.
- `saveSeen(stream, token, timestamp, heartbeatTimestamp)` and `saveHeartbeat(stream, timestamp)` overridden in both stores as single multi-field `HSET`s — atomic by construction, stronger than the SPI's non-atomic two-write default.
- `resetAfterHistoryLost` overridden with a Lua script (shared by both stores): the dead-processed guard is evaluated atomically with the removal — the race-free at-least-once coordination point the SPI requires from backends that can express conditional writes.

### Changed
- `flowwarden-stream-core` / testkit baseline: 1.0.0-rc.4 (the inherited `CheckpointStoreContractTest` grows eight heartbeat/reset contract tests, all passing).

### Removed

### Fixed

### Deprecated

### Security

## [1.0.0-rc.1] — 2026-07-06

### Added
- Initial bootstrap of the FlowWarden Redis satellite library.
- `RedisLockService` and `ReactiveRedisLockService` — Redis-backed implementations of the `LockService` SPI, using a Hash payload (`instanceId`, `acquiredAt`, `expiresAt`) and Lua scripts for atomic acquire / renew / release.
- `RedisCheckpointStore` and `ReactiveRedisCheckpointStore` — Redis-backed implementations of the `CheckpointStore` SPI, using a Hash payload with base64-encoded BSON resume tokens. Targeted `HSET` preserves the dual-token semantics from the core (`saveSeen` does not overwrite the processed pair, `saveProcessed` does not overwrite the seen pair).
- Spring Boot auto-configuration (`RedisStreamCoreAutoConfiguration`) wires the blocking backends automatically when `StringRedisTemplate` is on the classpath. Guarded by `@ConditionalOnMissingBean`, so explicit user beans win. The reactive variants ship in the library and are wired by user `@Bean` declaration (see README).
- `flowwarden.redis.key-prefix` configuration property (default `fw:`) for namespacing on a multi-tenant Redis.
- Integration tests inherit from `LockServiceContractTest` and `CheckpointStoreContractTest` of `flowwarden-stream-core-testkit`, validated against Testcontainers `redis:7-alpine`.

### Changed

### Removed

### Fixed

### Deprecated

### Security

[Unreleased]: https://github.com/flowwarden-io/flowwarden-redis/compare/v1.0.0-rc.2...HEAD
[1.0.0-rc.2]: https://github.com/flowwarden-io/flowwarden-redis/releases/tag/v1.0.0-rc.2
[1.0.0-rc.1]: https://github.com/flowwarden-io/flowwarden-redis/releases/tag/v1.0.0-rc.1
