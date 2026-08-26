<p align="center">
  <strong>FlowWarden Redis</strong><br/>
  Redis-backed <code>LockService</code> and <code>CheckpointStore</code> for FlowWarden Stream Core.
</p>

<p align="center">
  <a href="https://github.com/flowwarden-io/flowwarden-redis/actions/workflows/ci.yml"><img src="https://github.com/flowwarden-io/flowwarden-redis/actions/workflows/ci.yml/badge.svg?branch=main" alt="CI"></a>
  <a href="https://www.apache.org/licenses/LICENSE-2.0"><img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="License"></a>
  <a href="https://www.oracle.com/java/technologies/javase/jdk17-archive-downloads.html"><img src="https://img.shields.io/badge/Java-17%2B-orange.svg" alt="Java 17+"></a>
  <a href="https://spring.io/projects/spring-boot"><img src="https://img.shields.io/badge/Spring_Boot-3.x-brightgreen.svg" alt="Spring Boot 3.x"></a>
  <a href="https://central.sonatype.com/artifact/io.flowwarden/flowwarden-redis"><img src="https://img.shields.io/maven-central/v/io.flowwarden/flowwarden-redis.svg" alt="Maven Central"></a>
</p>

---

## What is this?

`flowwarden-redis` provides Redis-backed implementations of the `LockService` and `CheckpointStore` SPIs defined in [`flowwarden-stream-core`](https://github.com/flowwarden-io/flowwarden-stream-core). It is a drop-in alternative to the default MongoDB backends shipped in the core library, useful when your Spring Boot application already runs Redis (cache, sessions, pub/sub) and you'd prefer to keep your distributed locks and Change Stream checkpoints there instead of in MongoDB.

Both the blocking and reactive Spring Data Redis stacks are supported. Auto-configuration wires the right beans automatically.

## Add the dependency

**Maven**

```xml
<dependency>
    <groupId>io.flowwarden</groupId>
    <artifactId>flowwarden-redis</artifactId>
    <version>1.0.0-rc.1</version>
</dependency>
```

You'll also need `flowwarden-stream-core` (the SPI provider) and `spring-boot-starter-data-redis` (the Redis client). If you import the `flowwarden-bom`, version coordination is handled for you:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>io.flowwarden</groupId>
      <artifactId>flowwarden-bom</artifactId>
      <version>1.0.0-rc.3</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>io.flowwarden</groupId>
    <artifactId>flowwarden-stream-core</artifactId>
  </dependency>
  <dependency>
    <groupId>io.flowwarden</groupId>
    <artifactId>flowwarden-redis</artifactId>
  </dependency>
  <dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-redis</artifactId>
  </dependency>
</dependencies>
```

## Configure

Configure your Redis connection the standard Spring Boot way:

```yaml
spring:
  data:
    redis:
      host: localhost
      port: 6379
```

Optional: namespace the Redis keys used by FlowWarden (defaults to `fw:`):

```yaml
flowwarden:
  redis:
    key-prefix: "myapp:"
```

## What gets wired

When `StringRedisTemplate` is on the classpath (the default whenever you include `spring-boot-starter-data-redis`), FlowWarden registers:

- `RedisLockService` as the `LockService` bean — replaces the default `MongoLockService` from the core.
- `RedisCheckpointStore` as the `CheckpointStore` bean — replaces the default `MongoCheckpointStore`.

Both registrations are guarded by `@ConditionalOnMissingBean`, so any explicit `@Bean LockService` or `@Bean CheckpointStore` you declare wins. This is the escape hatch to cohabit Mongo + Redis backends in the same app (for example: locks in Redis, checkpoints in Mongo).

### Reactive variants

`ReactiveRedisLockService` and `ReactiveRedisCheckpointStore` ship in the library but are **not auto-configured** — Spring Boot's Redis starter creates both blocking and reactive templates whenever Lettuce is on the classpath, so there is no reliable conditional that picks one without surprising the user. Wire them explicitly when you want non-blocking Redis ops in a Spring WebFlux app:

```java
@Bean
LockService lockService(ReactiveStringRedisTemplate t, RedisStreamCoreProperties p) {
    return new ReactiveRedisLockService(t, p.getKeyPrefix());
}

@Bean
CheckpointStore checkpointStore(ReactiveStringRedisTemplate t, RedisStreamCoreProperties p) {
    return new ReactiveRedisCheckpointStore(t, p.getKeyPrefix());
}
```

The user beans win over the auto-configured blocking ones automatically.

## Storage layout

| Key | Type | Description |
|---|---|---|
| `{prefix}lock:{streamName}` | Hash | `instanceId`, `acquiredAt`, `expiresAt`. TTL via `PEXPIREAT` to `expiresAt`. |
| `{prefix}checkpoint:{streamName}` | Hash | `instanceId`, `lastSeenToken` (base64 BSON), `lastSeenTimestamp`, `lastProcessedToken` (base64 BSON), `lastProcessedTimestamp`, `metadata` (JSON). No TTL. |

Acquire / renew / release on locks are executed as Lua scripts to keep the compare-and-set atomic. `saveSeen` and `saveProcessed` use targeted `HSET` so the dual-token semantics from the core (see `Checkpoint` Javadoc) are preserved natively rather than via the SPI's read-modify-write default.

## Compatibility

| Component | Minimum | Recommended |
|---|---|---|
| Java | 17 | 21 |
| Spring Boot | 3.2.x | 3.2.x+ |
| Spring Data Redis | 3.2.x (via Boot BOM) | 3.2.x+ |
| Redis Server | 6.0 | 7.x+ |
| FlowWarden Stream Core | 1.0.0-rc.3 | 1.0.0-rc.3+ |

## FlowWarden Ecosystem

| Component | Description | License |
|-----------|-------------|---------|
| **[flowwarden-stream-core](https://github.com/flowwarden-io/flowwarden-stream-core)** | Declarative MongoDB Change Streams library for Spring Boot | Apache 2.0 |
| **[flowwarden-javers](https://github.com/flowwarden-io/flowwarden-javers)** | Native Javers audit stream integration | Apache 2.0 |
| **[flowwarden-redis](https://github.com/flowwarden-io/flowwarden-redis)** | Redis-backed `LockService` and `CheckpointStore` backends | Apache 2.0 |
| **[flowwarden-amqp](https://github.com/flowwarden-io/flowwarden-amqp)** | AMQP (RabbitMQ) publish-only dead-letter queue store | Apache 2.0 |
| **flowwarden-rabbit-streams** | RabbitMQ Streams-backed dead-letter queue store | Apache 2.0 |
| **flowwarden-reporter** | Connects your streams to FlowWarden Console for monitoring | Apache 2.0 |
| **FlowWarden Console** | Dashboard for monitoring, alerting, and managing Change Streams | Commercial |

## Documentation

Full documentation is available at **[docs.flowwarden.io](https://docs.flowwarden.io)** — start with the [Redis backend guide](https://docs.flowwarden.io/redis).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and the [Code of Conduct](https://github.com/flowwarden-io/.github/blob/main/CODE_OF_CONDUCT.md).

## License

Apache License 2.0 — see [LICENSE](LICENSE).
