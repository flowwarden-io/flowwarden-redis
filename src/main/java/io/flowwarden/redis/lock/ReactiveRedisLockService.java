/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.flowwarden.redis.lock;

import io.flowwarden.stream.spi.LockService;
import io.flowwarden.stream.spi.LockState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Reactive-template-backed {@link LockService}.
 *
 * <p>Implements the synchronous SPI by blocking on the underlying reactive call —
 * acceptable because lock operations are short single round-trips. Targets Spring
 * WebFlux applications that have a {@link ReactiveStringRedisTemplate} configured
 * rather than a blocking template.</p>
 */
public class ReactiveRedisLockService implements LockService {

    private static final Logger log = LoggerFactory.getLogger(ReactiveRedisLockService.class);

    private static final String F_INSTANCE = "instanceId";
    private static final String F_ACQUIRED = "acquiredAt";
    private static final String F_EXPIRES = "expiresAt";

    private final ReactiveStringRedisTemplate template;
    private final String keyPrefix;
    private final RedisScript<Long> acquireScript;
    private final RedisScript<Long> renewScript;
    private final RedisScript<Long> releaseScript;

    public ReactiveRedisLockService(ReactiveStringRedisTemplate template, String keyPrefix) {
        this.template = template;
        this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
        this.acquireScript = RedisScript.of(LockScripts.ACQUIRE_OR_REOWN, Long.class);
        this.renewScript = RedisScript.of(LockScripts.RENEW_IF_OWNER, Long.class);
        this.releaseScript = RedisScript.of(LockScripts.RELEASE_IF_OWNER, Long.class);
    }

    @Override
    public boolean tryAcquire(String streamName, String instanceId, Duration ttl) {
        try {
            long acquiredAt = System.currentTimeMillis();
            long expiresAt = acquiredAt + ttl.toMillis();
            Long result = template.execute(
                    acquireScript,
                    List.of(lockKey(streamName)),
                    List.of(instanceId, Long.toString(acquiredAt), Long.toString(expiresAt))
            ).blockFirst();
            boolean acquired = result != null && result == 1L;
            if (acquired) {
                log.debug("Acquired lock for stream '{}' (instanceId={})", streamName, instanceId);
            }
            return acquired;
        } catch (RuntimeException e) {
            log.warn("Failed to acquire lock for stream '{}': {}", streamName, e.getMessage());
            return false;
        }
    }

    @Override
    public boolean renew(String streamName, String instanceId, Duration ttl) {
        try {
            long expiresAt = System.currentTimeMillis() + ttl.toMillis();
            Long result = template.execute(
                    renewScript,
                    List.of(lockKey(streamName)),
                    List.of(instanceId, Long.toString(expiresAt))
            ).blockFirst();
            return result != null && result == 1L;
        } catch (RuntimeException e) {
            log.warn("Failed to renew lock for stream '{}': {}", streamName, e.getMessage());
            return false;
        }
    }

    @Override
    public void release(String streamName, String instanceId) {
        try {
            Long result = template.execute(
                    releaseScript,
                    List.of(lockKey(streamName)),
                    List.of(instanceId)
            ).blockFirst();
            if (result != null && result == 1L) {
                log.debug("Released lock for stream '{}' (instanceId={})", streamName, instanceId);
            }
        } catch (RuntimeException e) {
            log.warn("Failed to release lock for stream '{}': {}", streamName, e.getMessage());
        }
    }

    @Override
    public Optional<LockState> getLockState(String streamName) {
        try {
            Map<Object, Object> hash = template.<Object, Object>opsForHash()
                    .entries(lockKey(streamName))
                    .collectMap(Map.Entry::getKey, Map.Entry::getValue)
                    .block();
            if (hash == null || hash.isEmpty()) {
                return Optional.empty();
            }
            String instanceId = stringValue(hash, F_INSTANCE);
            Long acquiredAt = longValue(hash, F_ACQUIRED);
            Long expiresAt = longValue(hash, F_EXPIRES);
            if (instanceId == null || acquiredAt == null || expiresAt == null) {
                return Optional.empty();
            }
            return Optional.of(new LockState(
                    streamName,
                    instanceId,
                    Instant.ofEpochMilli(acquiredAt),
                    Instant.ofEpochMilli(expiresAt)));
        } catch (RuntimeException e) {
            log.warn("Failed to read lock state for stream '{}': {}", streamName, e.getMessage());
            return Optional.empty();
        }
    }

    private String lockKey(String streamName) {
        return keyPrefix + "lock:" + streamName;
    }

    private static String stringValue(Map<Object, Object> hash, String field) {
        Object value = hash.get(field);
        return value == null ? null : value.toString();
    }

    private static Long longValue(Map<Object, Object> hash, String field) {
        Object value = hash.get(field);
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
