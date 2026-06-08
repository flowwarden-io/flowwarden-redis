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

/**
 * Lua scripts that implement the atomic compare-and-set semantics of
 * {@code LockService}. Each script keeps acquire / renew / release as a single
 * Redis round-trip, avoiding races between read and write.
 *
 * <p>Spring Data Redis loads the scripts at first use and caches the SHA1 server-side,
 * so subsequent invocations run via {@code EVALSHA}.</p>
 */
final class LockScripts {

    /**
     * Acquire the lock if it is free OR already owned by the same instance.
     *
     * <p>KEYS[1] = lock key, ARGV[1] = instanceId, ARGV[2] = acquiredAt (epoch ms),
     * ARGV[3] = expiresAt (epoch ms). Returns 1 on success, 0 on conflict.</p>
     *
     * <p>Free OR reowned semantics map the contract test
     * {@code tryAcquire_succeedsIfAlreadyOwned}: the SPI must succeed when the
     * caller re-asks for a lock it already holds.</p>
     */
    static final String ACQUIRE_OR_REOWN = """
            local current = redis.call('HGET', KEYS[1], 'instanceId')
            if (not current) or (current == ARGV[1]) then
              redis.call('HSET', KEYS[1], 'instanceId', ARGV[1], 'acquiredAt', ARGV[2], 'expiresAt', ARGV[3])
              redis.call('PEXPIREAT', KEYS[1], ARGV[3])
              return 1
            end
            return 0
            """;

    /**
     * Renew the lock's TTL if and only if the caller still owns it.
     *
     * <p>KEYS[1] = lock key, ARGV[1] = instanceId, ARGV[2] = newExpiresAt (epoch ms).
     * Returns 1 on success, 0 if not the owner or no lock exists.</p>
     */
    static final String RENEW_IF_OWNER = """
            local current = redis.call('HGET', KEYS[1], 'instanceId')
            if current == ARGV[1] then
              redis.call('HSET', KEYS[1], 'expiresAt', ARGV[2])
              redis.call('PEXPIREAT', KEYS[1], ARGV[2])
              return 1
            end
            return 0
            """;

    /**
     * Delete the lock key if and only if the caller owns it. No-op otherwise.
     *
     * <p>KEYS[1] = lock key, ARGV[1] = instanceId. Returns 1 on delete, 0 on no-op.</p>
     */
    static final String RELEASE_IF_OWNER = """
            local current = redis.call('HGET', KEYS[1], 'instanceId')
            if current == ARGV[1] then
              redis.call('DEL', KEYS[1])
              return 1
            end
            return 0
            """;

    private LockScripts() {}
}
