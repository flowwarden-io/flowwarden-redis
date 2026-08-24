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
package io.flowwarden.redis.checkpoint;

/**
 * Lua scripts shared by {@link RedisCheckpointStore} and
 * {@link ReactiveRedisCheckpointStore}.
 *
 * <p>Only {@code resetAfterHistoryLost} needs a script: its dead-processed
 * guard must be evaluated atomically with the removal (the SPI's race-free
 * coordination point). Every other checkpoint write is a single multi-field
 * {@code HSET} — atomic by construction in Redis.</p>
 */
final class CheckpointScripts {

    private CheckpointScripts() {
    }

    /**
     * Sentinel for "null token" in script arguments — safe because encoded
     * tokens (base64 BSON) are never empty.
     */
    static final String NULL_SENTINEL = "";

    /**
     * KEYS[1] = checkpoint hash key; ARGV[1] = expected dead processed token
     * (or sentinel); ARGV[2] = fresh seen token (or sentinel); ARGV[3] = the
     * recovery timestamp (epoch millis).
     *
     * <p>Field names are duplicated from {@code RedisCheckpointStore.F_*} —
     * keep in sync. Semantics mirror the SPI contract: the processed pair is
     * removed only while it still equals the expected dead value (absence
     * matching absence); the seen pair and the heartbeat are installed
     * together for a fresh token, or cleared together for a null one (a
     * heartbeat without a recoverable position would be a lie). Everything
     * else in the hash is untouched.</p>
     */
    static final String RESET_AFTER_HISTORY_LOST = """
            local expected = ARGV[1]
            local fresh = ARGV[2]
            local ts = ARGV[3]
            local current = redis.call('HGET', KEYS[1], 'lastProcessedToken')
            local stillDead
            if expected == '' then
              stillDead = (current == false)
            else
              stillDead = (current == expected)
            end
            if stillDead then
              redis.call('HDEL', KEYS[1], 'lastProcessedToken', 'lastProcessedTimestamp')
            end
            if fresh == '' then
              redis.call('HDEL', KEYS[1], 'lastSeenToken', 'lastSeenTimestamp', 'lastHeartbeatTimestamp')
            else
              redis.call('HSET', KEYS[1], 'lastSeenToken', fresh, 'lastSeenTimestamp', ts, 'lastHeartbeatTimestamp', ts)
            end
            return 1
            """;
}
