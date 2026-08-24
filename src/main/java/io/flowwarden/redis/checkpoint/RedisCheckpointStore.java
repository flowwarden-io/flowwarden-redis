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

import io.flowwarden.redis.internal.BsonTokens;
import io.flowwarden.stream.spi.Checkpoint;
import io.flowwarden.stream.spi.CheckpointStore;
import org.bson.BsonDocument;
import org.bson.Document;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Redis-backed {@link CheckpointStore}.
 *
 * <p>Each checkpoint is a Redis Hash at key {@code {keyPrefix}checkpoint:{streamName}}.
 * Resume tokens are encoded as base64 BSON binary (see {@link BsonTokens}). The
 * targeted {@code HSET} used by {@link #saveSeen} / {@link #saveProcessed} respects
 * the dual-token semantics of the SPI: each method updates only its half of the
 * checkpoint without touching the other.</p>
 */
public class RedisCheckpointStore implements CheckpointStore {

    static final String F_INSTANCE = "instanceId";
    static final String F_SEEN_TOKEN = "lastSeenToken";
    static final String F_SEEN_TIMESTAMP = "lastSeenTimestamp";
    static final String F_PROCESSED_TOKEN = "lastProcessedToken";
    static final String F_PROCESSED_TIMESTAMP = "lastProcessedTimestamp";
    static final String F_HEARTBEAT_TIMESTAMP = "lastHeartbeatTimestamp";
    static final String F_METADATA = "metadata";

    /** Presence marker — guarantees the Hash exists after {@code save} even when all
     *  user-facing fields are null, so {@code findByStreamName} can distinguish
     *  "never saved" from "saved with empty state". */
    static final String F_PRESENT = "_v";

    private final StringRedisTemplate template;
    private final String keyPrefix;

    public RedisCheckpointStore(StringRedisTemplate template, String keyPrefix) {
        this.template = template;
        this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
    }

    @Override
    public void save(Checkpoint checkpoint) {
        Map<String, String> hash = new LinkedHashMap<>();
        hash.put(F_PRESENT, "1");
        putIfNotNull(hash, F_INSTANCE, checkpoint.instanceId());
        putIfNotNull(hash, F_SEEN_TOKEN, BsonTokens.encode(checkpoint.lastSeenToken()));
        putIfNotNull(hash, F_SEEN_TIMESTAMP, encodeInstant(checkpoint.lastSeenTimestamp()));
        putIfNotNull(hash, F_PROCESSED_TOKEN, BsonTokens.encode(checkpoint.lastProcessedToken()));
        putIfNotNull(hash, F_PROCESSED_TIMESTAMP, encodeInstant(checkpoint.lastProcessedTimestamp()));
        putIfNotNull(hash, F_HEARTBEAT_TIMESTAMP, encodeInstant(checkpoint.lastHeartbeatTimestamp()));
        putIfNotNull(hash, F_METADATA, encodeMetadata(checkpoint.metadata()));

        String key = checkpointKey(checkpoint.streamName());
        template.delete(key);
        template.<String, String>opsForHash().putAll(key, hash);
    }

    @Override
    public Optional<Checkpoint> findByStreamName(String streamName) {
        String key = checkpointKey(streamName);
        Map<Object, Object> raw = template.opsForHash().entries(key);
        if (raw == null || raw.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new Checkpoint(
                streamName,
                stringField(raw, F_INSTANCE),
                BsonTokens.decode(stringField(raw, F_SEEN_TOKEN)),
                decodeInstant(stringField(raw, F_SEEN_TIMESTAMP)),
                BsonTokens.decode(stringField(raw, F_PROCESSED_TOKEN)),
                decodeInstant(stringField(raw, F_PROCESSED_TIMESTAMP)),
                decodeInstant(stringField(raw, F_HEARTBEAT_TIMESTAMP)),
                decodeMetadata(stringField(raw, F_METADATA))
        ));
    }

    @Override
    public void saveSeen(String streamName, BsonDocument token, Instant timestamp) {
        HashOperations<String, String, String> ops = template.opsForHash();
        Map<String, String> updates = new HashMap<>();
        updates.put(F_SEEN_TOKEN, BsonTokens.encode(token));
        updates.put(F_SEEN_TIMESTAMP, encodeInstant(timestamp));
        ops.putAll(checkpointKey(streamName), updates);
    }

    @Override
    public void saveProcessed(String streamName, BsonDocument token, Instant timestamp) {
        HashOperations<String, String, String> ops = template.opsForHash();
        Map<String, String> updates = new HashMap<>();
        updates.put(F_PROCESSED_TOKEN, BsonTokens.encode(token));
        updates.put(F_PROCESSED_TIMESTAMP, encodeInstant(timestamp));
        ops.putAll(checkpointKey(streamName), updates);
    }

    @Override
    public void saveSeen(String streamName, BsonDocument token, Instant timestamp,
                         Instant heartbeatTimestamp) {
        // One multi-field HSET — the atomic position+confirmation write the
        // SPI asks for (its default is a non-atomic two-write fallback).
        HashOperations<String, String, String> ops = template.opsForHash();
        Map<String, String> updates = new HashMap<>();
        updates.put(F_SEEN_TOKEN, BsonTokens.encode(token));
        updates.put(F_SEEN_TIMESTAMP, encodeInstant(timestamp));
        updates.put(F_HEARTBEAT_TIMESTAMP, encodeInstant(heartbeatTimestamp));
        ops.putAll(checkpointKey(streamName), updates);
    }

    @Override
    public void saveHeartbeat(String streamName, Instant heartbeatTimestamp) {
        template.<String, String>opsForHash().put(checkpointKey(streamName),
                F_HEARTBEAT_TIMESTAMP, encodeInstant(heartbeatTimestamp));
    }

    @Override
    public void resetAfterHistoryLost(String streamName, BsonDocument freshSeenToken,
                                      BsonDocument expectedDeadProcessed, Instant timestamp) {
        // Lua: the dead-processed guard is evaluated atomically with the
        // removal — the SPI's race-free coordination point.
        template.execute(
                org.springframework.data.redis.core.script.RedisScript.of(
                        CheckpointScripts.RESET_AFTER_HISTORY_LOST, Long.class),
                Collections.singletonList(checkpointKey(streamName)),
                encodeOrSentinel(expectedDeadProcessed),
                encodeOrSentinel(freshSeenToken),
                encodeInstant(timestamp));
    }

    static String encodeOrSentinel(BsonDocument token) {
        String encoded = BsonTokens.encode(token);
        return encoded == null ? CheckpointScripts.NULL_SENTINEL : encoded;
    }

    @Override
    public void delete(String streamName) {
        template.delete(checkpointKey(streamName));
    }

    private String checkpointKey(String streamName) {
        return keyPrefix + "checkpoint:" + streamName;
    }

    private static void putIfNotNull(Map<String, String> hash, String field, String value) {
        if (value != null) {
            hash.put(field, value);
        }
    }

    private static String stringField(Map<Object, Object> hash, String field) {
        Object value = hash.get(field);
        return value == null ? null : value.toString();
    }

    static String encodeInstant(Instant instant) {
        return instant == null ? null : Long.toString(instant.toEpochMilli());
    }

    static Instant decodeInstant(String value) {
        if (value == null) {
            return null;
        }
        try {
            return Instant.ofEpochMilli(Long.parseLong(value));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    static String encodeMetadata(Map<String, Object> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return null;
        }
        return new Document(metadata).toJson();
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> decodeMetadata(String json) {
        if (json == null || json.isEmpty()) {
            return Collections.emptyMap();
        }
        return (Map<String, Object>) (Map<?, ?>) Document.parse(json);
    }
}
