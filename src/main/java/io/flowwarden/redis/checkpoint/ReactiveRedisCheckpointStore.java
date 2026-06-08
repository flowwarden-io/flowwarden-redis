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
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static io.flowwarden.redis.checkpoint.RedisCheckpointStore.F_INSTANCE;
import static io.flowwarden.redis.checkpoint.RedisCheckpointStore.F_METADATA;
import static io.flowwarden.redis.checkpoint.RedisCheckpointStore.F_PRESENT;
import static io.flowwarden.redis.checkpoint.RedisCheckpointStore.F_PROCESSED_TIMESTAMP;
import static io.flowwarden.redis.checkpoint.RedisCheckpointStore.F_PROCESSED_TOKEN;
import static io.flowwarden.redis.checkpoint.RedisCheckpointStore.F_SEEN_TIMESTAMP;
import static io.flowwarden.redis.checkpoint.RedisCheckpointStore.F_SEEN_TOKEN;
import static io.flowwarden.redis.checkpoint.RedisCheckpointStore.decodeInstant;
import static io.flowwarden.redis.checkpoint.RedisCheckpointStore.decodeMetadata;
import static io.flowwarden.redis.checkpoint.RedisCheckpointStore.encodeInstant;
import static io.flowwarden.redis.checkpoint.RedisCheckpointStore.encodeMetadata;

/**
 * Reactive-template-backed {@link CheckpointStore}.
 *
 * <p>Mirrors {@link RedisCheckpointStore} but consumes a
 * {@link ReactiveStringRedisTemplate}. The synchronous SPI contract is implemented
 * by blocking on the reactive call — fine for checkpoint operations which are
 * single-key Hash writes.</p>
 */
public class ReactiveRedisCheckpointStore implements CheckpointStore {

    private final ReactiveStringRedisTemplate template;
    private final String keyPrefix;

    public ReactiveRedisCheckpointStore(ReactiveStringRedisTemplate template, String keyPrefix) {
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
        putIfNotNull(hash, F_METADATA, encodeMetadata(checkpoint.metadata()));

        String key = checkpointKey(checkpoint.streamName());
        template.delete(key).block();
        template.<String, String>opsForHash().putAll(key, hash).block();
    }

    @Override
    public Optional<Checkpoint> findByStreamName(String streamName) {
        String key = checkpointKey(streamName);
        Map<Object, Object> raw = template.<Object, Object>opsForHash()
                .entries(key)
                .collectMap(Map.Entry::getKey, Map.Entry::getValue)
                .block();
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
                decodeMetadata(stringField(raw, F_METADATA))
        ));
    }

    @Override
    public void saveSeen(String streamName, BsonDocument token, Instant timestamp) {
        ReactiveHashOperations<String, String, String> ops = template.opsForHash();
        Map<String, String> updates = new HashMap<>();
        updates.put(F_SEEN_TOKEN, BsonTokens.encode(token));
        updates.put(F_SEEN_TIMESTAMP, encodeInstant(timestamp));
        ops.putAll(checkpointKey(streamName), updates).block();
    }

    @Override
    public void saveProcessed(String streamName, BsonDocument token, Instant timestamp) {
        ReactiveHashOperations<String, String, String> ops = template.opsForHash();
        Map<String, String> updates = new HashMap<>();
        updates.put(F_PROCESSED_TOKEN, BsonTokens.encode(token));
        updates.put(F_PROCESSED_TIMESTAMP, encodeInstant(timestamp));
        ops.putAll(checkpointKey(streamName), updates).block();
    }

    @Override
    public void delete(String streamName) {
        template.delete(checkpointKey(streamName)).block();
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
}
