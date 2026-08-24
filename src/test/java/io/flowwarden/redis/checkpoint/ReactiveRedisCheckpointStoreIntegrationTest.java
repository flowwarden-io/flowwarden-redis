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

import io.flowwarden.redis.test.SharedRedisContainer;
import io.flowwarden.stream.spi.Checkpoint;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.testkit.CheckpointStoreContractTest;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ReactiveRedisCheckpointStoreIntegrationTest extends CheckpointStoreContractTest {

    private LettuceConnectionFactory connectionFactory;
    private ReactiveStringRedisTemplate template;

    @Override
    protected CheckpointStore createCheckpointStore() {
        return new ReactiveRedisCheckpointStore(template, "test:");
    }

    @Override
    protected void cleanState() {
        if (connectionFactory == null) {
            RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(
                    SharedRedisContainer.host(),
                    SharedRedisContainer.port());
            connectionFactory = new LettuceConnectionFactory(config);
            connectionFactory.afterPropertiesSet();
            RedisSerializationContext<String, String> ctx = RedisSerializationContext
                    .<String, String>newSerializationContext(StringRedisSerializer.UTF_8)
                    .build();
            template = new ReactiveStringRedisTemplate(connectionFactory, ctx);
        }
        connectionFactory.getConnection().serverCommands().flushDb();
    }

    // --- Redis-specific: pin the ATOMIC Lua path, not just its result ----
    // Discriminating oracle: a hash field unknown to the SPI survives the
    // reset. The SPI's sequential fallback rebuilds the checkpoint through
    // save() (DEL + rewrite) and cannot preserve it — so a regression that
    // dropped the Lua override back to the default turns these red.

    @Test
    void resetAfterHistoryLost_guardMatch_runsInPlace_preservingUnknownFields() {
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var dead = BsonDocument.parse("{\"_data\": \"lua-dead\"}");
        var fresh = BsonDocument.parse("{\"_data\": \"lua-fresh\"}");
        store.save(new Checkpoint("lua-s", "pod-a", null, null, dead, now, null, Map.of()));
        template.opsForHash().put("test:checkpoint:lua-s", "customField", "keep-me").block();

        store.resetAfterHistoryLost("lua-s", fresh, dead, now.plusSeconds(60));

        var found = store.findByStreamName("lua-s").orElseThrow();
        assertThat(found.lastProcessedToken()).isNull();
        assertThat(found.lastSeenToken()).isEqualTo(fresh);
        assertThat(found.lastHeartbeatTimestamp()).isEqualTo(now.plusSeconds(60));
        assertThat(template.opsForHash().get("test:checkpoint:lua-s", "customField").block())
                .as("the atomic in-place script must preserve fields unknown to the SPI")
                .isEqualTo("keep-me");
    }

    @Test
    void resetAfterHistoryLost_guardMismatch_preservesProcessedAndUnknownFields() {
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var current = BsonDocument.parse("{\"_data\": \"lua-current\"}");
        var expected = BsonDocument.parse("{\"_data\": \"lua-expected\"}");
        var fresh = BsonDocument.parse("{\"_data\": \"lua-fresh\"}");
        store.save(new Checkpoint("lua-s2", "pod-a", null, null, current, now, null, Map.of()));
        template.opsForHash().put("test:checkpoint:lua-s2", "customField", "keep-me").block();

        store.resetAfterHistoryLost("lua-s2", fresh, expected, now.plusSeconds(60));

        var found = store.findByStreamName("lua-s2").orElseThrow();
        assertThat(found.lastProcessedToken())
                .as("the dead-processed guard must preserve a non-matching token")
                .isEqualTo(current);
        assertThat(found.lastSeenToken()).isEqualTo(fresh);
        assertThat(template.opsForHash().get("test:checkpoint:lua-s2", "customField").block())
                .isEqualTo("keep-me");
    }
}
