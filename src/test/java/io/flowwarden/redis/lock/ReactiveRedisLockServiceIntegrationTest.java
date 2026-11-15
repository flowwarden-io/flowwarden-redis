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

import io.flowwarden.redis.test.SharedRedisContainer;
import io.flowwarden.stream.spi.LockService;
import io.flowwarden.stream.spi.testkit.LockServiceContractTest;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.time.Duration;

class ReactiveRedisLockServiceIntegrationTest extends LockServiceContractTest {

    private static final String KEY_PREFIX = "test:";

    private LettuceConnectionFactory connectionFactory;
    private ReactiveStringRedisTemplate template;

    @Override
    protected LockService createLockService() {
        return new ReactiveRedisLockService(template, KEY_PREFIX);
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

    /**
     * A Redis lease is the key itself: it expires when its TTL elapses. Shorten the TTL to
     * 1 ms and wait for the server to drop the key — the real expiry, not a simulation.
     */
    @Override
    protected boolean expireLeaseInPlace(String streamName) {
        String key = KEY_PREFIX + "lock:" + streamName;
        template.expire(key, Duration.ofMillis(1)).block();
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (Boolean.TRUE.equals(template.hasKey(key).block())) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("Redis did not expire " + key + " within 2s");
            }
            Thread.onSpinWait();
        }
        return true;
    }
}
