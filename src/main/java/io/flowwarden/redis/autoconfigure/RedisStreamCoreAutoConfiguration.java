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
package io.flowwarden.redis.autoconfigure;

import io.flowwarden.redis.checkpoint.RedisCheckpointStore;
import io.flowwarden.redis.lock.RedisLockService;
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.LockService;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Wires the blocking Redis-backed implementations of {@link LockService} and
 * {@link CheckpointStore} when a {@code StringRedisTemplate} is available on the
 * classpath and no user-defined bean of the same type already exists.
 *
 * <p>The reactive variants ({@code ReactiveRedisLockService},
 * {@code ReactiveRedisCheckpointStore}) ship in the library but are <strong>not</strong>
 * wired automatically. Spring Boot's Redis auto-configuration creates both a
 * blocking and a reactive template whenever Lettuce is on the classpath (typical
 * Spring WebFlux setup), so there is no reliable conditional that would let us
 * pick one without surprising the user. Apps that explicitly want the reactive
 * variants declare them in their own {@code @Configuration}:</p>
 *
 * <pre>{@code
 * @Bean
 * LockService lockService(ReactiveStringRedisTemplate t, RedisStreamCoreProperties p) {
 *     return new ReactiveRedisLockService(t, p.getKeyPrefix());
 * }
 * }</pre>
 *
 * <p>{@code @ConditionalOnMissingBean} on the auto-configured beans defers to any
 * user-declared {@code LockService} / {@code CheckpointStore}, so user beans win
 * without further configuration.</p>
 */
@AutoConfiguration(after = RedisAutoConfiguration.class)
@ConditionalOnClass(RedisTemplate.class)
@ConditionalOnBean(StringRedisTemplate.class)
@EnableConfigurationProperties(RedisStreamCoreProperties.class)
public class RedisStreamCoreAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public LockService lockService(StringRedisTemplate template,
                                   RedisStreamCoreProperties properties) {
        return new RedisLockService(template, properties.getKeyPrefix());
    }

    @Bean
    @ConditionalOnMissingBean
    public CheckpointStore checkpointStore(StringRedisTemplate template,
                                           RedisStreamCoreProperties properties) {
        return new RedisCheckpointStore(template, properties.getKeyPrefix());
    }
}
