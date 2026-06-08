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
import io.flowwarden.stream.spi.CheckpointStore;
import io.flowwarden.stream.spi.testkit.CheckpointStoreContractTest;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

class RedisCheckpointStoreIntegrationTest extends CheckpointStoreContractTest {

    private LettuceConnectionFactory connectionFactory;
    private StringRedisTemplate template;

    @Override
    protected CheckpointStore createCheckpointStore() {
        return new RedisCheckpointStore(template, "test:");
    }

    @Override
    protected void cleanState() {
        if (connectionFactory == null) {
            RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(
                    SharedRedisContainer.host(),
                    SharedRedisContainer.port());
            connectionFactory = new LettuceConnectionFactory(config);
            connectionFactory.afterPropertiesSet();
            template = new StringRedisTemplate(connectionFactory);
            template.afterPropertiesSet();
        }
        template.getConnectionFactory().getConnection().serverCommands().flushDb();
    }
}
