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
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class RedisStreamCoreAutoConfigurationTest {

    private static final AutoConfigurations AUTO = AutoConfigurations.of(
            RedisAutoConfiguration.class,
            RedisStreamCoreAutoConfiguration.class);

    @Test
    void blockingTemplate_wiresBlockingBeans() {
        new ApplicationContextRunner()
                .withConfiguration(AUTO)
                .run(ctx -> {
                    assertThat(ctx).hasSingleBean(LockService.class);
                    assertThat(ctx).hasSingleBean(CheckpointStore.class);
                    assertThat(ctx.getBean(LockService.class)).isInstanceOf(RedisLockService.class);
                    assertThat(ctx.getBean(CheckpointStore.class)).isInstanceOf(RedisCheckpointStore.class);
                });
    }

    @Test
    void userDefinedBeans_takePrecedence() {
        new ApplicationContextRunner()
                .withConfiguration(AUTO)
                .withUserConfiguration(UserBeans.class)
                .run(ctx -> {
                    assertThat(ctx).hasSingleBean(LockService.class);
                    assertThat(ctx).hasSingleBean(CheckpointStore.class);
                    assertThat(ctx.getBean(LockService.class)).isSameAs(UserBeans.STUB_LOCK);
                    assertThat(ctx.getBean(CheckpointStore.class)).isSameAs(UserBeans.STUB_CHECKPOINT);
                });
    }

    @Test
    void customKeyPrefix_isHonored() {
        new ApplicationContextRunner()
                .withConfiguration(AUTO)
                .withPropertyValues("flowwarden.redis.key-prefix=myapp:")
                .run(ctx -> {
                    RedisStreamCoreProperties props = ctx.getBean(RedisStreamCoreProperties.class);
                    assertThat(props.getKeyPrefix()).isEqualTo("myapp:");
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class UserBeans {

        static final LockService STUB_LOCK = LockService.noOp();
        static final CheckpointStore STUB_CHECKPOINT = CheckpointStore.noOp();

        @Bean
        LockService userLockService() {
            return STUB_LOCK;
        }

        @Bean
        CheckpointStore userCheckpointStore() {
            return STUB_CHECKPOINT;
        }
    }
}
