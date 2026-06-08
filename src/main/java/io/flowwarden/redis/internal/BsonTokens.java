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
package io.flowwarden.redis.internal;

import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import java.nio.ByteBuffer;
import java.util.Base64;

/**
 * Codec for {@link BsonDocument} resume tokens stored as base64-encoded BSON binary
 * in Redis Hash fields. Lisible au {@code redis-cli HGETALL}, indépendant du
 * serializer Redis configuré côté application, et round-trip fidèle.
 */
public final class BsonTokens {

    private static final BsonDocumentCodec CODEC = new BsonDocumentCodec();

    private BsonTokens() {}

    public static String encode(BsonDocument token) {
        if (token == null) {
            return null;
        }
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        try (BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
            CODEC.encode(writer, token, EncoderContext.builder().build());
        }
        return Base64.getEncoder().encodeToString(buffer.toByteArray());
    }

    public static BsonDocument decode(String base64) {
        if (base64 == null) {
            return null;
        }
        byte[] bytes = Base64.getDecoder().decode(base64);
        try (BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(bytes))) {
            return CODEC.decode(reader, DecoderContext.builder().build());
        }
    }
}
