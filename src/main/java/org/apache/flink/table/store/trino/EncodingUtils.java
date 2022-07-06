/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.trino;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;

import java.util.Base64;

import static java.nio.charset.StandardCharsets.UTF_8;

/** Utils for encoding. */
public class EncodingUtils {

    private static final Base64.Encoder BASE64_ENCODER =
            java.util.Base64.getUrlEncoder().withoutPadding();

    private static final Base64.Decoder BASE64_DECODER = java.util.Base64.getUrlDecoder();

    public static <T> String encodeObjectToString(T t, Serialize<T> serialize) {
        try {
            DataOutputSerializer out = new DataOutputSerializer(128);
            serialize.apply(t, out);
            return new String(BASE64_ENCODER.encode(out.getCopyOfBuffer()), UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T decodeStringToObject(String encodedStr, Deserialize<T> deserialize) {
        final byte[] bytes = BASE64_DECODER.decode(encodedStr.getBytes(UTF_8));
        try {
            return deserialize.apply(new DataInputDeserializer(bytes));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Serializer. */
    public interface Serialize<T> {
        void apply(T t, DataOutputView out) throws Exception;
    }

    /** Deserializer. */
    public interface Deserialize<T> {
        T apply(DataInputView in) throws Exception;
    }
}
