/**
 * Copyright 2017 Hortonworks.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.hortonworks.streamline.streams.runtime.beam.kafka;

import com.google.common.base.*;
import com.hortonworks.registries.schemaregistry.client.*;
import com.hortonworks.streamline.streams.*;
import com.hortonworks.streamline.streams.common.*;
import com.hortonworks.streamline.streams.runtime.beam.*;
import org.apache.kafka.clients.consumer.*;
import org.slf4j.*;

import java.io.*;
import java.nio.*;
import java.util.*;

public class AvroKafkaTranslator {
    private static final Logger log = LoggerFactory.getLogger(AvroKafkaTranslator.class);
    private final String outputStream;
    private final String topic;
    private final String dataSourceId;
    private final String schemaRegistryUrl;
    private final Integer readerSchemaVersion;
    private transient volatile AvroStreamsSnapshotDeserializer avroStreamsSnapshotDeserializer;

    public AvroKafkaTranslator(String outputStream, String topic, String dataSourceId, String schemaRegistryUrl) {
        this(outputStream, topic, dataSourceId, schemaRegistryUrl, null);
    }

    public AvroKafkaTranslator(String outputStream, String topic, String dataSourceId, String schemaRegistryUrl, Integer readerSchemaVersion) {
        this.outputStream = outputStream;
        this.topic = topic;
        this.dataSourceId = dataSourceId;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.readerSchemaVersion = readerSchemaVersion;
    }

    public Object apply(ConsumerRecord<Object, ByteBuffer> consumerRecord) {
        Map<String, Object> keyValues = (Map<String, Object>) deserializer().deserialize(new ByteBufferInputStream(consumerRecord.value()),
                readerSchemaVersion);
        StreamlineEvent streamlineEvent = StreamlineEventImpl.builder().putAll(keyValues).dataSourceId(dataSourceId).build();
        return streamlineEvent;
    }


    public List<String> streams() {
        return Collections.singletonList(outputStream);
    }

    private AvroStreamsSnapshotDeserializer deserializer() {
        //initializing deserializer here should be synchronized (using DCL pattern?) when single threaded nature of kafka spout does not hold true anymore
        if (avroStreamsSnapshotDeserializer == null) {
            AvroStreamsSnapshotDeserializer deserializer = new AvroStreamsSnapshotDeserializer();
            Map<String, Object> config = new HashMap<>();
            config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), schemaRegistryUrl);
            deserializer.init(config);
            avroStreamsSnapshotDeserializer = deserializer;
        }
        return avroStreamsSnapshotDeserializer;
    }

    public static class ByteBufferInputStream extends InputStream {

        private final ByteBuffer buf;

        public ByteBufferInputStream(ByteBuffer buf) {
            this.buf = buf;
        }

        public int read() throws IOException {
            if (!buf.hasRemaining()) {
                return -1;
            }
            return buf.get() & 0xFF;
        }

        public int read(byte[] bytes, int off, int len) throws IOException {
            Preconditions.checkNotNull(bytes, "Given byte array can not be null");
            Preconditions.checkPositionIndexes(off, off + len, bytes.length);

            if (!buf.hasRemaining()) {
                return -1;
            }

            if (len == 0) {
                return 0;
            }

            int end = Math.min(len, buf.remaining());
            buf.get(bytes, off, end);
            return end;
        }
    }
}
