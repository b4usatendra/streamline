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
package com.hortonworks.streamline.streams.layout.beam.kafka;


import org.apache.kafka.clients.consumer.*;

import java.nio.*;

public class StreamlineEventToKafkaMapper {
    private final String keyName;

    public StreamlineEventToKafkaMapper(String keyName) {
        this.keyName = keyName;
    }

    public Object getKeyFromTuple(ConsumerRecord<Object, ByteBuffer> consumerRecord) {
        if ((consumerRecord == null)) {
            return null;
        }
        return consumerRecord.key();
        /*StreamlineEvent streamlineEvent = (StreamlineEvent) tuple.getValueByField(StreamlineEvent.STREAMLINE_EVENT);
        return StreamlineRuntimeUtil.getFieldValue(streamlineEvent, keyName);
*/
    }

    public Object getMessageFromTuple(ConsumerRecord<Object, ByteBuffer> consumerRecord) {
        return consumerRecord.value();
        //return tuple.getValueByField(StreamlineEvent.STREAMLINE_EVENT);
    }
}
