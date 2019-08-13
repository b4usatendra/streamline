package com.hortonworks.streamline.streams.layout.beam.rule.expression;

import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.FabricEventImpl;
import com.hortonworks.streamline.streams.layout.beam.rule.expression.aggregation.AggregationFunImpl;
import com.hortonworks.streamline.streams.layout.beam.rule.expression.aggregation.FunctionEnum;
import com.hortonworks.streamline.streams.layout.component.rule.expression.FieldExpression;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import joptsimple.internal.Strings;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;

/**
 * Created by Satendra Sahu on 12/27/18
 */
public class BeamUtilFunctions implements Serializable {

    //replace key name with alias name
    public static ParDo.SingleOutput<StreamlineEvent, StreamlineEvent> applyAlias(String aliasName,
        String key) {
        return ParDo.of(new DoFn<StreamlineEvent, StreamlineEvent>() {
            @ProcessElement
            public void processElement(@Element StreamlineEvent streamlineEvent,
                OutputReceiver<StreamlineEvent> out) {
                StreamlineEvent outEvent = streamlineEvent;
                if (streamlineEvent.containsKey(key)) {
                    Object value = streamlineEvent.get(key);
                    outEvent = streamlineEvent.addFieldAndValue(aliasName, value);
                }
                out.output(outEvent);
            }
        });
    }

    //TODO explore withoutDefaults
    public static Combine.Globally<StreamlineEvent, StreamlineEvent> applyCombiningStrategy(
        String fieldName, String function) {
        return AggregationFunImpl.globally(fieldName, FunctionEnum.enumOf(function).getClazz())
            .withoutDefaults();
    }

    //filter fields
    public static ParDo.SingleOutput<StreamlineEvent, StreamlineEvent> filterFields(
        String beamComponentId, Set<FieldExpression> filteredFields) {
        return ParDo.of(new DoFn<StreamlineEvent, StreamlineEvent>() {
            @ProcessElement
            public void processElement(@Element StreamlineEvent streamlineEvent,
                OutputReceiver<StreamlineEvent> out) {

                Map<String, Object> fields = new HashMap<>();
                //TODO validate datatype of the field
                for (FieldExpression field : filteredFields) {
                    if (streamlineEvent.containsKey(field.getValue().getName())) {
                        fields.put(field.getValue().getName(),
                            streamlineEvent.get(field.getValue().getName()));
                    }
                }
                StreamlineEvent newEvent = FabricEventImpl.builder()
                    .from(streamlineEvent)
                    .fieldsAndValues(fields)
                    .dataSourceId(beamComponentId)
                    .build();

                out.output(newEvent);
            }
        });
    }

    public static ParDo.SingleOutput<StreamlineEvent, KV<byte[], StreamlineEvent>> generateKey(
        String sinkId, String routingKey) {
        return ParDo.of(new DoFn<StreamlineEvent, KV<byte[], StreamlineEvent>>() {
            @ProcessElement
            public void processElement(@Element StreamlineEvent streamlineEvent,
                OutputReceiver<KV<byte[], StreamlineEvent>> out) {
                //TODO provision to use user specified key
                ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                byte[] recordRoutingKey = null;

                if (routingKey != null && streamlineEvent.containsKey(routingKey)) {
                    if (streamlineEvent.get(routingKey) instanceof Long) {
                        Long key = (Long) streamlineEvent.get(routingKey);
                        recordRoutingKey = buffer.putLong(key).array();
                    } else if (streamlineEvent.get(routingKey) instanceof String) {
                        String key = (String) streamlineEvent.get(routingKey);
                        recordRoutingKey = key.getBytes();
                    } else if (streamlineEvent.get(routingKey) instanceof Integer) {
                        Integer key = (Integer) streamlineEvent.get(routingKey);
                        recordRoutingKey = buffer.putInt(key).array();
                    } else if (streamlineEvent.get(routingKey) instanceof byte[]) {
                        recordRoutingKey = (byte[]) streamlineEvent.get(routingKey);
                    }
                } else {
                    buffer.putLong(UUID.randomUUID().getLeastSignificantBits());
                    recordRoutingKey = buffer.array();
                }

                StreamlineEvent newEvent = FabricEventImpl.builder().from(streamlineEvent)
                    .dataSourceId(sinkId)
                    .build();
                KV<byte[], StreamlineEvent> eventKV = KV.of(recordRoutingKey, newEvent);
                out.output(eventKV);
            }

        });
    }

    public static ParDo.SingleOutput<KV<Object, StreamlineEvent>, StreamlineEvent> extractStreamlineEvents(
        String beamSourceId) {
        return ParDo.of(new DoFn<KV<Object, StreamlineEvent>, StreamlineEvent>() {
            @ProcessElement
            public void processElement(@Element KV<Object, StreamlineEvent> kV,
                OutputReceiver<StreamlineEvent> out) {
                if (kV.getValue() != null) {
                    out.output(kV.getValue());
                }
            }
        });
    }

    public static ParDo.SingleOutput<StreamlineEvent, StreamlineEvent> addTimestamp(
        String timestampField) {
        return ParDo.of(new DoFn<StreamlineEvent, StreamlineEvent>() {
            @ProcessElement
            public void processElement(@Element StreamlineEvent streamlineEvent,
                OutputReceiver<StreamlineEvent> out) {

                long evemtTimestamp;
                if (Strings.isNullOrEmpty(timestampField)) {
                    evemtTimestamp = System.currentTimeMillis();
                } else {
                    //TODO only long timestamp is supported(BEAM Windowing Component)
                    evemtTimestamp = (Long) streamlineEvent.get(timestampField);
                }
                out.outputWithTimestamp(streamlineEvent, new Instant(evemtTimestamp));
            }

        });
    }

    public static WithTimestamps<StreamlineEvent> addTimestampToEvents(String timestampField) {
        return WithTimestamps.of(new SerializableFunction<StreamlineEvent, Instant>() {
            /**
             * Returns the result of invoking this function on the given input.
             *
             * @param streamlineEvent
             */
            @Override
            public Instant apply(StreamlineEvent streamlineEvent) {
                long evemtTimestamp;
                if (Strings.isNullOrEmpty(timestampField)) {
                    evemtTimestamp = System.currentTimeMillis();
                } else {
                    //TODO only long timestamp is supported(BEAM Windowing Component)
                    evemtTimestamp = (Long) streamlineEvent.get(timestampField);
                }
                return new Instant(evemtTimestamp);
            }
        });
    }
}
