package com.hortonworks.streamline.streams.common.utils;

import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.Constants;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.ParseException;

/**
 * Created by satendra.sahu on 2019-05-03
 */
public class FabricEventUtils {

    //package level access for testing
    public static Object getAvroRecord(StreamlineEvent streamlineEvent, Schema schema)
        throws ParseException {

        if (streamlineEvent.containsKey(StreamlineEvent.PRIMITIVE_PAYLOAD_FIELD)) {
            if (streamlineEvent.keySet().size() > 1) {
                throw new RuntimeException(
                    "Invalid schema, primitive schema can contain only one field.");
            }
            return streamlineEvent.get(StreamlineEvent.PRIMITIVE_PAYLOAD_FIELD);
        }

        GenericRecord event;
        event = new GenericData.Record(schema);

        //add event id
        event.put(Constants.FABRIC_ID,
            getAvroValue(streamlineEvent.getId(), schema.getField(Constants.FABRIC_ID).schema()));

        //add event metadata
        try {
            event.put(Constants.FABRIC_METADATA, getAvroValue(streamlineEvent.getHeader(),
                schema.getField(Constants.FABRIC_METADATA).schema()));
        }catch (Exception e){
            throw new ParseException(String.format("Unable to parse event metadata for schema: {} and version {}", streamlineEvent.getHeader().get(
                Constants.FABRIC_METADATA_SCHEMA),streamlineEvent.getHeader().get(
                Constants.FABRIC_METADATA_SCHEMA_VERSION)));
        }

        //add event data
        try {
            event.put(Constants.FABRIC_DATA, getAvroValue(streamlineEvent.getFieldsAndValues(),
                schema.getField(Constants.FABRIC_DATA).schema()));
        }catch (Exception e){
            throw new ParseException(String.format("Unable to parse event data for schema: {} and version {}", streamlineEvent.getHeader().get(
                Constants.FABRIC_METADATA_SCHEMA),streamlineEvent.getHeader().get(
                Constants.FABRIC_METADATA_SCHEMA_VERSION)));
        }

        return event;
    }

    private static Object getAvroValue(Object input, Schema schema) {
        if (input instanceof byte[] && Schema.Type.FIXED.equals(schema.getType())) {
            return new GenericData.Fixed(schema, (byte[]) input);
        } else if (input instanceof Map && !((Map) input).isEmpty()) {
            GenericRecord result;
            result = new GenericData.Record(schema);
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) input).entrySet()) {
                result.put(entry.getKey(),
                    getAvroValue(entry.getValue(), schema.getField(entry.getKey()).schema()));
            }
            return result;
        } else if (input instanceof Collection && !((Collection) input).isEmpty()) {
            // for array even though we(Schema in streamline registry) support different types of elements in an array, avro expects an array
            // schema to have elements of same type. Hence, for now we will restrict array to have elements of same type. Other option is convert
            // a  streamline Schema Array field to Record in avro. However, with that the issue is that avro Field constructor does not allow a
            // null name. We could potentiall hack it by plugging in a dummy name like arrayfield, but seems hacky so not taking that path
            List<Object> values = new ArrayList<>(((Collection) input).size());
            for (Object value : (Collection) input) {
                values.add(getAvroValue(value, schema.getElementType()));
            }
            return new GenericData.Array<Object>(schema, values);
        } else {
            return input;
        }
    }
}
