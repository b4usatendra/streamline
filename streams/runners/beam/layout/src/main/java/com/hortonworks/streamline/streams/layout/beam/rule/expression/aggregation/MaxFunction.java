package com.hortonworks.streamline.streams.layout.beam.rule.expression.aggregation;

import com.hortonworks.streamline.streams.*;
import com.hortonworks.streamline.streams.layout.component.rule.expression.*;

/**
 * Created by Satendra Sahu on 12/31/18
 */
public class MaxFunction extends BeamAggregationFunction {


    public MaxFunction(String fieldName) {
        super(fieldName);
        value = Long.MIN_VALUE;
    }

    @Override
    public void evaluate(StreamlineEvent streamlineEvent) {
        Number maxValue = (Number) value;

        if (streamlineEvent.containsKey(fieldName)) {
            int result = 0;
            NumberComparator comparator = null;
            if (streamlineEvent.get(fieldName) instanceof Integer) {
                comparator = new NumberComparator((Integer) streamlineEvent.get(fieldName));
                result = comparator.maxValue(maxValue.intValue());
            } else if (streamlineEvent.get(fieldName) instanceof Long) {
                comparator = new NumberComparator((Long) streamlineEvent.get(fieldName));
                result = comparator.maxValue(maxValue.longValue());
            } else if (streamlineEvent.get(fieldName) instanceof Double) {
                comparator = new NumberComparator((Double) streamlineEvent.get(fieldName));
                result = comparator.maxValue(maxValue.doubleValue());
            }

            if (result >= 0) {
                value = (Number) comparator.firstValue;
                event = streamlineEvent;
            }
        }
    }

    @Override
    public void compare(StreamlineEvent otherEvent) {
        evaluate(otherEvent);
    }

    @Override
    public Object getValue() {
        return value;
    }
}
