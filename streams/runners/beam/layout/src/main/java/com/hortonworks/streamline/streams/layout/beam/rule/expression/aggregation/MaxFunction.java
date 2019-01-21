package com.hortonworks.streamline.streams.layout.beam.rule.expression.aggregation;

import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.layout.component.rule.expression.NumberComparator;

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
      if ((streamlineEvent.get(fieldName) instanceof Number) && (streamlineEvent
          .get(fieldName) instanceof Comparable)) {
        comparator = new NumberComparator((Number) streamlineEvent.get(fieldName));
        result = comparator.maxValue(maxValue);
        if (result >= 0) {
          value = comparator.firstValue;
          event = streamlineEvent;
        }
      } else {
        throw new NumberFormatException("Cannot compare numbers");
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
