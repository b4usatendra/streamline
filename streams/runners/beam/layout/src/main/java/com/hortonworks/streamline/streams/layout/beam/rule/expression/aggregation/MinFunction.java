package com.hortonworks.streamline.streams.layout.beam.rule.expression.aggregation;

import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.layout.component.rule.expression.NumberComparator;

/**
 * Created by Satendra Sahu on 12/31/18
 */
public class MinFunction extends BeamAggregationFunction {


  public MinFunction(String fieldName) {
    super(fieldName);
    value = Long.MAX_VALUE;
  }

  @Override
  public void evaluate(StreamlineEvent streamlineEvent) {
    Number minValue = (Number) value;

    if (streamlineEvent.containsKey(fieldName)) {
      int result = 0;
      NumberComparator comparator = null;
      if (streamlineEvent.get(fieldName) instanceof Integer) {
        comparator = new NumberComparator((Integer) streamlineEvent.get(fieldName));
        result = comparator.maxValue(minValue.intValue());
      } else if (streamlineEvent.get(fieldName) instanceof Long) {
        comparator = new NumberComparator((Long) streamlineEvent.get(fieldName));
        result = comparator.maxValue(minValue.longValue());
      } else if (streamlineEvent.get(fieldName) instanceof Double) {
        comparator = new NumberComparator((Double) streamlineEvent.get(fieldName));
        result = comparator.maxValue(minValue.doubleValue());
      }

      if (result <= 0) {
        value = comparator.firstValue;
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
