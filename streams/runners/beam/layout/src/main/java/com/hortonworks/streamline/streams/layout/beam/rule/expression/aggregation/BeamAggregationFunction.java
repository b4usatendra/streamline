package com.hortonworks.streamline.streams.layout.beam.rule.expression.aggregation;

import com.hortonworks.streamline.streams.StreamlineEvent;
import java.io.Serializable;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

/**
 * Created by Satendra Sahu on 12/28/18
 */
@DefaultCoder(ByteArrayCoder.class)
public abstract class BeamAggregationFunction implements Serializable {

  protected String fieldName;
  protected StreamlineEvent event;
  protected Object value = 0;

  public BeamAggregationFunction(String fieldName) {
    this.fieldName = fieldName;
  }

  public abstract void evaluate(StreamlineEvent streamlineEvent);

  public abstract void compare(StreamlineEvent otherEvent);

  public String getFieldName() {
    return fieldName;
  }

  public StreamlineEvent getEvent() {
    return event;
  }

  public Object getValue() {
    return value;
  }
}


