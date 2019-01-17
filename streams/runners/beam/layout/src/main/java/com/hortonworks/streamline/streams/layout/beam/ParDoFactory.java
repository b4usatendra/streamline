package com.hortonworks.streamline.streams.layout.beam;

import com.hortonworks.streamline.streams.StreamlineEvent;
import java.io.Serializable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * Created by Satendra Sahu on 12/19/18
 */
public class ParDoFactory implements Serializable {

  public static ParDo.SingleOutput<StreamlineEvent, StreamlineEvent> createParDo(
      DoFn<StreamlineEvent, StreamlineEvent> doFn) {
    return ParDo.of(doFn);
  }
}

