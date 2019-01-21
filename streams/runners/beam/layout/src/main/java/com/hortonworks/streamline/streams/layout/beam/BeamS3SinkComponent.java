package com.hortonworks.streamline.streams.layout.beam;

import com.amazonaws.services.s3.AmazonS3;
import com.hortonworks.streamline.common.exception.ComponentConfigException;
import com.hortonworks.streamline.streams.StreamlineEvent;
import org.apache.beam.sdk.values.PCollection;

/**
 * Created by Satendra Sahu on 1/11/19
 */
public class BeamS3SinkComponent extends AbstractBeamComponent {

  private static AmazonS3 client;

  public void initialize() {

  }

  @Override
  public PCollection<StreamlineEvent> getOutputCollection() {
    return null;
  }

  @Override
  public void generateComponent(PCollection<StreamlineEvent> pCollection) {

  }

  @Override
  public void validateConfig() throws ComponentConfigException {

  }
}
