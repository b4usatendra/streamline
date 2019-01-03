package com.hortonworks.streamline.streams.layout.beam.kafka;

import com.hortonworks.streamline.streams.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.io.*;

/**
 * Created by Satendra Sahu on 12/19/18
 */
public class ParDoFactory implements Serializable {

    public static ParDo.SingleOutput<StreamlineEvent,StreamlineEvent> createParDo(DoFn<StreamlineEvent, StreamlineEvent> doFn) {
        return ParDo.of(doFn);
    }
}

