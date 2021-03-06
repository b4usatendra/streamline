/**
  * Copyright 2017 Hortonworks.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at

  *   http://www.apache.org/licenses/LICENSE-2.0

  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
 **/
package com.hortonworks.streamline.streams.runtime.storm.bolt.model;

import com.hortonworks.streamline.common.util.Utils;
import com.hortonworks.streamline.streams.layout.component.impl.model.ModelProcessor;

import org.apache.storm.pmml.model.ModelOutputs;
import org.apache.storm.pmml.runner.ModelRunner;
import org.apache.storm.pmml.runner.ModelRunnerFactory;
import org.apache.storm.pmml.runner.jpmml.JpmmlFactory;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;

public class StreamlineJPMMLModelRunnerFactory implements ModelRunnerFactory {
    protected static final Logger LOG = LoggerFactory.getLogger(StreamlineJPMMLModelRunnerFactory.class);

    private final String modelProcessorJson;
    private final ModelOutputs modelOutputs;

    public StreamlineJPMMLModelRunnerFactory(String modelProcessorJson, ModelOutputs modelOutputs) {
        this.modelProcessorJson = modelProcessorJson;
        this.modelOutputs = modelOutputs;
    }

    @Override
    public ModelRunner newModelRunner() {
        final ModelProcessor modelProcessor = Utils.createObjectFromJson(modelProcessorJson, ModelProcessor.class);
        PMML pmmlModel;
        try {
            pmmlModel = JpmmlFactory.newPmml(
                    new ByteArrayInputStream(modelProcessor.getPmml().getBytes()));
        } catch (Exception e) {
            throw new RuntimeException("Exception occurred while creating PMML model object", e);
        }

        return new StreamlineJPMMLModelRunner(
                modelProcessor.getOutputStreams(),
                modelProcessor.getId(),
                JpmmlFactory.newEvaluator(pmmlModel),
                modelOutputs);
    }
}
