/**
 * Copyright 2017 Hortonworks.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.streamline.streams.layout.beam;

import com.hortonworks.streamline.common.util.ProxyUtil;
import com.hortonworks.streamline.streams.layout.component.StreamlineComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.nio.file.Path;

class BeamFluxComponentFactory {
    private static final Logger LOG = LoggerFactory.getLogger(BeamFluxComponentFactory.class);

    private final Path extraJarsLocation;

    BeamFluxComponentFactory(Path extraJarsLocation) {
        this.extraJarsLocation = extraJarsLocation;
    }

    BeamComponent getFluxComponent(StreamlineComponent streamlineComponent) {
        ProxyUtil<BeamComponent> proxyUtil = new ProxyUtil<>(BeamComponent.class);
        try {
            BeamComponent beamComponent = proxyUtil.loadClassFromLibDirectory(extraJarsLocation, streamlineComponent.getTransformationClass());
            return beamComponent;
        } catch (ClassNotFoundException | MalformedURLException | InstantiationException | IllegalAccessException e) {
            LOG.error("Error while creating flux component", e);
            throw new RuntimeException(e);
        }
    }
}
