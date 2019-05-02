/**
 * Copyright 2017 Hortonworks.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/
package com.hortonworks.streamline.streams.actions.beam.topology;

import com.hortonworks.streamline.streams.actions.TopologyActionContext;
import com.hortonworks.streamline.streams.actions.TopologyActions;
import com.hortonworks.streamline.streams.actions.beam.topology.beamRunner.config.BeamAbstractRunner;
import com.hortonworks.streamline.streams.actions.beam.topology.beamRunner.config.MappedBeamRunnerImpl;
import com.hortonworks.streamline.streams.catalog.TopologyTestRunHistory;
import com.hortonworks.streamline.streams.cluster.catalog.Service;
import com.hortonworks.streamline.streams.layout.TopologyLayoutConstants;
import com.hortonworks.streamline.streams.layout.component.TopologyLayout;
import com.hortonworks.streamline.streams.layout.component.impl.testing.TestRunProcessor;
import com.hortonworks.streamline.streams.layout.component.impl.testing.TestRunRulesProcessor;
import com.hortonworks.streamline.streams.layout.component.impl.testing.TestRunSink;
import com.hortonworks.streamline.streams.layout.component.impl.testing.TestRunSource;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

/**
 * Beam implementation of the TopologyActions interface
 */
public class BeamTopologyActionsImpl implements TopologyActions {


    private BeamAbstractRunner beamAbstractRunner;
    private String beamRunner;
    private Service beamRunnerService;

    public BeamTopologyActionsImpl() {
    }

    @Override
    public void init(Map<String, Object> conf) {

        beamRunnerService = (Service) conf.get(TopologyLayoutConstants.BEAM_RUNNER_SERVICE);
        MappedBeamRunnerImpl mappedBeamRunner;
        try {
            mappedBeamRunner = MappedBeamRunnerImpl
                .valueOf(beamRunnerService.getName());
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(
                "Unsupported beam Runner: " + beamRunnerService.getName(), e);
        }

        String className = mappedBeamRunner.getClassName();
        try {
            Class<BeamAbstractRunner> clazz = (Class<BeamAbstractRunner>) Class.forName(className);
            beamAbstractRunner = clazz.newInstance();
            beamAbstractRunner.init(conf);

        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
            throw new RuntimeException(
                "Can't initialize Beam Runner instance - Class Name: " + className,
                e);
        }
    }

    @Override
    public void deploy(TopologyLayout topology, String mavenArtifacts, TopologyActionContext ctx,
        String asUser)
        throws Exception {
        beamAbstractRunner.deploy(topology, mavenArtifacts, ctx, asUser);
    }


    @Override
    public void runTest(TopologyLayout topology, TopologyTestRunHistory testRunHistory,
        String mavenArtifacts,
        Map<String, TestRunSource> testRunSourcesForEachSource,
        Map<String, TestRunProcessor> testRunProcessorsForEachProcessor,
        Map<String, TestRunRulesProcessor> testRunRulesProcessorsForEachProcessor,
        Map<String, TestRunSink> testRunSinksForEachSink, Optional<Long> durationSecs)
        throws Exception {
        beamAbstractRunner
            .runTest(topology, testRunHistory, mavenArtifacts, testRunSourcesForEachSource,
                testRunProcessorsForEachProcessor, testRunRulesProcessorsForEachProcessor,
                testRunSinksForEachSink, durationSecs);
    }

    @Override
    public boolean killTest(TopologyTestRunHistory testRunHistory) {
        return beamAbstractRunner.killTest(testRunHistory);
    }

    @Override
    public void kill(TopologyLayout topology, String asUser)
        throws Exception {
        beamAbstractRunner.kill(topology, asUser);
    }

    @Override
    public void validate(TopologyLayout topology)
        throws Exception {
        beamAbstractRunner.validate(topology);
    }

    @Override
    public void suspend(TopologyLayout topology, String asUser)
        throws Exception {
        beamAbstractRunner.suspend(topology, asUser);
    }

    @Override
    public void resume(TopologyLayout topology, String asUser)
        throws Exception {
        beamAbstractRunner.resume(topology, asUser);
    }

    @Override
    public Status status(TopologyLayout topology, String asUser)
        throws Exception {
        return beamAbstractRunner.status(topology, asUser);
    }

    @Override
    public LogLevelInformation configureLogLevel(TopologyLayout topology, LogLevel targetLogLevel,
        int durationSecs, String asUser)
        throws Exception {
        return beamAbstractRunner.configureLogLevel(topology, targetLogLevel, durationSecs, asUser);
    }

    @Override
    public LogLevelInformation getLogLevel(TopologyLayout topology, String asUser)
        throws Exception {
        return beamAbstractRunner.getLogLevel(topology, asUser);
    }

    /**
     * the Path where any topology specific artifacts are kept
     */
    @Override
    public Path getArtifactsLocation(TopologyLayout topology) {
        return beamAbstractRunner.getArtifactsLocation(topology);
    }

    @Override
    public Path getExtraJarsLocation(TopologyLayout topology) {
        return beamAbstractRunner.getExtraJarsLocation(topology);
    }

    @Override
    public String getRuntimeTopologyId(TopologyLayout topology, String asUser) {
        return beamAbstractRunner.getRuntimeTopologyId(topology, asUser);
    }
}