package com.hortonworks.streamline.streams.actions.beam.topology.beamRunner.config;

/**
 * Created by Satendra Sahu on 4/23/19
 */
public enum MappedBeamRunnerImpl {
    FLINK("com.hortonworks.streamline.streams.actions.beam.topology.beamRunner.FlinkRunner");

    private String className;

    MappedBeamRunnerImpl(String className) {
        this.className = className;
    }

    public String getClassName() {
        return className;
    }
}
