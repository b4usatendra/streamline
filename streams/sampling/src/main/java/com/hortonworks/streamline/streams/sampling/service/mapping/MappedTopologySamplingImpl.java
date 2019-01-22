package com.hortonworks.streamline.streams.sampling.service.mapping;

public enum MappedTopologySamplingImpl {
    STORM("com.hortonworks.streamline.streams.sampling.service.storm.StormTopologySamplingService"),
    FLINK("com.hortonworks.streamline.streams.sampling.service.beam.BeamTopologySamplingService");


    private final String className;

    MappedTopologySamplingImpl(String className) {
        this.className = className;
    }

    public String getClassName() {
        return className;
    }
}
