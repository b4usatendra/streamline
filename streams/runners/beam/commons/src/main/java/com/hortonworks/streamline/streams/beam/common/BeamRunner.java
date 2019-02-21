package com.hortonworks.streamline.streams.beam.common;

/**
 * Created by Karthik.K
 */
public enum BeamRunner {
    FLINK("FlinkRunner"),
    DIRECT("DirectRunner");

    private String runnerName;

    BeamRunner(String runnerName) {
        this.runnerName = runnerName;
    }

    public static String getRunnerName(String name) {
        for (BeamRunner runnerName : BeamRunner.values()) {
            if (runnerName.name().equals(name)) {
                return runnerName.runnerName;
            }
        }
        return null;
    }

    public String getRunnerName() {
        return runnerName;
    }
}