package com.aimyourtechnology.kafka.oracle.system;

import org.junit.jupiter.api.Test;

public class ServiceTest extends ServiceTestEnvironmentSetup {
    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output";

    @Test
    void t() {

    }

    @Override
    public String getInputTopic() {
        return INPUT_TOPIC;
    }

    @Override
    protected String getOutputTopic() {
        return OUTPUT_TOPIC;
    }
}
