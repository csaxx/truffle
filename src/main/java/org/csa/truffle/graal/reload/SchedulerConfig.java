package org.csa.truffle.graal.reload;

import java.io.Serializable;
import java.time.Duration;

public record SchedulerConfig(Duration interval) implements Serializable {}
