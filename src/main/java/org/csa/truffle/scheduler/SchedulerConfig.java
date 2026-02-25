package org.csa.truffle.scheduler;

import java.io.Serializable;
import java.time.Duration;

public record SchedulerConfig(Duration interval, Duration gracePeriod) implements Serializable {

    /** No grace period — reload errors are logged but never fatal. */
    public SchedulerConfig(Duration interval) {
        this(interval, Duration.ZERO);
    }
}
