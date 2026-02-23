package org.csa.truffle.graal.reload;

import org.csa.truffle.graal.source.PythonSourceConfig;

import java.io.Serializable;

/**
 * Identifies one named dataset: its source, and the schedule controlling how often it reloads.
 *
 * <p>{@code id} goes here rather than into {@link SchedulerConfig} — scheduling behaviour
 * (interval/grace period) is separate from dataset identity, and the same
 * {@code SchedulerConfig} can be reused across multiple datasets.
 */
public record DatasetConfig(
        String id,
        PythonSourceConfig sourceConfig,
        SchedulerConfig schedulerConfig
) implements Serializable {}
