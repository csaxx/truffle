package org.csa.truffle.reload;

import org.csa.truffle.SwitchablePythonSource;
import org.csa.truffle.graal.reload.DatasetConfig;
import org.csa.truffle.graal.reload.ScheduledReloader;
import org.csa.truffle.graal.reload.SchedulerConfig;
import org.csa.truffle.graal.source.DirectPythonSourceConfig;
import org.csa.truffle.graal.source.resource.ResourceSourceConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ScheduledReloaderTest {

    private static final SchedulerConfig INTERVAL = new SchedulerConfig(Duration.ofMillis(50));

    @Test
    void lastCheckedAt_setAfterStart() throws Exception {
        DatasetConfig cfg = new DatasetConfig("default", new ResourceSourceConfig("python_hr_v1"), INTERVAL);
        try (ScheduledReloader reloader = new ScheduledReloader(List.of(cfg))) {
            reloader.start();
            assertNotNull(reloader.getStatus().getLastCheckedAt());
        }
    }

    @Test
    void lastChangedAt_setWhenDataChanges() throws Exception {
        // First load always produces changes (new files are loaded)
        DatasetConfig cfg = new DatasetConfig("default", new ResourceSourceConfig("python_hr_v1"), INTERVAL);
        try (ScheduledReloader reloader = new ScheduledReloader(List.of(cfg))) {
            reloader.start();
            assertNotNull(reloader.getStatus().getLastChangedAt());
        }
    }

    @Test
    void lastChangedAt_notUpdatedOnSubsequentUnchangedReload() throws Exception {
        DatasetConfig cfg = new DatasetConfig("default",
                new ResourceSourceConfig("python_hr_v1"),
                new SchedulerConfig(Duration.ofMillis(50)));
        try (ScheduledReloader reloader = new ScheduledReloader(List.of(cfg))) {
            reloader.start();
            Instant changedAfterInit = reloader.getStatus().getLastChangedAt();
            assertNotNull(changedAfterInit, "initial load always detects changes");
            Instant checkedAfterInit = reloader.getStatus().getLastCheckedAt();
            Thread.sleep(200);   // several background ticks, same source = no changes
            assertEquals(changedAfterInit, reloader.getStatus().getLastChangedAt(),
                    "lastChangedAt should not advance when source is unchanged");
            assertTrue(reloader.getStatus().getLastCheckedAt().isAfter(checkedAfterInit),
                    "lastCheckedAt should advance even without changes");
        }
    }

    @Test
    void periodicReload_firesWithinInterval() throws Exception {
        DatasetConfig cfg = new DatasetConfig("default", new ResourceSourceConfig("python_hr_v1"), INTERVAL);
        try (ScheduledReloader reloader = new ScheduledReloader(List.of(cfg))) {
            reloader.start();
            Instant after = reloader.getStatus().getLastCheckedAt();
            // Wait long enough for at least one background tick
            Thread.sleep(INTERVAL.interval().toMillis() * 3);
            Instant later = reloader.getStatus().getLastCheckedAt();
            assertNotNull(later);
            assertTrue(later.isAfter(after),
                    "lastCheckedAt should advance after waiting > interval");
        }
    }

    @Test
    void close_stopsScheduler() throws Exception {
        DatasetConfig cfg = new DatasetConfig("default", new ResourceSourceConfig("python_hr_v1"), INTERVAL);
        ScheduledReloader reloader = new ScheduledReloader(List.of(cfg));
        reloader.start();
        reloader.close();

        Instant afterClose = reloader.getStatus().getLastCheckedAt();
        Thread.sleep(INTERVAL.interval().toMillis() * 3);
        Instant stillSame = reloader.getStatus().getLastCheckedAt();

        assertEquals(afterClose, stillSame,
                "lastCheckedAt should not advance after close()");
    }

    @Test
    void gracePeriod_exceeded_fatalErrorThrows() throws Exception {
        SwitchablePythonSource src = new SwitchablePythonSource("python_hr_v1");
        SchedulerConfig cfg = new SchedulerConfig(Duration.ofMillis(30), Duration.ofMillis(100));
        DatasetConfig dataset = new DatasetConfig("default", new DirectPythonSourceConfig(src), cfg);
        try (ScheduledReloader reloader = new ScheduledReloader(List.of(dataset))) {
            reloader.start();
            src.switchTo("nonexistent_python_dir");   // all subsequent reloads fail
            Thread.sleep(400);                         // >> grace period
            assertThrows(RuntimeException.class, reloader::checkForFatalError);
        }
    }

    @Test
    void gracePeriod_zero_neverFails() throws Exception {
        SwitchablePythonSource src = new SwitchablePythonSource("python_hr_v1");
        SchedulerConfig cfg = new SchedulerConfig(Duration.ofMillis(30), Duration.ZERO);
        DatasetConfig dataset = new DatasetConfig("default", new DirectPythonSourceConfig(src), cfg);
        try (ScheduledReloader reloader = new ScheduledReloader(List.of(dataset))) {
            reloader.start();
            src.switchTo("nonexistent_python_dir");
            Thread.sleep(400);
            assertDoesNotThrow(reloader::checkForFatalError);
        }
    }

    @Test
    void gracePeriod_recovery_resetsStreak() throws Exception {
        SwitchablePythonSource src = new SwitchablePythonSource("python_hr_v1");
        // Grace period: 300ms; cause errors for ~80ms, then recover
        SchedulerConfig cfg = new SchedulerConfig(Duration.ofMillis(30), Duration.ofMillis(300));
        DatasetConfig dataset = new DatasetConfig("default", new DirectPythonSourceConfig(src), cfg);
        try (ScheduledReloader reloader = new ScheduledReloader(List.of(dataset))) {
            reloader.start();
            src.switchTo("nonexistent_python_dir");
            Thread.sleep(80);                     // partial error streak, << grace
            src.switchTo("python_hr_v1");         // recover
            Thread.sleep(200);                    // enough ticks to confirm no fatal error
            assertDoesNotThrow(reloader::checkForFatalError);
        }
    }
}
