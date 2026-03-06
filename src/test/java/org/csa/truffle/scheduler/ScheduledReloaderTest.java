package org.csa.truffle.scheduler;

import org.csa.truffle.interpreter.polyglot.PolyglotContextConfig;
import org.csa.truffle.loader.SwitchableFileSource;
import org.csa.truffle.source.resource.ResourceSourceConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ScheduledReloaderTest {

    private static final SchedulerConfig INTERVAL = new SchedulerConfig(Duration.ofMillis(50));

    @Test
    void lastCheckedAt_setAfterStart() throws Exception {
        try (ScheduledReloader reloader = new ScheduledReloader(
                new ResourceSourceConfig("python_hr_v1"), INTERVAL, PolyglotContextConfig.MINIMAL, (status, interp) -> {
        })) {
            reloader.start();
            assertNotNull(reloader.getStatus().getLastCheckedAt());
        }
    }

    @Test
    void lastChangedAt_setWhenDataChanges() throws Exception {
        // First load always produces files (new files are loaded)
        try (ScheduledReloader reloader = new ScheduledReloader(
                new ResourceSourceConfig("python_hr_v1"), INTERVAL, PolyglotContextConfig.MINIMAL, (status, interp) -> {
        })) {
            reloader.start();
            assertNotNull(reloader.getStatus().getLastChangedAt());
        }
    }

    @Test
    void lastChangedAt_notUpdatedOnSubsequentUnchangedReload() throws Exception {
        try (ScheduledReloader reloader = new ScheduledReloader(
                new ResourceSourceConfig("python_hr_v1"),
                new SchedulerConfig(Duration.ofMillis(50)),
                PolyglotContextConfig.MINIMAL,
                (status, interp) -> {
                })) {
            reloader.start();
            Instant changedAfterInit = reloader.getStatus().getLastChangedAt();
            assertNotNull(changedAfterInit, "initial load always detects files");
            Instant checkedAfterInit = reloader.getStatus().getLastCheckedAt();
            Thread.sleep(200);   // several background ticks, same source = no files
            assertEquals(changedAfterInit, reloader.getStatus().getLastChangedAt(),
                    "lastChangedAt should not advance when source is unchanged");
            assertTrue(reloader.getStatus().getLastCheckedAt().isAfter(checkedAfterInit),
                    "lastCheckedAt should advance even without files");
        }
    }

    @Test
    void periodicReload_firesWithinInterval() throws Exception {
        try (ScheduledReloader reloader = new ScheduledReloader(
                new ResourceSourceConfig("python_hr_v1"), INTERVAL, PolyglotContextConfig.MINIMAL, (status, interp) -> {
        })) {
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
        ScheduledReloader reloader = new ScheduledReloader(
                new ResourceSourceConfig("python_hr_v1"), INTERVAL, PolyglotContextConfig.MINIMAL, (status, interp) -> {
        });
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
        SwitchableFileSource src = new SwitchableFileSource("python_hr_v1");
        SchedulerConfig cfg = new SchedulerConfig(Duration.ofMillis(30), Duration.ofMillis(100));
        try (ScheduledReloader reloader = new ScheduledReloader(src, cfg, PolyglotContextConfig.MINIMAL, (status, interp) -> {
        })) {
            reloader.start();
            src.switchTo("nonexistent_python_dir");   // all subsequent reloads fail
            Thread.sleep(400);                         // >> grace period
            assertThrows(RuntimeException.class, reloader::checkForFatalError);
        }
    }

    @Test
    void gracePeriod_zero_neverFails() throws Exception {
        SwitchableFileSource src = new SwitchableFileSource("python_hr_v1");
        SchedulerConfig cfg = new SchedulerConfig(Duration.ofMillis(30), Duration.ZERO);
        try (ScheduledReloader reloader = new ScheduledReloader(src, cfg, PolyglotContextConfig.MINIMAL, (status, interp) -> {
        })) {
            reloader.start();
            src.switchTo("nonexistent_python_dir");
            Thread.sleep(400);
            assertDoesNotThrow(reloader::checkForFatalError);
        }
    }

    @Test
    void gracePeriod_recovery_resetsStreak() throws Exception {
        SwitchableFileSource src = new SwitchableFileSource("python_hr_v1");
        // Grace period: 300ms; cause errors for ~80ms, then recover
        SchedulerConfig cfg = new SchedulerConfig(Duration.ofMillis(30), Duration.ofMillis(300));
        try (ScheduledReloader reloader = new ScheduledReloader(src, cfg, PolyglotContextConfig.MINIMAL, (status, interp) -> {
        })) {
            reloader.start();
            src.switchTo("nonexistent_python_dir");
            Thread.sleep(80);                     // partial error streak, << grace
            src.switchTo("python_hr_v1");         // recover
            Thread.sleep(200);                    // enough ticks to confirm no fatal error
            assertDoesNotThrow(reloader::checkForFatalError);
        }
    }

    @Test
    void callback_firedOnInitialLoad() throws Exception {
        AtomicInteger count = new AtomicInteger();
        try (ScheduledReloader reloader = new ScheduledReloader(
                new ResourceSourceConfig("python_hr_v1"), INTERVAL,
                PolyglotContextConfig.MINIMAL,
                (status, interp) -> count.incrementAndGet())) {
            reloader.start();
            assertEquals(1, count.get(), "callback fires exactly once on initial load");
        }
    }

    @Test
    void callback_notFiredOnUnchangedReload() throws Exception {
        AtomicInteger count = new AtomicInteger();
        try (ScheduledReloader reloader = new ScheduledReloader(
                new ResourceSourceConfig("python_hr_v1"),
                new SchedulerConfig(Duration.ofMillis(50)),
                PolyglotContextConfig.MINIMAL,
                (status, interp) -> count.incrementAndGet())) {
            reloader.start();
            assertEquals(1, count.get(), "callback fires once after initial load");
            Thread.sleep(200);   // several background ticks, source unchanged
            assertEquals(1, count.get(), "callback not fired again on unchanged source");
        }
    }
}
