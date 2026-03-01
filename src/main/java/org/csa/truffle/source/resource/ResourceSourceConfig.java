package org.csa.truffle.source.resource;

import org.csa.truffle.source.FileSourceConfig;

public record ResourceSourceConfig(String directory) implements FileSourceConfig {
}
