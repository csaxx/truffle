package org.csa.truffle.graal.source;

import java.io.Serializable;

public sealed interface PythonSourceConfig extends Serializable
        permits ResourceSourceConfig, GitSourceConfig, FileSourceConfig, S3SourceConfig {
}
