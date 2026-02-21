@echo off
rem run_tests.bat — wrapper around Maven that sets the correct JAVA_HOME and MVN path.
rem Usage:
rem   run_tests.bat compile
rem   run_tests.bat test
rem   run_tests.bat exec       (runs the Flink job via exec:exec)
rem   run_tests.bat package    (fat JAR, skips tests)
rem   run_tests.bat <any mvn goal/args>

setlocal

set "JAVA_HOME=C:\Program Files\Java\graalvm-jdk-21.0.10+8.1"
set "MVN=C:\Users\csa\.m2\wrapper\dists\apache-maven-3.9.12\bin\mvn.cmd"
set "PATH=%JAVA_HOME%\bin;%PATH%"

if "%~1"=="exec" (
    "%MVN%" exec:exec
    exit /b %errorlevel%
)

"%MVN%" %*

pause