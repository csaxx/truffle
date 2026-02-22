@echo off
rem run_job.bat — wrapper around Maven that sets the correct JAVA_HOME and MVN path.
rem Usage:
rem   run_job.bat compile
rem   run_job.bat test
rem   run_job.bat exec       (runs the Flink job via exec:exec)
rem   run_job.bat exec-git   (runs TruffleJobGit with GitPythonSource)
rem   run_job.bat package    (fat JAR, skips tests)
rem   run_job.bat <any mvn goal/args>

setlocal

set "JAVA_HOME=C:\Program Files\Java\graalvm-jdk-21.0.10+8.1"
set "MVN=C:\Users\csa\.m2\wrapper\dists\apache-maven-3.9.12\bin\mvn.cmd"
set "PATH=%JAVA_HOME%\bin;%PATH%"

if "%~1"=="exec-git" (
    "%MVN%" exec:exec@exec-git
    exit /b %errorlevel%
)

if "%~1"=="exec" (
    "%MVN%" exec:exec
    exit /b %errorlevel%
)

"%MVN%" %*

pause