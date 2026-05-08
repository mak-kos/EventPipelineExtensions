@REM ----------------------------------------------------------------------------
@REM Licensed to the Apache Software Foundation (ASF) under the Apache License,
@REM Version 2.0. See http://www.apache.org/licenses/LICENSE-2.0
@REM ----------------------------------------------------------------------------
@REM Apache Maven Wrapper startup batch script (self-contained, only-script type)
@REM
@REM Reads distributionUrl from .mvn\wrapper\maven-wrapper.properties,
@REM downloads Maven distribution into %USERPROFILE%\.m2\wrapper\dists\ if needed,
@REM then runs mvn.cmd with the passed arguments.
@REM ----------------------------------------------------------------------------

@echo off
setlocal enabledelayedexpansion

set "WRAPPER_DIR=%~dp0"
set "PROPS_FILE=%WRAPPER_DIR%.mvn\wrapper\maven-wrapper.properties"

if not exist "%PROPS_FILE%" (
  echo ERROR: %PROPS_FILE% not found 1>&2
  exit /b 1
)

REM Read distributionUrl from properties file
set "DIST_URL="
for /f "usebackq tokens=1,* delims==" %%A in ("%PROPS_FILE%") do (
  if /i "%%A"=="distributionUrl" set "DIST_URL=%%B"
)

if "%DIST_URL%"=="" (
  echo ERROR: distributionUrl not set in %PROPS_FILE% 1>&2
  exit /b 1
)

REM Derive distribution folder name (strip path, strip .zip, strip -bin)
for %%F in ("%DIST_URL%") do set "DIST_FILE=%%~nxF"
set "DIST_NAME=%DIST_FILE:.zip=%"
set "DIST_NAME=%DIST_NAME:-bin=%"

set "M2_HOME_BASE=%USERPROFILE%\.m2\wrapper\dists"
set "MAVEN_HOME=%M2_HOME_BASE%\%DIST_NAME%"

if not exist "%MAVEN_HOME%\bin\mvn.cmd" (
  echo Downloading Maven from %DIST_URL%
  if not exist "%M2_HOME_BASE%" mkdir "%M2_HOME_BASE%"
  set "TMP_ZIP=%TEMP%\%DIST_FILE%"
  powershell -NoProfile -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri '%DIST_URL%' -OutFile '!TMP_ZIP!'" || (
    echo ERROR: Failed to download Maven 1>&2
    exit /b 1
  )
  echo Extracting...
  powershell -NoProfile -Command "Expand-Archive -Force -Path '!TMP_ZIP!' -DestinationPath '%M2_HOME_BASE%'" || (
    echo ERROR: Failed to extract Maven 1>&2
    exit /b 1
  )
  del "!TMP_ZIP!" >nul 2>&1
)

if not exist "%MAVEN_HOME%\bin\mvn.cmd" (
  echo ERROR: %MAVEN_HOME%\bin\mvn.cmd not found after extraction 1>&2
  exit /b 1
)

call "%MAVEN_HOME%\bin\mvn.cmd" %*
exit /b %ERRORLEVEL%
