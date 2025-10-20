@echo off
setlocal

rem Compile the Java file
javac RenameAccount.java
if errorlevel 1 (
    echo Compilation failed.
    exit /b 1
)

rem Run the program with all passed arguments
java -cp ".;../../lib/*" RenameAccount %*

rem Delete the compiled class file to keep things clean
del RenameAccount.class >nul 2>&1

endlocal