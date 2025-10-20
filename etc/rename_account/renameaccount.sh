#!/bin/bash
set -e  # Exit immediately if a command fails

# Compile the Java file
javac RenameAccount.java

# Run the program with all passed arguments
java -cp ".:../../lib/*" RenameAccount "$@"

# Delete the compiled class file to keep things clean
rm -f RenameAccount.class
