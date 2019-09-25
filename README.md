# Implementation of a Publish-Subscribe system on a structured network using Chord and Scribe.

### Made by:

* Daniel Flamino
* Diogo Silvério
* Rita Macedo

### Folder Content:
```
README.md -> this file

1st Phase Report.pdf -> the project report

akka-project/ -> the actual project

akka-project/src/main/resources/aplication.conf -> application configuration

akka-project/test.sh -> script to run the tester

akka-project/target/pack/bin/main -> the application (after compiling)
```

### Notes:

* The test.sh script was made to run on git-bash for Windows and might not work
for other OS/terminal combos. It uses a hard coded IP address for the tester
which must be changed before running it.

* The project must be compiled using `sbt pack` before running it.

* For a list of available project commands, type `help` once the node is up and
running.
