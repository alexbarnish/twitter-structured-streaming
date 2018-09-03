#
chmod +x sbt/sbt (only if necessary)

# package - this may take a while in order to download all the dependencies
./sbt/sbt assembly

# to generate Eclipse project
./sbt/sbt eclipse

# at this point you can import into Scala IDE
