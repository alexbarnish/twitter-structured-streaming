resolvers += Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Spray Repository" at "http://repo.spray.cc/"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")
addSbtPlugin("com.github.gseitz" % "sbt-protobuf" % "0.6.3")
addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "5.2.4")
addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")



//addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.0")

//addSbtPlugin("com.github.gseitz" % "sbt-protobuf" % "0.3.3")

//addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "4.0.0")
