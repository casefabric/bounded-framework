# bounded-framework
The Bounded framework for Scala, Akka and Domain Driven Design

# Release to maven

The setup as written by [Leonard Ehrenfried](https://leonard.io/blog/2017/01/an-in-depth-guide-to-deploying-to-maven-central/) is used. Please note that releasing towards maven is done via the oss.sonatype.org site by closing the uploaded packages and releasing the thereafter. 

Release command = sbt +publishSigned 
This will release for 2.11 and 2.12. Via oss.sonatype.org you need to 'Close' the uploaded packages and thereafter 'Release' them. 
