# bounded-framework
The Bounded framework for Scala, Akka and Domain Driven Design

[![wercker status](https://app.wercker.com/status/e2dfa2afe8cb73d4b91b6d844dac7169/m/ "wercker status")](https://app.wercker.com/project/byKey/e2dfa2afe8cb73d4b91b6d844dac7169)

#Introduction

The Bounded framework gives scala akka based extensions to easy build and test a Domain Driven Design application. 
With constructs and test fixtures for Aggregate Root and Projections the basic building blocks are available and more will be added soon. 
See [Cafienne Bounded](https://cafienne.io/bounded) for more documentation and support

# Release to maven

The setup as written by [Leonard Ehrenfried](https://leonard.io/blog/2017/01/an-in-depth-guide-to-deploying-to-maven-central/) is used. Please note that releasing towards maven is done via the oss.sonatype.org site by closing the uploaded packages and releasing the thereafter. 

Release command = sbt +publishSigned 
This will release for 2.12. Via oss.sonatype.org you need to 'Close' the uploaded packages and thereafter 'Release' them.

# Release notes

# 0.1.6 [pending release]

In Aggregate Root tests no need to match for patterns in messages of exceptions on various command handling failures 
due to introduction of types representing different error scenarios. 
Use `TestableAggregateRoot#failure` to perform assertions on original command handling failure returned by Aggregate Root under test.
Both `TestableAggregateRoot#failure` & `TestableAggregateRoot#events` now signal situations of missing command.
More information about command provided in errors to aid with test fixes.

# 0.1.1 
Improvements for TestableAggregateRoot. When a command cannot be handled a CommandHandlingException is thrown. 
When a command should not be handled by this aggregate root an IllegalCommandException is thrown


