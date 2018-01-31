package bounded.test

import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, Suite}
import scala.language.postfixOps

import scala.concurrent.duration._

trait StopSystemAfterAll extends BeforeAndAfterAll {
  this: TestKit with Suite =>
  override protected def afterAll() {
    super.afterAll()
    TestKit.shutdownActorSystem(system, 30 seconds, verifySystemShutdown = true)
  }
}
