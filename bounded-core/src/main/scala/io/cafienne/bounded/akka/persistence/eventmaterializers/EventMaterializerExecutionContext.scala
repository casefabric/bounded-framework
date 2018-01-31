package bounded.akka.persistence.eventmaterializers

object EventMaterializerExecutionContext {
  implicit val executionContext =
    scala.concurrent.ExecutionContext.Implicits.global
}
