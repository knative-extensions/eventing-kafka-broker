package dev.knative.eventing.kafka.broker.core.filter.impl;

import dev.knative.eventing.kafka.broker.core.filter.Filter;
import io.cloudevents.CloudEvent;
import io.cloudevents.sql.EvaluationException;
import io.cloudevents.sql.EvaluationRuntime;
import io.cloudevents.sql.Expression;
import io.cloudevents.sql.Parser;
import io.cloudevents.sql.Type;

public class SqlFilter implements Filter {

  private final Expression expression;
  private final EvaluationRuntime runtime;

  public SqlFilter(String sqlExpression) {
    this.expression = Parser.parseDefault(sqlExpression);
    this.runtime = EvaluationRuntime.getDefault();
  }

  @Override
  public boolean test(CloudEvent cloudEvent) {
    try {
      Object value = this.expression.evaluate(this.runtime, cloudEvent);
      return (Boolean) this.runtime.cast(null /* TODO https://github.com/cloudevents/sdk-java/pull/396 */, value, Type.BOOLEAN);
    } catch (EvaluationException evaluationException) {
      return false;
    }
  }
}
