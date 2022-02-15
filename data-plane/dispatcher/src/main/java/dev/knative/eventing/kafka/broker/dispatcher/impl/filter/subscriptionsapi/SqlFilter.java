/*
 * Copyright © 2018 Knative Authors (knative-dev@googlegroups.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.knative.eventing.kafka.broker.dispatcher.impl.filter.subscriptionsapi;

import dev.knative.eventing.kafka.broker.dispatcher.Filter;
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
      Object value = this.expression.tryEvaluate(this.runtime, cloudEvent);
      return (Boolean) this.runtime.cast(value, Type.BOOLEAN);
    } catch (EvaluationException evaluationException) {
      return false;
    }
  }
}
