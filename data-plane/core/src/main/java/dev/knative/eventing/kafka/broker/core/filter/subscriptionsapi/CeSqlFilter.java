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
package dev.knative.eventing.kafka.broker.core.filter.subscriptionsapi;

import dev.knative.eventing.kafka.broker.core.filter.Filter;
import io.cloudevents.CloudEvent;
import io.cloudevents.sql.EvaluationContext;
import io.cloudevents.sql.EvaluationException;
import io.cloudevents.sql.Expression;
import io.cloudevents.sql.Parser;
import io.cloudevents.sql.Result;
import io.cloudevents.sql.Type;
import io.cloudevents.sql.impl.ExceptionFactoryImpl;
import io.cloudevents.sql.impl.runtime.EvaluationContextImpl;
import io.cloudevents.sql.impl.runtime.EvaluationResult;
import io.cloudevents.sql.impl.runtime.TypeCastingProvider;
import org.antlr.v4.runtime.misc.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CeSqlFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(CeSqlFilter.class);

    private final Expression expression;
    private final EvaluationContext evaluationContext;

    public CeSqlFilter(String sqlExpression) {
        this.expression = Parser.parseDefault(sqlExpression);
        this.evaluationContext = new EvaluationContextImpl(
                new Interval(0, sqlExpression.length()), sqlExpression, new ExceptionFactoryImpl(true));
    }

    @Override
    public boolean test(CloudEvent cloudEvent) {
        try {
            logger.debug(
                    "Testing event against CESQL expression. Expression {} - Event {}", this.expression, cloudEvent);
            Object value = this.expression
                    .evaluate(CeSqlRuntimeManager.getInstance().getRuntime(), cloudEvent)
                    .value();
            logger.debug(
                    "CESQL evaluation succeeded. Expression {} - Event {} - Result {}", expression, cloudEvent, value);
            Result castedResult =
                    TypeCastingProvider.cast(this.evaluationContext, new EvaluationResult(value), Type.BOOLEAN);
            return (Boolean)
                    castedResult.value(); // value has to be boolean, because otherwise the exception factory would have
            // thrown an exception
        } catch (EvaluationException evaluationException) {
            logger.error(
                    "Exception while evaluating CESQL expression. Test failed. Expression {} - Exception {}",
                    this.expression,
                    evaluationException);
            return false;
        }
    }
}
