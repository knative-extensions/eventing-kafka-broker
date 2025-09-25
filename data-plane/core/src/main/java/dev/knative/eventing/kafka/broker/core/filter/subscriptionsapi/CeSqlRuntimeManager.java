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

import io.cloudevents.sql.EvaluationRuntime;
import io.cloudevents.sql.Function;
import io.cloudevents.sql.impl.functions.BaseFunction;
import io.cloudevents.sql.impl.runtime.EvaluationRuntimeBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.vertx.core.Vertx;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CeSqlRuntimeManager {
    private static final Logger logger = LoggerFactory.getLogger(CeSqlRuntimeManager.class);

    private static final CeSqlRuntimeManager INSTANCE = new CeSqlRuntimeManager();

    private List<Function> functions;

    private EvaluationRuntime runtime;

    private CeSqlRuntimeManager() {
        this.runtime = EvaluationRuntime.builder().build();
        this.functions = new LinkedList<>();
    }

    public static CeSqlRuntimeManager getInstance() {
        return INSTANCE;
    }

    public EvaluationRuntime getRuntime() {
        return runtime;
    }

    public void registerFunction(BaseFunction function) {
        logger.info("Registering CeSql function: {}", function.name());
        this.functions.add(function);
        EvaluationRuntimeBuilder builder = EvaluationRuntime.builder();
        this.functions.forEach(builder::addFunction);
        this.runtime = builder.build();
    }

    public void registerKnVerifyCorrelationId(Vertx vertx, KubernetesClient kubernetesClient, String systemNamespace) {
        if (vertx != null && kubernetesClient != null && systemNamespace != null) {
            registerFunction(new KnVerifyCorrelationId(vertx, kubernetesClient, systemNamespace));
        }
    }
}
