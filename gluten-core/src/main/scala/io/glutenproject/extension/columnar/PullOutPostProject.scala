/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.glutenproject.extension.columnar

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.utils.PullOutProjectHelper

import org.apache.spark.sql.catalyst.expressions.{AliasHelper, Attribute}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec

/**
 * The output of the native plan is not completely consistent with Spark. When the output is
 * inconsistent, it is necessary to use post-project to adjust the output of native plan to match
 * the output of Spark, ensuring that the output data of the native plan can match the Spark plan
 * when a fallback occurs.
 */
object PullOutPostProject extends Rule[SparkPlan] with PullOutProjectHelper with AliasHelper {

  private def needsPostProjection(plan: SparkPlan): Boolean = {
    plan match {
      case agg: BaseAggregateExec =>
        val pullOutHelper =
          BackendsApiManager.getSparkPlanExecApiInstance.genHashAggregateExecPullOutHelper(
            agg.groupingExpressions,
            agg.aggregateExpressions,
            agg.aggregateAttributes)
        val allAggregateResultAttributes = pullOutHelper.allAggregateResultAttributes
        // If the result expressions has different size with output attribute,
        // post-projection is needed.
        agg.resultExpressions.size != allAggregateResultAttributes.size ||
        // Compare each item in result expressions and output attributes. Attribute in Alias
        // should be trimmed before checking.
        agg.resultExpressions.map(trimAliases).zip(allAggregateResultAttributes).exists {
          case (exprAttr: Attribute, resAttr) =>
            // If the result attribute and result expression has different name or type,
            // post-projection is needed.
            exprAttr.name != resAttr.name || exprAttr.dataType != resAttr.dataType
          case _ =>
            // If result expression is not instance of Attribute,
            // post-projection is needed.
            true
        }
      case _ => false
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = plan match {
    case agg: BaseAggregateExec if supportedAggregate(agg) && needsPostProjection(agg) =>
      val pullOutHelper =
        BackendsApiManager.getSparkPlanExecApiInstance.genHashAggregateExecPullOutHelper(
          agg.groupingExpressions,
          agg.aggregateExpressions,
          agg.aggregateAttributes)
      val newResultExpressions = pullOutHelper.allAggregateResultAttributes
      val newAgg = copyBaseAggregateExec(agg)(newResultExpressions = newResultExpressions)
      ProjectExec(agg.resultExpressions, newAgg)

    case _ => plan
  }
}
