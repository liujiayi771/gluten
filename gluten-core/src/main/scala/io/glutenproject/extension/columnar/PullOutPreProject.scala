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

import io.glutenproject.utils.PullOutProjectHelper

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, Partial}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ProjectExec, SortExec, SparkPlan, TakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, TypedAggregateExpression}

import scala.collection.mutable

/**
 * The native engine only supports executing Expressions within the project operator. When there are
 * Expressions within physical operators such as SortExec and HashAggregateExec in SparkPlan, we
 * need to pull out the Expressions to be executed earlier in the pre-project operator. This rule is
 * to transform the SparkPlan at the physical plan level, constructing a SparkPlan that supports
 * execution by the native engine.
 */
object PullOutPreProject extends Rule[SparkPlan] with PullOutProjectHelper {

  private def needsPreProject(plan: SparkPlan): Boolean = {
    plan match {
      case sort: SortExec =>
        sort.sortOrder.exists(o => isNotAttribute(o.child))
      case take: TakeOrderedAndProjectExec =>
        take.sortOrder.exists(o => isNotAttribute(o.child))
      case agg: BaseAggregateExec =>
        agg.groupingExpressions.exists(isNotAttribute) ||
        agg.aggregateExpressions.exists {
          expr =>
            if (expr.aggregateFunction.isInstanceOf[TypedAggregateExpression]) {
              // We cannot pull out the children of TypedAggregateExpression to pre-project,
              // and Gluten cannot support TypedAggregateExpression.
              false
            } else {
              expr.filter.exists(isNotAttribute) ||
              (expr.mode match {
                case Partial =>
                  expr.aggregateFunction.children.exists(isNotAttributeAndLiteral)
                case _ => false
              })
            }
        }
      case _ => false
    }
  }

  /**
   * Pull out Expressions in SortOrder's children, and return the new SortOrder that contains only
   * Attributes.
   */
  private def getNewSortOrder(
      sortOrders: Seq[SortOrder],
      expressionMap: mutable.HashMap[Expression, NamedExpression]): Seq[SortOrder] = {
    sortOrders.map {
      originalOrder =>
        val originalOrderExpressions = mutable.ArrayBuffer[Expression]()
        val newOrder = originalOrder
          .mapChildren {
            child =>
              val newChild = replaceExpressionWithAttribute(child, expressionMap)
              if (!newChild.semanticEquals(child)) {
                // When it is found that a child needs to be pulled out, the original child will
                // be added to the sameOrderExpressions. This ensures that the correct sort orders
                // can be obtained when retrieving the post-project outputOrdering. Spark will
                // verifying whether the output attributes of the post-project are a subset of
                // the references of sortOrder's children. Please check
                // AliasAwareQueryOutputOrdering.outputOrdering in Spark-3.4.
                originalOrderExpressions += child
              }
              newChild
          }
          .asInstanceOf[SortOrder]
        newOrder.copy(sameOrderExpressions =
          newOrder.sameOrderExpressions ++ originalOrderExpressions)
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = plan match {
    case sort: SortExec if needsPreProject(sort) =>
      val expressionMap = new mutable.HashMap[Expression, NamedExpression]()
      val newSortOrder = getNewSortOrder(sort.sortOrder, expressionMap)
      // The output of the sort operator is the same as the output of the child, therefore it
      // is necessary to retain the output columns of the child in the pre-projection, and
      // then add the expressions that need to be evaluated in the sortOrder. Finally, in the
      // post-projection, the additional columns need to be removed, leaving only the original
      // output of the child.
      val preProject = ProjectExec(
        eliminateProjectList(sort.child.outputSet, expressionMap.values.toSeq),
        sort.child)
      val newSort = sort.copy(sortOrder = newSortOrder, child = preProject)
      // The pre-project and post-project of SortExec always appear together, so it's more
      // convenient to handle them together. Therefore, SortExec's post-project will no longer
      // be pulled out separately.
      ProjectExec(sort.child.output, newSort)

    case topK: TakeOrderedAndProjectExec if needsPreProject(topK) =>
      val expressionMap = new mutable.HashMap[Expression, NamedExpression]()
      val newSortOrder = getNewSortOrder(topK.sortOrder, expressionMap)
      val preProject = ProjectExec(
        eliminateProjectList(topK.child.outputSet, expressionMap.values.toSeq),
        topK.child)
      topK.copy(sortOrder = newSortOrder, child = preProject)

    case agg: BaseAggregateExec if supportedAggregate(agg) && needsPreProject(agg) =>
      val expressionMap = new mutable.HashMap[Expression, NamedExpression]()
      // Handle groupingExpressions.
      val newGroupingExpressions =
        agg.groupingExpressions.toIndexedSeq.map(
          replaceExpressionWithAttribute(_, expressionMap).asInstanceOf[NamedExpression])

      // Handle aggregateExpressions.
      val newAggregateExpressions = agg.aggregateExpressions.toIndexedSeq.map {
        ae =>
          val newAggFuncChildren = ae.aggregateFunction.children.map {
            case literal: Literal => literal
            case other => replaceExpressionWithAttribute(other, expressionMap)
          }
          val newAggFunc = ae.aggregateFunction
            .withNewChildren(newAggFuncChildren)
            .asInstanceOf[AggregateFunction]
          val newFilter =
            ae.filter.map(replaceExpressionWithAttribute(_, expressionMap))
          ae.copy(aggregateFunction = newAggFunc, filter = newFilter)
      }

      val newAgg = copyBaseAggregateExec(agg)(
        newGroupingExpressions = newGroupingExpressions,
        newAggregateExpressions = newAggregateExpressions)
      val preProject = ProjectExec(
        eliminateProjectList(agg.child.outputSet, expressionMap.values.toSeq),
        agg.child)
      newAgg.withNewChildren(Seq(preProject))

    case _ => plan
  }
}
