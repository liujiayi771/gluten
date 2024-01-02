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
package io.glutenproject.extension

import io.glutenproject.execution._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.mutable.ListBuffer

object InsertPrePostProjections extends Rule[SparkPlan] with AliasHelper {

  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp(applyLocally)

  val applyLocally: PartialFunction[SparkPlan, SparkPlan] = {
    case agg: HashAggregateExecBaseTransformer =>
      var transformedPlan: SparkPlan = agg
      if (agg.needsPreProjection) {
        val projectExpressions = ListBuffer.empty[NamedExpression]
        // Handle groupingExpressions
        val newGroupingExpressions =
          agg.groupingExpressions.map(replaceProjectAttribute(_, projectExpressions))

        // Handle aggregateExpressions
        val newAggregateExpressions = agg.aggregateExpressions.map {
          ae =>
            val newAggFuncChildren = ae.aggregateFunction.children.map {
              case literal: Literal =>
                // Aggregation function support Literal input.
                literal
              case other =>
                // Replace other expressions if needed.
                replaceProjectAttribute(other, projectExpressions)
            }
            val newAggFunc = ae.aggregateFunction
              .withNewChildren(newAggFuncChildren)
              .asInstanceOf[AggregateFunction]
            // Handle filter
            val newFilter = ae.filter.map(replaceProjectAttribute(_, projectExpressions))
            ae.copy(aggregateFunction = newAggFunc, filter = newFilter)
        }

        transformedPlan = agg.copySelf(
          groupingExpressions = newGroupingExpressions,
          aggregateExpressions = newAggregateExpressions,
          child =
            ProjectExecTransformer(getProjectList(projectExpressions), agg.child, ProjectType.PRE)
        )
      }

      if (agg.needsPostProjection) {
        val projectList = agg.resultExpressions.map {
          case ne: NamedExpression => ne
          case other => Alias(other, other.toString())()
        }
        transformedPlan = ProjectExecTransformer(projectList, transformedPlan, ProjectType.POST)
      }
      transformedPlan

    case sort: SortExecTransformer =>
      val originalInputAttributes = sort.child.output
      var transformedPlan: SparkPlan = sort
      if (sort.needsPreProjection) {
        val projectExpressions = ListBuffer.empty[NamedExpression]
        // The output of the sort operator is the same as the output of the child, therefore it
        // is necessary to retain the output columns of the child in the pre-projection, and
        // then add the expressions that need to be evaluated in the sortOrder. Finally, in the
        // post-projection, the additional columns need to be removed, leaving only the original
        // output of the child.
        projectExpressions ++= originalInputAttributes
        val newSortOrder = sort.sortOrder.map {
          order =>
            val newChild = replaceProjectAttribute(order.child, projectExpressions)
            order.copy(child = newChild)
        }
        transformedPlan = sort.copy(
          sortOrder = newSortOrder,
          child =
            ProjectExecTransformer(getProjectList(projectExpressions), sort.child, ProjectType.PRE))
      }
      if (sort.needsPostProjection) {
        transformedPlan =
          ProjectExecTransformer(originalInputAttributes, transformedPlan, ProjectType.POST)
      }
      transformedPlan
  }

  /**
   * Collect the expressions that need to be placed in the Project, and at the same time return the
   * Attribute that need to be transformed in the transformer.
   * @param expr
   *   Expressions in transformer that may need to be replaced.
   * @param projectExpressions
   *   The container for collecting project expressions.
   * @return
   *   The new transformed Attribute need to be replaced in transformer variables.
   */
  private def replaceProjectAttribute(
      expr: Expression,
      projectExpressions: ListBuffer[NamedExpression]): NamedExpression = expr match {
    case ne: NamedExpression =>
      projectExpressions += ne
      ne.toAttribute
    case other =>
      val alias = Alias(other, other.toString())()
      projectExpressions += alias
      alias.toAttribute
  }

  private def getProjectList(list: ListBuffer[NamedExpression]): Seq[NamedExpression] = {
    ExpressionSet(list).toSeq.map(_.asInstanceOf[NamedExpression])
  }

  /**
   * This function is used to generate the transformed plan during validation, and it can return the
   * inserted pre-projection, post-projection, as well as the operator transformer. There are four
   * different scenarios in total.
   *   - post-projection -> transformer -> pre-projection
   *   - post-projection -> transformer
   *   - transformer -> pre-projection
   *   - transformer
   */
  def getTransformedPlan(transformer: SparkPlan): Seq[SparkPlan] = {
    val transformedPlan = transformer.transformUp(applyLocally)
    val allPlan = ListBuffer[SparkPlan](transformedPlan)
    transformedPlan match {
      case p: ProjectExecTransformer if p.isPostProjection =>
        val originPlan = p.child
        allPlan += originPlan
        originPlan.children.foreach {
          case pre: ProjectExecTransformer if pre.isPreProjection =>
            allPlan += pre
          case _ =>
        }
      case p: ProjectInsertSupport =>
        p.children.foreach {
          case pre: ProjectExecTransformer if pre.isPreProjection =>
            allPlan += pre
          case _ =>
        }
      case _ =>
    }
    allPlan
  }
}
