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
package io.glutenproject.extension.logical

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.utils.{LogicalPlanSelector, PullOutProjectHelper}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Equality, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.JOIN
import org.apache.spark.sql.execution.JoinSelectionShim
import org.apache.spark.sql.execution.joins.HashJoin

import scala.collection.mutable

case class PullOutPreProject(session: SparkSession)
  extends Rule[LogicalPlan]
  with PullOutProjectHelper {

  private def insertPreProjectIfNeeded(
      child: LogicalPlan,
      joinKeys: Seq[Expression]): (LogicalPlan, mutable.HashMap[Expression, NamedExpression]) = {
    val expressionMap = new mutable.HashMap[Expression, NamedExpression]()
    if (joinKeys.exists(isNotAttribute)) {
      joinKeys.toIndexedSeq.foreach(
        expr => replaceExpressionWithAttribute(expr.canonicalized, expressionMap))
      val preProject =
        Project(eliminateProjectList(child.outputSet, expressionMap.values.toSeq), child)
      (preProject, expressionMap)
    } else (child, expressionMap)
  }

  private def needsPreProject(plan: LogicalPlan): Boolean = plan match {
    case JoinSelectionShim.ExtractEquiJoinKeysShim(_, leftKeys, rightKeys, _, _, _, _) =>
      if (BackendsApiManager.getSettings.enableJoinKeysRewrite()) {
        HashJoin.rewriteKeyExpr(leftKeys).exists(isNotAttribute) ||
        HashJoin.rewriteKeyExpr(rightKeys).exists(isNotAttribute)
      } else {
        leftKeys.exists(isNotAttribute) || rightKeys.exists(isNotAttribute)
      }
    case _ => false
  }

  override def apply(plan: LogicalPlan): LogicalPlan = LogicalPlanSelector.maybe(session, plan) {
    if (GlutenConfig.getConf.enableAnsiMode) {
      // Gluten not support Ansi Mode, not pull out pre-project
      return plan
    }
    plan.transformWithPruning(_.containsPattern(JOIN)) {
      case join @ JoinSelectionShim
            .ExtractEquiJoinKeysShim(_, lKeys, rKeys, _, left, right, _) if needsPreProject(join) =>
        val (leftKeys, rightKeys) =
          if (BackendsApiManager.getSettings.enableJoinKeysRewrite()) {
            (HashJoin.rewriteKeyExpr(lKeys), HashJoin.rewriteKeyExpr(rKeys))
          } else {
            (lKeys, rKeys)
          }
        val (newLeft, leftMap) = insertPreProjectIfNeeded(left, leftKeys)
        val (newRight, rightMap) = insertPreProjectIfNeeded(right, rightKeys)

        val newCondition = if (leftMap.nonEmpty || rightMap.nonEmpty) {
          join.condition.map(_.transform {
            case p @ Equality(l, r) =>
              p.makeCopy(
                Array(
                  leftMap.getOrElse(l.canonicalized, l),
                  rightMap.getOrElse(r.canonicalized, r)))
          })
        } else {
          join.condition
        }
        // Add post-project to get the original output. If in the physical plan the buildSide
        // of the join is BuildLeft, the order of left and right will be exchanged in the
        // PullOutPostProject Rule.
        Project(join.output, join.copy(left = newLeft, right = newRight, condition = newCondition))
    }
  }
}
