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
package io.glutenproject.execution

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression.{AttributeReferenceTransformer, ConverterUtils, ExpressionConverter}
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.extensions.{AdvancedExtensionNode, ExtensionBuilder}
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.types.DataType

import com.google.protobuf.Any
import io.substrait.proto.JoinRel

import scala.collection.JavaConverters._

object JoinUtils {
  private def createEnhancement(output: Seq[Attribute]): com.google.protobuf.Any = {
    val inputTypeNodes = output.map {
      attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable)
    }
    // Normally the enhancement node is only used for plan validation. But here the enhancement
    // is also used in execution phase. In this case an empty typeUrlPrefix need to be passed,
    // so that it can be correctly parsed into json string on the cpp side.
    BackendsApiManager.getTransformerApiInstance.packPBMessage(
      TypeBuilder.makeStruct(false, inputTypeNodes.asJava).toProtobuf)
  }

  def createExtensionNode(output: Seq[Attribute], validation: Boolean): AdvancedExtensionNode = {
    // Use field [enhancement] in a extension node for input type validation.
    if (validation) {
      ExtensionBuilder.makeAdvancedExtension(createEnhancement(output))
    } else {
      null
    }
  }

  def preProjectionNeeded(keyExprs: Seq[Expression]): Boolean = {
    !keyExprs.forall(_.isInstanceOf[AttributeReference])
  }

  def createPreProjectionIfNeeded(
      keyExprs: Seq[Expression],
      inputNode: RelNode,
      inputNodeOutput: Seq[Attribute],
      partialConstructedJoinOutput: Seq[Attribute],
      substraitContext: SubstraitContext,
      operatorId: java.lang.Long,
      validation: Boolean): (Seq[(ExpressionNode, DataType)], RelNode, Seq[Attribute]) = {
    // Skip pre-projection if all keys are [AttributeReference]s,
    // which can be directly converted into SelectionNode.
    val keys = keyExprs.map {
      expr =>
        (
          ExpressionConverter
            .replaceWithExpressionTransformer(expr, partialConstructedJoinOutput)
            .asInstanceOf[AttributeReferenceTransformer]
            .doTransform(substraitContext.registeredFunction),
          expr.dataType)
    }
    (keys, inputNode, inputNodeOutput)
  }

  def createJoinExtensionNode(
      joinParameters: Any,
      output: Seq[Attribute]): AdvancedExtensionNode = {
    // Use field [optimization] in a extension node
    // to send some join parameters through Substrait plan.
    val enhancement = createEnhancement(output)
    ExtensionBuilder.makeAdvancedExtension(joinParameters, enhancement)
  }

  // Return the direct join output.
  protected def getDirectJoinOutput(
      joinType: JoinType,
      leftOutput: Seq[Attribute],
      rightOutput: Seq[Attribute]): (Seq[Attribute], Seq[Attribute]) = {
    joinType match {
      case _: InnerLike =>
        (leftOutput, rightOutput)
      case LeftOuter =>
        (leftOutput, rightOutput.map(_.withNullability(true)))
      case RightOuter =>
        (leftOutput.map(_.withNullability(true)), rightOutput)
      case FullOuter =>
        (leftOutput.map(_.withNullability(true)), rightOutput.map(_.withNullability(true)))
      case j: ExistenceJoin =>
        (leftOutput :+ j.exists, Nil)
      case LeftExistence(_) =>
        // LeftSemi | LeftAnti | ExistenceJoin.
        (leftOutput, Nil)
      case x =>
        throw new IllegalArgumentException(s"${getClass.getSimpleName} not take $x as the JoinType")
    }
  }

  protected def getDirectJoinOutputSeq(
      joinType: JoinType,
      leftOutput: Seq[Attribute],
      rightOutput: Seq[Attribute]): Seq[Attribute] = {
    val (left, right) = getDirectJoinOutput(joinType, leftOutput, rightOutput)
    left ++ right
  }

  // scalastyle:off argcount
  def createJoinRel(
      streamedKeyExprs: Seq[Expression],
      buildKeyExprs: Seq[Expression],
      condition: Option[Expression],
      substraitJoinType: JoinRel.JoinType,
      exchangeTable: Boolean,
      joinType: JoinType,
      joinParameters: Any,
      streamedRelNode: RelNode,
      buildRelNode: RelNode,
      streamedOutput: Seq[Attribute],
      buildOutput: Seq[Attribute],
      substraitContext: SubstraitContext,
      operatorId: java.lang.Long,
      validation: Boolean = false): RelNode = {
    // scalastyle:on argcount
    // Create pre-projection for build/streamed plan. Append projected keys to each side.
    val streamedKeys = streamedKeyExprs.map {
      expr =>
        (
          ExpressionConverter
            .replaceWithExpressionTransformer(expr, streamedOutput)
            .doTransform(substraitContext.registeredFunction),
          expr.dataType)
    }

    val buildKeys = buildKeyExprs.map {
      expr =>
        (
          ExpressionConverter
            .replaceWithExpressionTransformer(expr, streamedOutput ++ buildOutput)
            .doTransform(substraitContext.registeredFunction),
          expr.dataType)
    }

    // Combine join keys to make a single expression.
    val joinExpressionNode = streamedKeys
      .zip(buildKeys)
      .map {
        case ((leftKey, leftType), (rightKey, rightType)) =>
          HashJoinLikeExecTransformer.makeEqualToExpression(
            leftKey,
            leftType,
            rightKey,
            rightType,
            substraitContext.registeredFunction)
      }
      .reduce(
        (l, r) =>
          HashJoinLikeExecTransformer.makeAndExpression(l, r, substraitContext.registeredFunction))

    // Create post-join filter, which will be computed in hash join.
    val postJoinFilter = condition.map {
      expr =>
        ExpressionConverter
          .replaceWithExpressionTransformer(expr, streamedOutput ++ buildOutput)
          .doTransform(substraitContext.registeredFunction)
    }

    // Create JoinRel.
    val joinRel = RelBuilder.makeJoinRel(
      streamedRelNode,
      buildRelNode,
      substraitJoinType,
      joinExpressionNode,
      postJoinFilter.orNull,
      createJoinExtensionNode(joinParameters, streamedOutput ++ buildOutput),
      substraitContext,
      operatorId
    )

    // Result projection will drop the appended keys, and exchange columns order if BuildLeft.
    val resultProjection = if (exchangeTable) {
      val (leftOutput, rightOutput) =
        getDirectJoinOutput(joinType, buildOutput, streamedOutput)
      joinType match {
        case _: ExistenceJoin =>
          buildOutput.indices.map(ExpressionBuilder.makeSelection(_)) :+
            ExpressionBuilder.makeSelection(buildOutput.size)
        case LeftExistence(_) =>
          leftOutput.indices.map(ExpressionBuilder.makeSelection(_))
        case _ =>
          // Exchange the order of build and streamed.
          leftOutput.indices.map(
            idx => ExpressionBuilder.makeSelection(idx + streamedOutput.size)) ++
            rightOutput.indices
              .map(ExpressionBuilder.makeSelection(_))
      }
    } else {
      val (leftOutput, rightOutput) =
        getDirectJoinOutput(joinType, streamedOutput, buildOutput)
      if (joinType.isInstanceOf[ExistenceJoin]) {
        streamedOutput.indices.map(ExpressionBuilder.makeSelection(_)) :+
          ExpressionBuilder.makeSelection(streamedOutput.size)
      } else {
        leftOutput.indices.map(ExpressionBuilder.makeSelection(_)) ++
          rightOutput.indices.map(idx => ExpressionBuilder.makeSelection(idx + streamedOutput.size))
      }
    }

    val directJoinOutputs = if (exchangeTable) {
      getDirectJoinOutputSeq(joinType, buildOutput, streamedOutput)
    } else {
      getDirectJoinOutputSeq(joinType, streamedOutput, buildOutput)
    }
    RelBuilder.makeProjectRel(
      joinRel,
      new java.util.ArrayList[ExpressionNode](resultProjection.asJava),
      createExtensionNode(directJoinOutputs, validation),
      substraitContext,
      operatorId,
      directJoinOutputs.size
    )
  }

  def createTransformContext(
      exchangeTable: Boolean,
      output: Seq[Attribute],
      rel: RelNode,
      inputStreamedOutput: Seq[Attribute],
      inputBuildOutput: Seq[Attribute]): TransformContext = {
    val inputAttributes = if (exchangeTable) {
      inputBuildOutput ++ inputStreamedOutput
    } else {
      inputStreamedOutput ++ inputBuildOutput
    }
    TransformContext(inputAttributes, output, rel)
  }
}
