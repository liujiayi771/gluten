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

package io.glutenproject.substrait.rel;

import io.glutenproject.substrait.expression.ExpressionNode;
import io.glutenproject.substrait.extensions.AdvancedExtensionNode;
import io.substrait.proto.*;

import java.io.Serializable;
import java.util.ArrayList;

public class WindowTopKFilterRelNode implements RelNode, Serializable {
  private final RelNode input;
  private final int k;
  private final ArrayList<ExpressionNode> partitionExpressions = new ArrayList<>();
  private final ArrayList<SortField> sorts = new ArrayList<>();
  private final AdvancedExtensionNode extensionNode;

  public WindowTopKFilterRelNode(RelNode input,
                                 int k,
                                 ArrayList<ExpressionNode> partitionExpressions,
                                 ArrayList<SortField> sorts) {
    this.input = input;
    this.k = k;
    this.partitionExpressions.addAll(partitionExpressions);
    this.sorts.addAll(sorts);
    this.extensionNode = null;
  }

  public WindowTopKFilterRelNode(RelNode input,
                                 int k,
                                 ArrayList<ExpressionNode> partitionExpressions,
                                 ArrayList<SortField> sorts,
                                 AdvancedExtensionNode extensionNode) {
    this.input = input;
    this.k = k;
    this.partitionExpressions.addAll(partitionExpressions);
    this.sorts.addAll(sorts);
    this.extensionNode = extensionNode;
  }

  @Override
  public Rel toProtobuf() {
    RelCommon.Builder relCommonBuilder = RelCommon.newBuilder();
    relCommonBuilder.setDirect(RelCommon.Direct.newBuilder());

    WindowTopKFilterRel.Builder windowTopKFilterBuilder = WindowTopKFilterRel.newBuilder();
    windowTopKFilterBuilder.setCommon(relCommonBuilder.build());
    if (input != null) {
      windowTopKFilterBuilder.setInput(input.toProtobuf());
    }

    windowTopKFilterBuilder.setK(k);

    for(int i = 0; i < partitionExpressions.size(); i ++) {
      windowTopKFilterBuilder.addPartitionExpressions(i, partitionExpressions.get(i).toProtobuf());
    }

    for(int i = 0; i < sorts.size(); i ++) {
      windowTopKFilterBuilder.addSorts(i, sorts.get(i));
    }

    if (extensionNode != null) {
      windowTopKFilterBuilder.setAdvancedExtension(extensionNode.toProtobuf());
    }
    Rel.Builder builder = Rel.newBuilder();
    builder.setWindowTopkFilter(windowTopKFilterBuilder.build());
    return builder.build();
  }
}
