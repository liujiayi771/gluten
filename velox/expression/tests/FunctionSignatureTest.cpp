/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;

class FunctionSignatureTest : public functions::test::FunctionBaseTest {};

TEST_F(FunctionSignatureTest, validateFunctionSignature) {
  // Construct a valid function signature.
  ASSERT_NO_THROW(exec::FunctionSignatureBuilder()
                      .integerVariable("a_precision")
                      .integerVariable("a_scale")
                      .argumentType("DECIMAL(a_precision, a_scale)")
                      .returnType("DECIMAL(a_precision, a_scale)")
                      .build());

  VELOX_ASSERT_THROW(
      exec::FunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .argumentType("DECIMAL(b_precision, b_scale)")
          .returnType("DECIMAL(a_precision, a_scale)")
          .build(),
      "Type doesn't exist");

  VELOX_ASSERT_THROW(
      exec::FunctionSignatureBuilder()
          .argumentType("ARRAY(ANY)")
          .returnType("ARRAY(ANY)")
          .build(),
      "Type 'Any' cannot appear in return type");

  VELOX_ASSERT_THROW(
      exec::FunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .integerVariable("b_precision")
          .integerVariable("b_scale")
          .argumentType("DECIMAL(a_precision, a_scale)")
          .returnType("DECIMAL(a_precision, a_scale)")
          .build(),
      "Some integer variables are not used");

  // ANY in returnType.
  VELOX_ASSERT_THROW(
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .typeVariable("E")
          .argumentType("ARRAY(T)")
          .returnType("ARRAY(T)")
          .build(),
      "Some type variables are not used in the inputs");
}

class AggregateFunctionSignatureTest
    : public functions::test::FunctionBaseTest {};

TEST_F(AggregateFunctionSignatureTest, validateAggregateFunctionSignature) {
  // Valid aggregate function signature.
  ASSERT_NO_THROW(
      exec::AggregateFunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .argumentType("DECIMAL(a_precision, a_scale)")
          .intermediateType("ROW(DECIMAL(a_precision, a_scale), BIGINT)")
          .returnType("DECIMAL(a_precision, a_scale)")
          .build());

  // intermediateType contains variables.
  ASSERT_NO_THROW(
      exec::AggregateFunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .integerVariable("i_precision", "min(38, a_precision + 10)")
          .argumentType("DECIMAL(a_precision, a_scale)")
          .intermediateType("ROW(DECIMAL(i_precision, a_scale), BIGINT)")
          .returnType("DECIMAL(a_precision, a_scale)")
          .build());

  VELOX_ASSERT_THROW(
      exec::AggregateFunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .integerVariable("i_precision", "min(38, a_precision + 10)")
          .integerVariable("b_precision", "min(38, b_precision + 4)")
          .argumentType("DECIMAL(a_precision, a_scale)")
          .intermediateType("ROW(DECIMAL(i_precision, a_scale), BIGINT)")
          .returnType("DECIMAL(a_precision, a_scale)")
          .build(),
      "Some integer variables are not used");
}
