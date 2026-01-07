/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

/// Represents a 128-bit decimal value along with its precision and scale.
#include <type/HugeInt.h>
#include <optional>
#include <string>
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "bolt/functions/sparksql/DecimalArithmetic.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::test;
using namespace bytedance::bolt::functions::sparksql;
using namespace bytedance::bolt::functions::test;

// port from
// https://github.com/apache/arrow/blob/release-12.0.1-rc1/cpp/src/gandiva/precompiled/decimal_ops_test.cc

class Decimal128 {
 public:
  constexpr Decimal128(const int128_t& value, uint8_t precision, uint8_t scale)
      : value_(value), precision_(precision), scale_(scale) {}

  Decimal128(const std::string& val, uint8_t precision, uint8_t scale)
      : value_(HugeInt::parse(val)), precision_(precision), scale_(scale) {}

  uint8_t scale() const {
    return scale_;
  }

  uint8_t precision() const {
    return precision_;
  }

  int128_t& value() {
    return value_;
  }

  const int128_t& value() const {
    return value_;
  }

  std::string ToString() const {
    return std::to_string(value_) + "," + std::to_string(precision_) + "," +
        std::to_string(scale_);
  }
  enum Op { kOpAdd, kOpSubtract, kOpMultiply, kOpDivide, kOpMod };

 private:
  int128_t value_;
  uint8_t precision_;
  uint8_t scale_;
};
inline bool operator==(const Decimal128& left, const Decimal128& right) {
  return left.value() == right.value() &&
      left.precision() == right.precision() && left.scale() == right.scale();
}

inline Decimal128 operator-(const Decimal128& operand) {
  return Decimal128{-operand.value(), operand.precision(), operand.scale()};
}

const auto kThirtyFive9s(std::string(35, '9'));
const auto kThirtySix9s(std::string(36, '9'));
const auto kThirtyEight9s(std::string(38, '9'));

class TestDecimalSql : public ::testing::Test {
 protected:
  template <typename Operation, bool AllowPrecisionLoss>
  static void Verify(
      const Decimal128& x,
      const Decimal128& y,
      const Decimal128& expected_result,
      bool expected_overflow);

  template <typename Operation, bool AllowPrecisionLoss>
  static void VerifyAllSign(
      const Decimal128& left,
      const Decimal128& right,
      const Decimal128& expected_output,
      bool expected_overflow);

  template <bool AllowPrecisionLoss>
  void AddAndVerify(
      const Decimal128& x,
      const Decimal128& y,
      const Decimal128& expected_result,
      bool expected_overflow) {
    Verify<Addition, AllowPrecisionLoss>(
        x, y, expected_result, expected_overflow);
  }

  template <bool AllowPrecisionLoss>
  void SubtractAndVerify(
      const Decimal128& x,
      const Decimal128& y,
      const Decimal128& expected_result,
      bool expected_overflow) {
    Verify<Subtraction, AllowPrecisionLoss>(
        x, y, expected_result, expected_overflow);
  }

  template <bool AllowPrecisionLoss>
  void MultiplyAndVerify(
      const Decimal128& x,
      const Decimal128& y,
      const Decimal128& expected_result,
      bool expected_overflow) {
    Verify<Multiply, AllowPrecisionLoss>(
        x, y, expected_result, expected_overflow);
  }

  template <bool AllowPrecisionLoss>
  void MultiplyAndVerifyAllSign(
      const Decimal128& x,
      const Decimal128& y,
      const Decimal128& expected_result,
      bool expected_overflow) {
    VerifyAllSign<Multiply, AllowPrecisionLoss>(
        x, y, expected_result, expected_overflow);
  }

  template <bool AllowPrecisionLoss>
  void DivideAndVerify(
      const Decimal128& x,
      const Decimal128& y,
      const Decimal128& expected_result,
      bool expected_overflow) {
    Verify<Divide, AllowPrecisionLoss>(
        x, y, expected_result, expected_overflow);
  }

  template <bool AllowPrecisionLoss>
  void DivideAndVerifyAllSign(
      const Decimal128& x,
      const Decimal128& y,
      const Decimal128& expected_result,
      bool expected_overflow) {
    VerifyAllSign<Divide, AllowPrecisionLoss>(
        x, y, expected_result, expected_overflow);
  }
};

#define EXPECT_DECIMAL_EQ(                                                   \
    op,                                                                      \
    x,                                                                       \
    y,                                                                       \
    expected_result,                                                         \
    expected_overflow,                                                       \
    actual_result,                                                           \
    actual_overflow)                                                         \
  {                                                                          \
    EXPECT_TRUE(expected_overflow == actual_overflow)                        \
        << op << "(" << (x).ToString() << " and " << (y).ToString() << ")"   \
        << " expected overflow : " << expected_overflow                      \
        << " actual overflow : " << actual_overflow;                         \
    if (!expected_overflow) {                                                \
      EXPECT_TRUE(expected_result == actual_result)                          \
          << op << "(" << (x).ToString() << " and " << (y).ToString() << ")" \
          << " expected : " << expected_result.ToString()                    \
          << " actual : " << actual_result.ToString();                       \
    }                                                                        \
  }

template <typename Operation, bool AllowPrecisionLoss>
void TestDecimalSql::Verify(
    const Decimal128& x,
    const Decimal128& y,
    const Decimal128& expected_result,
    bool expected_overflow) {
  bool overflow = false;
  int64_t context = 0;
  const auto [rPrecision, rScale] =
      Operation::template computeResultPrecisionScale<AllowPrecisionLoss>(
          x.precision(), x.scale(), y.precision(), y.scale());
  const uint8_t aRescale =
      Operation::computeRescaleFactor(x.scale(), y.scale(), rScale);
  const uint8_t bRescale =
      Operation::computeRescaleFactor(y.scale(), x.scale(), rScale);
  Decimal128 result(0, rPrecision, rScale);

  Operation::template apply<int128_t, int128_t, int128_t>(
      result.value(),
      x.value(),
      y.value(),
      aRescale,
      bRescale,
      x.precision(),
      x.scale(),
      y.precision(),
      y.scale(),
      rPrecision,
      rScale,
      overflow);

  std::string op_name;
  if constexpr (std::is_same_v<Operation, Addition>) {
    op_name = "add";
  } else if constexpr (std::is_same_v<Operation, Subtraction>) {
    op_name = "subtract";
  } else if constexpr (std::is_same_v<Operation, Multiply>) {
    op_name = "multiply";
  } else if constexpr (std::is_same_v<Operation, Divide>) {
    op_name = "divide";
  } else {
    op_name = "unknown";
  }

  if constexpr (!AllowPrecisionLoss) {
    op_name += "_deny_precision_loss";
  }

  EXPECT_DECIMAL_EQ(
      op_name, x, y, expected_result, expected_overflow, result, overflow);
}

template <typename Operation, bool AllowPrecisionLoss>
void TestDecimalSql::VerifyAllSign(
    const Decimal128& left,
    const Decimal128& right,
    const Decimal128& expected_output,
    bool expected_overflow) {
  // both +ve
  Verify<Operation, AllowPrecisionLoss>(
      left, right, expected_output, expected_overflow);

  // left -ve
  Verify<Operation, AllowPrecisionLoss>(
      -left, right, -expected_output, expected_overflow);

  // right -ve
  Verify<Operation, AllowPrecisionLoss>(
      left, -right, -expected_output, expected_overflow);

  // both -ve
  Verify<Operation, AllowPrecisionLoss>(
      -left, -right, expected_output, expected_overflow);
}

TEST_F(TestDecimalSql, Add) {
  // fast-path
  AddAndVerify<true>(
      Decimal128{"201", 30, 3}, // x
      Decimal128{"301", 30, 3}, // y
      Decimal128{"502", 31, 3},
      false); // expected

  // max precision
  AddAndVerify<true>(
      Decimal128{"09999999999999999999999999999999000000", 38, 5}, // x
      Decimal128{"100", 38, 7}, // y
      Decimal128{"99999999999999999999999999999990000010", 38, 6},
      false); // expected

  // Both -ve
  AddAndVerify<true>(
      Decimal128{"-201", 30, 3}, // x
      Decimal128{"-301", 30, 2}, // y
      Decimal128{"-3211", 32, 3},
      false); // expected

  // -ve and max precision
  AddAndVerify<true>(
      Decimal128{"-09999999999999999999999999999999000000", 38, 5}, // x
      Decimal128{"-100", 38, 7}, // y
      Decimal128{"-99999999999999999999999999999990000010", 38, 6},
      false); // expected
}

TEST_F(TestDecimalSql, AddDenyPrecisionLoss) {
  // fast-path with strict precision requirements
  AddAndVerify<false>(
      Decimal128{"201", 30, 3}, // x
      Decimal128{"301", 30, 3}, // y
      Decimal128{"502", 31, 3},
      false); // expected (same as allow_precision_loss for this case)

  // Both -ve with strict precision
  AddAndVerify<false>(
      Decimal128{"-201", 30, 3}, // x
      Decimal128{"-301", 30, 2}, // y
      Decimal128{"-3211", 32, 3},
      false); // expected

  // max precision with strict requirements - update to expect overflow
  AddAndVerify<false>(
      Decimal128{"09999999999999999999999999999999000000", 38, 5}, // x
      Decimal128{"100", 38, 7}, // y
      Decimal128{"0", 38, 7}, // expected result is 0 with overflow
      true); // overflow expected in deny_precision_loss mode

  // -ve and max precision with strict precision - update to expect overflow
  AddAndVerify<false>(
      Decimal128{"-09999999999999999999999999999999000000", 38, 5}, // x
      Decimal128{"-100", 38, 7}, // y
      Decimal128{"0", 38, 7}, // expected result is 0 with overflow
      true); // overflow expected in deny_precision_loss mode
}

TEST_F(TestDecimalSql, Subtract) {
  // fast-path
  SubtractAndVerify<true>(
      Decimal128{"201", 30, 3}, // x
      Decimal128{"301", 30, 3}, // y
      Decimal128{"-100", 31, 3},
      false); // expected

  // max precision
  SubtractAndVerify<true>(
      Decimal128{"09999999999999999999999999999999000000", 38, 5}, // x
      Decimal128{"100", 38, 7}, // y
      Decimal128{"99999999999999999999999999999989999990", 38, 6},
      false); // expected

  // Both -ve
  SubtractAndVerify<true>(
      Decimal128{"-201", 30, 3}, // x
      Decimal128{"-301", 30, 2}, // y
      Decimal128{"2809", 32, 3},
      false); // expected

  // -ve and max precision
  SubtractAndVerify<true>(
      Decimal128{"-09999999999999999999999999999999000000", 38, 5}, // x
      Decimal128{"-100", 38, 7}, // y
      Decimal128{"-99999999999999999999999999999989999990", 38, 6},
      false); // expected
}

TEST_F(TestDecimalSql, SubtractDenyPrecisionLoss) {
  // fast-path with strict precision requirements
  SubtractAndVerify<false>(
      Decimal128{"201", 30, 3}, // x
      Decimal128{"301", 30, 3}, // y
      Decimal128{"-100", 31, 3},
      false); // expected

  // Both -ve with strict precision
  SubtractAndVerify<false>(
      Decimal128{"-201", 30, 3}, // x
      Decimal128{"-301", 30, 2}, // y
      Decimal128{"2809", 32, 3},
      false); // expected

  // max precision with strict precision - update to expect overflow
  SubtractAndVerify<false>(
      Decimal128{"09999999999999999999999999999999000000", 38, 5}, // x
      Decimal128{"100", 38, 7}, // y
      Decimal128{"0", 38, 7}, // expected result is 0 with overflow
      true); // overflow expected in deny_precision_loss mode

  // -ve and max precision with strict precision - update to expect overflow
  SubtractAndVerify<false>(
      Decimal128{"-09999999999999999999999999999999000000", 38, 5}, // x
      Decimal128{"-100", 38, 7}, // y
      Decimal128{"0", 38, 7}, // expected result is 0 with overflow
      true); // overflow expected in deny_precision_loss mode
}

TEST_F(TestDecimalSql, Multiply) {
  // fast-path : out_precision < 38
  MultiplyAndVerifyAllSign<true>(
      Decimal128{"201", 10, 3}, // x
      Decimal128{"301", 10, 2}, // y
      Decimal128{"60501", 21, 5}, // expected
      false); // overflow

  // right 0
  MultiplyAndVerify<true>(
      Decimal128{"201", 20, 3}, // x
      Decimal128{"0", 20, 2}, // y
      Decimal128{"0", 38, 5}, // expected
      false); // overflow

  // left 0
  MultiplyAndVerify<true>(
      Decimal128{"0", 20, 3}, // x
      Decimal128{"301", 20, 2}, // y
      Decimal128{"0", 38, 5}, // expected
      false); // overflow

  // out_precision == 38, small input values, no trimming of scale (scale <= 6
  // doesn't get trimmed).
  MultiplyAndVerify<true>(
      Decimal128{"201", 20, 3}, // x
      Decimal128{"301", 20, 2}, // y
      Decimal128{"60501", 38, 5}, // expected
      false); // overflow

  // out_precision == 38, large values, no trimming of scale (scale <= 6 doesn't
  // get trimmed).
  MultiplyAndVerifyAllSign<true>(
      Decimal128{"201", 20, 3}, // x
      Decimal128{kThirtyFive9s, 35, 2}, // y
      Decimal128{"20099999999999999999999999999999999799", 38, 5}, // expected
      false); // overflow

  // out_precision == 38, very large values, no trimming of scale (scale <= 6
  // doesn't get trimmed). overflow expected.
  MultiplyAndVerifyAllSign<true>(
      Decimal128{"201", 20, 3}, // x
      Decimal128{kThirtySix9s, 35, 2}, // y
      Decimal128{"0", 38, 5}, // expected
      true); // overflow

  MultiplyAndVerifyAllSign<true>(
      Decimal128{"201", 20, 3}, // x
      Decimal128{kThirtyEight9s, 35, 2}, // y
      Decimal128{"0", 38, 5}, // expected
      true); // overflow

  // out_precision == 38, small input values, trimming of scale.
  MultiplyAndVerifyAllSign<true>(
      Decimal128{"201", 20, 5}, // x
      Decimal128{"301", 20, 5}, // y
      Decimal128{"61", 38, 7}, // expected
      false); // overflow

  // out_precision == 38, large values, trimming of scale.
  MultiplyAndVerifyAllSign<true>(
      Decimal128{"201", 20, 5}, // x
      Decimal128{kThirtyFive9s, 35, 5}, // y
      Decimal128{"2010000000000000000000000000000000", 38, 6}, // expected
      false); // overflow

  // out_precision == 38, very large values, trimming of scale (requires convert
  // to 256).
  MultiplyAndVerifyAllSign<true>(
      Decimal128{kThirtyFive9s, 38, 20}, // x
      Decimal128{kThirtySix9s, 38, 20}, // y
      Decimal128{"9999999999999999999999999999999999890", 38, 6}, // expected
      false); // overflow

  // out_precision == 38, very large values, trimming of scale (requires convert
  // to 256). should cause overflow.
  MultiplyAndVerifyAllSign<true>(
      Decimal128{kThirtyFive9s, 38, 4}, // x
      Decimal128{kThirtySix9s, 38, 4}, // y
      Decimal128{"0", 38, 6}, // expected
      true); // overflow

  // corner cases.
  MultiplyAndVerifyAllSign<true>(
      Decimal128{UINT64_MAX, 38, 4}, // x
      Decimal128{UINT64_MAX, 38, 4}, // y
      Decimal128{"3402823669209384634264811192843491082", 38, 6}, // expected
      false); // overflow

  MultiplyAndVerifyAllSign<true>(
      Decimal128{UINT64_MAX, 38, 4}, // x
      Decimal128{INT64_MAX, 38, 4}, // y
      Decimal128{"1701411834604692317040171876053197783", 38, 6}, // expected
      false); // overflow

  MultiplyAndVerifyAllSign<true>(
      Decimal128{"201", 38, 38}, // x
      Decimal128{"301", 38, 38}, // y
      Decimal128{"0", 38, 37}, // expected
      false); // overflow

  MultiplyAndVerifyAllSign<true>(
      Decimal128{UINT64_MAX, 38, 38}, // x
      Decimal128{UINT64_MAX, 38, 38}, // y
      Decimal128{"0", 38, 37}, // expected
      false); // overflow

  MultiplyAndVerifyAllSign<true>(
      Decimal128{kThirtyFive9s, 38, 38}, // x
      Decimal128{kThirtySix9s, 38, 38}, // y
      Decimal128{"100000000000000000000000000000000", 38, 37}, // expected
      false); // overflow
}

TEST_F(TestDecimalSql, MultiplyDenyPrecisionLoss) {
  // Test with zero - this should never overflow
  MultiplyAndVerify<false>(
      Decimal128{"201", 20, 3}, // x
      Decimal128{"0", 20, 2}, // y
      Decimal128{"0", 38, 5}, // expected
      false); // overflow

  // Simple multiplication with small values that won't cause overflow
  MultiplyAndVerifyAllSign<false>(
      Decimal128{"201", 5, 2}, // x (smaller precision to avoid overflow)
      Decimal128{"301", 5, 2}, // y (smaller precision to avoid overflow)
      Decimal128{"60501", 11, 4}, // expected
      false); // overflow

  // Simple test with small values that might not require precision loss
  MultiplyAndVerify<false>(
      Decimal128{"10", 3, 0}, // x
      Decimal128{"20", 3, 0}, // y
      Decimal128{"200", 7, 0}, // expected
      false); // overflow
}

TEST_F(TestDecimalSql, Divide) {
  DivideAndVerifyAllSign<true>(
      Decimal128{"201", 10, 3}, // x
      Decimal128{"301", 10, 2}, // y
      Decimal128{"6677740863787", 23, 14}, // expected
      false); // overflow

  DivideAndVerifyAllSign<true>(
      Decimal128{"201", 20, 3}, // x
      Decimal128{"301", 20, 2}, // y
      Decimal128{"667774086378737542", 38, 19}, // expected
      false); // overflow

  DivideAndVerifyAllSign<true>(
      Decimal128{"201", 20, 3}, // x
      Decimal128{kThirtyFive9s, 35, 2}, // y
      Decimal128{"0", 38, 19}, // expected
      false); // overflow

  DivideAndVerifyAllSign<true>(
      Decimal128{kThirtyFive9s, 35, 6}, // x
      Decimal128{"201", 20, 3}, // y
      Decimal128{"497512437810945273631840796019900493", 38, 6}, // expected
      false); // overflow

  DivideAndVerifyAllSign<true>(
      Decimal128{kThirtyEight9s, 38, 20}, // x
      Decimal128{kThirtyFive9s, 38, 20}, // y
      Decimal128{"1000000000", 38, 6}, // expected
      false); // overflow

  DivideAndVerifyAllSign<true>(
      Decimal128{"31939128063561476055", 38, 8}, // x
      Decimal128{"10000", 20, 0}, // y
      Decimal128{"3193912806356148", 38, 8}, // expected
      false);

  // Corner cases
  DivideAndVerifyAllSign<true>(
      Decimal128{UINT64_MAX, 38, 4}, // x
      Decimal128{UINT64_MAX, 38, 4}, // y
      Decimal128{"1000000", 38, 6}, // expected
      false); // overflow

  DivideAndVerifyAllSign<true>(
      Decimal128{UINT64_MAX, 38, 4}, // x
      Decimal128{INT64_MAX, 38, 4}, // y
      Decimal128{"2000000", 38, 6}, // expected
      false); // overflow

  DivideAndVerifyAllSign<true>(
      Decimal128{UINT64_MAX, 19, 5}, // x
      Decimal128{INT64_MAX, 19, 5}, // y
      Decimal128{"20000000000000000001", 38, 19}, // expected
      false); // overflow

  DivideAndVerifyAllSign<true>(
      Decimal128{kThirtyFive9s, 38, 37}, // x
      Decimal128{kThirtyFive9s, 38, 38}, // y
      Decimal128{"10000000", 38, 6}, // expected
      false); // overflow

  // overflow
  DivideAndVerifyAllSign<true>(
      Decimal128{kThirtyEight9s, 38, 6}, // x
      Decimal128{"201", 20, 3}, // y
      Decimal128{"0", 38, 6}, // expected
      true);
}

TEST_F(TestDecimalSql, DivideDenyPrecisionLoss) {
  // Test basic division with strict precision requirements
  DivideAndVerifyAllSign<false>(
      Decimal128{"201", 10, 3}, // x
      Decimal128{"301", 10, 2}, // y
      Decimal128{"6677740863787", 23, 14}, // expected
      false); // overflow

  // Test DECIMAL(20,2)/DECIMAL(17,3) case (diff > 0)
  // Should return DECIMAL(38,18) in deny_precision_loss mode
  DivideAndVerify<false>(
      Decimal128{"500", 20, 2}, // x
      Decimal128{"1000", 17, 3}, // y
      Decimal128{
          "5000000000000000000", 38, 18}, // expected with 18 decimal places
      false); // overflow

  // Test DECIMAL(20,2)/DECIMAL(7,3) case (diff < 0)
  // Should return DECIMAL(31,10) in deny_precision_loss mode
  DivideAndVerify<false>(
      Decimal128{"500", 20, 2}, // x
      Decimal128{"1000", 7, 3}, // y
      Decimal128{"50000000000", 31, 10}, // expected with 10 decimal places
      false); // overflow

  // Test division with larger values and strict precision
  DivideAndVerifyAllSign<false>(
      Decimal128{kThirtyEight9s, 38, 20}, // x
      Decimal128{kThirtyFive9s, 38, 20}, // y
      Decimal128{"1000000000000000000000", 38, 18}, // expected
      false); // overflow

  // Test corner cases with strict precision
  DivideAndVerifyAllSign<false>(
      Decimal128{UINT64_MAX, 38, 4}, // x
      Decimal128{UINT64_MAX, 38, 4}, // y
      Decimal128{"1000000000000000000", 38, 18}, // expected
      false); // overflow
}
