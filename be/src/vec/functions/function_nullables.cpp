// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

class FunctionNullable : public IFunction {
public:
    static constexpr auto name = "nullable";

    static FunctionPtr create() { return std::make_shared<FunctionNullable>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(arguments[0]);
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    // trans nullable column to non-nullable column. If argument is already non-nullable, raise error.
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        ColumnPtr& col = block.get_by_position(arguments[0]).column;
        if (const auto* col_null = check_and_get_column<ColumnNullable>(col.get());
            col_null == nullptr) {
            // not null
            block.replace_by_position(
                    result, ColumnNullable::create(col, ColumnBool::create(input_rows_count, 0)));
        } else { // column is ColumnNullable
            block.replace_by_position(result, col->clone_resized(input_rows_count));
        }
        return Status::OK();
    }
};

class FunctionNonNullable : public IFunction {
public:
    static constexpr auto name = "non_nullable";

    static FunctionPtr create() { return std::make_shared<FunctionNonNullable>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return remove_nullable(arguments[0]);
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    // trans nullable column to non-nullable column. If argument is already non-nullable, raise error.
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto& data = block.get_by_position(arguments[0]);
        if (const auto* col_null = check_and_get_column<ColumnNullable>(data.column.get());
            col_null == nullptr) // raise error if input is not nullable.
        {
            return Status::InvalidArgument(
                    "Try to use originally non-nullable column {} in nullable's non-nullable "
                    "convertion.",
                    data.column->get_name());
        } else { // column is ColumnNullable
            if (col_null->has_null()) [[unlikely]] {
                return Status::InvalidArgument(
                        "There's NULL value in column {} which is illegal for non_nullable",
                        data.name);
            }
            const ColumnPtr& nest_col = col_null->get_nested_column_ptr();
            block.replace_by_position(result, nest_col->clone_resized(nest_col->size()));
        }
        return Status::OK();
    }
};

void register_function_nullables(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionNullable>();
    factory.register_function<FunctionNonNullable>();
}

} // namespace doris::vectorized
