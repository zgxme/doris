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

#pragma once

#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <ostream>
#include <string>

#include "common/status.h"
#include "data_type_number_serde.h"
#include "vec/columns/column.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"

namespace doris::vectorized {
class Arena;

template <PrimitiveType T = PrimitiveType::TYPE_DATE>
class DataTypeDate64SerDe : public DataTypeNumberSerDe<T> {
public:
    DataTypeDate64SerDe(int nesting_level = 1) : DataTypeNumberSerDe<T>(nesting_level) {};

    Status serialize_one_cell_to_json(
            const IColumn& column, int64_t row_num, BufferWritable& bw,
            typename DataTypeNumberSerDe<T>::FormatOptions& options) const override;
    Status serialize_column_to_json(
            const IColumn& column, int64_t start_idx, int64_t end_idx, BufferWritable& bw,
            typename DataTypeNumberSerDe<T>::FormatOptions& options) const override;
    Status deserialize_one_cell_from_json(
            IColumn& column, Slice& slice,
            const typename DataTypeNumberSerDe<T>::FormatOptions& options) const override;

    Status deserialize_column_from_json_vector(
            IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
            const typename DataTypeNumberSerDe<T>::FormatOptions& options) const override;

    Status write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                 arrow::ArrayBuilder* array_builder, int64_t start, int64_t end,
                                 const cctz::time_zone& ctz) const override;
    Status read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array, int64_t start,
                                  int64_t end, const cctz::time_zone& ctz) const override;
    Status write_column_to_mysql(
            const IColumn& column, MysqlRowBuffer<true>& row_buffer, int64_t row_idx,
            bool col_const,
            const typename DataTypeNumberSerDe<T>::FormatOptions& options) const override;
    Status write_column_to_mysql(
            const IColumn& column, MysqlRowBuffer<false>& row_buffer, int64_t row_idx,
            bool col_const,
            const typename DataTypeNumberSerDe<T>::FormatOptions& options) const override;

    Status write_column_to_orc(const std::string& timezone, const IColumn& column,
                               const NullMap* null_map, orc::ColumnVectorBatch* orc_col_batch,
                               int64_t start, int64_t end,
                               std::vector<StringRef>& buffer_list) const override;

protected:
    template <bool is_date>
    Status _read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array, int64_t start,
                                   int64_t end, const cctz::time_zone& ctz) const;

private:
    template <bool is_binary_format>
    Status _write_column_to_mysql(
            const IColumn& column, MysqlRowBuffer<is_binary_format>& result, int64_t row_idx,
            bool col_const, const typename DataTypeNumberSerDe<T>::FormatOptions& options) const;
};

class DataTypeDateTimeSerDe : public DataTypeDate64SerDe<PrimitiveType::TYPE_DATETIME> {
public:
    DataTypeDateTimeSerDe(int nesting_level = 1)
            : DataTypeDate64SerDe<PrimitiveType::TYPE_DATETIME>(nesting_level) {};

    Status serialize_column_to_json(const IColumn& column, int64_t start_idx, int64_t end_idx,
                                    BufferWritable& bw, FormatOptions& options) const override;

    Status serialize_one_cell_to_json(const IColumn& column, int64_t row_num, BufferWritable& bw,
                                      FormatOptions& options) const override;

    Status deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                          const FormatOptions& options) const override;
    Status deserialize_column_from_json_vector(IColumn& column, std::vector<Slice>& slices,
                                               uint64_t* num_deserialized,
                                               const FormatOptions& options) const override;
    Status read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array, int64_t start,
                                  int64_t end, const cctz::time_zone& ctz) const override;
};
} // namespace doris::vectorized
