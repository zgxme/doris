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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * ScalarFunction 'json_object'. This class is generated by GenerateFunction.
 * Builds a JSON object out of a variadic argument list.
 * By convention, the argument list consists of alternating keys and values.
 * Key arguments are coerced to text; value arguments are converted as per to_json or to_jsonb.
 */
public class JsonObject extends ScalarFunction implements CustomSignature, AlwaysNotNullable {

    /**
     * constructor with 0 or more arguments.
     */
    public JsonObject(Expression... varArgs) {
        super("json_object", ExpressionUtils.mergeArguments(varArgs));
    }

    @Override
    public FunctionSignature customSignature() {
        List<DataType> arguments = new ArrayList<>();
        for (int i = 0; i < arity(); i++) {
            if (i % 2 == 1) {
                arguments.add(getArgumentType(i));
            } else {
                arguments.add(VarcharType.SYSTEM_DEFAULT);
            }
        }
        return FunctionSignature.of(JsonType.INSTANCE, arguments);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if ((arity() & 1) == 1) {
            throw new AnalysisException("json_object can't be odd parameters, need even parameters: " + this.toSql());
        }
        for (int i = 0; i < arity(); i++) {
            if ((i & 1) == 0 && getArgumentType(i).isNullType()) {
                throw new AnalysisException("json_object key can't be NULL: " + this.toSql());
            }
        }
    }

    /**
     * withChildren.
     */
    @Override
    public JsonObject withChildren(List<Expression> children) {
        return new JsonObject(children.toArray(new Expression[0]));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitJsonObject(this, context);
    }
}
