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

package org.apache.doris.nereids.pattern;

import org.apache.doris.nereids.rules.expression.ExpressionMatchingContext;
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatchRule;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.SuperClassId;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.expressions.Expression;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** ExpressionPatternMapping */
public class ExpressionPatternRules extends TypeMappings<Expression, ExpressionPatternMatchRule> {
    private static final Logger LOG = LogManager.getLogger(ExpressionPatternRules.class);
    public final List<ExpressionPatternMatchRule> allRules;
    private BitSet typePatternIds;

    /** ExpressionPatternRules */
    public ExpressionPatternRules(List<ExpressionPatternMatchRule> typeMappings) {
        super(typeMappings);

        BitSet typePatternIds = new BitSet();
        for (ExpressionPatternMatchRule typeMapping : typeMappings) {
            Class<? extends Expression> topType = typeMapping.getType();
            typePatternIds.set(SuperClassId.getClassId(topType));
        }
        this.typePatternIds = typePatternIds;
        this.allRules = typeMappings;
    }

    @Override
    protected Set<Class<? extends Expression>> getChildrenClasses(Class<? extends Expression> clazz) {
        return org.apache.doris.nereids.pattern.GeneratedExpressionRelations.CHILDREN_CLASS_MAP.get(clazz);
    }

    public boolean hasCurrentAndChildrenRules(TreeNode<?> treeNode) {
        BitSet classTypes = treeNode.getAllChildrenTypes();
        if (!typePatternIds.intersects(classTypes)) {
            return false;
        }
        return true;
    }

    /** matchesAndApply */
    public Optional<Expression> matchesAndApply(Expression expr, ExpressionRewriteContext context, Expression parent) {
        List<ExpressionPatternMatchRule> rules = getSingleMapping(expr.getClass());
        ExpressionMatchingContext<Expression> matchingContext
                = new ExpressionMatchingContext<>(expr, parent, context);
        BitSet disableRules = context.cascadesContext.getConnectContext().getSessionVariable()
                .getDisableNereidsExpressionRules();
        switch (rules.size()) {
            case 0: {
                for (ExpressionPatternMatchRule multiMatchRule : multiMappings) {
                    if (!disableRules.get(multiMatchRule.getExpressionRuleType().type())
                            && multiMatchRule.matchesTypeAndPredicates(matchingContext)) {
                        Expression newExpr = multiMatchRule.apply(matchingContext);
                        if (!newExpr.equals(expr)) {
                            if (context.cascadesContext.isEnableExprTrace()) {
                                traceExprChanged(multiMatchRule, expr, newExpr, true);
                            }
                            return Optional.of(newExpr);
                        } else if (context.cascadesContext.isEnableExprTrace()) {
                            traceExprChanged(multiMatchRule, expr, newExpr, false);
                        }
                    }
                }
                return Optional.empty();
            }
            case 1: {
                ExpressionPatternMatchRule rule = rules.get(0);
                if (!disableRules.get(rule.getExpressionRuleType().type())
                        && rule.matchesPredicates(matchingContext)) {
                    Expression newExpr = rule.apply(matchingContext);
                    if (!newExpr.equals(expr)) {
                        if (context.cascadesContext.isEnableExprTrace()) {
                            traceExprChanged(rule, expr, newExpr, true);
                        }
                        return Optional.of(newExpr);
                    } else if (context.cascadesContext.isEnableExprTrace()) {
                        traceExprChanged(rule, expr, newExpr, false);
                    }
                }
                return Optional.empty();
            }
            default: {
                for (ExpressionPatternMatchRule rule : rules) {
                    if (!disableRules.get(rule.getExpressionRuleType().type())
                            && rule.matchesPredicates(matchingContext)) {
                        Expression newExpr = rule.apply(matchingContext);
                        if (!expr.equals(newExpr)) {
                            if (context.cascadesContext.isEnableExprTrace()) {
                                traceExprChanged(rule, expr, newExpr, true);
                            }
                            return Optional.of(newExpr);
                        } else if (context.cascadesContext.isEnableExprTrace()) {
                            traceExprChanged(rule, expr, newExpr, false);
                        }
                    }
                }
                return Optional.empty();
            }
        }
    }

    private static void traceExprChanged(
            ExpressionPatternMatchRule rule, Expression expr, Expression newExpr, boolean changed) {
        try {
            Field[] declaredFields = (rule.matchingAction).getClass().getDeclaredFields();
            Class<?> ruleClass;
            if (declaredFields.length == 0) {
                ruleClass = rule.matchingAction.getClass();
            } else {
                Field field = declaredFields[0];
                field.setAccessible(true);
                ruleClass = field.get(rule.matchingAction).getClass();
            }
            if (changed) {
                LOG.info("RULE: " + ruleClass + "\nbefore: " + expr + "\nafter: " + newExpr);
            } else {
                LOG.info("RULE: " + ruleClass + " not changed\nbefore: " + expr);
            }
        } catch (Throwable t) {
            LOG.error(t.getMessage(), t);
        }
    }
}
