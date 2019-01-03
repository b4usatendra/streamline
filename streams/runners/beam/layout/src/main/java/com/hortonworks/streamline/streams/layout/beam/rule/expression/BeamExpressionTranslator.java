package com.hortonworks.streamline.streams.layout.beam.rule.expression;

import com.hortonworks.registries.common.*;
import com.hortonworks.streamline.streams.*;
import com.hortonworks.streamline.streams.layout.beam.rule.expression.aggregation.*;
import com.hortonworks.streamline.streams.layout.component.rule.expression.*;
import org.apache.beam.sdk.values.*;

import java.util.*;

/**
 * Created by Satendra Sahu on 12/26/18
 */
public class BeamExpressionTranslator implements ExpressionVisitor {

    private PCollection<StreamlineEvent> pCollection;
    private Set<FieldExpression> projectedFields = new HashSet<>();

    public BeamExpressionTranslator(PCollection<StreamlineEvent> inputCollection) {
        this.pCollection = inputCollection;
    }

    @Override
    public void visit(BinaryExpression binaryExpression) {
        maybeParenthesize(binaryExpression, binaryExpression.getFirst());
        //getOperator(binaryExpression.getOperator());
        maybeParenthesize(binaryExpression, binaryExpression.getSecond());
    }

    @Override
    public void visit(FieldExpression fieldExpression) {
        projectedFields.add(fieldExpression);
    }

    @Override
    public void visit(ArrayFieldExpression arrayFieldExpression) {
        arrayFieldExpression.getExpression().accept(this);

    }

    @Override
    public void visit(MapFieldExpression mapFieldExpression) {
        mapFieldExpression.getExpression().accept(this);

    }

    @Override
    public void visit(AsExpression asExpression) {
        asExpression.getExpression().accept(this);
        applyAsFunction(asExpression.getAlias());
    }

    @Override
    public void visit(Literal literal) {
        literal.getValue();
    }

    @Override
    public void visit(FunctionExpression functionExpression) {
        //applyAggregationFunction(functionExpression);
        functionExpression.getFunction();

    }

    @Override
    public void visit(AggregateFunctionExpression aggregateFunctionExpression) {
        applyAggregationFunction(aggregateFunctionExpression);
        for (Expression expression : aggregateFunctionExpression.getOperands()) {
            expression.accept(this);
        }
    }

    private void maybeParenthesize(BinaryExpression parent, Expression child) {
        boolean paren = false;
        if (child instanceof BinaryExpression) {
            int childPrecedence = ((BinaryExpression) child).getOperator().getPrecedence();
            int parentPrecedence = parent.getOperator().getPrecedence();
            if (childPrecedence > parentPrecedence) {
                paren = true;
            }
        }
        if (paren) {
//            builder.append("(");
            child.accept(this);
            //          builder.append(")");
        } else {
            child.accept(this);
        }
    }


    private void applyAsFunction(String aliasName) {
        pCollection = pCollection.apply("aliasKeyParDo", BeamUtilFunctions.applyAlias(aliasName, AggregationFunImpl.AGGREGATED_FUNCTION_KEY));
        //TODO hard coded STRING as TYPE for alias field.
        new FieldExpression(Schema.Field.of(aliasName, Schema.Type.STRING)).accept(this);

    }

    private void applyAggregationFunction(FunctionExpression functionExpression) {
        String function = functionExpression.getFunction().getName();

        switch (function) {
            case "MAX":
                if (functionExpression.getOperands().size() == 1 && (functionExpression.getOperands().get(0) instanceof FieldExpression)) {
                    FieldExpression fieldExpression = (FieldExpression) functionExpression.getOperands().get(0);
                    Schema.Field field = fieldExpression.getValue();
                    pCollection = pCollection.apply("maxValueParDo",
                            BeamUtilFunctions.applyCombiningStrategy(field.getName(), function));
                }
                return;
            default:
                throw new UnsupportedOperationException(
                        String.format("Operator [%s] not supported. List of supported operators: %s",
                                function, Arrays.toString(Operator.values())));
        }
    }

    public Set<FieldExpression> getProjectedFields() {
        return projectedFields;
    }

    public PCollection<StreamlineEvent> getpCollection() {
        return pCollection;
    }
}
