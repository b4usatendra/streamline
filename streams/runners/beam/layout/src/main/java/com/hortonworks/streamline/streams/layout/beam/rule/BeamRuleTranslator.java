package com.hortonworks.streamline.streams.layout.beam.rule;

import com.google.common.base.*;
import com.hortonworks.registries.common.*;
import com.hortonworks.streamline.streams.*;
import com.hortonworks.streamline.streams.layout.beam.rule.expression.*;
import com.hortonworks.streamline.streams.layout.component.rule.expression.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.*;

import java.util.*;

/**
 * Created by Satendra Sahu on 12/26/18
 */
public class BeamRuleTranslator extends ExpressionRuntime {

    private static final Logger LOG = LoggerFactory.getLogger(BeamRuleTranslator.class);
    private PCollection<StreamlineEvent> pCollection;
    private String beamComponentId;

    public BeamRuleTranslator(String beamComponentId, PCollection<StreamlineEvent> inputCollection, Condition condition) {
        this(beamComponentId, inputCollection, condition, null);
    }

    public BeamRuleTranslator(String beamComponentId, PCollection<StreamlineEvent> inputCollection, Condition condition, Projection projection) {
        this(beamComponentId, inputCollection, condition, projection, null, null);
    }

    public BeamRuleTranslator(String beamComponentId, PCollection<StreamlineEvent> inputCollection, Condition condition, Projection projection, GroupBy groupBy, Having having) {
        super(condition, projection, groupBy, having);
        this.beamComponentId = beamComponentId;
        this.pCollection = inputCollection;
        handleProjection();
        handleFilter();
        handleGroupByHaving();
    }

    private void handleProjection() {
        if (projection != null) {
            BeamExpressionTranslator projectionTranslator = new BeamExpressionTranslator(pCollection);
            for (Expression expr : projection.getExpressions()) {
                expr.accept(projectionTranslator);
            }
            pCollection = projectionTranslator.getpCollection().apply("filterFieldsParDo", BeamUtilFunctions.filterFields(beamComponentId, projectionTranslator.getProjectedFields()));
        }
    }

    private void handleFilter() {
        if (condition != null) {
            BeamExpressionTranslator conditionTranslator = new BeamExpressionTranslator(pCollection);
            condition.getExpression().accept(conditionTranslator);
            LOG.debug("Built expression [{}] for filter condition [{}]", expression, condition);
        }
    }

    private void handleGroupByHaving() {
        if (groupBy != null) {
            List<String> groupByExpressions = new ArrayList<>();
            for (Expression expr : groupBy.getExpressions()) {
                BeamExpressionTranslator groupByTranslator = new BeamExpressionTranslator(pCollection);
                expr.accept(groupByTranslator);
            }
            groupByExpression = Joiner.on(",").join(groupByExpressions);
            if (having != null) {
                BeamExpressionTranslator havingTranslator = new BeamExpressionTranslator(pCollection);
                having.getExpression().accept(havingTranslator);
                LOG.debug("Built expression [{}] for having [{}]", havingExpression, having);
            }
        }
    }

    @Override
    public String asString() {
        return expression;
    }


    public PCollection<StreamlineEvent> getpCollection(){
        return pCollection;
    }

    @Override
    protected String getType(Schema.Field field) {
        switch (field.getType()) {
            case NESTED:
            case ARRAY:
                return "ANY";
            case STRING:
                return "VARCHAR";
            case LONG:
                return "BIGINT";
            default:
                return super.getType(field);
        }
    }

}
