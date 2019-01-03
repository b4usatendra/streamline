package com.hortonworks.streamline.streams.layout.beam.rule.expression.aggregation;

/**
 * Created by Satendra Sahu on 12/31/18
 */
public enum FunctionEnum {
    MAX(MaxFunction.class.getCanonicalName());


    private String clazz;

    public static FunctionEnum enumOf(String func) {

        for (FunctionEnum function : FunctionEnum.values()) {
            if (func.equals(function.name())) {
                return function;
            }
        }
        return null;

    }

    FunctionEnum(String clazz) {
        this.clazz = clazz;
    }

    public String getClazz() {
        return clazz;
    }
}
