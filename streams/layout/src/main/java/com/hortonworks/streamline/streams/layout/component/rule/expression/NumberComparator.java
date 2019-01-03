package com.hortonworks.streamline.streams.layout.component.rule.expression;

import java.io.*;

/**
 * Created by Satendra Sahu on 12/26/18
 */
public class NumberComparator<T extends Comparable<T>> implements Serializable {
    public T firstValue;
    private T maxValue;

    public NumberComparator(T firstValue) {
        this.firstValue = firstValue;
    }

    public int maxValue(T secondValue) {
        int cmp = firstValue.compareTo(secondValue);
        if (cmp == 0)
            maxValue = firstValue;
        else if (cmp < 0)
            maxValue = secondValue;
        else if (cmp > 0)
            maxValue = firstValue;

        return cmp;
    }

    public T getMaxValue() {
        return maxValue;
    }
}
