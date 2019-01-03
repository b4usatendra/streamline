package com.hortonworks.streamline.streams.layout.beam.rule.expression.aggregation;

import com.fasterxml.jackson.databind.*;
import com.hortonworks.streamline.streams.*;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.Combine.AccumulatingCombineFn.*;

import java.io.*;
import java.lang.reflect.*;

/**
 * Created by Satendra Sahu on 12/31/18
 */
public class AggregationFunImpl {

    public static final String AGGREGATED_FUNCTION_KEY = "aggregatedValue";

    private AggregationFunImpl() {
    }

    public static <event extends StreamlineEvent> Combine.Globally<event, StreamlineEvent> globally(String fieldName, String function) {
        return Combine.globally(AggregationFunImpl.of(fieldName, function));
    }

    public static <event extends StreamlineEvent>
    Combine.AccumulatingCombineFn<event, FindMax<event>, StreamlineEvent> of(String fieldName, String function) {
        return new AggregationFn<>(fieldName, function);
    }


    private static class AggregationFn<event extends StreamlineEvent>
            extends Combine.AccumulatingCombineFn<event, FindMax<event>, StreamlineEvent> {
        /**
         * Constructs a combining function that computes the mean over
         * a collection of values of type {@code N}.
         */

        private String fieldName;
        private String function;

        public AggregationFn(String fieldName, String function) {
            this.fieldName = fieldName;
            this.function = function;
        }

        @Override
        public FindMax<event> createAccumulator() {
            return new FindMax<>(this.fieldName, this.function);
        }

        @Override
        public Coder<FindMax<event>> getAccumulatorCoder(
                CoderRegistry registry, Coder<event> inputCoder) {
            return new FindMaxCoder<>();
        }
    }


    /*
     * Accumulator class
     * */

    static class FindMax<event extends StreamlineEvent> implements Accumulator<event, FindMax<event>, StreamlineEvent> {

        BeamAggregationFunction beamAggregationFunction;

        public FindMax(String fieldName, String function) {
            super();

            try {
                Class<BeamAggregationFunction> clazz = (Class<BeamAggregationFunction>) Class.forName(function);
                Constructor<BeamAggregationFunction> constructor = clazz.getConstructor(String.class);
                beamAggregationFunction = constructor.newInstance(fieldName);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
        }

        /**
         * Adds the given input value to this accumulator, modifying this accumulator.
         *
         * @param streamlineEvent
         */
        @Override
        public void addInput(event streamlineEvent) {
            beamAggregationFunction.evaluate(streamlineEvent);
        }

        /**
         * Adds the input values represented by the given accumulator into this accumulator.
         *
         * @param other
         */
        @Override
        public void mergeAccumulator(FindMax<event> other) {
            beamAggregationFunction.compare(other.beamAggregationFunction.getEvent());
        }

        /**
         * Returns the output value that is the result of combining all the input values represented
         * by this accumulator.
         */
        @Override
        public StreamlineEvent extractOutput() {
            return beamAggregationFunction.getEvent().addFieldAndValue(AGGREGATED_FUNCTION_KEY, beamAggregationFunction.getValue());
        }

        @Override
        @edu.umd.cs.findbugs.annotations.SuppressWarnings(
                value = "FE_FLOATING_POINT_EQUALITY",
                justification = "Comparing doubles directly since equals method is only used in coder test."
        )
        public boolean equals(Object other) {
            if (!(other instanceof FindMax)) {
                return false;
            }
            @SuppressWarnings("unchecked")
            FindMax<?> otherCountSum = (FindMax<?>) other;
            return (beamAggregationFunction.equals(otherCountSum.beamAggregationFunction));

        }

        @Override
        public int hashCode() {
            return beamAggregationFunction.hashCode();
        }

        @Override
        public String toString() {
            return beamAggregationFunction.toString();
        }
    }

    static class FindMaxCoder<event extends StreamlineEvent> extends AtomicCoder<FindMax<event>> {
        private static final StringUtf8Coder STRING_UTF_8_CODER = StringUtf8Coder.of();
        private static final ByteArrayCoder BYTE_ARRAY_CODER = ByteArrayCoder.of();
        private static final ObjectMapper mapper = new ObjectMapper();


        @Override
        public void encode(FindMax<event> value, OutputStream outStream)
                throws CoderException, IOException {
            BYTE_ARRAY_CODER.encode(mapper.writeValueAsBytes(value.beamAggregationFunction), outStream);

        }

        @Override
        public FindMax<event> decode(InputStream inStream)
                throws CoderException, IOException {

            return new FindMax<>(STRING_UTF_8_CODER.decode(inStream),
                    STRING_UTF_8_CODER.decode(inStream));
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {
            STRING_UTF_8_CODER.verifyDeterministic();
        }
    }

}