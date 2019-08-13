package com.hortonworks.streamline.streams.layout.beam.rule.expression.aggregation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.streamline.streams.StreamlineEvent;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.AccumulatingCombineFn.Accumulator;

/**
 * Created by Satendra Sahu on 12/31/18
 */
public class AggregationFunImpl {

  public static final String AGGREGATED_FUNCTION_KEY = "aggregatedValue";

  private AggregationFunImpl() {
  }

  public static <event extends StreamlineEvent> Combine.Globally<event, StreamlineEvent> globally(
      String fieldName, String function) {
    return Combine.globally(AggregationFunImpl.of(fieldName, function));
  }

  public static <event extends StreamlineEvent>
  Combine.AccumulatingCombineFn<event, Aggregator<event>, StreamlineEvent> of(String fieldName,
      String function) {
    return new AggregationFn<>(fieldName, function);
  }


  private static class AggregationFn<event extends StreamlineEvent>
      extends Combine.AccumulatingCombineFn<event, Aggregator<event>, StreamlineEvent> {

    /**
     * Constructs a combining function that computes the mean over a collection of values of type
     * {@code N}.
     */

    private String fieldName;
    private String function;

    public AggregationFn(String fieldName, String function) {
      this.fieldName = fieldName;
      this.function = function;
    }

    @Override
    public Aggregator<event> createAccumulator() {
      return new Aggregator<>(this.fieldName, this.function);
    }

    @Override
    public Coder<Aggregator<event>> getAccumulatorCoder(
        CoderRegistry registry, Coder<event> inputCoder) {
      return new AggregatorCoder<>();
    }
  }


  /*
   * Accumulator class
   * */

  static class Aggregator<event extends StreamlineEvent> implements
      Accumulator<event, Aggregator<event>, StreamlineEvent> {

    BeamAggregationFunction beamAggregationFunction;

    public Aggregator(String fieldName, String function) {
      super();

      try {
        Class<BeamAggregationFunction> clazz = (Class<BeamAggregationFunction>) Class
            .forName(function);
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
     */
    @Override
    public void addInput(event streamlineEvent) {
      beamAggregationFunction.evaluate(streamlineEvent);
    }

    /**
     * Adds the input values represented by the given accumulator into this accumulator.
     */
    @Override
    public void mergeAccumulator(Aggregator<event> other) {
      beamAggregationFunction.compare(other.beamAggregationFunction.getEvent());
    }

    /**
     * Returns the output value that is the result of combining all the input values represented by
     * this accumulator.
     */
    @Override
    public StreamlineEvent extractOutput() {
      return beamAggregationFunction.getEvent()
          .addFieldAndValue(AGGREGATED_FUNCTION_KEY, beamAggregationFunction.getValue());
    }

    @Override
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
        value = "FE_FLOATING_POINT_EQUALITY",
        justification = "Comparing doubles directly since equals method is only used in coder test."
    )
    public boolean equals(Object other) {
      if (!(other instanceof Aggregator)) {
        return false;
      }
      @SuppressWarnings("unchecked")
      Aggregator<?> otherCountSum = (Aggregator<?>) other;
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

  static class AggregatorCoder<event extends StreamlineEvent> extends AtomicCoder<Aggregator<event>> {

    private static final StringUtf8Coder STRING_UTF_8_CODER = StringUtf8Coder.of();
    private static final ByteArrayCoder BYTE_ARRAY_CODER = ByteArrayCoder.of();
    private static final ObjectMapper mapper = new ObjectMapper();


    @Override
    public void encode(Aggregator<event> value, OutputStream outStream)
        throws CoderException, IOException {
      BYTE_ARRAY_CODER.encode(mapper.writeValueAsBytes(value.beamAggregationFunction), outStream);

    }

    @Override
    public Aggregator<event> decode(InputStream inStream)
        throws CoderException, IOException {

      return new Aggregator<>(STRING_UTF_8_CODER.decode(inStream),
          STRING_UTF_8_CODER.decode(inStream));
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      STRING_UTF_8_CODER.verifyDeterministic();
    }
  }

}