/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.histogram.hdr;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.java.util.common.StringUtils;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.histogram.Histogram;
import io.druid.segment.ColumnSelectorFactory;
import org.HdrHistogram.DoubleHistogram;
import org.HdrHistogram.DoubleHistogramIterationValue;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.ArrayUtils;

import java.nio.ByteBuffer;
import java.util.*;

@JsonTypeName("hdrHistogram")
public class HdrHistogramAggregatorFactory extends AggregatorFactory
{
  /**
   * Set minimum highest trackable value of deserialized histograms to a number of milliseconds in an hour.
   * This value is purely an internal hint for HdrHistogram and DOES NOT cap the maximum recordable value for such
   * histogram.
   */
  public static final long MIN_BAR_FOR_HIGHEST_TO_LOWEST_VALUE_RATIO = 1000 * 60 * 60;

  private static final byte CACHE_TYPE_ID = 12;

  protected final String name;
  protected final String fieldName;
  protected final Long highestToLowestValueRatio;
  protected final Integer numberOfSignificantValueDigits;
  protected final Integer numBuckets;

  @JsonCreator
  public HdrHistogramAggregatorFactory(
          @JsonProperty("name") String name,
          @JsonProperty("fieldName") String fieldName,
          @JsonProperty("highestToLowestValueRatio") Long highestToLowestValueRatio,
          @JsonProperty("numberOfSignificantValueDigits") Integer numberOfSignificantValueDigits,
          @JsonProperty("numBuckets") Integer numBuckets)
  {
    this.name = name;
    this.fieldName = fieldName;
    this.numBuckets = numBuckets;
    this.highestToLowestValueRatio = highestToLowestValueRatio == null ? 2 : highestToLowestValueRatio;
    this.numberOfSignificantValueDigits = numberOfSignificantValueDigits == null ? 2 : numberOfSignificantValueDigits;
    Preconditions.checkArgument(this.numberOfSignificantValueDigits >= 0 && this.numberOfSignificantValueDigits <= 5,
            "numberOfSignificantValueDigits must be between 0 and 5");
    Preconditions.checkArgument(this.highestToLowestValueRatio > 0, "highestToLowestValueRatio must be greater than 0");
    Preconditions.checkArgument(this.numBuckets > 0, "numBuckets must be greater than 0");
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new HdrHistogramAggregator(
            highestToLowestValueRatio, numberOfSignificantValueDigits, metricFactory.makeFloatColumnSelector(fieldName));
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new HdrHistogramBufferAggregator(
        highestToLowestValueRatio, numberOfSignificantValueDigits, metricFactory.makeFloatColumnSelector(fieldName)
    );
  }

  @Override
  public Comparator getComparator()
  {
    return HdrHistogramAggregator.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return HdrHistogramAggregator.combineHistograms(lhs, rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new HdrHistogramFoldingAggregatorFactory(name, fieldName, highestToLowestValueRatio,
            numberOfSignificantValueDigits, numBuckets);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && other instanceof HdrHistogramAggregatorFactory) {
      HdrHistogramAggregatorFactory castedOther = (HdrHistogramAggregatorFactory) other;

      return new HdrHistogramFoldingAggregatorFactory(
          name,
          fieldName,
          Math.max(highestToLowestValueRatio, castedOther.highestToLowestValueRatio),
          Math.max(numberOfSignificantValueDigits, castedOther.numberOfSignificantValueDigits),
          Math.max(numBuckets, castedOther.numBuckets)
      );

    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(
        new HdrHistogramAggregatorFactory(
            name,
            fieldName,
            highestToLowestValueRatio,
            numberOfSignificantValueDigits,
            numBuckets)
    );
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof byte[]) {
      final ByteBuffer byteBuffer = ByteBuffer.wrap((byte[]) object);
      return DoubleHistogram.decodeFromByteBuffer(byteBuffer, MIN_BAR_FOR_HIGHEST_TO_LOWEST_VALUE_RATIO);
    } else if (object instanceof ByteBuffer) {
      return DoubleHistogram.decodeFromByteBuffer((ByteBuffer) object, MIN_BAR_FOR_HIGHEST_TO_LOWEST_VALUE_RATIO);
    } else if (object instanceof String) {
      final byte[] bytes = Base64.decodeBase64(StringUtils.toUtf8((String) object));
      final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      return DoubleHistogram.decodeFromByteBuffer(byteBuffer, MIN_BAR_FOR_HIGHEST_TO_LOWEST_VALUE_RATIO);
    } else {
      return object;
    }
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    final DoubleHistogram histogram = (DoubleHistogram) object;
    final double maxValue = histogram.getMaxValue();
    final double minValue = histogram.getMinValue();
    final DoubleHistogram.LinearBucketValues linearIterator =
            histogram.linearBucketValues((maxValue - minValue) / numBuckets);
    final List<Float> breaks = new ArrayList<>();
    final List<Double> counts = new ArrayList<>();
    for (DoubleHistogramIterationValue value : linearIterator) {
      final long currentCount = value.getCountAtValueIteratedTo();
      final double currentBreak = value.getValueIteratedTo();
      breaks.add((float) currentBreak);
      counts.add((double) currentCount);
    }
    final Float[] breaksArray = breaks.toArray(new Float[breaks.size()]);
    final Double[] countsArray = counts.toArray(new Double[counts.size()]);
    return new Histogram(ArrayUtils.toPrimitive(breaksArray), ArrayUtils.toPrimitive(countsArray));
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public Integer getNumberOfSignificantValueDigits() {
    return numberOfSignificantValueDigits;
  }

  @JsonProperty
  public Long getHighestToLowestValueRatio() {
    return highestToLowestValueRatio;
  }

  @JsonProperty
  public Integer getNumBuckets() {
    return numBuckets;
  }


  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
    return ByteBuffer.allocate(1 + fieldNameBytes.length + Longs.BYTES + Ints.BYTES * 2)
                     .put(CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .putLong(highestToLowestValueRatio)
                     .putInt(numberOfSignificantValueDigits)
                     .putInt(numBuckets)
                     .array();
  }

  @Override
  public String getTypeName()
  {
    return "HdrHistogram";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return new DoubleHistogram(highestToLowestValueRatio, numberOfSignificantValueDigits).getNeededByteBufferCapacity();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HdrHistogramAggregatorFactory that = (HdrHistogramAggregatorFactory) o;

    if (!Objects.equals(numberOfSignificantValueDigits, that.numberOfSignificantValueDigits)) {
      return false;
    }

    if (!Objects.equals(highestToLowestValueRatio, that.highestToLowestValueRatio)) {
      return false;
    }

    if (!Objects.equals(numBuckets, that.numBuckets)) {
      return false;
    }

    if (fieldName != null ? !fieldName.equals(that.fieldName) : that.fieldName != null) {
      return false;
    }

    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (fieldName != null ? fieldName.hashCode() : 0);
    result = 31 * result + highestToLowestValueRatio.hashCode();
    result = 31 * result + numberOfSignificantValueDigits.hashCode();
    result = 31 * result + numBuckets.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "HdrHistogramAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", highestToLowestValueRatio=" + highestToLowestValueRatio +
           ", numberOfSignificantValueDigits=" + numberOfSignificantValueDigits +
           ", numBuckets=" + numBuckets +
           '}';
  }
}
