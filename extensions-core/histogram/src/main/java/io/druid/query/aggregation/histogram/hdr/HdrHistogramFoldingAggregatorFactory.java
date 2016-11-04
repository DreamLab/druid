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
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.histogram.ApproximateHistogramAggregatorFactory;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;
import org.HdrHistogram.DoubleHistogram;

import java.nio.ByteBuffer;

@JsonTypeName("hdrHistogramFold")
public class HdrHistogramFoldingAggregatorFactory extends HdrHistogramAggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 13;

  @JsonCreator
  public HdrHistogramFoldingAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("highestToLowestValueRatio") Long highestToLowestValueRatio,
      @JsonProperty("numberOfSignificantValueDigits") Integer numberOfSignificantValueDigits
  )
  {
    super(name, fieldName, highestToLowestValueRatio, numberOfSignificantValueDigits, numBuckets);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);

    if (selector == null) {
      // gracefully handle undefined metrics

      selector = new ObjectColumnSelector<DoubleHistogram>()
      {
        @Override
        public Class<DoubleHistogram> classOfObject()
        {
          return DoubleHistogram.class;
        }

        @Override
        public DoubleHistogram get()
        {
          return new DoubleHistogram(highestToLowestValueRatio, numberOfSignificantValueDigits);
        }
      };
    }

    final Class cls = selector.classOfObject();
    if (cls.equals(Object.class) || DoubleHistogram.class.isAssignableFrom(cls)) {
      return new HdrHistogramFoldingAggregator(
          selector,
          highestToLowestValueRatio,
          numberOfSignificantValueDigits
      );
    }

    throw new IAE(
        "Incompatible type for metric[%s], expected a DoubleHistogram, got a %s",
        fieldName,
        cls
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);

    if (selector == null) {
      // gracefully handle undefined metrics

      selector = new ObjectColumnSelector<DoubleHistogram>()
      {
        @Override
        public Class<DoubleHistogram> classOfObject()
        {
          return DoubleHistogram.class;
        }

        @Override
        public DoubleHistogram get()
        {
          return new DoubleHistogram(highestToLowestValueRatio, numberOfSignificantValueDigits);
        }
      };
    }

    final Class cls = selector.classOfObject();
    if (cls.equals(Object.class) || DoubleHistogram.class.isAssignableFrom(cls)) {
      return new HdrHistogramFoldingBufferAggregator(selector, highestToLowestValueRatio, numberOfSignificantValueDigits);
    }

    throw new IAE(
        "Incompatible type for metric[%s], expected a DoubleHistogram, got a %s",
        fieldName,
        cls
    );
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new HdrHistogramFoldingAggregatorFactory(name, fieldName, highestToLowestValueRatio,
            numberOfSignificantValueDigits);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
    return ByteBuffer.allocate(1 + fieldNameBytes.length + Ints.BYTES * 2 + Floats.BYTES * 2)
                     .put(CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .putInt(resolution)
                     .putInt(numBuckets)
                     .putFloat(lowerLimit)
                     .putFloat(upperLimit)
                     .array();
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

    ApproximateHistogramAggregatorFactory that = (ApproximateHistogramAggregatorFactory) o;

    if (Float.compare(that.lowerLimit, lowerLimit) != 0) {
      return false;
    }
    if (numBuckets != that.numBuckets) {
      return false;
    }
    if (resolution != that.resolution) {
      return false;
    }
    if (Float.compare(that.upperLimit, upperLimit) != 0) {
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
    result = 31 * result + resolution;
    result = 31 * result + numBuckets;
    result = 31 * result + (lowerLimit != +0.0f ? Float.floatToIntBits(lowerLimit) : 0);
    result = 31 * result + (upperLimit != +0.0f ? Float.floatToIntBits(upperLimit) : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "ApproximateHistogramFoldingAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", resolution=" + resolution +
           ", numBuckets=" + numBuckets +
           ", lowerLimit=" + lowerLimit +
           ", upperLimit=" + upperLimit +
           '}';
  }
}

