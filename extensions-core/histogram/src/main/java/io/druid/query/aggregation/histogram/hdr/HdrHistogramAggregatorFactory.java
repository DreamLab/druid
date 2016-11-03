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
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.StringUtils;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.histogram.ApproximateHistogram;
import io.druid.query.aggregation.histogram.HdrHistogram;
import io.druid.query.aggregation.histogram.HdrHistogramAggregator;
import io.druid.query.aggregation.histogram.HdrHistogramBufferAggregator;
import io.druid.query.aggregation.histogram.HdrHistogramFoldingAggregatorFactory;
import io.druid.segment.ColumnSelectorFactory;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@JsonTypeName("hdrHistogram")
public class HdrHistogramAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 12;

  protected final String name;
  protected final String fieldName;

  protected final int resolution;
  protected final int numBuckets;

  protected final float lowerLimit;
  protected final float upperLimit;

  @JsonCreator
  public HdrHistogramAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("resolution") Integer resolution,
      @JsonProperty("numBuckets") Integer numBuckets,
      @JsonProperty("lowerLimit") Float lowerLimit,
      @JsonProperty("upperLimit") Float upperLimit

  )
  {
    this.name = name;
    this.fieldName = fieldName;
    this.resolution = resolution == null ? ApproximateHistogram.DEFAULT_HISTOGRAM_SIZE : resolution;
    this.numBuckets = numBuckets == null ? ApproximateHistogram.DEFAULT_BUCKET_SIZE : numBuckets;
    this.lowerLimit = lowerLimit == null ? Float.NEGATIVE_INFINITY : lowerLimit;
    this.upperLimit = upperLimit == null ? Float.POSITIVE_INFINITY : upperLimit;

    Preconditions.checkArgument(this.resolution > 0, "resolution must be greater than 1");
    Preconditions.checkArgument(this.numBuckets > 0, "numBuckets must be greater than 1");
    Preconditions.checkArgument(this.upperLimit > this.lowerLimit, "upperLimit must be greater than lowerLimit");
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new HdrHistogramAggregator(
            highestToLowestValueRatio, numberOfSignificantValueDigits, metricFactory.makeFloatColumnSelector(fieldName),
        resolution,
        lowerLimit,
        upperLimit
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new HdrHistogramBufferAggregator(
        metricFactory.makeFloatColumnSelector(fieldName),
        resolution,
        lowerLimit,
        upperLimit
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
    return new HdrHistogramFoldingAggregatorFactory(name, name, resolution, numBuckets, lowerLimit, upperLimit);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && other instanceof HdrHistogramAggregatorFactory) {
      HdrHistogramAggregatorFactory castedOther = (HdrHistogramAggregatorFactory) other;

      return new HdrHistogramFoldingAggregatorFactory(
          name,
          name,
          Math.max(resolution, castedOther.resolution),
          numBuckets,
          Math.min(lowerLimit, castedOther.lowerLimit),
          Math.max(upperLimit, castedOther.upperLimit)
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
            fieldName,
            fieldName,
            resolution,
            numBuckets,
            lowerLimit,
            upperLimit
        )
    );
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof byte[]) {
      final HdrHistogram ah = HdrHistogram.fromBytes((byte[]) object);
      ah.setLowerLimit(lowerLimit);
      ah.setUpperLimit(upperLimit);

      return ah;
    } else if (object instanceof ByteBuffer) {
      final HdrHistogram ah = HdrHistogram.fromBytes((ByteBuffer) object);
      ah.setLowerLimit(lowerLimit);
      ah.setUpperLimit(upperLimit);

      return ah;
    } else if (object instanceof String) {
      byte[] bytes = Base64.decodeBase64(StringUtils.toUtf8((String) object));
      final HdrHistogram ah = HdrHistogram.fromBytes(bytes);
      ah.setLowerLimit(lowerLimit);
      ah.setUpperLimit(upperLimit);

      return ah;
    } else {
      return object;
    }
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return ((HdrHistogram) object).toHistogram(numBuckets);
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
  public int getResolution()
  {
    return resolution;
  }

  @JsonProperty
  public float getLowerLimit()
  {
    return lowerLimit;
  }

  @JsonProperty
  public float getUpperLimit()
  {
    return upperLimit;
  }

  @JsonProperty
  public int getNumBuckets()
  {
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
    return ByteBuffer.allocate(1 + fieldNameBytes.length + Ints.BYTES * 2 + Floats.BYTES * 2)
                     .put(CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .putInt(resolution)
                     .putInt(numBuckets)
                     .putFloat(lowerLimit)
                     .putFloat(upperLimit).array();
  }

  @Override
  public String getTypeName()
  {
    return "HdrHistogram";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return new HdrHistogram(resolution).getMaxStorageSize();
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
    return "HdrHistogramAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", resolution=" + resolution +
           ", numBuckets=" + numBuckets +
           ", lowerLimit=" + lowerLimit +
           ", upperLimit=" + upperLimit +
           '}';
  }
}
