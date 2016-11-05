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

import com.google.common.collect.Ordering;
import io.druid.data.input.InputRow;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;
import org.HdrHistogram.DoubleHistogram;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import static io.druid.query.aggregation.histogram.hdr.HdrHistogramAggregatorFactory.*;

public class HdrHistogramFoldingSerde extends ComplexMetricSerde
{
  private static Ordering<DoubleHistogram> comparator = new Ordering<DoubleHistogram>()
  {
    @Override
    public int compare(
        DoubleHistogram rhs, DoubleHistogram lhs
    )
    {
      return HdrHistogramAggregator.COMPARATOR.compare(rhs, lhs);
    }
  }.nullsFirst();

  @Override
  public String getTypeName()
  {
    return "hdrHistogram";
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<DoubleHistogram> extractedClass()
      {
        return DoubleHistogram.class;
      }

      @Override
      public DoubleHistogram extractValue(InputRow inputRow, String metricName)
      {
        Object rawValue = inputRow.getRaw(metricName);

        if (rawValue instanceof DoubleHistogram) {
          return (DoubleHistogram) rawValue;
        } else {
          List<String> dimValues = inputRow.getDimension(metricName);
          if (dimValues != null && dimValues.size() > 0) {
            Iterator<String> values = dimValues.iterator();

            final DoubleHistogram h = new DoubleHistogram(2);

            while (values.hasNext()) {
              float value = Float.parseFloat(values.next());
              h.recordValue(value);
            }
            return h;
          } else {
            return new DoubleHistogram(2);
          }
        }
      }
    };
  }

  @Override
  public void deserializeColumn(
      ByteBuffer byteBuffer, ColumnBuilder columnBuilder
  )
  {
    final GenericIndexed column = GenericIndexed.read(byteBuffer, getObjectStrategy());
    columnBuilder.setComplexColumn(new ComplexColumnPartSupplier(getTypeName(), column));
  }

  public ObjectStrategy getObjectStrategy()
  {
    return new ObjectStrategy<DoubleHistogram>()
    {
      @Override
      public Class<? extends DoubleHistogram> getClazz()
      {
        return DoubleHistogram.class;
      }

      @Override
      public DoubleHistogram fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
        readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
        return DoubleHistogram.decodeFromByteBuffer(readOnlyBuffer, MIN_BAR_FOR_HIGHEST_TO_LOWEST_VALUE_RATIO);
      }

      @Override
      public byte[] toBytes(DoubleHistogram h)
      {
        if (h == null) {
          return new byte[]{};
        }
        final ByteBuffer byteBuffer = ByteBuffer.allocate(h.getNeededByteBufferCapacity());
        byteBuffer.clear();
        h.encodeIntoByteBuffer(byteBuffer);
        return byteBuffer.array();
      }

      @Override
      public int compare(DoubleHistogram lhs, DoubleHistogram rhs)
      {
        return comparator.compare(lhs, rhs);
      }
    };
  }
}
