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

import com.google.common.primitives.Longs;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.FloatColumnSelector;
import org.HdrHistogram.DoubleHistogram;

import java.util.Comparator;

public class HdrHistogramAggregator implements Aggregator
{
  public static final Comparator COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object lhs, Object rhs)
    {
      return Longs.compare(((DoubleHistogram) lhs).getTotalCount(), ((DoubleHistogram) rhs).getTotalCount());
    }
  };

  static Object combineHistograms(Object lhs, Object rhs)
  {
    final DoubleHistogram lhsHistogram = (DoubleHistogram) lhs;
    lhsHistogram.add((DoubleHistogram) rhs);
    return lhsHistogram;
  }

  private final FloatColumnSelector selector;
  private final long highestToLowestValueRatio;
  private final int numberOfSignificantValueDigits;

  private DoubleHistogram histogram;

  public HdrHistogramAggregator(
          long highestToLowestValueRatio, int numberOfSignificantValueDigits, FloatColumnSelector selector)
  {
    this.highestToLowestValueRatio = highestToLowestValueRatio;
    this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
    this.selector = selector;
    this.histogram = new DoubleHistogram(this.highestToLowestValueRatio, this.numberOfSignificantValueDigits);
  }

  @Override
  public void aggregate()
  {
    histogram.recordValue(selector.get());
  }

  @Override
  public void reset()
  {
    this.histogram = new DoubleHistogram(highestToLowestValueRatio, numberOfSignificantValueDigits);
  }

  @Override
  public Object get()
  {
    return histogram;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("HdrHistogramAggregator does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("HdrHistogramAggregator does not support getLong()");
  }

  @Override
  public String getName()
  {
    throw new UnsupportedOperationException("getName is deprecated");
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
