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


import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;
import org.HdrHistogram.DoubleHistogram;

public class HdrHistogramFoldingAggregator implements Aggregator
{
  private final ObjectColumnSelector<DoubleHistogram> selector;
  private final long highestToLowestValueRatio;
  private final int numberOfSignificantValueDigits;

  private DoubleHistogram histogram;

  public HdrHistogramFoldingAggregator(
          ObjectColumnSelector<DoubleHistogram> selector,
          long highestToLowestValueRatio, int numberOfSignificantValueDigits)
  {
    this.selector = selector;
    this.highestToLowestValueRatio = highestToLowestValueRatio;
    this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
    this.histogram = new DoubleHistogram(highestToLowestValueRatio, numberOfSignificantValueDigits);
  }

  @Override
  public void aggregate()
  {
    final DoubleHistogram rhs = selector.get();
    if (rhs == null) {
      return;
    }
    histogram.add(rhs);
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
    throw new UnsupportedOperationException("HdrHistogramFoldingAggregator does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("HdrHistogramFoldingAggregator does not support getLong()");
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
