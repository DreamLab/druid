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

import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ObjectColumnSelector;
import org.HdrHistogram.DoubleHistogram;

import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;

public class HdrHistogramFoldingBufferAggregator implements BufferAggregator
{
  private final ObjectColumnSelector<DoubleHistogram> selector;
  private final long highestToLowestValueRatio;
  private final int numberOfSignificantValueDigits;

  public HdrHistogramFoldingBufferAggregator(
          ObjectColumnSelector<DoubleHistogram> selector,
          long highestToLowestValueRatio, int numberOfSignificantValueDigits)
  {
    this.selector = selector;
    this.highestToLowestValueRatio = highestToLowestValueRatio;
    this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    final ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    final DoubleHistogram initialHistogram = new DoubleHistogram(highestToLowestValueRatio, numberOfSignificantValueDigits);
    initialHistogram.encodeIntoCompressedByteBuffer(mutationBuffer, Deflater.BEST_SPEED);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    final ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    final DoubleHistogram doubleHistogram;
    try {
      doubleHistogram = DoubleHistogram.decodeFromCompressedByteBuffer(buf, Long.MAX_VALUE);
      doubleHistogram.add(selector.get());
      mutationBuffer.position(position);
      doubleHistogram.encodeIntoCompressedByteBuffer(buf, Deflater.BEST_SPEED);
    } catch (DataFormatException e) {
      e.printStackTrace();
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.asReadOnlyBuffer();
    mutationBuffer.position(position);
    final DoubleHistogram doubleHistogram;
    try {
      doubleHistogram = DoubleHistogram.decodeFromCompressedByteBuffer(buf, Long.MAX_VALUE);
      return doubleHistogram;
    } catch (DataFormatException e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("HdrHistogramFoldingBufferAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("HdrHistogramFoldingBufferAggregator does not support getLong()");
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
