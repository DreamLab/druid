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

package io.druid.query.aggregation.histogram;

import com.google.common.primitives.Floats;
import io.druid.query.aggregation.Histogram;

import java.util.Arrays;
import java.util.Random;

public class ApproximateHistogramErrorBenchmark
{
  private boolean debug = true;
  private int numBuckets = 20;
  private int numBreaks = numBuckets + 1;
  private int numPerHist = 50;
  private int numHists = 10;
  private int resolution = 50;
  private int combinedResolution = 100;
  private Random rand = new Random(2);

  public ApproximateHistogramErrorBenchmark setDebug(boolean debug)
  {
    this.debug = debug;
    return this;
  }

  public ApproximateHistogramErrorBenchmark setNumBuckets(int numBuckets)
  {
    this.numBuckets = numBuckets;
    return this;
  }

  public ApproximateHistogramErrorBenchmark setNumBreaks(int numBreaks)
  {
    this.numBreaks = numBreaks;
    return this;
  }

  public ApproximateHistogramErrorBenchmark setNumPerHist(int numPerHist)
  {
    this.numPerHist = numPerHist;
    return this;
  }

  public ApproximateHistogramErrorBenchmark setNumHists(int numHists)
  {
    this.numHists = numHists;
    return this;
  }

  public ApproximateHistogramErrorBenchmark setResolution(int resolution)
  {
    this.resolution = resolution;
    return this;
  }

  public ApproximateHistogramErrorBenchmark setCombinedResolution(int combinedResolution)
  {
    this.combinedResolution = combinedResolution;
    return this;
  }


  public static void main(String[] args)
  {
    ApproximateHistogramErrorBenchmark approxHist = new ApproximateHistogramErrorBenchmark();
    System.out.println(
        Arrays.toString(
            approxHist.setDebug(true)
                      .setNumPerHist(50)
                      .setNumHists(10000)
                      .setResolution(50)
                      .setCombinedResolution(100)
                      .getErrors()
        )
    );


    ApproximateHistogramErrorBenchmark approxHist2 = new ApproximateHistogramErrorBenchmark();
    int[] numHistsArray = new int[]{10, 100, 1000, 10000, 100000};
    float[] errs1 = new float[numHistsArray.length];
    float[] errs2 = new float[numHistsArray.length];
    for (int i = 0; i < numHistsArray.length; ++i) {
      float[] tmp = approxHist2.setDebug(false).setNumHists(numHistsArray[i]).setCombinedResolution(100).getErrors();
      errs1[i] = tmp[0];
      errs2[i] = tmp[1];
    }

    System.out
          .format("Number of histograms for folding                           : %s \n", Arrays.toString(numHistsArray));
    System.out.format("Errors for approximate histogram                           : %s \n", Arrays.toString(errs1));
    System.out.format("Errors for approximate histogram, ruleFold                 : %s \n", Arrays.toString(errs2));
    /**
     * Exact Histogram Sum:
     * 500000.0
     * Approximate Histogram Sum:
     * 500000.0
     * Approximate Histogram Rule Fold Sum:
     * 500000.0
     * Exact Histogram:
     * HistogramVisual{counts=[18.0, 111.0, 528.0, 1915.0, 6029.0, 15644.0, 32884.0, 56121.0, 78882.0, 89667.0, 83724.0, 63268.0, 39285.0, 20038.0, 8188.0, 2758.0, 739.0, 163.0, 34.0, 4.0], breaks=[-4.392460346221924, -3.937209129333496, -3.4819581508636475, -3.0267069339752197, -2.571455955505371, -2.1162047386169434, -1.6609535217285156, -1.205702543258667, -0.7504513263702393, -0.2952003479003906, 0.1600508689880371, 0.6153020858764648, 1.0705533027648926, 1.5258045196533203, 1.9810552597045898, 2.4363064765930176, 2.8915576934814453, 3.346808910369873, 3.8020596504211426, 4.2573113441467285, 4.712562084197998], quantiles=[-4.306333541870117, 4.62015962600708]}
     * Approximate Histogram:
     * Histogram{breaks=[-4.392460346221924, -3.937209129333496, -3.4819581508636475, -3.0267069339752197, -2.571455955505371, -2.1162047386169434, -1.6609535217285156, -1.205702543258667, -0.7504513263702393, -0.2952003479003906, 0.1600508689880371, 0.6153020858764648, 1.0705533027648926, 1.5258045196533203, 1.9810552597045898, 2.4363064765930176, 2.8915576934814453, 3.346808910369873, 3.8020596504211426, 4.2573113441467285, 4.712562084197998], counts=[18.18688201904297, 113.68641662597656, 531.0787963867188, 1930.25341796875, 6075.6728515625, 15512.9111328125, 33247.05078125, 55968.0, 78442.9921875, 89821.2265625, 83909.2109375, 63149.140625, 39225.16015625, 20111.654296875, 8231.3369140625, 2755.321044921875, 748.5662841796875, 169.19137573242188, 34.29652404785156, 5.0597052574157715]}
     * Approximate Histogram Rule Fold:
     * Histogram{breaks=[-4.392460346221924, -3.937209129333496, -3.4819581508636475, -3.0267069339752197, -2.571455955505371, -2.1162047386169434, -1.6609535217285156, -1.205702543258667, -0.7504513263702393, -0.2952003479003906, 0.1600508689880371, 0.6153020858764648, 1.0705533027648926, 1.5258045196533203, 1.9810552597045898, 2.4363064765930176, 2.8915576934814453, 3.346808910369873, 3.8020596504211426, 4.2573113441467285, 4.712562084197998], counts=[18.0, 118.0, 448.0, 2070.0, 6253.4287109375, 14341.8154296875, 32814.1015625, 57232.91015625, 78829.875, 89754.0, 83643.5859375, 63318.046875, 36789.0546875, 23818.173828125, 6349.0, 3342.0, 635.0, 177.0, 46.0, 2.0]}
     * Error for approximate histogram: 0.0036178932
     * Error for approximate histogram, ruleFold: 0.024102252
     * Error ratio for AHRF: 6.6619577
     * [0.0036178932, 0.024102252, 6.6619577]
     * Number of histograms for folding                           : [10, 100, 1000, 10000, 100000]
     * Errors for approximate histogram                           : [0.03507633, 0.017702274, 0.009043395, 0.013922866, 0.005329785]
     * Errors for approximate histogram, ruleFold                 : [0.056815416, 0.033417534, 0.03451164, 0.04620145, 0.014469914]
     */
  }

  private float[] getErrors()
  {
    final int numValues = numHists * numPerHist;
    final float[] values = new float[numValues];

    for (int i = 0; i < numValues; ++i) {
      values[i] = (float) rand.nextGaussian();
    }

    float min = Floats.min(values);
    min = (float) (min < 0 ? 1.02 : .98) * min;
    float max = Floats.max(values);
    max = (float) (max < 0 ? .98 : 1.02) * max;
    final float stride = (max - min) / numBuckets;
    final float[] breaks = new float[numBreaks];
    for (int i = 0; i < numBreaks; i++) {
      breaks[i] = min + stride * i;
    }

    Histogram h = new Histogram(breaks);
    for (float v : values) {
      h.offer(v);
    }
    double[] hcounts = h.asVisual().counts;

    ApproximateHistogram ah1 = new ApproximateHistogram(resolution);
    ApproximateHistogram ah2 = new ApproximateHistogram(combinedResolution);
    ApproximateHistogram tmp = new ApproximateHistogram(resolution);
    for (int i = 0; i < numValues; ++i) {
      tmp.offer(values[i]);
      if ((i + 1) % numPerHist == 0) {
        ah1.fold(tmp);
        ah2.foldRule(tmp, null, null);
        tmp = new ApproximateHistogram(resolution);
      }
    }
    double[] ahcounts1 = ah1.toHistogram(breaks).getCounts();
    double[] ahcounts2 = ah2.toHistogram(breaks).getCounts();

    float err1 = 0;
    float err2 = 0;
    for (int j = 0; j < hcounts.length; j++) {
      err1 += (float)Math.abs((hcounts[j] - ahcounts1[j]) / numValues);
      err2 += (float)Math.abs((hcounts[j] - ahcounts2[j]) / numValues);
    }

    if (debug) {
      float sum = 0;
      for (double v : hcounts) {
        sum += (float)v;
      }
      System.out.println("Exact Histogram Sum:");
      System.out.println(sum);
      sum = 0;
      for (double v : ahcounts1) {
        sum += (float)v;
      }
      System.out.println("Approximate Histogram Sum:");
      System.out.println(sum);
      sum = 0;
      for (double v : ahcounts2) {
        sum += (float)v;
      }
      System.out.println("Approximate Histogram Rule Fold Sum:");
      System.out.println(sum);
      System.out.println("Exact Histogram:");
      System.out.println(h.asVisual());
      System.out.println("Approximate Histogram:");
      System.out.println(ah1.toHistogram(breaks));
      System.out.println("Approximate Histogram Rule Fold:");
      System.out.println(ah2.toHistogram(breaks));
      System.out.format("Error for approximate histogram: %s \n", err1);
      System.out.format("Error for approximate histogram, ruleFold: %s \n", err2);
      System.out.format("Error ratio for AHRF: %s \n", err2 / err1);
    }
    return new float[]{err1, err2, err2 / err1};
  }

}
