/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.loadbalancer.servo.internal;

import com.netflix.servo.tag.Tag;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Captures a HdrHistogram and sends it to pre-defined Server Counters.
 * The buckets are min, max, 50%, 90%, 99%, 99.9%, and 99.99%
 */
public class HdrHistogramServoTimer {
    private final Histogram histogram;

    private ThreadLocalAdderCounter min;

    private ThreadLocalAdderCounter max;

    private ThreadLocalAdderCounter p50;

    private ThreadLocalAdderCounter p90;

    private ThreadLocalAdderCounter p99;

    private ThreadLocalAdderCounter p99_9;

    private ThreadLocalAdderCounter p99_99;

    private HdrHistogramServoTimer(String label) {
        this.histogram = new ConcurrentHistogram(TimeUnit.MINUTES.toNanos(1), 2);

        min = ThreadLocalAdderCounter.newThreadLocalAdderCounter(label + "_min");
        max = ThreadLocalAdderCounter.newThreadLocalAdderCounter(label + "_max");
        p50 = ThreadLocalAdderCounter.newThreadLocalAdderCounter(label + "_p50");
        p90 = ThreadLocalAdderCounter.newThreadLocalAdderCounter(label + "_p90");
        p99 = ThreadLocalAdderCounter.newThreadLocalAdderCounter(label + "_p99");
        p99_9 = ThreadLocalAdderCounter.newThreadLocalAdderCounter(label + "_p99_9");
        p99_99 = ThreadLocalAdderCounter.newThreadLocalAdderCounter(label + "_p99_99");
    }


    private HdrHistogramServoTimer(String label, List<Tag> tags) {
        this.histogram = new ConcurrentHistogram(TimeUnit.MINUTES.toNanos(1), 2);

        min = ThreadLocalAdderCounter.newThreadLocalAdderCounter(label + "_min", tags);
        max = ThreadLocalAdderCounter.newThreadLocalAdderCounter(label + "_max", tags);
        p50 = ThreadLocalAdderCounter.newThreadLocalAdderCounter(label + "_p50", tags);
        p90 = ThreadLocalAdderCounter.newThreadLocalAdderCounter(label + "_p90", tags);
        p99 = ThreadLocalAdderCounter.newThreadLocalAdderCounter(label + "_p99", tags);
        p99_9 = ThreadLocalAdderCounter.newThreadLocalAdderCounter(label + "_p99_9", tags);
        p99_99 = ThreadLocalAdderCounter.newThreadLocalAdderCounter(label + "_p99_99", tags);
    }

    public static HdrHistogramServoTimer newInstance(String label) {
        return new HdrHistogramServoTimer(label);
    }

    public static HdrHistogramServoTimer newInstance(String label, Tag... tags) {
        return newInstance(label, Arrays.asList(tags));
    }

    public static HdrHistogramServoTimer newInstance(String label, List<Tag> tags) {
        return new HdrHistogramServoTimer(label, tags);
    }

    /**
     * Records a value for to the histogram and updates the Servo counter buckets
     * @param value the value to update
     */
    public void record(long value) {
        histogram.recordValue(value);

        min.increment(histogram.getMinValue());
        max.increment(histogram.getMaxValue());

        p50.increment(histogram.getValueAtPercentile(50));
        p90.increment(histogram.getValueAtPercentile(90));
        p99.increment(histogram.getValueAtPercentile(99));
        p99_9.increment(histogram.getValueAtPercentile(99.9));
        p99_99.increment(histogram.getValueAtPercentile(99.99));
    }

    /**
     * Prints the current {@link Histogram} to a String
     */
    public String histrogramToString()  {
        String retVal = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(bos);
        histogram.outputPercentileDistribution(ps, 1000.0);

        try {
            retVal = bos.toString(Charset.defaultCharset().name());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return retVal;
    }

    public long getMin() {
        return min.get();
    }

    public long getMax() {
        return max.get();
    }

    public long getP50() {
        return p50.get();
    }

    public long getP90() {
        return p90.get();
    }

    public long getP99() {
        return p99.get();
    }

    public long getP99_9() {
        return p99_9.get();
    }

    public long getP99_99() {
        return p99_99.get();
    }

}