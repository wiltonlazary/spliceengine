/*
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
package com.splicemachine.orc.metadata.statistics;

import com.splicemachine.orc.input.SpliceOrcNewInputFormat;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

public class DecimalStatistics
        implements RangeStatistics<BigDecimal>
{
    // 1 byte to denote if null
    public static final long DECIMAL_VALUE_BYTES_OVERHEAD = Byte.BYTES;

    private final BigDecimal minimum;
    private final BigDecimal maximum;

    public DecimalStatistics(BigDecimal minimum, BigDecimal maximum)
    {
        checkArgument(minimum == null || maximum == null || minimum.compareTo(maximum) <= 0, "minimum is not less than maximum");
        this.minimum = minimum;
        this.maximum = maximum;
    }

    public static ColumnStatistics getPartitionColumnStatistics(String value) throws IOException {
        DecimalStatistics decimalStatistics = null;
        if(value != null) {
            BigDecimal bigDecimal = BigDecimal.valueOf(Double.parseDouble(value));
            decimalStatistics = new DecimalStatistics(bigDecimal,
                bigDecimal);
        }
        return new ColumnStatistics(SpliceOrcNewInputFormat.DEFAULT_PARTITION_SIZE,
            null,null,null,null,null,decimalStatistics,null);
    }

    @Override
    public BigDecimal getMin()
    {
        return minimum;
    }

    @Override
    public BigDecimal getMax()
    {
        return maximum;
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
        DecimalStatistics that = (DecimalStatistics) o;
        return Objects.equals(minimum, that.minimum) &&
                Objects.equals(maximum, that.maximum);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(minimum, maximum);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("minimum", minimum)
                .add("maximum", maximum)
                .toString();
    }
}