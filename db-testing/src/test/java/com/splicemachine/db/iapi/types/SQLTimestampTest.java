/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.splicemachine.db.iapi.types;

import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.junit.Assert;
import org.junit.Test;
import java.sql.Timestamp;
import java.util.GregorianCalendar;

/**
 *
 * Test Class for SQLTimestamp
 *
 */
public class SQLTimestampTest {

        @Test
        public void serdeValueData() throws Exception {
                UnsafeRow row = new UnsafeRow();
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row),1);
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                SQLTimestamp value = new SQLTimestamp(timestamp);
                SQLTimestamp valueA = new SQLTimestamp();
                value.write(writer, 0);
                valueA.read(row,0);
                Assert.assertEquals("SerdeIncorrect",timestamp.toString(),valueA.getTimestamp(new GregorianCalendar()).toString());
            }

        @Test
        public void serdeNullValueData() throws Exception {
                UnsafeRow row = new UnsafeRow();
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row),1);
                SQLTimestamp value = new SQLTimestamp();
                SQLTimestamp valueA = new SQLTimestamp();
                value.write(writer, 0);
                Assert.assertTrue("SerdeIncorrect", valueA.isNull());
            }
    
        @Test
        public void serdeKeyData() throws Exception {
                GregorianCalendar gc = new GregorianCalendar();
                long currentTimeMillis = System.currentTimeMillis();
                SQLTimestamp value1 = new SQLTimestamp(new Timestamp(currentTimeMillis));
                SQLTimestamp value2 = new SQLTimestamp(new Timestamp(currentTimeMillis+200));
                SQLTimestamp value1a = new SQLTimestamp();
                SQLTimestamp value2a = new SQLTimestamp();
                PositionedByteRange range1 = new SimplePositionedMutableByteRange(value1.encodedKeyLength());
                PositionedByteRange range2 = new SimplePositionedMutableByteRange(value2.encodedKeyLength());
                value1.encodeIntoKey(range1, Order.ASCENDING);
                value2.encodeIntoKey(range2, Order.ASCENDING);
                range1.setPosition(0);
                range2.setPosition(0);
                value1a.decodeFromKey(range1);
                value2a.decodeFromKey(range2);
                Assert.assertEquals("1 incorrect",value1.getTimestamp(gc),value1a.getTimestamp(gc));
                Assert.assertEquals("2 incorrect",value2.getTimestamp(gc),value2a.getTimestamp(gc));
        }
}
