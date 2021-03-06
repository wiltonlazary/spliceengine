/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.dbTesting.functionTests.tests.lang;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import junit.framework.Test;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * This class contains test cases for the correct handling of predicates in
 * SQL queries.
 */
public class PredicateTest extends BaseJDBCTestCase {
    public PredicateTest(String name) {
        super(name);
    }

    public static Test suite() {
        // We're testing engine functionality, so run in embedded only.
        return TestConfiguration.embeddedSuite(PredicateTest.class);
    }

    /**
     * DERBY-2282: Test that we're able to compute the transitive closure of
     * predicates with constants on the left side of the comparison operator.
     */
    public void testTransitiveClosureWithConstantsOnLeftSide()
            throws SQLException, IOException {

        setAutoCommit(false); // let tables be cleaned up automatically

        Statement s = createStatement();

        // insert test data
        s.execute("create table t1 (i int)");
        s.execute("create table t2 (j int)");
        s.execute("insert into t1 values 1, 5, 7, 11, 13, 17, 19");
        s.execute("insert into t2 values 23, 29, 31, 37, 43, 47, 53");
        s.execute("insert into t1 select 23 * i from t1 where i < 19");
        s.execute("insert into t2 select 23 * j from t2 where j < 55");

        // enable runtime statistics
        s.execute("call syscs_util.syscs_set_runtimestatistics(1)");

        // Following will show two qualifiers for T2 and three for T1
        // because transitive closure adds two new qualifiers, "t2.j >= 23"
        // and "t1.i <= 30" to the list.
        JDBC.assertSingleValueResultSet(
                s.executeQuery(
                    "select i from t1, t2 where " +
                    "t1.i = t2.j and t1.i >= 23 and t2.j <= 30"),
                "23");

        List expectedOperators = Arrays.asList(new String[] {
                    "Operator: <", "Operator: <=",
                    "Operator: <", "Operator: <=", "Operator: ="
                });

        assertEquals(expectedOperators, extractOperators(getStatistics()));

        // But if we put the constants on the left-hand side, we didn't
        // detect the transitive closure and thus we had a single qualifier
        // for T2 and only two qualifiers for T1.
        JDBC.assertSingleValueResultSet(
                s.executeQuery(
                    "select i from t1, t2 where " +
                    "t1.i = t2.j and 23 <= t1.i and 30 >= t2.j"),
                "23");

        // Verify that we now have all the expected qualifiers.
        assertEquals(expectedOperators, extractOperators(getStatistics()));

        // Now check that we get the same plan with parameters instead of
        // constants on the right-hand side.

        PreparedStatement paramRight = prepareStatement(
                "select i from t1, t2 where " +
                "t1.i = t2.j and t1.i >= ? and t2.j <= ?");
        paramRight.setInt(1, 23);
        paramRight.setInt(2, 30);

        JDBC.assertSingleValueResultSet(paramRight.executeQuery(), "23");
        assertEquals(expectedOperators, extractOperators(getStatistics()));

        // Same plan expected with parameters on the left-hand side.

        PreparedStatement paramLeft = prepareStatement(
                "select i from t1, t2 where " +
                "t1.i = t2.j and ? <= t1.i and ? >= t2.j");
        paramLeft.setInt(1, 23);
        paramLeft.setInt(2, 30);

        JDBC.assertSingleValueResultSet(paramLeft.executeQuery(), "23");
        assertEquals(expectedOperators, extractOperators(getStatistics()));
    }

    /**
     * Get the runtime statistics for the previous statement executed on the
     * default connection (if collection of runtime statistics has been
     * enabled).
     *
     * @return a string with the runtime statistics
     */
    private String getStatistics() throws SQLException {
        ResultSet rs = createStatement().executeQuery(
                "values syscs_util.syscs_get_runtimestatistics()");
        rs.next();
        String stats = rs.getString(1);
        JDBC.assertEmpty(rs);
        return stats;
    }

    /**
     * Extract all the operators from the runtime statistics.
     *
     * @param stats the runtime statistics
     * @return a list of all operators
     */
    private List extractOperators(String stats) throws IOException {
        ArrayList ops = new ArrayList();
        BufferedReader r = new BufferedReader(new StringReader(stats));
        String line;
        while ((line = r.readLine()) != null) {
            line = line.trim();
            if (line.startsWith("Operator: ")) {
                ops.add(line);
            }
        }
        return ops;
    }
}
