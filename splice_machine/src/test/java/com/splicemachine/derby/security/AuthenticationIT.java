/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.security;

import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceUserWatcher;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AuthenticationIT {

    private static final String AUTH_IT_USER = "auth_it_user";
    private static final String AUTH_IT_PASS = "test_password";

    @Rule
    public SpliceUserWatcher spliceUserWatcher1 = new SpliceUserWatcher(AUTH_IT_USER, AUTH_IT_PASS);

    @BeforeClass
    public static void setup() throws SQLException {
        Statement s = SpliceNetConnection.getConnection().createStatement();
        s.execute("call SYSCS_UTIL.SYSCS_CREATE_USER('dgf','dgf')");
        s.execute("call SYSCS_UTIL.SYSCS_CREATE_USER('jy','jy')");
    }

    @AfterClass
    public static void cleanup() throws SQLException {
        Statement s = SpliceNetConnection.getConnection().createStatement();
        s.execute("call SYSCS_UTIL.SYSCS_DROP_USER('dgf')");
        s.execute("call SYSCS_UTIL.SYSCS_DROP_USER('jy')");
    }

    @Test
    public void valid() throws SQLException {
        SpliceNetConnection.getConnectionAs(AUTH_IT_USER, AUTH_IT_PASS);
    }

    @Test
    public void validUsernameIsNotCaseSensitive() throws SQLException {
        SpliceNetConnection.getConnectionAs(AUTH_IT_USER.toUpperCase(), AUTH_IT_PASS);
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // bad password
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test(expected = SQLNonTransientConnectionException.class)
    public void badPassword() throws SQLException {
        SpliceNetConnection.getConnectionAs(AUTH_IT_USER, "bad_password");
    }

    @Test(expected = SQLNonTransientConnectionException.class)
    public void badPasswordExtraCharAtStart() throws SQLException {
        SpliceNetConnection.getConnectionAs(AUTH_IT_USER, "a" + AUTH_IT_PASS);
    }

    @Test(expected = SQLNonTransientConnectionException.class)
    public void badPasswordExtraCharAtEnd() throws SQLException {
        SpliceNetConnection.getConnectionAs(AUTH_IT_USER, AUTH_IT_PASS + "a");
    }

    @Test(expected = SQLNonTransientConnectionException.class)
    public void badPasswordCase() throws SQLException {
        SpliceNetConnection.getConnectionAs(AUTH_IT_USER, AUTH_IT_PASS.toUpperCase());
    }

    @Test(expected = SQLNonTransientConnectionException.class)
    public void badPasswordZeroLength() throws SQLException {
        SpliceNetConnection.getConnectionAs(AUTH_IT_USER, "");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // bad username
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test(expected = SQLNonTransientConnectionException.class)
    public void badUsername() throws SQLException {
        SpliceNetConnection.getConnectionAs("bad_username", AUTH_IT_PASS);
    }


    //DB-4618
    @Test
    public void invalidDbname() throws SQLException {
        String url = "jdbc:splice://localhost:1527/anotherdb;user=user;password=passwd";
        try {
            DriverManager.getConnection(url, new Properties());
            fail("Expected authentication failure");
        } catch (SQLNonTransientConnectionException e) {
            Assert.assertTrue(e.getSQLState().compareTo("08004") == 0);
        }
    }


    @Test
    public void impersonation() throws SQLException {
        String url = "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin;impersonate=dgf";
        try (Connection c = DriverManager.getConnection(url, new Properties())) {
            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("values USER")) {
                    assertTrue(rs.next());
                    assertEquals("DGF", rs.getString(1));
                }
            }

        }
        url = "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin;impersonate=jy";
        try (Connection c = DriverManager.getConnection(url, new Properties())) {
            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("values USER")) {
                    assertTrue(rs.next());
                    assertEquals("JY", rs.getString(1));
                }
            }

        }
        url = "jdbc:splice://localhost:1527/splicedb;user=dgf;password=dgf;impersonate=splice";
        try (Connection c = DriverManager.getConnection(url, new Properties())) {
            try (Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("values USER")) {
                    assertTrue(rs.next());
                    assertEquals("SPLICE", rs.getString(1));
                }
            }
        }
    }

    @Test(expected = Exception.class)
    public void failedImpersonation() throws SQLException {
        String url = "jdbc:splice://localhost:1527/splicedb;user=dgf;password=dgf;impersonate=jy";
        try (Connection c = DriverManager.getConnection(url, new Properties())) {
            fail("Expected error");
        }
    }

    @Test(expected = Exception.class)
    public void userWithNoImpersonation() throws SQLException {
        String url = "jdbc:splice://localhost:1527/splicedb;user=jy;password=jy;impersonate=dgf";
        try (Connection c = DriverManager.getConnection(url, new Properties())) {
            fail("Expected error");
        }
    }
}
