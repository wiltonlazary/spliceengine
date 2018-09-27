/*
 * Copyright (c) 2012 - 2018 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.derby.iapi.sql.olap.AbstractOlapResult;

/**
 * Created by jyuan on 10/9/18.
 */
public class SamplingResult extends AbstractOlapResult {

    byte[][] results;

    public SamplingResult(){}

    public SamplingResult(byte[][] results) {
        this.results = results;
    }

    public byte[][] getResults() {
        return results;
    }

    @Override
    public boolean isSuccess(){
        return true;
    }
}
