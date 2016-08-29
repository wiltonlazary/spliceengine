/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.impl.ast;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.impl.sql.compile.QueryTreeNode;
import org.spark_project.guava.base.Predicate;

/**
 * Visitor that applies a visitor until a stopping point defined by a predicate. Parameters of the traversal
 * defined by the wrapped visitor.
 *
 * @author P Trolard
 *         Date: 30/10/2013
 */
public class VisitUntilVisitor implements Visitor {
    private boolean stop = false;
    final Visitor v;
    final Predicate<? super Visitable> pred;

    public VisitUntilVisitor(final Visitor v, final Predicate<? super Visitable> pred){
        this.v = v;
        this.pred = pred;
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
        if (pred.apply(node)){
            stop = true;
            return node;
        }
        return v.visit(node, parent);
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return v.visitChildrenFirst(node);
    }

    @Override
    public boolean stopTraversal() {
        return stop;
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        return v.skipChildren(node);
    }
}
