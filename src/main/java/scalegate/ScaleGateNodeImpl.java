/*  Copyright (C) 2015  Ioannis Nikolakopoulos,
 * 			Daniel Cederman,
 * 			Vincenzo Gulisano,
 * 			Marina Papatriantafilou,
 * 			Philippas Tsigas
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *  Contact: Ioannis (aka Yiannis) Nikolakopoulos ioaniko@chalmers.se
 *  	     Vincenzo Gulisano vincenzo.gulisano@chalmers.se
 *
 */

package scalegate;

import java.util.concurrent.atomic.AtomicReferenceArray;

//TODO Parametrize with <Tuple> generic, requiring it to be Comparable

public class ScaleGateNodeImpl {

    final AtomicReferenceArray<ScaleGateNodeImpl> next;
    final ScaleGateTuple obj;
    final ScaleGateImpl.WriterThreadLocalData ln;
    final int writerID;
    volatile boolean assigned;

    ScaleGateNodeImpl(int levels, ScaleGateTuple t, ScaleGateImpl.WriterThreadLocalData ln, int writerID) {
        next = new AtomicReferenceArray<ScaleGateNodeImpl>(levels);
        for (int i = 0; i < levels; i++) {
            next.set(i, null);
        }
        this.obj = t;
        this.assigned = false;
        this.ln = ln;
        this.writerID = writerID;
    }

    ScaleGateNodeImpl getNext(int level) {
        return next.get(level);
    }

    public ScaleGateTuple getTuple() {
        return this.obj;
    }

    void setNext(int i, ScaleGateNodeImpl newNode) {
        next.set(i, newNode);
    }

    boolean trySetNext(int i, ScaleGateNodeImpl oldNode,
                              ScaleGateNodeImpl newNode) {
        return next.compareAndSet(i, oldNode, newNode);
    }

    boolean isLastAdded() {
        // read this as volatile
        return this == ln.written;
    }

}
