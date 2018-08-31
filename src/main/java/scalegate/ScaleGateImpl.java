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

import java.util.Random;

public class ScaleGateImpl implements ScaleGate {

    final int maxlevels;
    ScaleGateNodeImpl head;
    final ScaleGateNodeImpl tail;

    final int numberOfWriters;
    final int numberOfReaders;

    // Lightweight synchronized timestamp counter to minimise read loss
    final long[] lastWritersTimestamp;

    // Arrays of source/reader id local data
    WriterThreadLocalData[] writertld;
    ReaderThreadLocalData[] readertld;


    public ScaleGateImpl(int maxlevels, int writers, int readers) {
        this.maxlevels = maxlevels;

        this.head = new ScaleGateNodeImpl(maxlevels, null, null, -1);
        this.tail = new ScaleGateNodeImpl(maxlevels, null, null, -1);

        for (int i = 0; i < maxlevels; i++)
            head.setNext(i, tail);

        this.numberOfWriters = writers;
        this.numberOfReaders = readers;

        writertld = new WriterThreadLocalData[numberOfWriters];
        for (int i = 0; i < numberOfWriters; i++) {
            writertld[i] = new WriterThreadLocalData(head);
        }

        readertld = new ReaderThreadLocalData[numberOfReaders];
        for (int i = 0; i < numberOfReaders; i++) {
            readertld[i] = new ReaderThreadLocalData(head);
        }

        // This should not be used again, only the writer/reader-local variables
        head = null;

        // Initialise last writers timestamp
        lastWritersTimestamp = new long[numberOfWriters];
    }

    /*
     * (non-Javadoc)
     */
    @Override
    public ScaleGateTuple getNextReadyTuple(int readerID) {
        ScaleGateNodeImpl next = getReaderLocal(readerID).localHead.getNext(0);

        if (next != tail && !next.isLastAdded() && next.getTuple().getTimestamp() < getMinWritersTimestamp()) {
            getReaderLocal(readerID).localHead = next;
            return next.getTuple();
        }

        return null;
    }

    // Add a tuple
    @Override
    public void addTuple(ScaleGateTuple tuple, int writerID) {
        this.internalAddTuple(tuple, writerID);
    }

    private void insertNode(ScaleGateNodeImpl fromNode, ScaleGateNodeImpl newNode, final ScaleGateTuple obj, final int level) {
        while (true) {
            ScaleGateNodeImpl next = fromNode.getNext(level);
            if (next == tail || next.getTuple().compareTo(obj) > 0) {
                newNode.setNext(level, next);
                if (fromNode.trySetNext(level, next, newNode)) {
                    break;
                }
            } else {
                fromNode = next;
            }
        }
    }

    private ScaleGateNodeImpl internalAddTuple(ScaleGateTuple obj, int inputID) {
        int levels = 1;
        WriterThreadLocalData ln = getWriterLocal(inputID);

        while (ln.rand.nextBoolean() && levels < maxlevels)
            levels++;

        ScaleGateNodeImpl newNode = new ScaleGateNodeImpl(levels, obj, ln, inputID);
        ScaleGateNodeImpl[] update = ln.update;
        ScaleGateNodeImpl curNode = update[maxlevels - 1];

        for (int i = maxlevels - 1; i >= 0; i--) {
            ScaleGateNodeImpl tx = curNode.getNext(i);

            while (tx != tail && tx.getTuple().compareTo(obj) < 0) {
                curNode = tx;
                tx = curNode.getNext(i);
            }

            update[i] = curNode;
        }

        for (int i = 0; i < levels; i++) {
            this.insertNode(update[i], newNode, obj, i);
        }

        ln.written = newNode;

        lastWritersTimestamp[inputID] = obj.getTimestamp();

        return newNode;
    }

    private long getMinWritersTimestamp() {
        long min = lastWritersTimestamp[0];
        for (int i = 1; i < lastWritersTimestamp.length; i++) {
            if (lastWritersTimestamp[i] < min) {
                min = lastWritersTimestamp[i];
            }
        }
        return min;
    }

    private WriterThreadLocalData getWriterLocal(int writerID) {
        return writertld[writerID];
    }

    private ReaderThreadLocalData getReaderLocal(int readerID) {
        return readertld[readerID];
    }

    class WriterThreadLocalData {
        // reference to the last written node by the respective writer
        volatile ScaleGateNodeImpl written;
        ScaleGateNodeImpl[] update;
        final Random rand;

        WriterThreadLocalData(ScaleGateNodeImpl localHead) {
            update = new ScaleGateNodeImpl[maxlevels];
            written = localHead;
            for (int i = 0; i < maxlevels; i++) {
                update[i] = localHead;
            }
            rand = new Random();
        }
    }

    protected class ReaderThreadLocalData {
        ScaleGateNodeImpl localHead;

        ReaderThreadLocalData(ScaleGateNodeImpl lhead) {
            localHead = lhead;
        }
    }
}
