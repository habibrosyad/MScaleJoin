package mscalejoin;

import mscalejoin.common.Tuple;

public class Buffer {
    private BufferNode head;
    private BufferNode tail;
    private LocalData[] readers;

    public Buffer(int numberOfThreads) {
        head = new BufferNode(null);
        tail = head;

        readers = new LocalData[numberOfThreads];

        for (int i = 0; i < numberOfThreads; i++) {
            readers[i] = new LocalData(head);
        }

        head = null;
    }

    void addTuple(Tuple tuple) {
        BufferNode newNode = new BufferNode(tuple);

        while (true) {
            if (tail.trySetNext(newNode)) {
                tail = newNode;
                break;
            }
        }
    }

    Tuple getNextReadyTuple(int id, int counter) {
        BufferNode next = readers[id].localHead.getNext();

        if (next != null && next.getTuple().compareCounterTo(counter) <= 0) {
            readers[id].localHead = next;
            return next.getTuple();
        }

        return null;
    }

    private class LocalData {
        BufferNode localHead;

        LocalData(BufferNode localHead) {
            this.localHead = localHead;
        }
    }
}
