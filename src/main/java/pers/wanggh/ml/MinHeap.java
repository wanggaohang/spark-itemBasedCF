package pers.wanggh.ml;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

/**
 * Created by wanggh on 16/8/20.
 */
public class MinHeap implements Serializable {
    private int max_num;
    private PriorityQueue<Tuple2<Long, Float>> heap = new PriorityQueue<>(new SerializableComparator());

    class SerializableComparator implements Comparator<Tuple2<Long, Float>>,Serializable{
        @Override
        public int compare(Tuple2<Long, Float> o1, Tuple2<Long, Float> o2) {
            return o1._2().compareTo(o2._2());
        }
    }

    public MinHeap(int max_num) {
        this.max_num = max_num;
    }

    public MinHeap add(Tuple2<Long, Float> t) {
        if (heap.size() < max_num) {
            heap.add(t);
        } else {
            float min = heap.peek()._2;
            if (t._2 > min) {
                heap.poll();
                heap.add(t);
            }
        }
        return this;
    }

    public PriorityQueue<Tuple2<Long, Float>> getHeap(){
        return heap;
    }

    public MinHeap addAll(MinHeap new_heap){
        for(Tuple2<Long, Float>t : new_heap.getHeap()){
            add(t);
        }
        return this;
    }

    public List<Tuple2<Long, Float>> getSortedItems() {
        return heap
                .stream()
                .sorted((a, b) -> b._2.compareTo(a._2))
                .collect(Collectors.toList());
    }

    public static void main(String[] args) {
        MinHeap heap = new MinHeap(5);
        heap.add(new Tuple2<>(1l, 0.0f));
        heap.add(new Tuple2<>(1l, 3.0f));
        heap.add(new Tuple2<>(1l, 2.0f));
        heap.add(new Tuple2<>(1l, 1.0f));
        heap.add(new Tuple2<>(1l, 10.0f));
        heap.add(new Tuple2<>(1l, 18.0f));
        heap.add(new Tuple2<>(1l, 7.0f));
        heap.add(new Tuple2<>(1l, 11.0f));
        heap.add(new Tuple2<>(1l, 6.0f));

        MinHeap heap2 = new MinHeap(5);
        heap2.add(new Tuple2<>(1l, 0.0f));
        heap2.add(new Tuple2<>(1l, 30.0f));
        heap2.add(new Tuple2<>(1l, 28.0f));
        heap2.add(new Tuple2<>(1l, 1.0f));
        heap2.add(new Tuple2<>(1l, 10.0f));
        heap2.add(new Tuple2<>(1l, 18.0f));

        heap2.addAll(heap);

        System.out.println(heap2.getSortedItems());
    }
}
