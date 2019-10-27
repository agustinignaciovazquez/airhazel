package ar.edu.itba.pod.api.combiner;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class SumCombinerFactory<T> implements CombinerFactory<T, Long, Long> {
    @Override
    public Combiner<Long, Long> newCombiner(T t) {
        return new SumCombiner();
    }

    private class SumCombiner extends Combiner<Long, Long> {

        private AtomicLong sum = new AtomicLong(0);

        @Override
        public void reset() {
            sum = new AtomicLong(0);
        }

        @Override
        public void combine(Long aLong) {
            sum.addAndGet(aLong);
        }

        @Override
        public Long finalizeChunk() {
            return sum.get();
        }
    }
}
