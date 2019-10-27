package ar.edu.itba.pod.api.reducer;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class CountReducerFactory<T> implements ReducerFactory<T, Long, Long> {
    @Override
    public Reducer<Long, Long> newReducer(T t) {
        return new MovementCountReducer();
    }

    private class MovementCountReducer extends Reducer<Long, Long> {

        private AtomicLong sum;

        @Override
        public void beginReduce () {
            sum = new AtomicLong(0);
        }

        @Override
        public void reduce(Long value) {
            sum.getAndAdd(value);
        }

        @Override
        public Long finalizeReduce() {
            return sum.get();
        }
    }
}
