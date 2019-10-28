package ar.edu.itba.pod.api.combiner;

import ar.edu.itba.pod.api.model.Pair;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class PrivateFlightCombinerFactory<T> implements CombinerFactory<T, Boolean, Pair<Long, Long>> {

    @Override
    public Combiner<Boolean, Pair<Long, Long>> newCombiner(T key) {
        return new PrivateFlightCombiner();
    }

    private class PrivateFlightCombiner extends Combiner<Boolean, Pair<Long, Long>> {

        private AtomicLong privateFlights = new AtomicLong(0);
        private AtomicLong totalFlights = new AtomicLong(0);

        @Override
        public void combine(Boolean privateFlight) {
            totalFlights.addAndGet(1L);
            if (privateFlight)
                privateFlights.addAndGet(1L);
        }

        @Override
        public Pair<Long, Long> finalizeChunk() {
            return new Pair<>(privateFlights.get(), totalFlights.get());
        }

        @Override
        public void reset() {
            privateFlights = new AtomicLong(0);
            totalFlights = new AtomicLong(0);
        }
    }

}
