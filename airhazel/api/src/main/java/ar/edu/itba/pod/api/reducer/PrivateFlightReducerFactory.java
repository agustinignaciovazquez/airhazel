package ar.edu.itba.pod.api.reducer;

import ar.edu.itba.pod.api.model.Pair;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class PrivateFlightReducerFactory implements ReducerFactory<String, Pair<Long, Long>, Double> {

    @Override
    public Reducer<Pair<Long, Long>, Double> newReducer(String pair) {
        return new InternationalMovementsPercentageReducer();
    }

    private class InternationalMovementsPercentageReducer extends Reducer<Pair<Long, Long>, Double> {

        private AtomicLong privateFlights;
        private AtomicLong totalFlights;

        @Override
        public void beginReduce () {
            privateFlights = new AtomicLong(0);
            totalFlights = new AtomicLong(0);
        }

        @Override
        public void reduce(Pair<Long, Long> pair) {
            privateFlights.addAndGet(pair.getKey());
            totalFlights.addAndGet(pair.getValue());
        }

        @Override
        public Double finalizeReduce() {
           return 100.0 * (double) privateFlights.get() / (double) totalFlights.get();
        }
    }
}
