package ar.edu.itba.pod.api.collator;

import ar.edu.itba.pod.api.util.Constants;
import com.hazelcast.map.impl.MapEntrySimple;
import com.hazelcast.mapreduce.Collator;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CalculatePercentageFlightCollator implements Collator<Map.Entry<String, Long>, Set<Map.Entry<String, Double>>>, Constants {

    private int n;

    public CalculatePercentageFlightCollator(int n) {
        this.n = n;
    }

    @Override
    public Set<Map.Entry<String, Double>> collate(Iterable<Map.Entry<String, Long>> values) {
        Long totalAirlines = getTotalFromKey(values);
        Set<Map.Entry<String, Double>> result = new TreeSet<>((o1, o2) -> {
            if (!o1.getValue().equals(o2.getValue())){
                return (o1.getValue().compareTo(o2.getValue()));
            }
            return o1.getKey().compareTo(o2.getKey());
        });

        for (Map.Entry<String, Long> value : values) {
            result.add(new MapEntrySimple<>(value.getKey(), 100.0 * value.getValue()/totalAirlines));
        }

        return result.stream().limit(n).collect(Collectors.toSet());
    }

    private Long getTotalFromKey(Iterable<Map.Entry<String, Long>> iterable){
        Optional<Map.Entry<String,Long>> total = StreamSupport.stream(iterable.spliterator(), false)
                .filter(s -> TOTAL_AIRLINE_COUNT.equals(s.getKey()))
                .findFirst();
        if(!total.isPresent())
            throw new IllegalStateException();
        return total.get().getValue();
    }
}