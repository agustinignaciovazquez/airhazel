package ar.edu.itba.pod.api.collator;

import com.hazelcast.mapreduce.Collator;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class PercentageFlightCollator implements Collator<Map.Entry<String, Double>, Set<Map.Entry<String, Double>>> {

    private int n;

    public PercentageFlightCollator(int n) {
        this.n = n;
    }

    @Override
    public Set<Map.Entry<String, Double>> collate(Iterable<Map.Entry<String, Double>> values) {
        Set<Map.Entry<String, Double>> result = new TreeSet<>((o1, o2) -> {
            if (!o1.getValue().equals(o2.getValue())){
                return (o2.getValue().compareTo(o1.getValue()));
            }
            return o1.getKey().compareTo(o2.getKey());
        });

        for (Map.Entry<String, Double> value : values) {
            result.add(value);
        }

        return result.stream().limit(n).collect(Collectors.toSet());
    }
}