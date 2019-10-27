package ar.edu.itba.pod.api.collator;

import ar.edu.itba.pod.api.model.Pair;
import com.hazelcast.mapreduce.Collator;

import java.util.*;

public class FlightPerPairMinimumCollator implements Collator<Map.Entry<Pair<String, String>, Long>, Set<Map.Entry<Pair<String, String>, Long>>> {
    private Long minimum;

    public FlightPerPairMinimumCollator(Long minimum) {
        this.minimum = minimum;
    }

    @Override
    public Set<Map.Entry<Pair<String, String>, Long>> collate(Iterable<Map.Entry<Pair<String, String>, Long>> values) {
        Set<Map.Entry<Pair<String, String>, Long>> results = new TreeSet<>((o1, o2) -> {
            if (!o1.getValue().equals(o2.getValue())){
                return (o2.getValue().compareTo(o1.getValue()));
            }
            String s1 = o1.getKey().getKey() + o1.getKey().getValue();
            String s2 = o2.getKey().getKey() + o2.getKey().getValue();
            return s1.compareTo(s2);
        });

        for (Map.Entry<Pair<String, String>, Long> value : values) {
            if (value.getValue() >= minimum) {
                results.add(value);
            }
        }

        return results;
    }

}
