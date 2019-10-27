package ar.edu.itba.pod.api.collator;

import com.hazelcast.mapreduce.Collator;

import java.util.*;

public class FlightPerAirportCollator implements Collator<Map.Entry<String, Long>, Set<Map.Entry<String, Long>>> {
    @Override
    public Set<Map.Entry<String, Long>> collate(Iterable<Map.Entry<String, Long>> values) {
        Set<Map.Entry<String, Long>> result = new TreeSet<>((o1, o2) -> {
            if (!o1.getValue().equals(o2.getValue())){
                return (o2.getValue().compareTo(o1.getValue()));
            }
            return o1.getKey().compareTo(o2.getKey());
        });

        for (Map.Entry<String, Long> value : values) {
            result.add(value);
        }
        return result;
    }
}
