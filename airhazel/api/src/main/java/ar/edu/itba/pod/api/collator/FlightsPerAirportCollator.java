package ar.edu.itba.pod.api.collator;

import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.stream.Collectors;

public class FlightsPerAirportCollator implements Collator<Map.Entry<String, Long>, Set<Map.Entry<String, Long>>> {
    private Integer n;

    public FlightsPerAirportCollator(int n) {
        this.n = n;
    }

    public FlightsPerAirportCollator(){
        this.n = null;
    }

    @Override
    public Set<Map.Entry<String, Long>> collate(Iterable<Map.Entry<String, Long>> values) {
        Set<Map.Entry<String, Long>> result = new TreeSet<>((o1, o2) -> {
            //Descendant by quantity of flights
            if (!o1.getValue().equals(o2.getValue())){
                return (o1.getValue().compareTo(o2.getValue()));
            }
            return o1.getKey().compareTo(o2.getKey());
        });

        for (Map.Entry<String, Long> value : values) {
            result.add(value);
        }

        if(n == null)
            return result;

        return result.stream().limit(n).collect(Collectors.toSet());
    }
}
