package ar.edu.itba.pod.api.collator;

import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.stream.Collectors;

public class FlightsCollator implements Collator<Map.Entry<String, Long>, List<Map.Entry<String, Long>>> {
    private Integer n;

    public FlightsCollator(int n) {
        this.n = n;
    }

    public FlightsCollator(){
        this.n = null;
    }

    @Override
    public List<Map.Entry<String, Long>> collate(Iterable<Map.Entry<String, Long>> values) {
        Set<Map.Entry<String, Long>> result = new TreeSet<>((o1, o2) -> {
            //Descendant by quantity of flights
            if (!o1.getValue().equals(o2.getValue())){
                return (o2.getValue().compareTo(o1.getValue()));
            }
            return o1.getKey().compareTo(o2.getKey());
        });

        for (Map.Entry<String, Long> value : values) {
            result.add(value);
        }

        if(n == null)
            return new ArrayList<>(result);

        return result.stream().limit(n).collect(Collectors.toList());
    }
}
