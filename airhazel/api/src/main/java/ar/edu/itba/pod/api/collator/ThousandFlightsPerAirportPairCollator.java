package ar.edu.itba.pod.api.collator;

import ar.edu.itba.pod.api.model.Pair;
import com.hazelcast.map.impl.MapEntrySimple;
import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.stream.Collectors;

public class ThousandFlightsPerAirportPairCollator implements Collator<Map.Entry<String, Long>,
        List<Map.Entry<Pair<String, String>, Long>>> {
    @Override
    public List<Map.Entry<Pair<String, String>, Long>> collate(Iterable<Map.Entry<String, Long>> values) {
        Map<Long, List<String>> pairs = new HashMap<>();
        for (Map.Entry<String, Long> value : values) {
            Long key = (value.getValue() / 1000) * 1000;
            if(key > 0)
                addAirportToMap(pairs,key,value.getKey());
        }

        Set<Map.Entry<Pair<String, String>, Long>> result = new TreeSet<>((o1, o2) -> {
            if (!o2.getValue().equals(o1.getValue())){
                return (o2.getValue().compareTo(o1.getValue()));
            }
            String s1 = o1.getKey().getKey() + o1.getKey().getValue();
            String s2 = o2.getKey().getKey() + o2.getKey().getValue();
            return s1.compareTo(s2);
        });

        for (Map.Entry<Long, List<String>> value : pairs.entrySet()) {
           for(String airport1: value.getValue()){
               for(String airport2: value.getValue()){
                   Pair<String,String> p;
                   Integer cmp =airport1.compareTo(airport2);
                   if(cmp == 0)
                       continue;

                   if (cmp < 0)
                       p = new Pair<>(airport1,airport2);
                   else
                       p = new Pair<>(airport2,airport1);

                   result.add(new MapEntrySimple<>(p,value.getKey()));
               }
           }
        }

        return result.stream().collect(Collectors.toList());
    }
    private static void addAirportToMap(Map<Long,List<String>> map, Long key, String airport) {
            map.computeIfAbsent(key, k -> new ArrayList<>());
            map.get(key).add(airport);
    }
}
