package ar.edu.itba.pod.api.collator;

import ar.edu.itba.pod.api.util.Constants;
import com.hazelcast.map.impl.MapEntrySimple;
import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CalculatePercentageAirlineCollator implements Collator<Map.Entry<String, Long>, List<Map.Entry<String, Double>>>, Constants {

    private int n;

    public CalculatePercentageAirlineCollator(int n) {
        this.n = n;
    }

    @Override
    public List<Map.Entry<String, Double>> collate(Iterable<Map.Entry<String, Long>> values) {
        Long totalAirlines = getTotalFromKey(values);
        Set<Map.Entry<String, Double>> result = new TreeSet<>((o1, o2) -> {
            if (!o1.getValue().equals(o2.getValue())){
                return (o2.getValue().compareTo(o1.getValue()));
            }
            return o1.getKey().compareTo(o2.getKey());
        });

        Optional<Map.Entry<String,Double>> others = Optional.empty();
        for (Map.Entry<String, Long> value : values) {
            if(!TOTAL_AIRLINE_COUNT.equals(value.getKey())) {
                Map.Entry<String,Double> entry =new MapEntrySimple<>(value.getKey(), 100.0 * value.getValue()/totalAirlines);
                //Ignore others value so its not sorted
                if(!OTHERS_AIRLINE_COUNT.equals(entry.getKey())){
                    result.add(entry);
                }else{
                    //Store the others entry for further use
                    others = Optional.ofNullable(entry);
                }
            }

        }

        List<Map.Entry<String, Double>> r = result.stream().limit(n).collect(Collectors.toList());
        others.ifPresent(r::add);
        return r;
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