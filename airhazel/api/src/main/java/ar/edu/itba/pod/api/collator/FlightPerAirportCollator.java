package ar.edu.itba.pod.api.collator;

import com.hazelcast.mapreduce.Collator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class FlightPerAirportCollator implements Collator<Map.Entry<String, Long>, List<Map.Entry<String, Long>>> {
    @Override
    public List<Map.Entry<String, Long>> collate(Iterable<Map.Entry<String, Long>> values) {
        List<Map.Entry<String, Long>> result = new ArrayList<>();
        for (Map.Entry<String, Long> value : values) {
            result.add(value);
        }

        /* Order by descending number of movements */
        Comparator<Map.Entry<String, Long>> c = Comparator.comparing(Map.Entry::getValue, Comparator.reverseOrder());
        /* Order by OACI */
        c = c.thenComparing(Map.Entry::getKey);
        result.sort(c);
        return result;
    }
}
