package ar.edu.itba.pod.api.predicates;

import com.hazelcast.mapreduce.KeyPredicate;

import java.util.List;

public class KeyStringListPredicate implements KeyPredicate<String> {
    private List<String> strings;

    public KeyStringListPredicate(List<String> strings) {
        this.strings = strings;
    }

    @Override
    public boolean evaluate(String str) {
        return strings.stream().anyMatch(s -> s.equals(str));
    }
}
