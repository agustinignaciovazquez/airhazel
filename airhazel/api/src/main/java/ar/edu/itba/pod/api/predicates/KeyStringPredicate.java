package ar.edu.itba.pod.api.predicates;

import com.hazelcast.mapreduce.KeyPredicate;

public class KeyStringPredicate implements KeyPredicate<String> {

    private String str;

    public KeyStringPredicate(String str) {
        this.str = str;
    }

    @Override
    public boolean evaluate(String s) {
        return s.equals(str);
    }
}
