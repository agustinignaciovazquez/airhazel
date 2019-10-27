package ar.edu.itba.pod.api.mapper;

import ar.edu.itba.pod.api.model.Flight;
import ar.edu.itba.pod.api.model.Pair;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class FlightPerAirport implements Mapper<String, Flight, Pair<String, String>, Pair<Long, Long>> {
    @Override
    public void map(String s, Flight flight, Context<Pair<String, String>, Pair<Long, Long>> context) {
        
    }
}
