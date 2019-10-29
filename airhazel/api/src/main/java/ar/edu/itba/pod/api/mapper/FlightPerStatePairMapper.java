package ar.edu.itba.pod.api.mapper;

import ar.edu.itba.pod.api.model.Airport;
import ar.edu.itba.pod.api.model.Flight;
import ar.edu.itba.pod.api.model.Pair;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.Optional;

public class FlightPerStatePairMapper implements Mapper<String, Flight, Pair<String, String>, Long>, HazelcastInstanceAware {
    private transient HazelcastInstance instance;
    private final String randomString;

    public FlightPerStatePairMapper(String randomString) {
        this.randomString = randomString;
    }

    @Override
    public void map(String s, Flight flight, Context<Pair<String, String>, Long> context) {
        IMap<String, Airport> airports = instance.getMap("g13-airports-"+randomString);

        Optional<Airport> firstAirport = Optional.ofNullable(airports.get(flight.getDestinationOaci()));
        Optional<Airport> secondAirport = Optional.ofNullable(airports.get(flight.getOriginOaci()));
        if (firstAirport.isPresent() && secondAirport.isPresent()){
            String state1 = firstAirport.get().getState();
            String state2 = secondAirport.get().getState();

            Integer cmp = state1.compareTo(state2);
            //Ignore flights to the same state
            if (cmp == 0)
               return;

            //Make sure only one pair of states it's sent
            if (cmp < 0){
                context.emit(new Pair<>(state1, state2), 1L);
            }else{
                context.emit(new Pair<>(state2, state1), 1L);
            }
        }
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        instance = hazelcastInstance;
    }
}