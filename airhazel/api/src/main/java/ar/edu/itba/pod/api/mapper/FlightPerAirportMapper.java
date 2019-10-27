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

public class FlightPerAirportMapper implements Mapper<String, Flight, String, Long>, HazelcastInstanceAware {
    private transient HazelcastInstance instance;

    @Override
    public void map(String s, Flight flight, Context<String ,Long> context) {
        switch (flight.getFlightType()){
            case DEPARTURE:
                EmitIfAirportIsPresent(flight.getOriginOaci(),context);
            case LANDING:
                EmitIfAirportIsPresent(flight.getDestinationOaci(),context);
            default:
                throw new IllegalStateException();
        }
    }

    private void EmitIfAirportIsPresent(String airportOaci, Context<String ,Long> context){
        IMap<String, Airport> airports = instance.getMap("airports");
        Optional<Airport> airport = Optional.ofNullable(airports.get(airportOaci));
        airport.ifPresent(ap -> context.emit(airportOaci, 1L));
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        instance = hazelcastInstance;
    }
}
