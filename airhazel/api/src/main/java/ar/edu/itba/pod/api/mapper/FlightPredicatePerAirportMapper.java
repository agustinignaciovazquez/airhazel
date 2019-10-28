package ar.edu.itba.pod.api.mapper;

import ar.edu.itba.pod.api.model.Flight;
import ar.edu.itba.pod.api.model.predicates.FlightPredicate;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;


public class FlightPredicatePerAirportMapper implements Mapper<String, Flight, String, Boolean>{
    private FlightPredicate flightPredicate;
    public FlightPredicatePerAirportMapper(FlightPredicate flightPredicate){
        this.flightPredicate = flightPredicate;
    }

    @Override
    public void map(String s, Flight flight, Context<String, Boolean> context) {
        Boolean aBoolean = flightPredicate.FlightPredicate(flight);
        context.emit(flight.getOriginOaci(), aBoolean);
        context.emit(flight.getDestinationOaci(), aBoolean);
    }
}

