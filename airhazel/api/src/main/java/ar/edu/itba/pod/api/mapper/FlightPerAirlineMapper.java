package ar.edu.itba.pod.api.mapper;

import ar.edu.itba.pod.api.model.Flight;
import ar.edu.itba.pod.api.model.predicates.FlightPredicate;
import ar.edu.itba.pod.api.util.Constants;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;


public class FlightPerAirlineMapper implements Mapper<String, Flight, String, Long>, Constants {
    @Override
    public void map(String s, Flight flight, Context<String, Long> context) {
        if(flight.getAirlineName().isEmpty() || flight.getAirlineName().equals("N/A"))
            context.emit(OTHERS_AIRLINE_COUNT, 1L);
        else
            context.emit(flight.getAirlineName(), 1L);
        context.emit(TOTAL_AIRLINE_COUNT,1L);
    }
}