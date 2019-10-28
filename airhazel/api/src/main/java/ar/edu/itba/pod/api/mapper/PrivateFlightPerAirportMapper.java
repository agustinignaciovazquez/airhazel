package ar.edu.itba.pod.api.mapper;

import ar.edu.itba.pod.api.model.Airport;
import ar.edu.itba.pod.api.model.Flight;
import ar.edu.itba.pod.api.model.Pair;
import ar.edu.itba.pod.api.model.enums.FlightClass;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class PrivateFlightPerAirportMapper implements Mapper<String, Flight, String, Boolean>{

    @Override
    public void map(String s, Flight flight, Context<String, Boolean> context) {
        Boolean privateFlight = isPrivateFlight(flight);
        context.emit(flight.getOriginOaci(), privateFlight);
        context.emit(flight.getDestinationOaci(), privateFlight);
    }

    private boolean isPrivateFlight(Flight flight) {
        List<FlightClass> flightClasses = Arrays.asList(FlightClass.PRIVATE_INTERNATIONAL,FlightClass.PRIVATE_NATIONAL);
        return flightClasses.stream().anyMatch(fc -> fc == flight.getFlightClass());
    }
}

