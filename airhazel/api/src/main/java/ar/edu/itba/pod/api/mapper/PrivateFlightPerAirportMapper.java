package ar.edu.itba.pod.api.mapper;

import ar.edu.itba.pod.api.model.Flight;
import ar.edu.itba.pod.api.model.enums.FlightClass;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.Arrays;
import java.util.List;


public class PrivateFlightPerAirportMapper implements Mapper<String, Flight, String, Boolean>{
    public PrivateFlightPerAirportMapper(){
    }

    @Override
    public void map(String s, Flight flight, Context<String, Boolean> context) {
        Boolean aBoolean = isPrivateFlight(flight);
        context.emit(flight.getOriginOaci(), aBoolean);
        context.emit(flight.getDestinationOaci(), aBoolean);
    }

    private boolean isPrivateFlight(Flight flight){
        List<FlightClass> flightClasses = Arrays.asList(FlightClass.PRIVATE_INTERNATIONAL,FlightClass.PRIVATE_NATIONAL);
        return flightClasses.stream().anyMatch(fc -> fc == flight.getFlightClass());
    }

}

