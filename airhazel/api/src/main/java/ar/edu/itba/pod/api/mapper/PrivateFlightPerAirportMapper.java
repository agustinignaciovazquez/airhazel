package ar.edu.itba.pod.api.mapper;

import ar.edu.itba.pod.api.model.Airport;
import ar.edu.itba.pod.api.model.Flight;
import ar.edu.itba.pod.api.model.enums.FlightClass;

import ar.edu.itba.pod.api.model.enums.field.FlightField;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;


public class PrivateFlightPerAirportMapper implements Mapper<String, Flight, String, Boolean>, HazelcastInstanceAware {

    private transient HazelcastInstance instance;

    @Override
    public void map(String s, Flight flight, Context<String, Boolean> context) {
        IMap<String, Airport> airports = instance.getMap("airports");
        Boolean isPrivate = isPrivateFlight(flight);
        EmitIfAirportIsPresent(airports, flight, FlightField.ORIGIN_OACI,isPrivate,context);
        EmitIfAirportIsPresent(airports, flight, FlightField.DESTINATION_OACI,isPrivate,context);

    }

    private void EmitIfAirportIsPresent(IMap<String, Airport> airports, Flight flight, FlightField flightField, Boolean isPrivate, Context<String ,Boolean> context){
        Optional<Airport> airport = Optional.ofNullable(airports.get(flight.getField(flightField)));
        airport.ifPresent(ap -> context.emit(flight.getField(flightField), isPrivate));
    }

    private boolean isPrivateFlight(Flight flight){
        List<FlightClass> flightClasses = Arrays.asList(FlightClass.PRIVATE_INTERNATIONAL,FlightClass.PRIVATE_NATIONAL);
        return flightClasses.stream().anyMatch(fc -> fc.equals(flight.getFlightClass()));
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        instance = hazelcastInstance;
    }
}

