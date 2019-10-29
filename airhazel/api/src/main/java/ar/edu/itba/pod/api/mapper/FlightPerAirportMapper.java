package ar.edu.itba.pod.api.mapper;

import ar.edu.itba.pod.api.model.Airport;
import ar.edu.itba.pod.api.model.Flight;
import ar.edu.itba.pod.api.model.Pair;
import ar.edu.itba.pod.api.model.enums.FlightClassification;
import ar.edu.itba.pod.api.model.enums.field.FlightField;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class FlightPerAirportMapper implements Mapper<String, Flight, String, Long>, HazelcastInstanceAware {
    private transient HazelcastInstance instance;
    private final String randomString;
    private static Logger LOGGER = LoggerFactory.getLogger(FlightPerAirportMapper.class);

    public FlightPerAirportMapper(String randomString) {
        this.randomString = randomString;
    }

    @Override
    public void map(String s, Flight flight, Context<String ,Long> context) {
        IMap<String, Airport> airports = instance.getMap("g13-airports-"+randomString);
        switch (flight.getFlightType()){
            case DEPARTURE:
                EmitIfAirportIsPresent(airports, flight, FlightField.ORIGIN_OACI,context);
                break;
            case LANDING:
                EmitIfAirportIsPresent(airports, flight, FlightField.DESTINATION_OACI,context);
                break;
            default:
                LOGGER.debug("FlightPerAirportMapper arg: {}", flight.getFlightType());
                throw new IllegalStateException();
        }
    }

    private void EmitIfAirportIsPresent(IMap<String, Airport> airports, Flight flight, FlightField flightField, Context<String ,Long> context){
        Optional<Airport> airport = Optional.ofNullable(airports.get(flight.getField(flightField)));
        airport.ifPresent(ap -> context.emit(flight.getField(flightField), 1L));
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        instance = hazelcastInstance;
    }
}
