package ar.edu.itba.pod.api.mapper;

import ar.edu.itba.pod.api.model.Flight;
import ar.edu.itba.pod.api.model.enums.field.FlightField;
import ar.edu.itba.pod.api.model.enums.FlightType;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class FlightTypePerAirportMapper implements Mapper<String, Flight, String, Long> {
    private FlightType flightType;
    private FlightField flightField;

    public FlightTypePerAirportMapper(FlightType flightType, FlightField flightField) {
        this.flightType = flightType;
        this.flightField = flightField;
    }

    @Override
    public void map(String s, Flight flight, Context<String, Long> context) {
        if (flight.getFlightType() == flightType){
            context.emit(flight.getField(flightField), 1L);
        }
    }
}
