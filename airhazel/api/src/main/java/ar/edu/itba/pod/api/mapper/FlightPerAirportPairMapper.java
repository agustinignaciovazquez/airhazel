package ar.edu.itba.pod.api.mapper;

import ar.edu.itba.pod.api.model.Airport;
import ar.edu.itba.pod.api.model.Flight;
import ar.edu.itba.pod.api.model.Pair;
import ar.edu.itba.pod.api.model.enums.field.FlightField;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class FlightPerAirportPairMapper implements Mapper<String, Flight, Pair<String, String>, Long> {
    private static Logger LOGGER = LoggerFactory.getLogger(FlightPerAirportPairMapper.class);

    @Override
    public void map(String s, Flight flight, Context<Pair<String, String>, Long> context) {
        switch (flight.getFlightType()){
            case DEPARTURE:
                EmitAirportPair(flight,FlightField.ORIGIN_OACI,context);
                break;
            case LANDING:
                EmitAirportPair(flight, FlightField.DESTINATION_OACI,context);
                break;
            default:
                LOGGER.debug("FlightPerAirportMapper arg: {}", flight.getFlightType());
                throw new IllegalStateException();
        }
    }

    private void EmitAirportPair(Flight flight, FlightField flightField, Context<Pair<String, String>, Long> context){
        //context.emit(new Pair<>(flight.getOriginOaci(),flight.getDestinationOaci()), 1L);
        String originOaci = flight.getOriginOaci();
        String destinationOaci = flight.getDestinationOaci();

        Integer cmp = originOaci.compareTo(destinationOaci);
        //Ignore flights to the same state
        if (cmp == 0)
            return;

        if (cmp > 0){
            //ACA QUE HAGO? context.emit(new Pair<>(originOaci, destinationOaci), 1L);

        }else{
            // ACA TMB context.emit(new Pair<>(destinationOaci, originOaci), 1L);
        }
    }

    private FlightField getOppositeField(FlightField flightField) {
        if (flightField == FlightField.ORIGIN_OACI)
            return FlightField.DESTINATION_OACI;
        if (flightField == FlightField.DESTINATION_OACI)
            return FlightField.ORIGIN_OACI;
        throw new IllegalArgumentException();
    }
}
