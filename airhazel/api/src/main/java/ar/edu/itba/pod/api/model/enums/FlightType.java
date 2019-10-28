package ar.edu.itba.pod.api.model.enums;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum FlightType {
    DEPARTURE, LANDING;

    private static Logger LOGGER = LoggerFactory.getLogger(FlightType.class);

    public static FlightType fromSpanishString(final String str) {
        String uppercase = str.toUpperCase();
        switch (uppercase){
            case "DESPEGUE":
                return FlightType.DEPARTURE;
            case "ATERRIZAJE":
                return FlightType.LANDING;
            default:
                LOGGER.debug("FlightType arg: {}", str);
                throw new IllegalArgumentException();
        }
    }

    public static FlightType fromString(final String str) {
        try{
            return valueOf(str.toUpperCase());
        }catch (IllegalArgumentException exc){
            return fromSpanishString(str);
        }
    }

    public String getLowerName() {
        return name().toLowerCase();
    }

    public String getName() {
        return this.name();
    }
}
