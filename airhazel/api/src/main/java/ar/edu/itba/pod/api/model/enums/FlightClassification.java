package ar.edu.itba.pod.api.model.enums;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum FlightClassification {
    CABOTAGE, INTERNATIONAL, NOT_AVAILABLE;

    private static Logger LOGGER = LoggerFactory.getLogger(FlightClassification.class);

    public static FlightClassification fromSpanishString(final String str) {
        String uppercase = str.toUpperCase();
        switch (uppercase){
            case "CABOTAJE":
                return FlightClassification.CABOTAGE;
            case "INTERNACIONAL":
                return FlightClassification.INTERNATIONAL;
            case "N/A":
                return FlightClassification.NOT_AVAILABLE;
            default:
                LOGGER.debug("FlightClassification arg: {}", str);
                throw new IllegalArgumentException();
        }
    }

    public static FlightClassification fromString(final String str) {
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
