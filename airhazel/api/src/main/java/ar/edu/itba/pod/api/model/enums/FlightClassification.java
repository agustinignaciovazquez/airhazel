package ar.edu.itba.pod.api.model.enums;

public enum FlightClassification {
    CABOTAGE, INTERNATIONAL, NOT_AVAILABLE;

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
