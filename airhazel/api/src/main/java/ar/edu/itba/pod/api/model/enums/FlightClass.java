package ar.edu.itba.pod.api.model.enums;

public enum FlightClass {
    NOT_REGULAR, REGULAR, PRIVATE_INTERNATIONAL, PRIVATE_NATIONAL;

    public static FlightClass fromSpanishString(final String str) {
        String uppercase = str.toUpperCase();
        switch (uppercase){
            case "NO REGULAR":
                return FlightClass.NOT_REGULAR;
            case "REGULAR":
                return FlightClass.REGULAR;
            case "VUELO PRIVADO CON MATRÍCULA EXTRANJERA":
                return FlightClass.PRIVATE_INTERNATIONAL;
            case "VUELO PRIVADO CON MATRÍCULA NACIONAL":
                return FlightClass.PRIVATE_NATIONAL;
            default:
                throw new IllegalArgumentException();
        }
    }

    public static FlightClass fromString(final String str) {
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
