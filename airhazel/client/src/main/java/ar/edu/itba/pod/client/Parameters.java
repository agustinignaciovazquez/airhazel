package ar.edu.itba.pod.client;

public class Parameters {

    private String query = System.getProperty("query", "5");
    private String flightsInPath = System.getProperty("flightsInPath", "/Users/agustinvazquez/Downloads/pod/movimientos.csv");
    private String airportsInPath = System.getProperty("airportsInPath", "/Users/agustinvazquez/Downloads/pod/aeropuertos.csv");
    private String outPath = System.getProperty("outPath", "query5.csv");
    private String timeOutPath = System.getProperty("timeOutPath", "time.txt");
    private String oaci = System.getProperty("oaci", "SAEZ");
    private String n = System.getProperty("n", "5");
    private String min = System.getProperty("min", "1000");
    private String addresses = System.getProperty("addresses", "127.0.0.1");

    public String getQuery() {
        return query;
    }

    public String getFlightsInPath() {
        return flightsInPath;
    }

    public String getAirportsInPath() {
        return airportsInPath;
    }

    public String getOutPath() {
        return outPath;
    }

    public String getTimeOutPath() {
        return timeOutPath;
    }

    public String getOaci() {
        return oaci;
    }

    public Integer getN() {
        return Integer.valueOf(n);
    }

    public Long getMin() {
        return new Long(min);
    }

    public String getAddresses() {
        return addresses;
    }
}