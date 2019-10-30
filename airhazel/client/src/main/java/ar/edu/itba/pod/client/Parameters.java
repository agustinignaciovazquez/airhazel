package ar.edu.itba.pod.client;

public class Parameters {

    private String query = System.getProperty("query", "1");
    private String dInPath = System.getProperty("inPath", ".");
    private String dOutPath = System.getProperty("outPath", ".");
    private String timeOutPath = System.getProperty("timeOutPath", "time.txt");
    private String oaci = System.getProperty("oaci", "SAEZ");
    private String n = System.getProperty("n", "5");
    private String min = System.getProperty("min", "1000");
    private String addresses = System.getProperty("addresses", "127.0.0.1");

    public String getQuery() {
        return query;
    }

    public String getFlightsInPath() {
        return dInPath+"/movimientos.csv";
    }

    public String getAirportsInPath() {
        return dInPath+"/aeropuertos.csv";
    }

    public String getOutPath() {
        return dOutPath+"/query"+query+".csv";
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