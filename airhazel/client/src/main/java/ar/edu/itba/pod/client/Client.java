package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.query.*;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

public class Client {
    private static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args)  {
        LOGGER.info("airhazel client Starting ...");

        ClientConfig clientConfig = new ClientConfig();
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        clientConfig.getGroupConfig().setName("g13-tp2").setPassword("123456aa");

        Parameters p = new Parameters();
        networkConfig.addAddress(p.getAddresses().split(","));

        HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);

        File airportsFile = new File(p.getAirportsInPath());
        File flightsFile = new File(p.getFlightsInPath());

        Query query;

        switch (p.getQuery()){
            case "1":
                query = new FlightsPerAirportQuery(hazelcastInstance, airportsFile, flightsFile);
                break;
            case "2":
                query = new CabotageFlightsPerAirlineQuery(hazelcastInstance,airportsFile,flightsFile,p.getN());
                break;
            case "3":

                break;
            case "4":
                query = new FlightsPerOriginAirportQuery(hazelcastInstance,airportsFile,flightsFile, p.getOaci(), p.getN());
                break;
            case "5":
                query = new PrivateFlightsPerAirportQuery(hazelcastInstance,airportsFile,flightsFile,p.getN());
                break;
            case "6":
                query = new FlightsPerStatePairQuery(hazelcastInstance,airportsFile,flightsFile,p.getMin());
                break;
            default:
                LOGGER.error("Client: Invalid query number");
                return;
        }

        Logger logger;
        try {
            logger = new Logger(p.getTimeOutPath(), Client.class);
            logger.info("Inicio de la lectura de archivos");
            query.readFiles();
            logger.info("Fin de la lectura de archivos");
            logger.info("Inicio del trabajo map/reduce");
            query.mapReduce();
            query.log(Paths.get(p.getOutPath()));
            logger.info("Fin del trabajo map/reduce");
            logger.close();
        } catch (IOException e) {
            LOGGER.error("Client: I/O Exception while writing in log");
        }

        hazelcastInstance.shutdown();
    }


}
