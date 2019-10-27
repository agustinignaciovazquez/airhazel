package ar.edu.itba.pod.client;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Logger {

    private String path = null;
    private Class logClass;
    private FileWriter fileWriter;

    public Logger(String path, Class logClass) throws IOException {
        this.path = path;
        this.logClass = logClass;
        if (this.path != null){
            this.fileWriter = new FileWriter(path, true);
        }
    }

    public Logger(Class logClass){
        this.logClass = logClass;
    }

    public void info(String description) throws IOException {
        Date dNow = new Date( );
        SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd hh:mm:ss:SSSS");
        StringBuilder sb = new StringBuilder();
        sb.append(ft.format(dNow)).append(" INFO ");
        sb.append(this.logClass).append(" - ");
        sb.append(description);
        if (this.path == null){
            System.out.println(sb.toString());
        }else{
            fileWriter.write(sb.toString());
            fileWriter.write('\n');
        }
    }

    public void close() throws IOException {
        this.fileWriter.close();
    }
}