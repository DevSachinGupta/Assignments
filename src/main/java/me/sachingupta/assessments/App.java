package me.sachingupta.assessments;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class App 
{
    public static void main( String[] args )
    {
    	// System.setProperty("HADOOP_HOME", "C:\\MyData\\Programs\\spark-3.1.1-bin-hadoop3.2");
    	Logger.getLogger("org.apache").setLevel(Level.WARN);
        Assessment3 ass = new Assessment3();
        ass.execute();
    }
}
