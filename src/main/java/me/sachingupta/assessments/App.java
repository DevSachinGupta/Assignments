package me.sachingupta.assessments;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	Logger.getLogger("org.apache").setLevel(Level.WARN);
        Assessment2 ass = new Assessment2();
        ass.execute();
    }
}
