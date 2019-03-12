package amp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * AMP Application!
 *
 */
@SpringBootApplication
@ComponentScan("bloomfilter")
public class AMPApplication 
{
    public static void main( String[] args )
    {
      SpringApplication.run(AMPApplication.class, args);
    }
    
}
