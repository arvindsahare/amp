# amp
Amp Filter implementation

## Bloom Filter
Filtering service provides endpoints for creating, validating, updating and monitoring endpoint for 
Device filter.
Filter is developed using probabilistic bloom filter

1. Environment Variable
	amp.filter:
    		`bit.count`
    		`fpp`
2. Maven


### Getting Started

Skip any steps that have already been completed on your machine.

1. Install Java version 1.8+
   ```
   brew cask install java
   ```
   
2. Install maven
   ```
   brew install maven
   ```
    
3. Build the AMP application
   ```
   - Build `mvn clean install`
   - To skip tests: `mvn clean install -DskipTests`
   ```

4. Start the server
    ```
    mvn spring-boot:run
    ``` 
      
