package bloomfilter;

import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import lombok.extern.log4j.Log4j2;

@RestController
@Log4j2
public class DeviceFilterController {

  @Value("${amp.filter.bit.count}")
  private int filterBitCount;

  @Value("${amp.filter.fpp}")
  private float falsePositive;

  private static BloomFilter<String> bloomFilter;

  private static long addedDeviceCount = 0;

  private static boolean isFilterConfigured = false;

  private static Date filterLastlyConfiguredDate;

  private static Date filterLastlyUpdateDate;

  @GetMapping("/")
  @ResponseBody
  public String index() {
    return "Greetings from Device Filter Controller!";
  }

  /**
   * Creates bloom filter based on configured values
   *    - amp.filter.bit.count (Bit array to be allocated)
   *    - amp.filter.bit.fpp (False positive percentage)
   *    - amp.filter.bit.factor (Load factor if needs to be added)
   *    - String Funnel
   * @param deviceIds: List of existing device Id(String format) which needs to be added to filter
   * @return deviceIds size which was fetched for filter creation. 
   */
  @PostMapping("/bloom/filter/create")
  public @ResponseBody ResponseEntity<Long> createStringBloomFilter(
      @RequestBody List<String> deviceIds) {
    log.info("########################## Create Bloom Filter ##########################");
    bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), filterBitCount,
        falsePositive);
    deviceIds.forEach(deviceId -> bloomFilter.put(deviceId));
    addedDeviceCount = deviceIds.size();
    isFilterConfigured = true;
    filterLastlyConfiguredDate = new Date();
    return new ResponseEntity<>(addedDeviceCount, HttpStatus.OK);
  }

  /**
   * Creates bloom filter based on configured values and passed Device UUIDs
   *    - amp.filter.bit.count (Bit array to be allocated)
   *    - amp.filter.bit.fpp (False positive percentage)
   *    - amp.filter.bit.factor (Load factor if needs to be added)
   *    - String Funnel
   * @param deviceIds: List of existing device Id(UUID format) which needs to be added to filter
   * @return deviceIds size which was fetched for filter creation. 
   */
  @PostMapping("/bloom/filter/uuid/create")
  public @ResponseBody ResponseEntity<Long> createUUIDBloomFilter(@RequestBody List<UUID> deviceIds) {
    log.info("########################## Create Bloom Filter ##########################");
    bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), filterBitCount,
        falsePositive);
    deviceIds.forEach(deviceId -> bloomFilter.put(deviceId.toString()));
    addedDeviceCount = deviceIds.size();
    isFilterConfigured = true;
    filterLastlyConfiguredDate = new Date();
    return new ResponseEntity<>(addedDeviceCount, HttpStatus.OK);
  }

  /**
   * Adds list of device Id(String format) to existing filter
   * 
   * @param deviceIds: List of new device Id(String format) which needs to be added to filter
   * @return deviceIds size which was fetched for filter creation. 
   */
  @PatchMapping("/bloom/filter/batch/add")
  public @ResponseBody ResponseEntity<Long> addNewDevicesToBloomFilter(
      @RequestBody List<String> deviceIds) {
    log.info(
        "########################## Update new devices To Bloom Filter ##########################");
    if (isFilterConfigured) {
      deviceIds.forEach(deviceId -> bloomFilter.put(deviceId));
      addedDeviceCount = addedDeviceCount + deviceIds.size();
      filterLastlyUpdateDate = new Date();
    } else {
      createStringBloomFilter(deviceIds);
    }
    return new ResponseEntity<>(addedDeviceCount, HttpStatus.OK);
  }

  /**
   * Adds list of device Id(UUID format) to existing filter
   * 
   * @param deviceIds: List of new device Id(UUID format) which needs to be added to filter
   * @return deviceIds size which was fetched for filter creation. 
   */
  @PatchMapping("/bloom/filter/uuid/batch/add")
  public @ResponseBody ResponseEntity<Long> addNewDeviceUUIDsToBloomFilter(
      @RequestBody List<UUID> deviceIds) {
    log.info(
        "########################## Update new devices UUID To Bloom Filter ##########################");
    if (isFilterConfigured) {
      deviceIds.forEach(deviceId -> bloomFilter.put(deviceId.toString()));
      addedDeviceCount = addedDeviceCount + deviceIds.size();
      filterLastlyUpdateDate = new Date();
    } else {
      createUUIDBloomFilter(deviceIds);
    }
    return new ResponseEntity<>(addedDeviceCount, HttpStatus.OK);
  }

  /**
   * Adds single device Id(String format) to existing filter
   * 
   * @param deviceId: New device Id(String format) which needs to be added to filter
   * @return Count of deviceIds added to filter. 
   */
  @PatchMapping("/bloom/filter/add")
  public static @ResponseBody ResponseEntity<Long> addNewDeviceToBloomFilter(@RequestParam(name = "deviceId") String deviceId) {
    log.info(
        "########################## Update new devices To Bloom Filter ##########################");
    if (isFilterConfigured) {
      bloomFilter.put(deviceId);
      addedDeviceCount = addedDeviceCount + 1;
      filterLastlyUpdateDate = new Date();
    }
    return new ResponseEntity<>(addedDeviceCount, HttpStatus.OK);
  }

  /**
   * Adds single device Id(UUID format) to existing filter
   * 
   * @param deviceId: New device Id(UUID format) which needs to be added to filter
   * @return Count of deviceIds added to filter. 
   */
  @PatchMapping("/bloom/filter/uuid/add")
  public static @ResponseBody ResponseEntity<Long> addNewDeviceUUIDToBloomFilter(@RequestParam(name = "deviceId") UUID deviceId) {
    log.info(
        "########################## Update new devices UUID To Bloom Filter ##########################");
    if (isFilterConfigured) {
      bloomFilter.put(deviceId.toString());
      addedDeviceCount = addedDeviceCount + 1;
      filterLastlyUpdateDate = new Date();
    }
    return new ResponseEntity<>(addedDeviceCount, HttpStatus.OK);
  }

  /**
   * Validates if passed in device ID is registered device or not.
   * @param deviceId: Device Id which needs to be validated
   * @return Boolean
   */
  @GetMapping("/bloom/filter/validate")
  public static @ResponseBody ResponseEntity<Boolean> isDevicePresentInFilter(@RequestParam(name = "deviceId") String deviceId) {
    boolean isDevicePresent = false;
    if (isFilterConfigured) {
      isDevicePresent = bloomFilter.mightContain(deviceId);
    }
    return new ResponseEntity<>(isDevicePresent, HttpStatus.OK);
  }

  /**
   * Distinct elements which are added to the filter
   * @return Long
   */
  @GetMapping("/bloom/filter/deviceCount")
  public static @ResponseBody ResponseEntity<Long> distinctElementCount() {
    long distinctElementCount = 0;
    if (isFilterConfigured) {
      distinctElementCount = bloomFilter.approximateElementCount();
    }
    return new ResponseEntity<>(distinctElementCount, HttpStatus.OK);
  }

  /**
   * Date on which filter was configured from start
   * @return Date
   */
  @GetMapping("/bloom/filter/date")
  public static @ResponseBody ResponseEntity<Date> filterConfigureDate() {
    if (isFilterConfigured) {
      return new ResponseEntity<>(filterLastlyConfiguredDate, HttpStatus.OK);
    }
    // Older date to mention filter is not configured
    Calendar c = Calendar.getInstance();
    c.set(1999, 1, 1);
    return new ResponseEntity<>(c.getTime(), HttpStatus.OK);
  }
  
  /**
   * Method to check if any new device id was added to filter after creation
   * @return Boolean
   */
  @GetMapping("/bloom/filter/isUpdated")
  public static @ResponseBody ResponseEntity<Boolean> isFilterUpdated() {
    if (isFilterConfigured && filterLastlyUpdateDate != null) {
      return new ResponseEntity<>(filterLastlyUpdateDate.after(filterLastlyConfiguredDate), HttpStatus.OK);
    }
    return new ResponseEntity<>(false, HttpStatus.OK);
  }

  /**
   * Expected false positive percentage based on configuration parameters
   * @return Double
   */
  @GetMapping("/bloom/filter/fpp")
  public static @ResponseBody ResponseEntity<Double> expectedFpp() {
    double expectedFpp = 0;
    if (isFilterConfigured) {
      expectedFpp = bloomFilter.expectedFpp();
    }
    return new ResponseEntity<>(expectedFpp, HttpStatus.OK);
  }

}
