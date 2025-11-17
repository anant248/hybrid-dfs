package client;

/*
  This unit test will test the end-to-end functionality of the distributed log querier program.
  Before running this test, make sure to run the log_generator.sh script which will generate a
  test log file(vm${i}_test.log, i is the server index) on each of the server and then startup
  each server by providing the test log filename corresponding to that server(eg: vm1_test.log)
  as an argument.

  This unit test will cover frequent, rare, text/pattern that appears only once, pattern that appears only a few times
  across all log files.
*/

public class DistributedLogQuerierTest {
    public static void main(String[] args) {
        // This test will grep the frequent pattern and compare the returned total count with the actual count
        String grepOptions1 = "-c";
        String frequentPattern = "ERROR";
        int totalMatchingLinesCount1 = DistributedLogQuerierClient.start(grepOptions1, frequentPattern);
        if(totalMatchingLinesCount1 == 9810){
            System.out.println("Expected total: 9810");
            System.out.println("Actual total: " + totalMatchingLinesCount1);
            System.out.println("TEST 1 PASSED\n");
        }
        else{
            System.out.println("Expected total: 9810");
            System.out.println("Actual total: " + totalMatchingLinesCount1);
            System.out.println("TEST 1 FAILED\n");
        }

        // This test will grep the medium pattern and compare the returned total count with the actual count
        String grepOptions2 = "-c -i";
        String rarePattern = "iNfO";
        int totalMatchingLinesCount2 = DistributedLogQuerierClient.start(grepOptions2, rarePattern);
        if(totalMatchingLinesCount2 == 100){
            System.out.println("Expected total: 100");
            System.out.println("Actual total: " + totalMatchingLinesCount2);
            System.out.println("TEST 2 PASSED\n");
        }
        else{
            System.out.println("Expected total: 100");
            System.out.println("Actual total: " + totalMatchingLinesCount2);
            System.out.println("TEST 2 FAILED\n");
        }

        // This test will grep the very rare pattern and compare the returned total count with the actual count
        String grepOptions3 = "-c";
        String oneTimeOccurringPattern = "ERROR: The server responded with 404";
        int totalMatchingLinesCount3 = DistributedLogQuerierClient.start(grepOptions3, oneTimeOccurringPattern);
        if(totalMatchingLinesCount3 == 1){
            System.out.println("Expected total: 1");
            System.out.println("Actual total: " + totalMatchingLinesCount3);
            System.out.println("TEST 3 PASSED\n");
        }
        else{
            System.out.println("Expected total: 1");
            System.out.println("Actual total: " + totalMatchingLinesCount3);
            System.out.println("TEST 3 FAILED\n");
        }

        // This test will grep the not so rare pattern and compare the returned total count with the actual count
        String grepOptions4 = "-c";
        String fewTimesOccurringPattern = "ERROR: The server responded with 403";
        int totalMatchingLinesCount4 = DistributedLogQuerierClient.start(grepOptions4, fewTimesOccurringPattern);
        if(totalMatchingLinesCount4 == 9){
            System.out.println("Expected total: 9");
            System.out.println("Actual total: " + totalMatchingLinesCount4);
            System.out.println("TEST 4 PASSED\n");
        }
        else{
            System.out.println("Expected total: 9");
            System.out.println("Actual total: " + totalMatchingLinesCount4);
            System.out.println("TEST 4 FAILED\n");
        }

        // This test will grep the very rare pattern and compare the returned total count with the actual count
        String grepOptions5 = "-c";
        String noOccurrencePattern = "map reduce";
        int totalMatchingLinesCount5 = DistributedLogQuerierClient.start(grepOptions5, noOccurrencePattern);
        if(totalMatchingLinesCount5 == 0){
            System.out.println("Expected total: 0");
            System.out.println("Actual total: " + totalMatchingLinesCount5);
            System.out.println("TEST 5 PASSED\n");
        }
        else{
            System.out.println("Expected total: 0");
            System.out.println("Actual total: " + totalMatchingLinesCount5);
            System.out.println("TEST 5 FAILED\n");
        }

        // This test will perform grep with multiple options and compare the returned total count with the actual count
        String multipleGrepoptions = "-c -n -i";
        String mixedPattern = "warNing";
        int totalMatchingLinesCount6 = DistributedLogQuerierClient.start(multipleGrepoptions, mixedPattern);
        if(totalMatchingLinesCount6 == 100){
            System.out.println("Expected total: 100");
            System.out.println("Actual total: " + totalMatchingLinesCount6);
            System.out.println("TEST 6 PASSED\n");
        }
        else{
            System.out.println("Expected total: 100");
            System.out.println("Actual total: " + totalMatchingLinesCount6);
            System.out.println("TEST 6 FAILED\n");
        }

        // This test will grep the very rare pattern and compare the returned total count with the actual count
        String multiplePatternOptions = "-c -i -E";
        String multiplePatterns = "INfO|WARNiNG";
        int totalMatchingLinesCount7 = DistributedLogQuerierClient.start(multiplePatternOptions, multiplePatterns);
        if(totalMatchingLinesCount7 == 200){
            System.out.println("Expected total: 200");
            System.out.println("Actual total: " + totalMatchingLinesCount7);
            System.out.println("TEST 7 PASSED\n");
        }
        else{
            System.out.println("Expected total: 200");
            System.out.println("Actual total: " + totalMatchingLinesCount7);
            System.out.println("TEST 7 FAILED\n");
        }
    }
}
