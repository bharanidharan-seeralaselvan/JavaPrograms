import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class NameSearch {
    private static final String FILE_URL = "http://norvig.com/big.txt";
    private static final int BATCH_SIZE = 1000;

    public static void main(String[] args) {
        try {
            List<String> batches = readTextFromUrl(FILE_URL, BATCH_SIZE);

            //For creating concurrent threads.
            //Reference - https://crunchify.com/how-to-run-multiple-threads-concurrently-in-java-executorservice-approach/
            //fixed number of threads is set to batch size.
            ExecutorService executor = Executors.newFixedThreadPool(batches.size());
            List<NameMatcher> matchers = new ArrayList<>();
            int batchId = 1;
            for (String batch : batches) {
                NameMatcher matcher = new NameMatcher(batch, batchId++);
                executor.execute(matcher);
                matchers.add(matcher);
            }

            executor.shutdown();
            //termination after 5 minutes
            if (!executor.awaitTermination(5L, TimeUnit.MINUTES)) {
                executor.shutdownNow();
            }

            ResultAggregator aggregator = new ResultAggregator(matchers);
            aggregator.printResults();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    //Reading file content from URL
    private static List<String> readTextFromUrl(String fileUrl, int batchSize) throws IOException {
        List<String> batches = new ArrayList<>();

        URL url = new URL(fileUrl);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
            StringBuilder batch = new StringBuilder();
            String line;
            int count = 0;

            while ((line = reader.readLine()) != null) {
                batch.append(line);
                count++;

                if (count == batchSize) {
                    batches.add(batch.toString());
                    batch.setLength(0);
                    count = 0;
                }
            }

            if (batch.length() > 0) {
                batches.add(batch.toString());
            }
        }

        return batches;
    }
}


class NameMatcher implements Runnable {
    private static final String[] FIRST_NAMES = {"James", "John", "Robert", "Michael", "William", "David", "Richard",
            "Charles", "Joseph", "Thomas", "Christopher", "Daniel", "Paul", "Mark", "Donald", "George", "Kenneth",
            "Steven", "Edward", "Brian", "Ronald", "Anthony", "Kevin", "Jason", "Matthew", "Gary", "Timothy", "Jose",
            "Larry", "Jeffrey", "Frank", "Scott", "Eric", "Stephen", "Andrew", "Raymond", "Gregory", "Joshua", "Jerry",
            "Dennis", "Walter", "Patrick", "Peter", "Harold", "Douglas", "Henry", "Carl", "Arthur", "Ryan", "Roger"};
    private int batchId;
    private String content;
    private Map<String, List<String>> resultMap;

    public NameMatcher(String content, int batchId) {
        this.batchId = batchId;
        this.content = content;
        this.resultMap = new HashMap<>();
    }

    /**
     * This method parse through content of batch for the mentioned names
     */
    @Override
    public void run() {
        int lineOffset = (batchId - 1) * 1000;
        int count = 0;
        for (String name : FIRST_NAMES) {
            List<String> positions = new ArrayList<>();
            int index = content.indexOf(name);
            lineOffset = lineOffset + count;
            while (index != -1) {
                positions.add("[lineOffset=" + lineOffset + ", charOffset=" + index + "]");
                index = content.indexOf(name, index + 1);
            }
            resultMap.put(name, positions);
        }

    }

    public Map<String, List<String>> getResultMap() {
        return resultMap;
    }

}

class ResultAggregator {
    private final List<NameMatcher> matchers;

    public ResultAggregator(List<NameMatcher> matchers) {
        this.matchers = matchers;
    }

    /**
     * Print the final results by combining all the results from matchers
     */
    public void printResults() {
        Map<String, List<String>> aggregatedResults = new HashMap<>();

        for (NameMatcher matcher : matchers) {
            Map<String, List<String>> resultMap = matcher.getResultMap();

            for (Map.Entry<String, List<String>> entry : resultMap.entrySet()) {
                String word = entry.getKey();
                List<String> positions = entry.getValue();

                if (!aggregatedResults.containsKey(word)) {
                    aggregatedResults.put(word, new ArrayList<>());
                }

                aggregatedResults.get(word).addAll(positions);
            }
        }
        for (Map.Entry<String, List<String>> entry : aggregatedResults.entrySet()) {
            String word = entry.getKey();
            List<String> positions = entry.getValue();

            System.out.println(word + " --> " + positions);
        }
    }
}