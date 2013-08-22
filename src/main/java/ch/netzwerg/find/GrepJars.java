package ch.netzwerg.find;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.file.Files.*;

/**
 * @author rahel.luethy@gmail.com
 */
public class GrepJars {

  private static final String SEARCH_PATH = ".";
  private static final String FILE_NAME_PATTERN = "*-sources.jar";

  public static void main(String[] args) throws IOException, InterruptedException {
    GrepJobScheduler jobScheduler = new GrepJobScheduler();
    long startTime = System.currentTimeMillis();
    triggerSearch(jobScheduler);
    jobScheduler.shutdown();
    long executionTime = System.currentTimeMillis() - startTime;
    System.out.println("DONE (" + executionTime + " ms)");
  }

  private static Path triggerSearch(GrepJobScheduler jobScheduler) throws IOException {
    FileFinder finder = new FileFinder(FILE_NAME_PATTERN, jobScheduler);
    Path startingDir = Paths.get(SEARCH_PATH);
    return walkFileTree(startingDir, finder);
  }

  /**
  * Schedules an extraction/grep job for each file returned by the finder.
  */
  private static class GrepJobScheduler implements FileFinder.MatchingFileAcceptor {

    private ExecutorService executorService = Executors.newFixedThreadPool(4);

    @Override
    public void acceptMatch(Path path) {
      executorService.submit(new GrepJarJob(path));
    }

    public void shutdown() throws InterruptedException {
      this.executorService.shutdown();
    }
  }

  private static class GrepJarJob implements Callable<Void> {

    private final Path path;

    private GrepJarJob(Path path) {
      this.path = path;
    }

    @Override
    public Void call() throws Exception {
      System.out.println(path);
      return null;
    }
  }

}
