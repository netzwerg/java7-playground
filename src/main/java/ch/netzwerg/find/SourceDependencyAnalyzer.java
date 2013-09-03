package ch.netzwerg.find;

import static java.nio.file.Files.walkFileTree;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

/**
 * @author rahel.luethy@gmail.com
 */
public class SourceDependencyAnalyzer {

  private static final String ROOT_PATH = "J:\\spu\\test\\maven\\repository\\ch\\basler\\";
  public static final int THREAD_COUNT = 4;
  private static final ConcurrentUpdateSolrServer SERVER = new ConcurrentUpdateSolrServer("http://localhost:8983/solr/baloise", 10, THREAD_COUNT);
  private static final String FILE_NAME_PATTERN = "*-sources.jar";
  private static final Logger LOGGER = Logger.getLogger(SourceDependencyAnalyzer.class.getName());
  private static final List<String> WHITE_LIST = Arrays.asList(".java", ".xml", ".properties");
  private static final AtomicInteger COUNTER = new AtomicInteger();

  public static void main(String[] args) throws IOException, InterruptedException {
    UploadScheduler scheduler = new UploadScheduler();
    searchAndUpload(scheduler, Arrays.asList(args));
    scheduler.shutdown();
  }

  private static void searchAndUpload(UploadScheduler jobScheduler, List<String> dirsToIndex) throws IOException {
    for (String dirToIndex : dirsToIndex) {
      FileFinder finder = new FileFinder(FILE_NAME_PATTERN, jobScheduler);
      Path startingDir = Paths.get(ROOT_PATH + dirToIndex);
      LOGGER.info(String.format("Searching file tree '%s'...", startingDir));
      walkFileTree(startingDir, finder);
    }
  }

  /**
   * Schedules an extraction/upload job for each file returned by the finder.
   */
  private static class UploadScheduler implements FileFinder.MatchingFileAcceptor {

    private ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);

    @Override
    public void acceptMatch(Path path) {
      executorService.submit(new SolrUploadJob(path));
    }

    public void shutdown() throws InterruptedException {
      this.executorService.shutdown();
    }
  }

  private static class SolrUploadJob implements Callable<Void> {

    private final Path path;

    private SolrUploadJob(Path path) {
      this.path = path;
    }

    @Override
    public Void call() throws Exception {
      String pathString = path.toString().toLowerCase();
      if (pathString.endsWith("-test-sources.jar") || pathString.contains("snapshot")) {
        LOGGER.info(String.format("Skipping\t'%s'", path));
      }
      else {
        LOGGER.info(String.format("Extracting\t'%s'", path));
        JarFile jarFile = new JarFile(path.toFile());
        Enumeration<JarEntry> entries = jarFile.entries();
        while (entries.hasMoreElements()) {
          JarEntry entry = entries.nextElement();
          if (entry.isDirectory()) {
            LOGGER.info(String.format("Skipping dirs\t'%s'", entry.getName()));
          }
          else if (matchesWhiteList(entry.getName())) {
            InputStream input = jarFile.getInputStream(entry);
            String contents = new String(IOUtils.toByteArray(input));
            String id = path + ":" + entry.getName();
            LOGGER.info(String.format("Processing\t'%s'", id));
            UpdateResponse rsp = uploadSolrDoc(id, contents);
            LOGGER.info("Completed with status " + rsp.getStatus());
            closeStream(input);
          }
          else {
            LOGGER.info(String.format("Skipping non-whitelisted\t'%s'", entry.getName()));
          }
        }
      }
      return null;
    }

    private static boolean matchesWhiteList(String name) {
      for (String suffix : WHITE_LIST) {
        if (name.toLowerCase().endsWith(suffix)) {
          return true;
        }
      }
      return false;
    }

    private static UpdateResponse uploadSolrDoc(String id, String contents) throws SolrServerException, IOException {
      SolrInputDocument doc = new SolrInputDocument();
      doc.setField("id", id);
      doc.setField("text", contents);

      UpdateRequest req = new UpdateRequest();
      req.setAction(UpdateRequest.ACTION.COMMIT, false, false);
      req.add(doc);
      UpdateResponse update = req.process(SERVER);
      LOGGER.info("Uploaded document #" + COUNTER.incrementAndGet());
      return update;
    }

    private static void closeStream(InputStream input) {
      try {
        if (input != null) {
          input.close();
        }
      }
      catch (Throwable t) {
        // ignorance is bliss
      }
    }

  }

}
