
package org.apache.accumulo.core.util;

import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.clientImpl.ConnectorImpl;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connector uses static resource that create threads and use memory. The only way to clean up these
 * static resource related to Connectors created using ZookeeperInstance is to use this class.
 *
 * <p>
 * This class is not needed when only using {@link AccumuloClient}. The new AccumuloClient API that
 * replaces Connector is closable. For code that only uses AccumuloClient, when all AccumuloClients
 * are closed resources are cleaned up. Connectors that are derived from an AccumuloClient do not
 * necessitate the use of this code.
 *
 * @deprecated since 2.0.0 Use only {@link AccumuloClient} instead. Also, make sure you close the
 *             AccumuloClient instances.
 */
@Deprecated(since = "2.0.0")
public class CleanUp {

  private static final Logger log = LoggerFactory.getLogger(CleanUp.class);

  /**
   * Kills all threads created by internal Accumulo singleton resources. After this method is
   * called, no Connector will work in the current classloader.
   *
   * @param conn If available, Connector object to close resources on. Will accept null otherwise.
   */
  public static void shutdownNow(Connector conn) {
    SingletonManager.setMode(Mode.CLIENT);
    waitForZooKeeperClientThreads();
    if (conn != null) {
      try {
        ConnectorImpl connImpl = (ConnectorImpl) conn;
        connImpl.getAccumuloClient().close();
      } catch (Exception e) {
        log.error("Failed to close AccumuloClient: {}", e.getMessage(), e);
      }
    }
  }

  /**
   * As documented in https://issues.apache.org/jira/browse/ZOOKEEPER-1816, ZooKeeper.close() is a
   * non-blocking call. This method waits for the ZooKeeper internal threads to exit.
   */
  private static void waitForZooKeeperClientThreads() {
    Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
    for (Thread thread : threadSet) {
      if (isZooKeeperClientThread(thread)) {
        waitForThreadToDie(thread);
      }
    }
  }

  /**
   * Checks whether a thread is a ZooKeeper client thread in the current ClassLoader.
   *
   * @param thread The thread to check.
   * @return true if the thread is a ZooKeeper client thread, false otherwise.
   */
  private static boolean isZooKeeperClientThread(Thread thread) {
    return thread.getClass().getName().startsWith("org.apache.zookeeper.ClientCnxn")
        && thread.getContextClassLoader().equals(Thread.currentThread().getContextClassLoader());
  }

  /**
   * Waits for the given thread to die.
   *
   * @param thread The thread to wait for.
   */
  private static void waitForThreadToDie(Thread thread) {
    while (thread.isAlive()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        log.error("Interrupted while waiting for thread to die: {}", e.getMessage(), e);
      }
    }
  }
}
