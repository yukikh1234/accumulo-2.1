
package org.apache.accumulo.core.util.threads;

class NamedRunnable implements Runnable {

  private final String name;
  private final Runnable task;

  NamedRunnable(String name, Runnable task) {
    this.name = name;
    this.task = task;
  }

  public String getName() {
    return name;
  }

  @Override
  public void run() {
    task.run();
  }
}
