
package org.apache.accumulo.core.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

public final class HostAndPort implements Serializable, Comparable<HostAndPort> {
  private static final int NO_PORT = -1;

  private final String host;
  private final int port;
  private final boolean hasBracketlessColons;

  private HostAndPort(String host, int port, boolean hasBracketlessColons) {
    this.host = host;
    this.port = port;
    this.hasBracketlessColons = hasBracketlessColons;
  }

  private static final Comparator<HostAndPort> COMPARATOR = Comparator.nullsFirst(
      Comparator.comparing(HostAndPort::getHost).thenComparingInt(h -> h.getPortOrDefault(0)));

  public String getHost() {
    return host;
  }

  public boolean hasPort() {
    return port >= 0;
  }

  public int getPort() {
    checkState(hasPort(), "the address does not include a port");
    return port;
  }

  public static HostAndPort fromParts(String host, int port) {
    checkArgument(isValidPort(port), "Port out of range: %s", port);
    HostAndPort parsedHost = fromString(host);
    checkArgument(!parsedHost.hasPort(), "Host has a port: %s", host);
    return new HostAndPort(parsedHost.host, port, parsedHost.hasBracketlessColons);
  }

  public static HostAndPort fromString(String hostPortString) {
    Objects.requireNonNull(hostPortString, "hostPortString variable was null!");

    String host;
    String portString = null;
    boolean hasBracketlessColons = false;

    if (hostPortString.startsWith("[")) {
      String[] hostAndPort = getHostAndPortFromBracketedHost(hostPortString);
      host = hostAndPort[0];
      portString = hostAndPort[1];
    } else {
      int colonPos = hostPortString.indexOf(':');
      if (colonPos >= 0 && hostPortString.indexOf(':', colonPos + 1) == -1) {
        host = hostPortString.substring(0, colonPos);
        portString = hostPortString.substring(colonPos + 1);
      } else {
        host = hostPortString;
        hasBracketlessColons = (colonPos >= 0);
      }
    }

    int port = NO_PORT;
    if (portString != null && !portString.trim().isEmpty()) {
      checkArgument(!portString.startsWith("+"), "Unparseable port number: %s", hostPortString);
      port = parsePort(portString, hostPortString);
    }

    return new HostAndPort(host, port, hasBracketlessColons);
  }

  private static int parsePort(String portString, String hostPortString) {
    try {
      int port = Integer.parseInt(portString);
      checkArgument(isValidPort(port), "Port number out of range: %s", hostPortString);
      return port;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Unparseable port number: " + hostPortString);
    }
  }

  private static String[] getHostAndPortFromBracketedHost(String hostPortString) {
    checkArgument(hostPortString.charAt(0) == '[',
        "Bracketed host-port string must start with a bracket: %s", hostPortString);

    int colonIndex = hostPortString.indexOf(':');
    int closeBracketIndex = hostPortString.lastIndexOf(']');

    checkArgument(colonIndex > -1 && closeBracketIndex > colonIndex,
        "Invalid bracketed host/port: %s", hostPortString);

    String host = hostPortString.substring(1, closeBracketIndex);
    if (closeBracketIndex + 1 == hostPortString.length()) {
      return new String[] {host, ""};
    } else {
      checkArgument(hostPortString.charAt(closeBracketIndex + 1) == ':',
          "Only a colon may follow a close bracket: %s", hostPortString);
      validatePortDigits(hostPortString, closeBracketIndex);
      return new String[] {host, hostPortString.substring(closeBracketIndex + 2)};
    }
  }

  private static void validatePortDigits(String hostPortString, int closeBracketIndex) {
    for (int i = closeBracketIndex + 2; i < hostPortString.length(); ++i) {
      checkArgument(Character.isDigit(hostPortString.charAt(i)), "Port must be numeric: %s",
          hostPortString);
    }
  }

  public HostAndPort withDefaultPort(int defaultPort) {
    checkArgument(isValidPort(defaultPort));
    return hasPort() || port == defaultPort ? this
        : new HostAndPort(host, defaultPort, hasBracketlessColons);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other instanceof HostAndPort) {
      HostAndPort that = (HostAndPort) other;
      return Objects.equals(this.host, that.host) && this.port == that.port
          && this.hasBracketlessColons == that.hasBracketlessColons;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port, hasBracketlessColons);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(host.length() + 8);
    if (host.indexOf(':') >= 0) {
      builder.append('[').append(host).append(']');
    } else {
      builder.append(host);
    }
    if (hasPort()) {
      builder.append(':').append(port);
    }
    return builder.toString();
  }

  private static boolean isValidPort(int port) {
    return port >= 0 && port <= 65535;
  }

  private static final long serialVersionUID = 0;

  public int getPortOrDefault(int defaultPort) {
    return hasPort() ? port : defaultPort;
  }

  @Override
  public int compareTo(HostAndPort other) {
    return COMPARATOR.compare(this, other);
  }
}
