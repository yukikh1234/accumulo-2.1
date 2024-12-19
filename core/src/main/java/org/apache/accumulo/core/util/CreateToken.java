
package org.apache.accumulo.core.util;

import java.io.Console;

import org.apache.accumulo.core.cli.ClientOpts.PasswordConverter;
import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken.Properties;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken.TokenProperty;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.start.spi.KeywordExecutable;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class CreateToken implements KeywordExecutable {

  private Console reader = null;

  private Console getConsoleReader() {
    if (reader == null) {
      reader = System.console();
    }
    return reader;
  }

  static class Opts extends Help {
    @Parameter(names = {"-u", "--user"}, description = "Connection user")
    public String principal = null;

    @Parameter(names = "-p", converter = PasswordConverter.class,
        description = "Connection password")
    public String password = null;

    @Parameter(names = "--password", converter = PasswordConverter.class,
        description = "Enter the connection password", password = true)
    public String securePassword = null;

    @Parameter(names = {"-tc", "--tokenClass"},
        description = "The class of the authentication token")
    public String tokenClassName = PasswordToken.class.getName();
  }

  public static void main(String[] args) {
    new CreateToken().execute(args);
  }

  @Override
  public String keyword() {
    return "create-token";
  }

  @Override
  public String description() {
    return "Creates authentication token";
  }

  @Override
  public void execute(String[] args) {
    Opts opts = parseOptions(args);

    String pass = resolvePassword(opts);

    try {
      String principal = resolvePrincipal(opts);

      AuthenticationToken token = createAuthenticationToken(opts);

      Properties props = gatherTokenProperties(token, pass);

      token.init(props);

      printTokenDetails(opts, principal, token);

    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  private Opts parseOptions(String[] args) {
    Opts opts = new Opts();
    opts.parseArgs("accumulo create-token", args);
    return opts;
  }

  private String resolvePassword(Opts opts) {
    return (opts.password != null) ? opts.password : opts.securePassword;
  }

  private String resolvePrincipal(Opts opts) {
    if (opts.principal == null) {
      return getConsoleReader().readLine("Username (aka principal): ");
    }
    return opts.principal;
  }

  private AuthenticationToken createAuthenticationToken(Opts opts)
      throws ReflectiveOperationException {
    return Class.forName(opts.tokenClassName).asSubclass(AuthenticationToken.class)
        .getDeclaredConstructor().newInstance();
  }

  private Properties gatherTokenProperties(AuthenticationToken token, String pass) {
    Properties props = new Properties();
    for (TokenProperty tp : token.getProperties()) {
      String input = getInputForTokenProperty(tp, pass);
      props.put(tp.getKey(), input);
    }
    return props;
  }

  private String getInputForTokenProperty(TokenProperty tp, String pass) {
    if (pass != null && tp.getKey().equals("password")) {
      return pass;
    }
    if (tp.getMask()) {
      return getConsoleReader().readLine(tp.getDescription() + ": ", '*');
    }
    return getConsoleReader().readLine(tp.getDescription() + ": ");
  }

  private void printTokenDetails(Opts opts, String principal, AuthenticationToken token) {
    System.out.println("auth.type = " + opts.tokenClassName);
    System.out.println("auth.principal = " + principal);
    System.out.println("auth.token = " + ClientProperty.encodeToken(token));
  }
}
