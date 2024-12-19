
package org.apache.accumulo.core.constraints;

import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This constraint ensures mutations do not have deletes.
 *
 * @since 2.0.0
 * @deprecated since 2.1.0 Use {@link org.apache.accumulo.core.data.constraints.NoDeleteConstraint}
 */
@Deprecated(since = "2.1.0")
@SuppressFBWarnings(value = "NM_SAME_SIMPLE_NAME_AS_SUPERCLASS",
    justification = "Same name used for compatibility during deprecation cycle")
public class NoDeleteConstraint extends org.apache.accumulo.core.data.constraints.NoDeleteConstraint
    implements Constraint {

  private static final short DELETE_VIOLATION_CODE = 1;
  private static final String DELETE_VIOLATION_MESSAGE = "Deletes are not allowed";

  @Override
  public String getViolationDescription(short violationCode) {
    return violationCode == DELETE_VIOLATION_CODE ? DELETE_VIOLATION_MESSAGE : null;
  }

  @Override
  public List<Short> check(Constraint.Environment env, Mutation mutation) {
    if (containsDelete(mutation)) {
      return Collections.singletonList(DELETE_VIOLATION_CODE);
    }
    return null;
  }

  private boolean containsDelete(Mutation mutation) {
    for (ColumnUpdate update : mutation.getUpdates()) {
      if (update.isDeleted()) {
        return true;
      }
    }
    return false;
  }
}
