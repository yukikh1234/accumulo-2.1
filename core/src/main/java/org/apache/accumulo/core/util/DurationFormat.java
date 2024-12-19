
package org.apache.accumulo.core.util;

public class DurationFormat {
  private final String str;

  public DurationFormat(long time, String space) {
    if (time == 0) {
      str = "-";
    } else {
      str = formatDuration(time, space);
    }
  }

  private String formatDuration(long time, String space) {
    long ms = time % 1000;
    time /= 1000;

    if (time == 0) {
      return formatMilliseconds(ms);
    }

    long sec = time % 60;
    time /= 60;

    if (time == 0) {
      return formatSecondsMilliseconds(sec, ms, space);
    }

    long min = time % 60;
    time /= 60;

    if (time == 0) {
      return formatMinutesSeconds(min, sec, space);
    }

    long hr = time % 24;
    time /= 24;

    if (time == 0) {
      return formatHoursMinutes(hr, min, space);
    }

    long day = time % 365;
    time /= 365;

    if (time == 0) {
      return formatDaysHours(day, hr, space);
    }

    long yr = time;
    return formatYearsDays(yr, day, space);
  }

  private String formatMilliseconds(long ms) {
    return String.format("%dms", ms);
  }

  private String formatSecondsMilliseconds(long sec, long ms, String space) {
    return String.format("%ds" + space + "%dms", sec, ms);
  }

  private String formatMinutesSeconds(long min, long sec, String space) {
    return String.format("%dm" + space + "%ds", min, sec);
  }

  private String formatHoursMinutes(long hr, long min, String space) {
    return String.format("%dh" + space + "%dm", hr, min);
  }

  private String formatDaysHours(long day, long hr, String space) {
    return String.format("%dd" + space + "%dh", day, hr);
  }

  private String formatYearsDays(long yr, long day, String space) {
    return String.format("%dy" + space + "%dd", yr, day);
  }

  @Override
  public String toString() {
    return str;
  }
}
