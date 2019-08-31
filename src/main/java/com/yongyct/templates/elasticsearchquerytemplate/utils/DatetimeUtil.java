package com.yongyct.templates.elasticsearchquerytemplate.utils;

import java.util.concurrent.TimeUnit;

public class DatetimeUtil {

	private static final String UNIT_SECOND = "SECOND";
	private static final String UNIT_MINUTE = "MINUTE";
	private static final String UNIT_HOUR = "HOUR";
	private static final String UNIT_DAY = "DAY";

	public static long getMsFromDurationValue(String duration, String delimiter) {

		String unit = duration.split(delimiter)[1].toUpperCase().trim().replaceAll("S$", "");
		int amt = Integer.parseInt(duration.split(delimiter)[0].trim());
		long ms = 0;

		switch (unit) {

		case UNIT_SECOND:
			ms = TimeUnit.SECONDS.toMillis(amt);
			break;
		case UNIT_MINUTE:
			ms = TimeUnit.MINUTES.toMillis(amt);
			break;
		case UNIT_HOUR:
			ms = TimeUnit.HOURS.toMillis(amt);
			break;
		case UNIT_DAY:
			ms = TimeUnit.DAYS.toMillis(amt);
			break;
		default:
			break;

		}

		return ms;

	}

}
