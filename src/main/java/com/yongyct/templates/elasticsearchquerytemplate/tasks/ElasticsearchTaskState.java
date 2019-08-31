package com.yongyct.templates.elasticsearchquerytemplate.tasks;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * POJO class for storing state of {@link ElasticsearchScrollTask}
 * 
 * @author tommy.yong
 *
 */
@Getter
@Setter
@AllArgsConstructor
public class ElasticsearchTaskState {

	private String lastIncrementFieldValue;
	private long recordsCount;

	public void incrementRecordsCount() {
		recordsCount++;
	}
}
