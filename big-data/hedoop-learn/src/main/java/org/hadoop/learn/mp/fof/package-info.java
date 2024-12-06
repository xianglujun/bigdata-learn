/**
 * 该包主要用于实现好友推荐功能，存储数据格式为:
 *
 * tom hellohadoop
 * hadoop tom hive world
 * vorld hadoop hello hive
 * Cat tom hivemr hive hello
 * hive cat hadoop world hello mr
 * hello tom world hive mr
 *
 * 数据做一下解释：
 * 相邻的两个字符，表示直接好友的关系，其他的则表示间接好友。
 *
 * 数据分析的目的：
 * 1. 找出间接好友，并找到间接好友数量统计最多的前两个，作为添加好友推荐候选项
 */
package org.hadoop.learn.mp.fof;