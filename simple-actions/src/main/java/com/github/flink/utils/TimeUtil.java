package com.github.flink.utils;

import org.apache.commons.lang3.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/26 3:30 PM
 */
public class TimeUtil {
    private static SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    /**
     * 判断通过时间是否在限定时间内
     *
     * @param passingTime
     * @param timeRangeRule
     * @return
     * @throws Exception
     */
    public static Boolean isLicenseNumberLimitTime(Long passingTime, String timeRangeRule) throws Exception{
        String range1 = "", range2 = "", range3 = "", range4 = "";

        if(StringUtils.isNoneBlank(timeRangeRule)){
            String[] timeRanges = timeRangeRule.split(",");

            if(timeRanges.length >= 2){
                String[] rangesStart = timeRanges[0].split("-");

                if(rangesStart.length >= 2){
                    range1 = rangesStart[0];
                    range2 = rangesStart[1];
                }

                String[] rangesEnd = timeRanges[1].split("-");

                if(rangesEnd.length >= 2){
                    range1 = rangesEnd[0];
                    range2 = rangesEnd[1];
                }
            }
        }

        //两种情况,一种是有两个限制时间段,另一种是只有一个限制时间段
        if(!"".equals(range1) && !"".equals(range2)){
            String today = dateFormat.format(new Date());

            if(!"".equalsIgnoreCase(range3) && !"".equalsIgnoreCase(range4)){
                String limitBegin1 = today + " " + range1 + ":00";
                String limitEnd1 = today + " " + range2 + ":00";
                String limitBegin2 = today + " " + range3 + ":00";
                String limitEnd2 = today + " " + range4 + ":00";

                long tBegin1 = getTimeMillis(limitBegin1);
                long tEnd1 = getTimeMillis(limitEnd1);
                long tBegin2 = getTimeMillis(limitBegin2);
                long tEnd2 = getTimeMillis(limitEnd2);

                if((tBegin1 <= passingTime && passingTime <= tEnd1) ||
                        (tBegin2 <= passingTime && passingTime <= tEnd2)){
                    return true;
                }
            }else{
                String limitBegin = today + " " + range1 + ":00";
                String limitEnd = today + " " + range2 + ":00";

                long tBegin = getTimeMillis(limitBegin);
                long tEnd = getTimeMillis(limitEnd);

                if(tBegin <= passingTime && passingTime <= tEnd){
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * 获取long型时间表示
     *
     * @param dateString
     * @return
     * @throws Exception
     */
    public static long getTimeMillis(String dateString) throws Exception {
        Date date = dateTimeFormat.parse(dateString);
        return date.getTime();
    }

    /**
     * 获取时间的字符串表示
     *
     * @param milliSecond
     * @return
     */
    public static String milliSecondToTimestampString(Long milliSecond){
        return dateTimeFormat.format(new Date(milliSecond));
    }

    /**
     * 获取该时间是周几
     *
     * @param dateStr
     * @return
     * @throws Exception
     */
    public static int dayOfWeek(String dateStr) throws Exception{
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = simpleDateFormat.parse(dateStr);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int week = calendar.get(Calendar.DAY_OF_WEEK) - 1;

        if(week <= 0){
            week = 7;
        }

        return week;
    }
}
