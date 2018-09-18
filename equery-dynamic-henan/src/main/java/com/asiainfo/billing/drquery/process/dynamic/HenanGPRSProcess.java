package com.asiainfo.billing.drquery.process.dynamic;

import com.asiainfo.billing.drquery.cache.CacheProvider;
import com.asiainfo.billing.drquery.cache.ICache;
import com.asiainfo.billing.drquery.datasource.query.DefaultQueryParams;
import com.asiainfo.billing.drquery.exception.BusinessException;
import com.asiainfo.billing.drquery.exception.DrqueryRuntimeException;
import com.asiainfo.billing.drquery.model.MetaModel;
import com.asiainfo.billing.drquery.process.ProcessException;
import com.asiainfo.billing.drquery.process.core.DRCommonProcess;
import com.asiainfo.billing.drquery.process.core.request.CommonDRProcessRequest;
import com.asiainfo.billing.drquery.process.dto.BaseDTO;
import com.asiainfo.billing.drquery.process.dto.PageDTO;
import com.asiainfo.billing.drquery.process.operation.fieldEscape.CommonFieldEscapeOperation;
import com.asiainfo.billing.drquery.utils.DateUtil;
import com.asiainfo.billing.drquery.utils.NumberUtils;
import com.asiainfo.billing.drquery.utils.PropertiesUtil;
import com.asiainfo.billing.drquery.utils.ServiceLocator;
import com.asiainfo.ocsearch.expression.namespace.CommonStore;
import com.asiainfo.ocsearch.listener.ThreadPoolManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * Created by mac on 2017/11/16.
 */
public class HenanGPRSProcess extends DRCommonProcess {

    public static final int timeout = Integer.parseInt(PropertiesUtil.getProperty("drquery.service/runtime.properties",
            "redis.expiretime", "300"));
    ICache redisCache = ServiceLocator.getInstance().getService("redisCache", ICache.class);

    public static final Log log = LogFactory.getLog(Process.class);


    /**
     *
     * @param countCache
     * @param pageCache
     * @param limit
     * @param request
     * @return
     */
    public String[] buildSQL(int countCache, String pageCache, int limit, CommonDRProcessRequest request) {
        String md5Phone = (String) new CommonStore().md5Prefix(request.getParam("phoneNo"));
        String sql = "", countQuery = "", detailQuery = "", startKey = "", stopKey = "";
        if("F6".equals(request.getInterfaceType())) {
            String startTime = request.getParam("startDate");
            String endTime = request.getParam("endDate");
            List<String> months = DateUtil.getMonthsBetween(startTime, endTime, "yyyyMMdd");
            if(pageCache == null)
                startKey = md5Phone + startTime;
            else
                startKey = pageCache;
            stopKey = md5Phone + endTime + "g";
            for(String month : months) {
                sql +=  "select $1 "+
                        " from THB_GPRS_CHARGE_"  + month  +
                        " where id >= '" + startKey + "' and id < '"+ stopKey + "' union all ";
            }
            sql = sql.substring(0, sql.lastIndexOf("union"));
            countQuery = sql.replace("$1", "count(1) as c");
            detailQuery = sql.replace("$1", "ID,DATA_DATE,PHONE_NO,CHARGING_ID,EPARCHY_CODE,START_TIME,DURATION,CHARGE_FLOW,TOTAL_FLOW");
        } else if("F7".equals(request.getInterfaceType())) {
            if(pageCache == null)
                startKey = md5Phone + request.getParam("dataDate");
            else
                startKey = pageCache;
            stopKey = md5Phone + request.getParam("dataDate") + "g";
            sql +=  "select $1 "+
                    " from THB_GPRS_FLOW_"  + request.getParam("dataDate").substring(0, 6)  +
                    " where id >= '" + startKey + "' and id < '"+ stopKey + "' and data_date='" + request.getParam("dataDate") + "'" +
                    "  and CHARGING_ID='" + request.getParam("chargingId") + "'" +
                    //"  and to_date(start_time, 'yyyyMMddHHmmss') >= to_date('" + request.getParam("startTime") + "', 'yyyyMMddHHmmss') - 0.0000115740741 * 60 * 5 " +
                    "  and to_date(start_time, 'yyyyMMddHHmmss') >= to_date('" + request.getParam("startTime") + "', 'yyyyMMddHHmmss')" +
                    " union all ";
            sql = sql.substring(0, sql.lastIndexOf("union"));
            countQuery = sql.replace("$1", "count(1) as c");
            detailQuery = sql.replace("$1", "ID,DATA_DATE,PHONE_NO,CHARGING_ID,NET_TYPE,ACCESS_MODE,TERM_BRAND,TERM_MODEL,START_TIME,DURATION,TOTAL_FLOW,UP_FLOW,DOWN_FLOW,PROTOCOL_TYPE_ID,BUSI_ID,BUSI_REMARK,hour_id");
        } else if("F8".equals(request.getInterfaceType())) {
            if(pageCache == null)
                startKey = md5Phone + request.getParam("dataTime");
            else
                startKey = pageCache;
            stopKey = md5Phone + request.getParam("dataTime") + "g";
            sql +=  "select $1 "+
                    " from THB_GPRS_WAP_"  + request.getParam("dataTime").substring(0, 6)   +
                    " where id >= '" + startKey + "' and id < '"+ stopKey + "' and data_time='" + request.getParam("dataTime") + "'" +
                    "  and CHARGING_ID='" + request.getParam("chargingId") + "'" +
                    "  and busi_id='" + request.getParam("busiId") +  "'" +
                    //"  and to_date(start_time, 'yyyyMMddHHmmss') >= to_date('" + request.getParam("startTime") + "', 'yyyyMMddHHmmss') - 0.0000115740741 * 60 * 5 " +
                    "  and to_date(start_time, 'yyyyMMddHHmmss') >= to_date('" + request.getParam("startTime") + "', 'yyyyMMddHHmmss') " +
                    " union all ";
            sql = sql.substring(0, sql.lastIndexOf("union"));
            countQuery = sql.replace("$1", "count(1) as c");
            detailQuery = sql.replace("$1", "ID,DATA_TIME,PHONE_NO,CHARGING_ID,BUSI_ID,START_TIME,DURATION,TOTAL_FLOW,UP_FLOW,DOWN_FLOW,BUSI_REMARK");
        } else if("F13".equals(request.getInterfaceType()) || "F14".equals(request.getInterfaceType())) {
            String appId = request.getParam("appId");
            String mainDomain = request.getParam("mainDomain");
            String startTime = request.getParam("startTime");
            String endTime = request.getParam("endTime");
            String orderColumnCode = request.getParam("orderColumnCode");
            String orderFlag = request.getParam("orderFlag");
            String dateFormat = startTime.length() > 8 ? "yyyyMMddHHmmss" : "yyyyMMdd";
            String dayTable = PropertiesUtil.getProperty("drquery.service/runtime.properties", "ocnosql.query.isDayTable");
            List<String> tableSuffixes = dayTable.equals("true") ?
                    DateUtil.getDaysBetween(startTime, endTime, dateFormat) :
                    DateUtil.getMonthsBetween(startTime, endTime, dateFormat);
            if(pageCache == null)
                startKey = md5Phone + startTime;
            else
                startKey = pageCache;
            stopKey = md5Phone + endTime + "g";
            String tablePrefix = PropertiesUtil.getProperty("drquery.service/runtime.properties", "ocnosql.query.tablePrefix");
            for(String suffix : tableSuffixes) {
                //TODO rowkey构建方式待定，所以查询条件待定
                sql +=  "select $1 "+
                        " from $6"  + suffix  +
                        " where id >= '" + startKey + "' and id < '"+ stopKey + "'" +
                        " and to_date(START_TIME, '$5') >= to_date('" + startTime + "', '$5') " +
                        " and to_date(END_TIME, '$5') < to_date('" + endTime + "', '$5') " +
                        " $2 " + " $3 " + " $4 " + " union all ";
                sql = sql.replace("$5" , dateFormat);
//                ocnosql.query.tablePrefix=GPRS_

                sql = sql.replace("$6", tablePrefix);
                if (appId != null && appId.length() > 0)
                    sql = sql.replace("$2", "and APP_ID = '"+ appId +"'");
                else
                    sql = sql.replace("$2", "");
                if (mainDomain != null && mainDomain.length() > 0)
                    sql = sql.replace("$3", "and MAIN_DOMAIN = '"+ mainDomain +"'");
                else
                    sql = sql.replace("$3", "");

            }
            sql = sql.substring(0, sql.lastIndexOf("union"));
            countQuery = sql.replace("$1", "count(1) as c").replace("$4", "");
            //TODO 表结构中没有appname；apptype为apptypeid；apnId为apn
            if("F13".equals(request.getInterfaceType()))
                detailQuery = sql.replace("$1", "ID, START_TIME, END_TIME, APP_TYPE_ID, APP_ID, MAIN_DOMAIN, APN, substr(IMEI,1,8) as IMEI,BRAND_ID, BRAND_MODEL_ID, USER_AGENT, FLOW");
            else
                detailQuery = sql.replace("$1", "ID, START_TIME, END_TIME, APP_TYPE_ID, APP_ID, MAIN_DOMAIN, APN, substr(IMEI,1,8) as IMEI，BRAND_ID, BRAND_MODEL_ID, USER_AGENT, FLOW, URL, SERVICE_HOST_IP, RAT, REGION_NAME");
            if (orderColumnCode != null && orderFlag != null)
                detailQuery = detailQuery.replace("$4", "ORDER BY " + camelToUnderline(orderColumnCode) + " " + orderFlag);
            else
                detailQuery = detailQuery.replace("$4", "");
        } else if("F12".equals(request.getInterfaceType())) {
            String appId = request.getParam("appId");
            String mainDomain = request.getParam("mainDomain");
            String startTime = request.getParam("startTime");
            String endTime = request.getParam("endTime");
            String orderColumnCode = request.getParam("orderColumnCode");
            String orderFlag = request.getParam("orderFlag");
            String dateFormat = startTime.length() > 8 ? "yyyyMMddHHmmss" : "yyyyMMdd";
            String dayTable = PropertiesUtil.getProperty("drquery.service/runtime.properties", "ocnosql.query.isDayTable");
            List<String> tableSuffixes = dayTable.equals("true") ?
                    DateUtil.getDaysBetween(startTime, endTime, dateFormat) :
                    DateUtil.getMonthsBetween(startTime, endTime, dateFormat);
            if(pageCache == null)
                startKey = md5Phone + startTime;
            else
                startKey = pageCache;
            stopKey = md5Phone + endTime + "g";
            String tablePrefix = PropertiesUtil.getProperty("drquery.service/runtime.properties", "ocnosql.query.tablePrefix");
            for(String suffix : tableSuffixes) {
                //TODO rowkey构建方式待定，所以查询条件待定
                sql +=  "select $1 "+
                        " from $6"  + suffix  +
                        " where id >= '" + startKey + "' and id < '"+ stopKey + "'" +
                        //" and to_date(START_TIME, '$5') >= to_date('" + startTime + "', '$5') " +
                        //" and to_date(END_TIME, '$5') < to_date('" + endTime + "', '$5') " +
                        " $2 " + " $3 " + " $4 " + " union all ";
                //sql = sql.replace("$5" , dateFormat);
//                ocnosql.query.tablePrefix=GPRS_

                sql = sql.replace("$6", tablePrefix);
                if (appId != null && appId.length() > 0)
                    sql = sql.replace("$2", "and APP_ID = '"+ appId +"'");
                else
                    sql = sql.replace("$2", "");
                if (mainDomain != null && mainDomain.length() > 0)
                    sql = sql.replace("$3", "and MAIN_DOMAIN = '"+ mainDomain +"'");
                else
                    sql = sql.replace("$3", "");

            }
            sql = sql.substring(0, sql.lastIndexOf("union"));
            countQuery = sql.replace("$1", "count(1) as c").replace("$4", "");
            //TODO 表结构中没有appname；apptype为apptypeid；apnId为apn
            detailQuery = sql.replace("$1", "ID,TERM_MANUF_ID,TERM_MODEL_ID,START_TIME,APP_ID,LAC,CI,PHONE_NO,ACCE_URL,TERM_TYPE_ID,SITE_CHANNEL_ID,C_GGSN_IP,CHARGING_ID,BUSI_TYPE_ID,EX_COMP_DOMAIN,SITE_NAME_ID,ACCESS_MODE_ID,round(TO_NUMBER(UP_FLOW)/1024,4) as UPFLOW,round(TO_NUMBER(DOWN_FLOW)/1024,4) as DOWNFLOW,round(TO_NUMBER(FLOW)/1024,4) as ALIASFLOW,REV_01");
            if (orderColumnCode != null && orderFlag != null)
                detailQuery = detailQuery.replace("$4", "ORDER BY " + camelToUnderline(orderColumnCode) + " " + orderFlag);
            else
                detailQuery = detailQuery.replace("$4", "");
        }

        if(limit > 0) {
            detailQuery += " limit "+ (limit + 1);
        }

        String[] sqls;
        if(countCache == -1) {
            sqls = new String[]{countQuery, detailQuery};
        } else {
            sqls = new String[]{detailQuery};
        }
        return sqls;
    }
    /**
     * 多表汇总查询top数据,内部接口
     *
     * @param request
     * @param viewMeta
     * @param extendParams
     * @return
     * @throws ProcessException
     * @throws BusinessException
     */
    public BaseDTO processF11(CommonDRProcessRequest request, MetaModel viewMeta, final Map extendParams) throws ProcessException, BusinessException {
        String startTime = request.getParam("startTime");
        String endTime = request.getParam("endTime");
        if(StringUtils.isEmpty(startTime) || startTime.length() != 14) {
            throw new IllegalArgumentException("startTime is invalid, required format yyyyMMddHHMMss, but found: " + startTime);
        }
        if(StringUtils.isEmpty(endTime) || endTime.length() != 14) {
            throw new IllegalArgumentException("endTime is invalid, required format yyyyMMddHHMMss, but found: " + startTime);
        }
        String phoneNo = request.getParam("phoneNo");
        //标识希望返回前多少条记录。
        String topNum = request.getParam("topNum");
        //当前需要分类的字段。按照应用名称分类，则其值为appName;按照一级域名分类，则其值为mainDomain
        String groupColumnCode = request.getParam("groupColumnCode");
        if("appId".equals(groupColumnCode)){//按app分组
            groupColumnCode = "BUSI_TYPE_ID,APP_ID";
        }else if("mainDomain".equals(groupColumnCode)){
            groupColumnCode = "main_domain"; //按main_domain分组
        }
        String dayTable = PropertiesUtil.getProperty("drquery.service/runtime.properties", "ocnosql.query.isDayTable");
        List<String> months = DateUtil.getSuffixesBetween(dayTable, startTime, endTime, "yyyyMMddHHmmss");

        String sql = "";
        String md5Phone = (String) new CommonStore().md5Prefix(phoneNo);
        String startKey = md5Phone + startTime;
        String stopKey = md5Phone + endTime + "g";
        for(String month : months) {
            sql += "select " + groupColumnCode  +
                    " , round(sum(to_number(flow)/1024),4) flow, sum(1)  record_count " +
                    " from GPRS_" + month  +
                    " where id >= '" + startKey + "' and id < '"+ stopKey + "' "+
                    " group by " + groupColumnCode +" union all ";
        }
        sql = sql.substring(0, sql.lastIndexOf("union"));
        sql = "select " + groupColumnCode + " groupValue , '' groupValueName, round(sum(flow),4) groupTotalFlow, sum(record_count) groupRecordCount " +
                " from ( " + sql + " )" + " as t1 " +
                " group by " + groupColumnCode + " order by groupTotalFlow desc ";

        List<Map<String, String>> list = loadData(DefaultQueryParams.newBuilder().buildSQL(sql));
        //转换groupValue成groupValueName,如果没有转义成功则设置为“其他”
        try{
            CommonFieldEscapeOperation fieldEscape1 = new CommonFieldEscapeOperation();
            list = fieldEscape1.execute(list, viewMeta, request);
        }catch (Exception e) {
            throw new DrqueryRuntimeException(e);
        }

        int topCount = 0;//计数器
        double totalFlow = 0.0;
        double otherFlow2 = 0.0;
        long othersRecourdCount = 0;
        int iTopNum = Integer.valueOf(topNum);//要返回的汇总的top app个数
        List<Map<String,String>> returnList = new ArrayList<Map<String, String>>();
        for(Map<String, String> record : list) {
            if(null == record.get("groupTotalFlow") || record.get("groupTotalFlow").equals("null")
                    || record.get("groupTotalFlow").equals("")){
                record.put("groupTotalFlow", "0");
            }
            totalFlow += NumberUtils.parseDouble(record.get("groupTotalFlow"));
            if(record.get("groupValueName")!=null && !record.get("groupValueName").equals("")
                    && record.get("groupValueName")!= "null" && topCount < iTopNum){
                topCount++;
                returnList.add(record);
            }else{
                otherFlow2 += NumberUtils.parseDouble(record.get("groupTotalFlow"));
                othersRecourdCount += NumberUtils.parseLong(record.get("groupRecordCount"));
            }
        }

        if( otherFlow2 > 0 ) {    //汇总topnum以外的数据
            Map<String,String> others = new HashMap<String,String>();
            others.put("groupValue", "others");
            others.put("groupValueName", "其他");
            others.put("groupRecordCount", String.valueOf(othersRecourdCount));
	       /* if("F12".equals(request.getInterfaceType())) {
	        	if(totalFlow != 0) {
	        		others.put("GROUP_TOTAL_FLOW_PERCENT", otherFlow2 / totalFlow + "");
	        	}else{
	        		others.put("GROUP_TOTAL_FLOW_PERCENT",  "0");
	        	}
	        }else if("F11".equals(request.getInterfaceType()))*/{
                others.put("groupTotalFlow", String.valueOf(new java.text.DecimalFormat("#.000").format(otherFlow2)));
            }
            returnList.add(others);
        }

        PageDTO dto = new PageDTO(returnList, returnList.size());
        /*if("F11".equals(request.getInterfaceType())) */{//内部接口
            Map<String,String> extData = new HashMap<String,String>();
            extData.put("groupCount", String.valueOf(returnList.size()));
            extData.put("totalFlow", String.valueOf(new java.text.DecimalFormat("#.000").format(totalFlow)));
            dto.setExtData(extData);
        }
        return dto;
    }

    //外部接口
    public BaseDTO processF12(CommonDRProcessRequest request, MetaModel viewMeta, final Map extendParams) throws ProcessException, BusinessException {
        String phoneNo = request.getParam("phoneNo");
        int startIndex = Integer.valueOf(request.getParam("startIndex"));
        int limit = -1;
        String offset = request.getParam("offset");
        if(StringUtils.isNotEmpty(offset)) {
            limit = Integer.parseInt(offset);
        }

        String cacheKey = request.generateCacheKey();
        int countCache = -1;
        String pageCache = null;
        List counterAndRowKey = CacheProvider.getCountAndRowkeyInfo(cacheKey, startIndex);
        if(counterAndRowKey.get(0) != null) {  //不是第一次查询
            countCache = (Integer) counterAndRowKey.get(0);
            pageCache = (String) counterAndRowKey.get(1);
        }

        String[] sqls = buildSQL(countCache, pageCache, limit, request);

        List<Future<List<Map<String, String>>>> futures = new ArrayList<Future<List<Map<String, String>>>>();
        for(int i = 0; i < sqls.length; i++) {
            final String querySql = sqls[i];
            Callable<List<Map<String, String>>> callable = new Callable<List<Map<String, String>>>() {
                //@Override
                public List<Map<String, String>> call() throws Exception {
                    return loadData(DefaultQueryParams.newBuilder().buildSQL(querySql));
                }
            };
            ThreadPoolManager.getExecutor("getQuery").submit(callable);
        }
        int i = 0;
        List<Map<String, String>> countRecords = null;
        List<Map<String, String>> detailRecords = null;
        try {
            for (Future<List<Map<String, String>>> future : futures) {
                if(i == 0 && countCache == -1)
                    countRecords = future.get();
                else
                    detailRecords = future.get();
                i ++;
            }
        } catch (Exception e) {
            throw new ProcessException("execute query exception", e);
        }
        int totalCount = 0;
        if(countCache == -1) {
            for (Map<String, String> record : countRecords) {
                totalCount += Integer.parseInt(record.get("C"));
            }
            CacheProvider.put(cacheKey, -9, totalCount, timeout);
        } else {
            totalCount = countCache;
        }
        String nextKey = null;
        if(limit != -1 && detailRecords.size() == limit + 1) {
            nextKey = detailRecords.get(detailRecords.size() -1).get("ID");
            CacheProvider.put(cacheKey, startIndex + limit, nextKey, timeout);
            detailRecords = detailRecords.subList(0, limit);
        }
        Map<String, Integer> extData = new HashMap<String, Integer>();
        extData.put("totalCount", totalCount);
        extData.put("startIndex", startIndex);
        extData.put("offset", limit);
        return new PageDTO(detailRecords, extData, totalCount);
    }

    public static final char UNDERLINE='_';
    public static String camelToUnderline(String param){
        if (param==null||"".equals(param.trim())){
            return "";
        }
        int len=param.length();
        StringBuilder sb=new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            char c=param.charAt(i);
            if (Character.isUpperCase(c)){
                sb.append(UNDERLINE);
                sb.append(Character.toLowerCase(c));
            }else{
                sb.append(c);
            }
        }
        return sb.toString();
    }

}
