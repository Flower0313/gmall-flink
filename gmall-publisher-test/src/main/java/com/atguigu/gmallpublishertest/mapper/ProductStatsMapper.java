package com.atguigu.gmallpublishertest.mapper;

import com.atguigu.gmall.realtime.bean.ProductStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName gmall-flink-ProductStats
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月24日1:38 - 周五
 * @Describe 商品统计接口
 */
public interface ProductStatsMapper {
    //按某一天的总交易额
    @Select("select " +
            "   sum(order_amount) " +
            "from product_stats " +
            "where toYYYYMMDD(stt)=#{date}")
    public BigDecimal getGMV(@Param("date") int date);


    /*
     * Explain
     *  统计某天不同品牌商品交易额排名,实际去查询的操作
     *  List(Map["tm_id":"9","tm_name":"华为"],Map["tm_id":"10","tm_name":"apple"],...)
     * */
    @Select("select tm_id,tm_name," +
            "sum(order_amount) order_amount " +
            "from product_stats " +
            "where toYYYYMMDD(stt)=#{date} group by tm_id,tm_name " +
            "having order_amount>0  order by  order_amount  desc limit #{limit} ")
    public List<Map> getProductStatsByTrademark(@Param("date") int date, @Param("limit") int limit);


    //统计某天不同类别商品交易额排名
    @Select("select " +
            "    category3_name," +
            "    sum(order_amount) order_amount " +
            "from product_stats " +
            "where toYYYYMMDD(stt) = #{date} " +
            "group by category3_name " +
            "having order_amount>0 " +
            "order by order_amount desc limit #{limit}")
    public List<Map> getProductStatsGroupByCategory3(@Param("date") int date, @Param("limit") int limit);


    //统计某天不同SPU商品交易额排名
    @Select("select spu_id," +
            "       spu_name," +
            "       sum(order_amount) order_amount," +
            "       sum(order_ct)     order_ct " +
            "from product_stats " +
            "where toYYYYMMDD(stt) = #{date} " +
            "group by spu_id, spu_name " +
            "having order_amount > 0 " +
            "order by order_amount desc limit #{limit}")
    public List<Map> getProductStatsGroupBySpu(@Param("date") int date, @Param("limit") int limit);


}
