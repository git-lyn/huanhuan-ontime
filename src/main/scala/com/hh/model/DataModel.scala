/*
 * Copyright (c) 2018. Atguigu Inc. All Rights Reserved.
 */

package com.hh.model

/**
  * 广告黑名单
  * @author lyn
  *
  */
case class AdBlacklist(userid:Long)

/**
  * 用户广告点击量
  * @author lyn
  *
  */
case class AdUserClickCount(date:String,
                            userid:Long,
                            adid:Long,
                            clickCount:Long)


/**
  * 广告实时统计
  * @author lyn
  *
  */
case class AdStat(date:String,
                  province:String,
                  city:String,
                  adid:Long,
                  clickCount:Long)

/**
  * 各省top3热门广告
  * @author lyn
  *
  */
case class AdProvinceTop3(date:String,
                          province:String,
                          adid:Long,
                          clickCount:Long)

/**
  * 广告点击趋势
  * @author lyn
  *
  */
case class AdClickTrend(date:String,
                        hour:String,
                        minute:String,
                        adid:Long,
                        clickCount:Long)


/**
 * 首页埋点点击量实时查询
 * @param date
 * @param position
 * @param userid
 * @param clickCount
 */
case class IndexClickCount(date: String,
                           position: String,
                           userid: String,
                           clickCount: Long)