CREATE PROCEDURE SMY.PROC_CR_CRD_TXN_DLY_SMY(ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_CR_CRD_TXN_DLY_SMY.sql
-- Procedure name: 			SMY.PROC_CR_CRD_TXN_DLY_SMY
-- Source Table:				SOR.CR_CRD,SOR.CRD,SOR.CC_AC_AR,SOR.CC_AC_TXN_DTL
-- Target Table: 				SMY.CR_CRD_TXN_DLY_SMY
-- Project:             ZJ RCCB EDW
--
-- Purpose:             
--
--=============================================================================
-- Creation Date:       2009.11.9
-- Origin Author:       Wang Youbing
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2009-11-9   Wang Youbing     Create SP File
-- 2009-11-13	 Wang Youbing     Update SP File	
-- 2009-12-18  Xu Yan           Added a new column 'REPYMT_TP_ID'
-- 2010-01-04  Xu Yan           Updated the conditional statement for the CR_CRD_SMY to include the transactions of the final day
-- 2010-01-13  Xu Yan           Included '11920004 --已收卡'
-- 2010-01-20  Xu Yan           Updated the conditional statement for the TEL_POS
-- 2010-01-20  Xu Yan           Removed all the conditional limit on Card Lcs
-- 2010-01-21  Xu Yan           Updated the joining condition for TEL_POS
-- 2010-01-21  Xu Yan           Restored the modification on card LCS
-- 2010-03-01  Xu Yan           Changed the SMY_STEPNUM position.
-- 2010-03-01  Xu Yan           Updated for the monthly summary.
-- 2011-05-24  Wang Youbing     Updated for 贷记卡消费取数逻辑
-- 2012-02-27  Zheng Bin        Updated for 交易渠道的取数逻辑
-------------------------------------------------------------------------------
LANGUAGE SQL
BEGIN
/*声明异常处理使用变量*/
DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
DECLARE SMY_STEPNUM INT DEFAULT 0;                     --过程内部位置标记
DECLARE SMY_STEPDESC VARCHAR(100) DEFAULT '';          --过程内部位置描述
DECLARE SMY_DATE DATE;                                 --临时日期变量
DECLARE SMY_RCOUNT INT;                                --DML语句作用记录数
DECLARE SMY_PROCNM VARCHAR(100);                        --存储过程名称
DECLARE at_end SMALLINT DEFAULT 0;--
/*声明存储过程使用变量*/
DECLARE CUR_YEAR SMALLINT;                             --年
DECLARE CUR_MONTH SMALLINT;                            --月
DECLARE CUR_DAY INTEGER;                               --日
DECLARE YR_FIRST_DAY DATE;                             --本年初1月1日
DECLARE QTR_FIRST_DAY DATE;                            --本季度第1日
DECLARE MONTH_FIRST_DAY DATE;                          --本月第1日
DECLARE NEXT_YR_FIRST_DAY DATE;                        --下年1月1日
DECLARE NEXT_QTR_FIRST_DAY DATE;                       --下季度第1日
DECLARE NEXT_MONTH_FIRST_DAY DATE;                     --下月第1日
DECLARE MONTH_DAY SMALLINT;                            --本月天数
DECLARE YR_DAY SMALLINT;                               --本年天数
DECLARE QTR_DAY SMALLINT;                              --本季度天数
DECLARE NEXT_DAY SMALLINT;                             --下一天
--DECLARE MAX_ACG_DT DATE;                               --最大会计日期
--DECLARE DELETE_SQL VARCHAR(200);                       --删除历史表动态SQL

/*1.定义针对SQL异常情况的句柄(EXIT方式).
  2.将出现SQL异常时在存储过程中的位置(SMY_STEPNUM),位置描述(SMY_STEPDESC)，SQLCODE(SMY_SQLCODE)记入表SMY_LOG中作调试用.
  3.调用RESIGNAL重新引发异常,跳出存储过程执行体,对引发SQL异常之前存储过程体中所完成的操作进行回滚.*/
DECLARE CONTINUE HANDLER FOR NOT FOUND
SET at_end=1;  --
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
	SET SMY_SQLCODE = SQLCODE;--
  ROLLBACK;--
  SET SMY_STEPNUM = SMY_STEPNUM+1;--
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
  COMMIT;--
  RESIGNAL;--
END;--
DECLARE CONTINUE HANDLER FOR SQLWARNING
BEGIN
  SET SMY_SQLCODE = SQLCODE;--
  SET SMY_STEPNUM = SMY_STEPNUM+1;--
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
  COMMIT;--
END;--
/*变量赋值*/
SET SMY_PROCNM = 'PROC_CR_CRD_TXN_DLY_SMY';--
SET SMY_DATE=ACCOUNTING_DATE;--
SET CUR_YEAR=YEAR(ACCOUNTING_DATE);  --取当前年份
SET CUR_MONTH=MONTH(ACCOUNTING_DATE); --取当前月份
SET CUR_DAY=DAY(ACCOUNTING_DATE);     --取当日
SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');--
SET NEXT_YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR+1))||'-01-01');--
SET YR_DAY=DAYS(NEXT_YR_FIRST_DAY)-DAYS(YR_FIRST_DAY);--
IF CUR_MONTH IN (1,2,3) THEN 
   SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');--
   SET NEXT_QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-04-01');--
ELSEIF CUR_MONTH IN (4,5,6) THEN 
       SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-04-01');--
       SET NEXT_QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-07-01');--
    ELSEIF CUR_MONTH IN (7,8,9) THEN 
           SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-07-01');--
           SET NEXT_QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-10-01');--
        ELSE
            SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-10-01');--
            SET NEXT_QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR+1))||'-01-01');--
END IF;--
SET QTR_DAY=DAYS(NEXT_QTR_FIRST_DAY)-DAYS(QTR_FIRST_DAY);--
SET MONTH_FIRST_DAY=DATE(TRIM(CHAR(TRIM(CHAR(CUR_YEAR))||'-'||RIGHT('0'||TRIM(CHAR(CUR_MONTH)),2)||'-01')));--
IF CUR_MONTH=12 THEN
   SET NEXT_MONTH_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR+1))||'-01-01');--
ELSE
   SET NEXT_MONTH_FIRST_DAY=DATE(TRIM(CHAR(TRIM(CHAR(CUR_YEAR))||'-'||RIGHT('0'||TRIM(CHAR(CUR_MONTH+1)),2)||'-01')));--
END IF;--
SET MONTH_DAY=DAYS(NEXT_MONTH_FIRST_DAY)-DAYS(MONTH_FIRST_DAY);--
SET NEXT_DAY=DAY(DATE(ACCOUNTING_DATE)+1 DAYS);--
--SELECT MAX(ACG_DT) INTO MAX_ACG_DT FROM SMY.CR_CRD_TXN_DLY_SMY;--
--SET DELETE_SQL='ALTER TABLE SMY.HIST_CR_CRD_TXN_DLY_SMY ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE';--

/*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE,并插入新的起始标志*/
DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
COMMIT;--
SET SMY_STEPNUM = SMY_STEPNUM+1;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, 'PROCEDURE START.', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

/*数据恢复与备份*/
DELETE FROM SMY.CR_CRD_TXN_DLY_SMY WHERE ACG_DT=ACCOUNTING_DATE;--
COMMIT;--

SET SMY_STEPNUM = SMY_STEPNUM+1;--
SET SMY_STEPDESC = '定义系统临时表,按照维度汇总,临时存放当日交易数据.';--

DECLARE GLOBAL TEMPORARY TABLE TMP(OU_ID CHAR(18),
                                   CRD_TP_ID INTEGER,
                                   CRD_Brand_TP_Id INTEGER,
                                   CRD_PRVL_TP_ID INTEGER,
                                   ENT_IDV_IND INTEGER,
                                   MST_CRD_IND INTEGER,
                                   NGO_CRD_IND INTEGER,
                                   MULT_CCY_F SMALLINT,
                                   AST_RSK_ASES_RTG_TP_CD CHARACTER(2),
                                   LN_FIVE_RTG_STS INTEGER,
                                   PD_GRP_CD CHAR(2),
                                   PD_SUB_CD CHAR(3),
                                   TXN_CNL_TP_CD CHAR(2),
                                   CASH_TFR_IND INTEGER,
                                   DB_CR_IND INTEGER,
                                   CNSPN_TXN_F SMALLINT,  ---WYB 20081118                                   
                                   CCY CHAR(3),
                                   ACG_DT DATE,
                                   CDR_YR INTEGER,
                                   CDR_MTH INTEGER,
                                   ISSU_CRD_OU_Id CHAR(18),
                                   NBR_TXN INTEGER,
                                   TXN_AMT DECIMAL(17,2)
                                   ,REPYMT_TP_ID INTEGER
                                   )
ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE   IN TS_USR_TMP32K PARTITIONING KEY(OU_ID);--
INSERT INTO SESSION.TMP
Select A.APL_ACPT_OU_IP_ID AS OU_ID,
       A.CC_TP_ID AS CRD_TP_ID,
       B.CRD_BRND_TP_ID AS CRD_Brand_TP_Id,
       A.CRD_PRVL_TP_ID AS CRD_PRVL_TP_ID,
       B.ENT_IDV_CST_IND AS ENT_IDV_IND,
       A.MST_CRD_IND AS MST_CRD_IND,
       B.NGO_CRD_IND AS NGO_CRD_IND,
       B.MULTI_CCY_F AS MULT_CCY_F,
       C.AST_RSK_ASES_RTG_TP_CD AS AST_RSK_ASES_RTG_TP_CD,
       C.LN_FR_RSLT_TP_ID AS LN_FIVE_RTG_STS,
       B.PD_GRP_CD AS PD_GRP_CD,
       B.PD_SUB_CD AS PD_SUB_CD,
       -- D.TXN_CNL_TP_CD AS TXN_CNL_TP_CD,                                                    --20120227old_version modified by zhengbin 
     (CASE WHEN E.CNL_TP=21690010 THEN 'PT' ELSE D.TXN_CNL_TP_CD END) AS TXN_CNL_TP_CD,        --20120227new_version modified by zhengbin 
       D.CASH_TFR_IND AS CASH_TFR_IND,
       D.DB_CR_IND AS DB_CR_IND,
       -- (CASE WHEN SUBSTR(E.MCHNT_AC_AR_ID,2,1)='1' THEN 1 ELSE 0 END) AS CNSPN_TXN_F,                    --WYB 20081118 20120227old_version modified by zhengbin 
     (CASE WHEN (SUBSTR(E.MCHNT_AC_AR_ID,2,1)='1' and E.ALS_CNL='CP' ) THEN 1 ELSE 0 END) AS CNSPN_TXN_F,   --20120227new_version modified by zhengbin 
       D.DNMN_CCY_ID AS CCY,
       ACCOUNTING_DATE AS ACG_DT,
       CUR_YEAR AS CDR_YR,
       CUR_MONTH AS CDR_MTH,
       A.ISSU_CRD_OU_IP_ID AS ISSU_CRD_OU_Id,
       COUNT(1) AS NBR_TXN,
       SUM(COALESCE(D.TXN_AMT,0)) AS TXN_AMT  
       ,D.REPYMT_TP_ID   
from SOR.CR_CRD A
inner join SOR.CRD B
--------------------------Start on 20100104-------------------------------------------------
--on A.CC_NO =B.CRD_NO AND B.CRD_LCS_TP_ID=11920001
on A.CC_NO =B.CRD_NO
--------------------------End on 20100104-------------------------------------------------
inner join SOR.CC_AC_AR C
on B.AC_AR_ID=C.CC_AC_AR_ID AND A.PRIM_CCY_ID=C.DNMN_CCY_ID
inner join SOR.CC_AC_TXN_DTL D
on B.AC_AR_ID=D.CC_AC_AR_ID and A.PRIM_CCY_ID=D.DNMN_CCY_ID and D.TXN_TP_ID=20460007 and D.TXN_ACG_DT=ACCOUNTING_DATE and D.DEL_F = 0
left join SOR.STMT_DEP_AC_RGST E
-----------------------------------Start on 20100120-----------------------------------------------
--on D.CC_AC_AR_ID=E.AC_AR_ID AND D.DNMN_CCY_ID=E.DNMN_CCY_ID AND D.SUB_TXN_RUN_NBR=E.SUB_TXN_RUN_NBR AND D.ORIG_TXN_RUN_NBR=E.ORG_TXN_RUN_NBR AND E.TXN_DT=ACCOUNTING_DATE AND E.CNL_TP=21690008 and ALS_CNL='CP'   --E.CNL_TP='电话POS'
--on D.CC_AC_AR_ID=E.AC_AR_ID AND D.DNMN_CCY_ID=E.DNMN_CCY_ID AND D.SUB_TXN_RUN_NBR=E.SUB_TXN_RUN_NBR AND D.ORIG_TXN_RUN_NBR=E.ORG_TXN_RUN_NBR AND E.TXN_DT=ACCOUNTING_DATE and ALS_CNL='CP' AND E.DEL_F =0 --E.CNL_TP='电话POS'
-----------Start on 20100121-------------------------------
-----------Start On 20110524-------------------------------
--on D.CC_AC_AR_ID=E.AC_AR_ID AND D.DNMN_CCY_ID=E.DNMN_CCY_ID AND D.ORIG_TXN_RUN_NBR=E.ORG_TXN_RUN_NBR AND E.TXN_DT=ACCOUNTING_DATE and ALS_CNL='CP' AND E.DEL_F =0 --E.CNL_TP='电话POS'
--on D.VCHR_NO=E.CC_AC_AR_ID AND D.DNMN_CCY_ID=E.DNMN_CCY_ID AND D.ORIG_TXN_RUN_NBR=E.ORG_TXN_RUN_NBR AND E.TXN_DT=ACCOUNTING_DATE and ALS_CNL='CP' AND E.DEL_F =0 -- 20120227old_version
on D.VCHR_NO=E.CC_AC_AR_ID AND D.DNMN_CCY_ID=E.DNMN_CCY_ID AND D.ORIG_TXN_RUN_NBR=E.ORG_TXN_RUN_NBR AND E.TXN_DT=ACCOUNTING_DATE AND E.DEL_F =0 -- 20120227new_version modified by zhengbin 

-----------Start On 20110524-------------------------------
-----------End on 20100121-------------------------------
-----------------------------------End on 20100120-----------------------------------------------
-----------------------------------Start on 20100120-----------------------------------------------

-------------------------Start on 20100104-----------------------------------------------------
----------------------Start on 20100113-----------------
--where B.CRD_LCS_TP_ID=11920001 -- 正常
where B.CRD_LCS_TP_ID in (
													 11920001 -- 正常
													,11920002  --新开卡未启用
              				    ,11920003  --新换卡未启用
                          ,11920004 --已收卡
                          )
----------------------End on 20100113-----------------
      or
      B.END_DT = SMY_DATE
-------------------------End on 20100104-----------------------------------------------------

-----------------------------------End on 20100120-----------------------------------------------
GROUP BY A.APL_ACPT_OU_IP_ID,
         A.CC_TP_ID,
         B.CRD_BRND_TP_ID,
         A.CRD_PRVL_TP_ID,
         B.ENT_IDV_CST_IND,
         A.MST_CRD_IND,
         B.NGO_CRD_IND,
         B.MULTI_CCY_F,
         C.AST_RSK_ASES_RTG_TP_CD,
         C.LN_FR_RSLT_TP_ID,
         B.PD_GRP_CD,
         B.PD_SUB_CD,
        -- D.TXN_CNL_TP_CD,                                                    --20120227old_version modified by zhengbin 
         (CASE WHEN E.CNL_TP=21690010 THEN 'PT' ELSE D.TXN_CNL_TP_CD END),        --20120227new_version modified by zhengbin 
         D.CASH_TFR_IND,
         D.DB_CR_IND,
         --(CASE WHEN SUBSTR(E.MCHNT_AC_AR_ID,2,1)='1' THEN 1 ELSE 0 END) AS CNSPN_TXN_F,                         ---20120227old_version modified by zhengbin 
         (CASE WHEN (SUBSTR(E.MCHNT_AC_AR_ID,2,1)='1' and E.ALS_CNL='CP' ) THEN 1 ELSE 0 END),     ---20120227new_version modified by zhengbin 
         D.DNMN_CCY_ID,
         A.ISSU_CRD_OU_IP_ID
         ,D.REPYMT_TP_ID;--

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
SET SMY_STEPNUM = SMY_STEPNUM+1;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

IF CUR_DAY=1 THEN                                                                     --月初
   IF CUR_MONTH IN (4,7,10) THEN                                                      --季初非年初
      
      SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
      SET SMY_STEPDESC = '插入汇总表当日交易数据.';--
      
      INSERT INTO SMY.CR_CRD_TXN_DLY_SMY(OU_ID,
                                   CRD_TP_ID,
                                   CRD_Brand_TP_Id,
                                   CRD_PRVL_TP_ID,
                                   ENT_IDV_IND,
                                   MST_CRD_IND,
                                   NGO_CRD_IND,
                                   MULT_CCY_F,
                                   AST_RSK_ASES_RTG_TP_CD,
                                   LN_FIVE_RTG_STS,
                                   PD_GRP_CD,
                                   PD_SUB_CD,
                                   TXN_CNL_TP_CD,
                                   CASH_TFR_IND,
                                   DB_CR_IND,
                                   CNSPN_TXN_F,
                                   CCY,
                                   ACG_DT,
                                   CDR_YR,
                                   CDR_MTH,
                                   ISSU_CRD_OU_Id,
                                   NBR_TXN,
                                   TXN_AMT,
                                   TOT_MTD_TXN_AMT,
                                   TOT_MTD_NBR_TXN,
                                   TOT_QTD_TXN_AMT,
                                   TOT_QTD_NBR_TXN,
                                   TOT_YTD_TXN_AMT,
                                   TOT_YTD_NBR_TXN
                                   ,REPYMT_TP_ID
                                   )
      SELECT OU_ID,
             CRD_TP_ID,
             CRD_Brand_TP_Id,
             CRD_PRVL_TP_ID,
             ENT_IDV_IND,
             MST_CRD_IND,
             NGO_CRD_IND,
             MULT_CCY_F,
             AST_RSK_ASES_RTG_TP_CD,
             LN_FIVE_RTG_STS,
             PD_GRP_CD,
             PD_SUB_CD,
             TXN_CNL_TP_CD,
             CASH_TFR_IND,
             DB_CR_IND,
             CNSPN_TXN_F,
             CCY,
             ACCOUNTING_DATE AS ACG_DT,
             CDR_YR,
             ----------Start on 20100301-------------
             --CDR_MTH,
             CUR_MONTH,
             ----------End on 20100301-------------
             ISSU_CRD_OU_Id,
             0 AS NBR_TXN,
             0 AS TXN_AMT,
             0 AS TOT_MTD_TXN_AMT,
             0 AS TOT_MTD_NBR_TXN,
             0 AS TOT_QTD_TXN_AMT,
             0 AS TOT_QTD_NBR_TXN,
             TOT_YTD_TXN_AMT,
             TOT_YTD_NBR_TXN
             ,REPYMT_TP_ID
      FROM SMY.CR_CRD_TXN_DLY_SMY
      WHERE ACG_DT=DATE(ACCOUNTING_DATE) - 1 DAYS;--
            
      GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
      SET SMY_STEPNUM = SMY_STEPNUM+1;--
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
      COMMIT;--
      
      --SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
      SET SMY_STEPDESC = '使用当日交易数据更新汇总表.';--
      
      MERGE INTO SMY.CR_CRD_TXN_DLY_SMY S
      USING SESSION.TMP T
      ON S.OU_ID=T.OU_ID
      AND S.CRD_TP_ID=T.CRD_TP_ID
      AND S.CRD_Brand_TP_Id=T.CRD_Brand_TP_Id
      AND S.CRD_PRVL_TP_ID=T.CRD_PRVL_TP_ID
      AND S.ENT_IDV_IND=T.ENT_IDV_IND
      AND S.MST_CRD_IND=T.MST_CRD_IND
      AND S.NGO_CRD_IND=T.NGO_CRD_IND
      AND S.MULT_CCY_F=T.MULT_CCY_F
      AND S.AST_RSK_ASES_RTG_TP_CD=T.AST_RSK_ASES_RTG_TP_CD
      AND S.LN_FIVE_RTG_STS=T.LN_FIVE_RTG_STS
      AND S.PD_GRP_CD=T.PD_GRP_CD
      AND S.PD_SUB_CD=T.PD_SUB_CD
      AND S.TXN_CNL_TP_CD=T.TXN_CNL_TP_CD
      AND S.CASH_TFR_IND=T.CASH_TFR_IND
      AND S.DB_CR_IND=T.DB_CR_IND
      AND S.CNSPN_TXN_F=T.CNSPN_TXN_F
      AND S.CCY=T.CCY
      AND S.ACG_DT=T.ACG_DT
      AND S.REPYMT_TP_ID = T.REPYMT_TP_ID
      WHEN MATCHED 
      THEN UPDATE SET(OU_ID,
                      CRD_TP_ID,
                      CRD_Brand_TP_Id,
                      CRD_PRVL_TP_ID,
                      ENT_IDV_IND,
                      MST_CRD_IND,
                      NGO_CRD_IND,
                      MULT_CCY_F,
                      AST_RSK_ASES_RTG_TP_CD,
                      LN_FIVE_RTG_STS,
                      PD_GRP_CD,
                      PD_SUB_CD,
                      TXN_CNL_TP_CD,
                      CASH_TFR_IND,
                      DB_CR_IND,
                      CNSPN_TXN_F,
                      CCY,
                      ACG_DT,
                      CDR_YR,
                      CDR_MTH,
                      ISSU_CRD_OU_Id,
                      NBR_TXN,
                      TXN_AMT,
                      TOT_MTD_TXN_AMT,
                      TOT_MTD_NBR_TXN,
                      TOT_QTD_TXN_AMT,
                      TOT_QTD_NBR_TXN,
                      TOT_YTD_TXN_AMT,
                      TOT_YTD_NBR_TXN                      
                      )
                    =(T.OU_ID,
                      T.CRD_TP_ID,
                      T.CRD_Brand_TP_Id,
                      T.CRD_PRVL_TP_ID,
                      T.ENT_IDV_IND,
                      T.MST_CRD_IND,
                      T.NGO_CRD_IND,
                      T.MULT_CCY_F,
                      T.AST_RSK_ASES_RTG_TP_CD,
                      T.LN_FIVE_RTG_STS,
                      T.PD_GRP_CD,
                      T.PD_SUB_CD,
                      T.TXN_CNL_TP_CD,
                      T.CASH_TFR_IND,
                      T.DB_CR_IND,
                      T.CNSPN_TXN_F,
                      T.CCY,
                      T.ACG_DT,
                      T.CDR_YR,
                      T.CDR_MTH,
                      T.ISSU_CRD_OU_Id,
                      T.NBR_TXN,
                      T.TXN_AMT,
                      T.TXN_AMT,
                      T.NBR_TXN,
                      T.TXN_AMT,
                      T.NBR_TXN,
                      S.TOT_YTD_TXN_AMT+T.TXN_AMT,
                      S.TOT_YTD_NBR_TXN+T.NBR_TXN                      
                      )
      WHEN NOT MATCHED
      THEN INSERT(OU_ID,
                  CRD_TP_ID,
                  CRD_Brand_TP_Id,
                  CRD_PRVL_TP_ID,
                  ENT_IDV_IND,
                  MST_CRD_IND,
                  NGO_CRD_IND,
                  MULT_CCY_F,
                  AST_RSK_ASES_RTG_TP_CD,
                  LN_FIVE_RTG_STS,
                  PD_GRP_CD,
                  PD_SUB_CD,
                  TXN_CNL_TP_CD,
                  CASH_TFR_IND,
                  DB_CR_IND,
                  CNSPN_TXN_F,
                  CCY,
                  ACG_DT,
                  CDR_YR,
                  CDR_MTH,
                  ISSU_CRD_OU_Id,
                  NBR_TXN,
                  TXN_AMT,
                  TOT_MTD_TXN_AMT,
                  TOT_MTD_NBR_TXN,
                  TOT_QTD_TXN_AMT,
                  TOT_QTD_NBR_TXN,
                  TOT_YTD_TXN_AMT,
                  TOT_YTD_NBR_TXN
                  ,REPYMT_TP_ID
                  )
           VALUES(T.OU_ID,
                  T.CRD_TP_ID,
                  T.CRD_Brand_TP_Id,
                  T.CRD_PRVL_TP_ID,
                  T.ENT_IDV_IND,
                  T.MST_CRD_IND,
                  T.NGO_CRD_IND,
                  T.MULT_CCY_F,
                  T.AST_RSK_ASES_RTG_TP_CD,
                  T.LN_FIVE_RTG_STS,
                  T.PD_GRP_CD,
                  T.PD_SUB_CD,
                  T.TXN_CNL_TP_CD,
                  T.CASH_TFR_IND,
                  T.DB_CR_IND,
                  T.CNSPN_TXN_F,
                  T.CCY,
                  T.ACG_DT,
                  T.CDR_YR,
                  T.CDR_MTH,
                  T.ISSU_CRD_OU_Id,
                  T.NBR_TXN,
                  T.TXN_AMT,
                  T.TXN_AMT,
                  T.NBR_TXN,
                  T.TXN_AMT,
                  T.NBR_TXN,
                  T.TXN_AMT,
                  T.NBR_TXN
                  ,T.REPYMT_TP_ID
                  );--
      
      GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
      SET SMY_STEPNUM = SMY_STEPNUM+1;--
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);  --
      COMMIT;       --
      
   ELSEIF CUR_MONTH=1 THEN                                                              --年初
          
          --SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
          SET SMY_STEPDESC = '插入汇总表当日交易数据.';--
   
          INSERT INTO SMY.CR_CRD_TXN_DLY_SMY(OU_ID,
                                             CRD_TP_ID,
                                             CRD_Brand_TP_Id,
                                             CRD_PRVL_TP_ID,
                                             ENT_IDV_IND,
                                             MST_CRD_IND,
                                             NGO_CRD_IND,
                                             MULT_CCY_F,
                                             AST_RSK_ASES_RTG_TP_CD,
                                             LN_FIVE_RTG_STS,
                                             PD_GRP_CD,
                                             PD_SUB_CD,
                                             TXN_CNL_TP_CD,
                                             CASH_TFR_IND,
                                             DB_CR_IND,
                                             CNSPN_TXN_F,
                                             CCY,
                                             ACG_DT,
                                             CDR_YR,
                                             CDR_MTH,
                                             ISSU_CRD_OU_Id,
                                             NBR_TXN,
                                             TXN_AMT,
                                             TOT_MTD_TXN_AMT,
                                             TOT_MTD_NBR_TXN,
                                             TOT_QTD_TXN_AMT,
                                             TOT_QTD_NBR_TXN,
                                             TOT_YTD_TXN_AMT,
                                             TOT_YTD_NBR_TXN
                                             ,REPYMT_TP_ID)
          SELECT S.OU_ID,
                 S.CRD_TP_ID,
                 S.CRD_Brand_TP_Id,
                 S.CRD_PRVL_TP_ID,
                 S.ENT_IDV_IND,
                 S.MST_CRD_IND,
                 S.NGO_CRD_IND,
                 S.MULT_CCY_F,
                 S.AST_RSK_ASES_RTG_TP_CD,
                 S.LN_FIVE_RTG_STS,
                 S.PD_GRP_CD,
                 S.PD_SUB_CD,
                 S.TXN_CNL_TP_CD,
                 S.CASH_TFR_IND,
                 S.DB_CR_IND,
                 S.CNSPN_TXN_F,
                 S.CCY,
                 S.ACG_DT,
                 S.CDR_YR,
                 S.CDR_MTH,
                 S.ISSU_CRD_OU_Id,
                 S.NBR_TXN,
                 S.TXN_AMT,
                 S.TXN_AMT,
                 S.NBR_TXN,
                 S.TXN_AMT,
                 S.NBR_TXN,
                 S.TXN_AMT,
                 S.NBR_TXN
                 ,S.REPYMT_TP_ID
          FROM SESSION.TMP S;--
                
          GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
          SET SMY_STEPNUM = SMY_STEPNUM+1;--
          INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
          COMMIT;--
        ELSE                                                                             --月初非季初非年初
      
          --SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
          SET SMY_STEPDESC = '插入汇总表当日交易数据.';--
          
          INSERT INTO SMY.CR_CRD_TXN_DLY_SMY(OU_ID,
                                             CRD_TP_ID,
                                             CRD_Brand_TP_Id,
                                             CRD_PRVL_TP_ID,
                                             ENT_IDV_IND,
                                             MST_CRD_IND,
                                             NGO_CRD_IND,
                                             MULT_CCY_F,
                                             AST_RSK_ASES_RTG_TP_CD,
                                             LN_FIVE_RTG_STS,
                                             PD_GRP_CD,
                                             PD_SUB_CD,
                                             TXN_CNL_TP_CD,
                                             CASH_TFR_IND,
                                             DB_CR_IND,
                                             CNSPN_TXN_F,
                                             CCY,
                                             ACG_DT,
                                             CDR_YR,
                                             CDR_MTH,
                                             ISSU_CRD_OU_Id,
                                             NBR_TXN,
                                             TXN_AMT,
                                             TOT_MTD_TXN_AMT,
                                             TOT_MTD_NBR_TXN,
                                             TOT_QTD_TXN_AMT,
                                             TOT_QTD_NBR_TXN,
                                             TOT_YTD_TXN_AMT,
                                             TOT_YTD_NBR_TXN
                                             ,REPYMT_TP_ID)
          SELECT OU_ID,
                 CRD_TP_ID,
                 CRD_Brand_TP_Id,
                 CRD_PRVL_TP_ID,
                 ENT_IDV_IND,
                 MST_CRD_IND,
                 NGO_CRD_IND,
                 MULT_CCY_F,
                 AST_RSK_ASES_RTG_TP_CD,
                 LN_FIVE_RTG_STS,
                 PD_GRP_CD,
                 PD_SUB_CD,
                 TXN_CNL_TP_CD,
                 CASH_TFR_IND,
                 DB_CR_IND,
                 CNSPN_TXN_F,
                 CCY,
                 ACCOUNTING_DATE AS ACG_DT,
                 CDR_YR,
                 -------------Start on 20100301------------
                 --CDR_MTH,
                 CUR_MONTH,
                 -------------End on 20100301------------
                 ISSU_CRD_OU_Id,
                 0 AS NBR_TXN,
                 0 AS TXN_AMT,
                 0 AS TOT_MTD_TXN_AMT,
                 0 AS TOT_MTD_NBR_TXN,
                 TOT_QTD_TXN_AMT,
                 TOT_QTD_NBR_TXN,
                 TOT_YTD_TXN_AMT,
                 TOT_YTD_NBR_TXN
                 ,REPYMT_TP_ID
          FROM SMY.CR_CRD_TXN_DLY_SMY
          WHERE ACG_DT=DATE(ACCOUNTING_DATE) - 1 DAYS;--
                
          GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
          SET SMY_STEPNUM = SMY_STEPNUM+1;--
          INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
          COMMIT;--
          
          --SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
          SET SMY_STEPDESC = '使用当日交易数据更新汇总表.';--
          
          MERGE INTO SMY.CR_CRD_TXN_DLY_SMY S
          USING SESSION.TMP T
          ON S.OU_ID=T.OU_ID
          AND S.CRD_TP_ID=T.CRD_TP_ID
          AND S.CRD_Brand_TP_Id=T.CRD_Brand_TP_Id
          AND S.CRD_PRVL_TP_ID=T.CRD_PRVL_TP_ID
          AND S.ENT_IDV_IND=T.ENT_IDV_IND
          AND S.MST_CRD_IND=T.MST_CRD_IND
          AND S.NGO_CRD_IND=T.NGO_CRD_IND
          AND S.MULT_CCY_F=T.MULT_CCY_F
          AND S.AST_RSK_ASES_RTG_TP_CD=T.AST_RSK_ASES_RTG_TP_CD
          AND S.LN_FIVE_RTG_STS=T.LN_FIVE_RTG_STS
          AND S.PD_GRP_CD=T.PD_GRP_CD
          AND S.PD_SUB_CD=T.PD_SUB_CD
          AND S.TXN_CNL_TP_CD=T.TXN_CNL_TP_CD
          AND S.CASH_TFR_IND=T.CASH_TFR_IND
          AND S.DB_CR_IND=T.DB_CR_IND
          AND S.CNSPN_TXN_F=T.CNSPN_TXN_F
          AND S.CCY=T.CCY
          AND S.ACG_DT=T.ACG_DT
          AND S.REPYMT_TP_ID = T.REPYMT_TP_ID
          WHEN MATCHED 
          THEN UPDATE SET(OU_ID,
                          CRD_TP_ID,
                          CRD_Brand_TP_Id,
                          CRD_PRVL_TP_ID,
                          ENT_IDV_IND,
                          MST_CRD_IND,
                          NGO_CRD_IND,
                          MULT_CCY_F,
                          AST_RSK_ASES_RTG_TP_CD,
                          LN_FIVE_RTG_STS,
                          PD_GRP_CD,
                          PD_SUB_CD,
                          TXN_CNL_TP_CD,
                          CASH_TFR_IND,
                          DB_CR_IND,
                          CNSPN_TXN_F,
                          CCY,
                          ACG_DT,
                          CDR_YR,
                          CDR_MTH,
                          ISSU_CRD_OU_Id,
                          NBR_TXN,
                          TXN_AMT,
                          TOT_MTD_TXN_AMT,
                          TOT_MTD_NBR_TXN,
                          TOT_QTD_TXN_AMT,
                          TOT_QTD_NBR_TXN,
                          TOT_YTD_TXN_AMT,
                          TOT_YTD_NBR_TXN)
                        =(T.OU_ID,
                          T.CRD_TP_ID,
                          T.CRD_Brand_TP_Id,
                          T.CRD_PRVL_TP_ID,
                          T.ENT_IDV_IND,
                          T.MST_CRD_IND,
                          T.NGO_CRD_IND,
                          T.MULT_CCY_F,
                          T.AST_RSK_ASES_RTG_TP_CD,
                          T.LN_FIVE_RTG_STS,
                          T.PD_GRP_CD,
                          T.PD_SUB_CD,
                          T.TXN_CNL_TP_CD,
                          T.CASH_TFR_IND,
                          T.DB_CR_IND,
                          T.CNSPN_TXN_F,
                          T.CCY,
                          T.ACG_DT,
                          T.CDR_YR,
                          T.CDR_MTH,
                          T.ISSU_CRD_OU_Id,
                          T.NBR_TXN,
                          T.TXN_AMT,
                          T.TXN_AMT,
                          T.NBR_TXN,
                          S.TOT_QTD_TXN_AMT+T.TXN_AMT,
                          S.TOT_QTD_NBR_TXN+T.NBR_TXN,
                          S.TOT_YTD_TXN_AMT+T.TXN_AMT,
                          S.TOT_YTD_NBR_TXN+T.NBR_TXN)
          WHEN NOT MATCHED
          THEN INSERT(OU_ID,
                      CRD_TP_ID,
                      CRD_Brand_TP_Id,
                      CRD_PRVL_TP_ID,
                      ENT_IDV_IND,
                      MST_CRD_IND,
                      NGO_CRD_IND,
                      MULT_CCY_F,
                      AST_RSK_ASES_RTG_TP_CD,
                      LN_FIVE_RTG_STS,
                      PD_GRP_CD,
                      PD_SUB_CD,
                      TXN_CNL_TP_CD,
                      CASH_TFR_IND,
                      DB_CR_IND,
                      CNSPN_TXN_F,
                      CCY,
                      ACG_DT,
                      CDR_YR,
                      CDR_MTH,
                      ISSU_CRD_OU_Id,
                      NBR_TXN,
                      TXN_AMT,
                      TOT_MTD_TXN_AMT,
                      TOT_MTD_NBR_TXN,
                      TOT_QTD_TXN_AMT,
                      TOT_QTD_NBR_TXN,
                      TOT_YTD_TXN_AMT,
                      TOT_YTD_NBR_TXN
                      ,REPYMT_TP_ID)
               VALUES(T.OU_ID,
                      T.CRD_TP_ID,
                      T.CRD_Brand_TP_Id,
                      T.CRD_PRVL_TP_ID,
                      T.ENT_IDV_IND,
                      T.MST_CRD_IND,
                      T.NGO_CRD_IND,
                      T.MULT_CCY_F,
                      T.AST_RSK_ASES_RTG_TP_CD,
                      T.LN_FIVE_RTG_STS,
                      T.PD_GRP_CD,
                      T.PD_SUB_CD,
                      T.TXN_CNL_TP_CD,
                      T.CASH_TFR_IND,
                      T.DB_CR_IND,
                      T.CNSPN_TXN_F,
                      T.CCY,
                      T.ACG_DT,
                      T.CDR_YR,
                      T.CDR_MTH,
                      T.ISSU_CRD_OU_Id,
                      T.NBR_TXN,
                      T.TXN_AMT,
                      T.TXN_AMT,
                      T.NBR_TXN,
                      T.TXN_AMT,
                      T.NBR_TXN,
                      T.TXN_AMT,
                      T.NBR_TXN
                      ,T.REPYMT_TP_ID
                      );--
          
          GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
          SET SMY_STEPNUM = SMY_STEPNUM+1;--
          INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);  --
          COMMIT;       --
                    
   END IF;--
ELSE                                                                                          ---非月初
   --SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
   SET SMY_STEPDESC = '插入汇总表当日交易数据.';--
   
   INSERT INTO SMY.CR_CRD_TXN_DLY_SMY(OU_ID,
                                      CRD_TP_ID,
                                      CRD_Brand_TP_Id,
                                      CRD_PRVL_TP_ID,
                                      ENT_IDV_IND,
                                      MST_CRD_IND,
                                      NGO_CRD_IND,
                                      MULT_CCY_F,
                                      AST_RSK_ASES_RTG_TP_CD,
                                      LN_FIVE_RTG_STS,
                                      PD_GRP_CD,
                                      PD_SUB_CD,
                                      TXN_CNL_TP_CD,
                                      CASH_TFR_IND,
                                      DB_CR_IND,
                                      CNSPN_TXN_F,
                                      CCY,
                                      ACG_DT,
                                      CDR_YR,
                                      CDR_MTH,
                                      ISSU_CRD_OU_Id,
                                      NBR_TXN,
                                      TXN_AMT,
                                      TOT_MTD_TXN_AMT,
                                      TOT_MTD_NBR_TXN,
                                      TOT_QTD_TXN_AMT,
                                      TOT_QTD_NBR_TXN,
                                      TOT_YTD_TXN_AMT,
                                      TOT_YTD_NBR_TXN
                                      ,REPYMT_TP_ID)
   SELECT OU_ID,
          CRD_TP_ID,
          CRD_Brand_TP_Id,
          CRD_PRVL_TP_ID,
          ENT_IDV_IND,
          MST_CRD_IND,
          NGO_CRD_IND,
          MULT_CCY_F,
          AST_RSK_ASES_RTG_TP_CD,
          LN_FIVE_RTG_STS,
          PD_GRP_CD,
          PD_SUB_CD,
          TXN_CNL_TP_CD,
          CASH_TFR_IND,
          DB_CR_IND,
          CNSPN_TXN_F,
          CCY,
          ACCOUNTING_DATE AS ACG_DT,
          CDR_YR,
          CDR_MTH,
          ISSU_CRD_OU_Id,
          0 AS NBR_TXN,
          0 AS TXN_AMT,
          TOT_MTD_TXN_AMT,
          TOT_MTD_NBR_TXN,
          TOT_QTD_TXN_AMT,
          TOT_QTD_NBR_TXN,
          TOT_YTD_TXN_AMT,
          TOT_YTD_NBR_TXN
          ,REPYMT_TP_ID
   FROM SMY.CR_CRD_TXN_DLY_SMY
   WHERE ACG_DT=DATE(ACCOUNTING_DATE) - 1 DAYS;--
         
   GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
   SET SMY_STEPNUM = SMY_STEPNUM+1;--
   INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
   COMMIT;--
   
   --SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
   SET SMY_STEPDESC = '使用当日交易数据更新汇总表.';--
   
   MERGE INTO SMY.CR_CRD_TXN_DLY_SMY S
   USING SESSION.TMP T
   ON S.OU_ID=T.OU_ID
   AND S.CRD_TP_ID=T.CRD_TP_ID
   AND S.CRD_Brand_TP_Id=T.CRD_Brand_TP_Id
   AND S.CRD_PRVL_TP_ID=T.CRD_PRVL_TP_ID
   AND S.ENT_IDV_IND=T.ENT_IDV_IND
   AND S.MST_CRD_IND=T.MST_CRD_IND
   AND S.NGO_CRD_IND=T.NGO_CRD_IND
   AND S.MULT_CCY_F=T.MULT_CCY_F
   AND S.AST_RSK_ASES_RTG_TP_CD=T.AST_RSK_ASES_RTG_TP_CD
   AND S.LN_FIVE_RTG_STS=T.LN_FIVE_RTG_STS
   AND S.PD_GRP_CD=T.PD_GRP_CD
   AND S.PD_SUB_CD=T.PD_SUB_CD
   AND S.TXN_CNL_TP_CD=T.TXN_CNL_TP_CD
   AND S.CASH_TFR_IND=T.CASH_TFR_IND
   AND S.DB_CR_IND=T.DB_CR_IND
   AND S.CNSPN_TXN_F=T.CNSPN_TXN_F
   AND S.CCY=T.CCY
   AND S.ACG_DT=T.ACG_DT
   AND S.REPYMT_TP_ID = T.REPYMT_TP_ID
   WHEN MATCHED 
   THEN UPDATE SET(OU_ID,
                   CRD_TP_ID,
                   CRD_Brand_TP_Id,
                   CRD_PRVL_TP_ID,
                   ENT_IDV_IND,
                   MST_CRD_IND,
                   NGO_CRD_IND,
                   MULT_CCY_F,
                   AST_RSK_ASES_RTG_TP_CD,
                   LN_FIVE_RTG_STS,
                   PD_GRP_CD,
                   PD_SUB_CD,
                   TXN_CNL_TP_CD,
                   CASH_TFR_IND,
                   DB_CR_IND,
                   CNSPN_TXN_F,
                   CCY,
                   ACG_DT,
                   CDR_YR,
                   CDR_MTH,
                   ISSU_CRD_OU_Id,
                   NBR_TXN,
                   TXN_AMT,
                   TOT_MTD_TXN_AMT,
                   TOT_MTD_NBR_TXN,
                   TOT_QTD_TXN_AMT,
                   TOT_QTD_NBR_TXN,
                   TOT_YTD_TXN_AMT,
                   TOT_YTD_NBR_TXN)
                 =(T.OU_ID,
                   T.CRD_TP_ID,
                   T.CRD_Brand_TP_Id,
                   T.CRD_PRVL_TP_ID,
                   T.ENT_IDV_IND,
                   T.MST_CRD_IND,
                   T.NGO_CRD_IND,
                   T.MULT_CCY_F,
                   T.AST_RSK_ASES_RTG_TP_CD,
                   T.LN_FIVE_RTG_STS,
                   T.PD_GRP_CD,
                   T.PD_SUB_CD,
                   T.TXN_CNL_TP_CD,
                   T.CASH_TFR_IND,
                   T.DB_CR_IND,
                   T.CNSPN_TXN_F,
                   T.CCY,
                   T.ACG_DT,
                   T.CDR_YR,
                   T.CDR_MTH,
                   T.ISSU_CRD_OU_Id,
                   T.NBR_TXN,
                   T.TXN_AMT,
                   S.TOT_MTD_TXN_AMT+T.TXN_AMT,
                   S.TOT_MTD_NBR_TXN+T.NBR_TXN,
                   S.TOT_QTD_TXN_AMT+T.TXN_AMT,
                   S.TOT_QTD_NBR_TXN+T.NBR_TXN,
                   S.TOT_YTD_TXN_AMT+T.TXN_AMT,
                   S.TOT_YTD_NBR_TXN+T.NBR_TXN)
   WHEN NOT MATCHED
   THEN INSERT(OU_ID,
               CRD_TP_ID,
               CRD_Brand_TP_Id,
               CRD_PRVL_TP_ID,
               ENT_IDV_IND,
               MST_CRD_IND,
               NGO_CRD_IND,
               MULT_CCY_F,
               AST_RSK_ASES_RTG_TP_CD,
               LN_FIVE_RTG_STS,
               PD_GRP_CD,
               PD_SUB_CD,
               TXN_CNL_TP_CD,
               CASH_TFR_IND,
               DB_CR_IND,
               CNSPN_TXN_F,
               CCY,
               ACG_DT,
               CDR_YR,
               CDR_MTH,
               ISSU_CRD_OU_Id,
               NBR_TXN,
               TXN_AMT,
               TOT_MTD_TXN_AMT,
               TOT_MTD_NBR_TXN,
               TOT_QTD_TXN_AMT,
               TOT_QTD_NBR_TXN,
               TOT_YTD_TXN_AMT,
               TOT_YTD_NBR_TXN
               ,REPYMT_TP_ID)
        VALUES(T.OU_ID,
               T.CRD_TP_ID,
               T.CRD_Brand_TP_Id,
               T.CRD_PRVL_TP_ID,
               T.ENT_IDV_IND,
               T.MST_CRD_IND,
               T.NGO_CRD_IND,
               T.MULT_CCY_F,
               T.AST_RSK_ASES_RTG_TP_CD,
               T.LN_FIVE_RTG_STS,
               T.PD_GRP_CD,
               T.PD_SUB_CD,
               T.TXN_CNL_TP_CD,
               T.CASH_TFR_IND,
               T.DB_CR_IND,
               T.CNSPN_TXN_F,
               T.CCY,
               T.ACG_DT,
               T.CDR_YR,
               T.CDR_MTH,
               T.ISSU_CRD_OU_Id,
               T.NBR_TXN,
               T.TXN_AMT,
               T.TXN_AMT,
               T.NBR_TXN,
               T.TXN_AMT,
               T.NBR_TXN,
               T.TXN_AMT,
               T.NBR_TXN
               ,T.REPYMT_TP_ID);--
   
   GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
   SET SMY_STEPNUM = SMY_STEPNUM+1;--
   INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);  --
   COMMIT; --
END IF;--

IF NEXT_DAY=1 THEN
   DELETE FROM SMY.CR_CRD_TXN_MTHLY_SMY WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;--
   INSERT INTO SMY.CR_CRD_TXN_MTHLY_SMY(OU_ID,
                                        CRD_TP_ID,
                                        CRD_Brand_TP_Id,
                                        CRD_PRVL_TP_ID,
                                        ENT_IDV_IND,
                                        MST_CRD_IND,
                                        NGO_CRD_IND,
                                        MULT_CCY_F,
                                        AST_RSK_ASES_RTG_TP_CD,
                                        LN_FIVE_RTG_STS,
                                        PD_GRP_CD,
                                        PD_SUB_CD,
                                        TXN_CNL_TP_CD,
                                        CASH_TFR_IND,
                                        DB_CR_IND,
                                        CNSPN_TXN_F,
                                        CCY,
                                        ACG_DT,
                                        CDR_YR,
                                        CDR_MTH,
                                        ISSU_CRD_OU_Id,
                                        TOT_MTD_TXN_AMT,
                                        TOT_MTD_NBR_TXN,
                                        TOT_QTD_TXN_AMT,
                                        TOT_QTD_NBR_TXN,
                                        TOT_YTD_TXN_AMT,
                                        TOT_YTD_NBR_TXN
                                        ,REPYMT_TP_ID)
    SELECT OU_ID,
           CRD_TP_ID,
           CRD_Brand_TP_Id,
           CRD_PRVL_TP_ID,
           ENT_IDV_IND,
           MST_CRD_IND,
           NGO_CRD_IND,
           MULT_CCY_F,
           AST_RSK_ASES_RTG_TP_CD,
           LN_FIVE_RTG_STS,
           PD_GRP_CD,
           PD_SUB_CD,
           TXN_CNL_TP_CD,
           CASH_TFR_IND,
           DB_CR_IND,
           CNSPN_TXN_F,
           CCY,
           ACG_DT,
           ----------------Start on 20100301----------------
           --CDR_YR,
           CUR_YEAR,
           --CDR_MTH,
           CUR_MONTH,
           ----------------End on 20100301----------------
           ISSU_CRD_OU_Id,
           TOT_MTD_TXN_AMT,
           TOT_MTD_NBR_TXN,
           TOT_QTD_TXN_AMT,
           TOT_QTD_NBR_TXN,
           TOT_YTD_TXN_AMT,
           TOT_YTD_NBR_TXN
           ,REPYMT_TP_ID
    FROM SMY.CR_CRD_TXN_DLY_SMY
    WHERE ACG_DT=ACCOUNTING_DATE;--
END IF;--

--SET SMY_STEPNUM=6 ;--
SET SMY_STEPDESC = '存储过程结束!';--
SET SMY_STEPNUM = SMY_STEPNUM+1;--
INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
VALUES(SMY_PROCNM, SMY_DATE, 0, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

END@