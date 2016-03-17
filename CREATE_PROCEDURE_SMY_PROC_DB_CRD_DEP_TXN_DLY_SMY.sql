CREATE PROCEDURE SMY.PROC_DB_CRD_DEP_TXN_DLY_SMY(ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_DB_CRD_DEP_TXN_DLY_SMY.sql
-- Procedure name: 			SMY.PROC_DB_CRD_DEP_TXN_DLY_SMY
-- Source Table:				SOR.SOR.DB_CRD,SOR.CRD,sor.DMD_DEP_AR_TXN_DTL
-- Target Table: 				SMY.DB_CRD_DEP_TXN_DLY_SMY
-- Project:             ZJ RCCB EDW
-- Note                 Delete and Insert and Update
-- Purpose:             
--
--=============================================================================
-- Creation Date:       2009.11.09
-- Origin Author:       Peng Jie
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2009-11-09   Peng Jie        Create SP File	
-- 2009-12-16   Xu Yan          Updated the joining rule of the deposit transactions using VCHR_NO instead.
-- 2010-01-04   Xu Yan          Updated the conditional statement for the DB_CRD_SMY to include the transactions of the final day
-- 2010-01-13   Xu Yan          Updated the previous bug for 'IS_NONGXIN_CRD_F' 
-- 2010-01-13   Xu Yan          Updated the previous bug for accumated amount.
-- 2010-01-14   Xu Yan          Included 11920008	已换卡
-- 2010-01-20   Xu Yan          Updated the conditional statement for the TEL_POS
-- 2010-01-20   Xu Yan          Removed all the conditional statement on card LCS
-- 2010-01-21   Xu Yan          Updated the joining condition for TEL_POS
-- 2010-01-21   Xu Yan          Restored the modification on card LCS
-- 2010-03-22   Xu Yan          Added a new logic to identify the '410860他代本取款' transactions
-- 2012-02-27   Zheng Bin       Updated for 交易渠道的取数逻辑
-- 2012-02-29   Chen XiaoWen    拆分临时表TMP_DB_CRD_DEP_TXN_DLY_SMY,增加中间临时表缓存数据,以利用索引等。
-- 2012-05-29   Chen XiaoWen    去除SOR.DMD_DEP_AR_TXN_DTL.TXN_TP_ID=20460007(正常交易)的限制
-------------------------------------------------------------------------------
LANGUAGE SQL
BEGIN
/*声明异常处理使用变量*/
DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
DECLARE SMY_STEPNUM INT DEFAULT 1;                     --过程内部位置标记
DECLARE SMY_STEPDESC VARCHAR(100) DEFAULT '';          --过程内部位置描述
DECLARE SMY_DATE DATE;                                 --临时日期变量
DECLARE SMY_RCOUNT INT;                                --DML语句作用记录数
DECLARE SMY_PROCNM VARCHAR(100);    --
DECLARE CUR_YEAR SMALLINT;--
DECLARE CUR_MONTH SMALLINT;--
DECLARE CUR_DAY INTEGER;--
DECLARE MAX_ACG_DT DATE;--

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
	SET SMY_SQLCODE = SQLCODE;--
  ROLLBACK;--
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
  COMMIT;--
  RESIGNAL;--
END;--
DECLARE CONTINUE HANDLER FOR SQLWARNING
BEGIN
  SET SMY_SQLCODE = SQLCODE;--
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
  COMMIT;--
END;--

/*变量赋值*/
SET SMY_PROCNM = 'PROC_DB_CRD_DEP_TXN_DLY_SMY';--
SET SMY_DATE=ACCOUNTING_DATE;--
SET CUR_YEAR=YEAR(ACCOUNTING_DATE);  --取当前年份
SET CUR_MONTH=MONTH(ACCOUNTING_DATE); --取当前月份
SET CUR_DAY=DAY(ACCOUNTING_DATE);     --取当日
SELECT MAX(ACG_DT) INTO MAX_ACG_DT FROM SMY.DB_CRD_DEP_TXN_DLY_SMY;--


DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
COMMIT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, '存储过程开始执行', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

/*数据恢复与备份*/
IF MAX_ACG_DT=ACCOUNTING_DATE THEN
   DELETE FROM SMY.DB_CRD_DEP_TXN_DLY_SMY WHERE ACG_DT = ACCOUNTING_DATE;--
   COMMIT;--
END IF;--

SET SMY_STEPNUM = SMY_STEPNUM+1;--
SET SMY_STEPDESC = '创建临时表,并把当日数据插入';--

DECLARE GLOBAL TEMPORARY TABLE TMP_DB_CRD_DEP_TXN_DLY_SMY(OU_ID             CHARACTER(18)       ,
                                                          CRD_TP_ID         INTEGER             ,
                                                          PSBK_RLTD_F       SMALLINT            ,
                                                          IS_NONGXIN_CRD_F  SMALLINT            ,
                                                          ENT_IDV_IND       INTEGER             ,
                                                          TXN_CNL_TP_CD     CHARACTER(2)        ,
                                                          CASH_TFR_IND      INTEGER             ,
                                                          DB_CR_IND         INTEGER             ,
                                                          CNSPN_TXN_F       SMALLINT            ,
                                                          CCY               CHARACTER(3)        ,
                                                          ACG_DT            DATE                ,
                                                          CDR_YR            SMALLINT            ,
                                                          CDR_MTH           SMALLINT            ,
                                                          NBR_TXN           INTEGER             ,
                                                          TXN_AMT           DECIMAL(17,2)       )
ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(OU_ID); --

DECLARE GLOBAL TEMPORARY TABLE S_TMP AS (
    SELECT 
         VCHR_NO
        ,ORIG_TXN_RUN_NBR
        ,SUB_TXN_RUN_NBR
        ,TXN_CNL_TP_ID
        ,case when TXN_CODE = '410860' then 16040001 else CASH_TFR_IND end as CASH_TFR_IND
        ,DB_CR_IND
        ,DNMN_CCY_ID
        ,TXN_DT
        ,TXN_AMT
    FROM SOR.DMD_DEP_AR_TXN_DTL TXN
) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(VCHR_NO);

CREATE INDEX SESSION.IDX_TMP_1 ON SESSION.S_TMP(VCHR_NO,ORIG_TXN_RUN_NBR,SUB_TXN_RUN_NBR,TXN_CNL_TP_ID,CASH_TFR_IND,DB_CR_IND,DNMN_CCY_ID,TXN_DT);

INSERT INTO SESSION.S_TMP
SELECT 
     VCHR_NO
    ,ORIG_TXN_RUN_NBR
    ,SUB_TXN_RUN_NBR
    ,TXN_CNL_TP_ID
    ,case when TXN_CODE = '410860' then 16040001 else CASH_TFR_IND end as CASH_TFR_IND
    ,DB_CR_IND
    ,DNMN_CCY_ID
    ,TXN_DT
    ,TXN_AMT
FROM SOR.DMD_DEP_AR_TXN_DTL TXN
WHERE TXN.TXN_DT=SMY_DATE AND TXN.DEL_F = 0
;

DECLARE GLOBAL TEMPORARY TABLE S_TMP_STMT_DEP_AC_RGST AS (
    SELECT 
         AC_AR_ID
        ,DNMN_CCY_ID
        ,ORG_TXN_RUN_NBR
        ,SUB_TXN_RUN_NBR
        ,TXN_DT
        ,MCHNT_AC_AR_ID
        ,ALS_CNL
        ,CNL_TP
    FROM SOR.STMT_DEP_AC_RGST
) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(AC_AR_ID,DNMN_CCY_ID,ORG_TXN_RUN_NBR);

CREATE INDEX SESSION.IDX_TMP_AC_RGST ON SESSION.S_TMP_STMT_DEP_AC_RGST(AC_AR_ID,DNMN_CCY_ID,ORG_TXN_RUN_NBR);

INSERT INTO SESSION.S_TMP_STMT_DEP_AC_RGST
SELECT 
     AC_AR_ID
    ,DNMN_CCY_ID
    ,ORG_TXN_RUN_NBR
    ,SUB_TXN_RUN_NBR
    ,TXN_DT
    ,MCHNT_AC_AR_ID
    ,ALS_CNL
    ,CNL_TP
FROM SOR.STMT_DEP_AC_RGST
WHERE TXN_DT =SMY_DATE AND DEL_F = 0
;

DECLARE GLOBAL TEMPORARY TABLE TMP_TMP AS (
    SELECT 
         coalesce(CRD.OU_ID, '') as OU_ID
        ,CRD.DB_CRD_TP_ID as DB_CRD_TP_ID
        ,CRD.PSBK_RLTD_F as PSBK_RLTD_F
        ,CRD.IS_NONGXIN_CRD_F as IS_NONGXIN_CRD_F
        ,CRD.ENT_IDV_IND as ENT_IDV_IND
        ,VALUE(case when d.CNL_TP=21690010 then 'PT' else c.TXN_CNL_TP_ID end,'') as TXN_CNL_TP_CD
        ,coalesce(c.CASH_TFR_IND, -1) as CASH_TFR_IND
        ,coalesce(c.DB_CR_IND, -1) as DB_CR_IND
        ,VALUE(case when (substr(d.MCHNT_AC_AR_ID,2,1)='1' and d.ALS_CNL='CP') then 1 else 0 end,-1) as CNSPN_TXN_F
        ,CRD.CCY as CCY
        ,1 as TOT_COUNT
        ,coalesce(c.TXN_AMT, 0) as TOT_AMT
    FROM SMY.DB_CRD_SMY CRD
    left join SESSION.S_TMP c on CRD.CRD_NO=c.VCHR_NO and CRD.CCY = c.DNMN_CCY_ID
    left join SESSION.S_TMP_STMT_DEP_AC_RGST d on CRD.AC_AR_ID = d.AC_AR_ID AND c.DNMN_CCY_ID = d.DNMN_CCY_ID and c.ORIG_TXN_RUN_NBR= d.ORG_TXN_RUN_NBR
) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(OU_ID);

CREATE INDEX SESSION.IDX_TMP_TMP_1 ON SESSION.TMP_TMP(OU_ID,DB_CRD_TP_ID,PSBK_RLTD_F,IS_NONGXIN_CRD_F,ENT_IDV_IND,TXN_CNL_TP_CD,CASH_TFR_IND,DB_CR_IND,CNSPN_TXN_F,CCY);

INSERT INTO SESSION.TMP_TMP
WITH TMP AS(
    SELECT VCHR_NO,ORIG_TXN_RUN_NBR,SUB_TXN_RUN_NBR,TXN_CNL_TP_ID,CASH_TFR_IND,DB_CR_IND,DNMN_CCY_ID,TXN_DT,
    COUNT(*) as TOT_COUNT,SUM(TXN_AMT) as TOT_AMT
    FROM SESSION.S_TMP
    GROUP BY VCHR_NO,ORIG_TXN_RUN_NBR,SUB_TXN_RUN_NBR,TXN_CNL_TP_ID,CASH_TFR_IND,DB_CR_IND,DNMN_CCY_ID,TXN_DT
    )
SELECT 
     coalesce(CRD.OU_ID, '')                                   
    ,CRD.DB_CRD_TP_ID
    ,CRD.PSBK_RLTD_F    
    ,CRD.IS_NONGXIN_CRD_F
    ,CRD.ENT_IDV_IND
    ,VALUE(case when d.CNL_TP=21690010 then 'PT' else c.TXN_CNL_TP_ID end,'')
    ,coalesce(c.CASH_TFR_IND, -1)                      
    ,coalesce(c.DB_CR_IND, -1)
    ,VALUE(case when (substr(d.MCHNT_AC_AR_ID,2,1)='1' and d.ALS_CNL='CP') then 1 else 0 end,-1)
    ,CRD.CCY
    ,coalesce(c.TOT_COUNT, 0)
    ,coalesce(c.TOT_AMT, 0)
FROM SMY.DB_CRD_SMY CRD
left join TMP c on CRD.CRD_NO=c.VCHR_NO and CRD.CCY = c.DNMN_CCY_ID
left join SESSION.S_TMP_STMT_DEP_AC_RGST d on CRD.AC_AR_ID = d.AC_AR_ID AND c.DNMN_CCY_ID = d.DNMN_CCY_ID and c.ORIG_TXN_RUN_NBR= d.ORG_TXN_RUN_NBR
where CRD_LCS_TP_ID in (11920001,11920008) or CRD.END_DT = SMY_DATE
;

INSERT INTO SESSION.TMP_DB_CRD_DEP_TXN_DLY_SMY
(
    OU_ID,
	  CRD_TP_ID,
	  PSBK_RLTD_F,
	  IS_NONGXIN_CRD_F, 
	  ENT_IDV_IND,
	  TXN_CNL_TP_CD,
	  CASH_TFR_IND,
	  DB_CR_IND,
	  CNSPN_TXN_F,
	  CCY,
	  ACG_DT,
	  CDR_YR,
	  CDR_MTH,
	  NBR_TXN,
	  TXN_AMT
)
SELECT
    OU_ID,
    DB_CRD_TP_ID,
    PSBK_RLTD_F,
    IS_NONGXIN_CRD_F,
    ENT_IDV_IND,
    TXN_CNL_TP_CD,
    CASH_TFR_IND,
    DB_CR_IND,
    CNSPN_TXN_F,
    CCY,
    SMY_DATE,
    CUR_YEAR,
    CUR_MONTH,
    sum(TOT_COUNT),
    sum(TOT_AMT)
FROM SESSION.TMP_TMP
GROUP BY
    OU_ID,
    DB_CRD_TP_ID,
    PSBK_RLTD_F,
    IS_NONGXIN_CRD_F,
    ENT_IDV_IND,
    TXN_CNL_TP_CD,
    CASH_TFR_IND,
    DB_CR_IND,
    CNSPN_TXN_F,
    CCY
;

/*
INSERT INTO SESSION.TMP_DB_CRD_DEP_TXN_DLY_SMY
 (OU_ID,
	CRD_TP_ID,
	PSBK_RLTD_F,
	IS_NONGXIN_CRD_F, 
	ENT_IDV_IND,
	TXN_CNL_TP_CD,
	CASH_TFR_IND,
	DB_CR_IND,
	CNSPN_TXN_F,
	CCY,
	ACG_DT,
	CDR_YR,
	CDR_MTH,
	NBR_TXN,
	TXN_AMT)      
WITH TMP AS (
  SELECT 
   VCHR_NO                           
  ,ORIG_TXN_RUN_NBR                  
  ,SUB_TXN_RUN_NBR                   
  ,TXN_CNL_TP_ID  
  ------------------Start on 20100322-----------------------                   
  --,CASH_TFR_IND 
  ,case when TXN_CODE = '410860' --'他代本取款'
        then 16040001 --取现
        else CASH_TFR_IND
        end as CASH_TFR_IND
  ------------------End on 20100322-----------------------                        
  ,DB_CR_IND                         
  ,DNMN_CCY_ID                       
  ,TXN_DT                            
  ,COUNT(*) as TOT_COUNT             
  ,SUM(TXN_AMT) as TOT_AMT 
FROM SOR.DMD_DEP_AR_TXN_DTL TXN
WHERE TXN.TXN_DT=SMY_DATE AND TXN.DEL_F = 0
GROUP BY
  VCHR_NO                           
  ,TXN_CNL_TP_ID     
  ------------------Start on 20100322-----------------------                 
  --,CASH_TFR_IND                         
  ,case when TXN_CODE = '410860' --'他代本取款'
        then 16040001 --取现
        else CASH_TFR_IND
        end
  ------------------End on 20100322-----------------------          
  ,DB_CR_IND                         
  ,DNMN_CCY_ID                       
  ,ORIG_TXN_RUN_NBR                  
  ,SUB_TXN_RUN_NBR                     
  ,TXN_DT 
 )
 , TMP_STMT_DEP_AC_RGST AS (SELECT 
                          AC_AR_ID
                         ,DNMN_CCY_ID
                         ,ORG_TXN_RUN_NBR
                         ,SUB_TXN_RUN_NBR
                         ,TXN_DT
                         ,MCHNT_AC_AR_ID
                         ,ALS_CNL    --20120227old_version modified by zhengbin 
                         ,CNL_TP     --20120227old_version modified by zhengbin 
                       FROM SOR.STMT_DEP_AC_RGST
                       ------------------Start on 20100120----------------------------
                       --WHERE CNL_TP=21690004  and  ALS_CNL='CP'  and TXN_DT =SMY_DATE)
                       --WHERE ALS_CNL='CP'  and TXN_DT =SMY_DATE AND DEL_F = 0)      ---20120227old_version modified by zhengbin 
                       WHERE TXN_DT =SMY_DATE AND DEL_F = 0)                          ---20120227new_version modified by zhengbin 
                       ------------------Start on 20100120----------------------------
SELECT 
   coalesce(CRD.OU_ID, '')                                   
  ,CRD.DB_CRD_TP_ID
  ,CRD.PSBK_RLTD_F    
  ----------Start on 20100113--------------
  --,CRD.PSBK_RLTD_F
  ,CRD.IS_NONGXIN_CRD_F
  ----------End on 20100113--------------
  ,CRD.ENT_IDV_IND
 -- ,coalesce(c.TXN_CNL_TP_ID, '')                                           --20120227old_version modified by zhengbin 
  ,VALUE(case when d.CNL_TP=21690010 then 'PT' else c.TXN_CNL_TP_ID end,'')  --20120227new_version modified by zhengbin 
  ,coalesce(c.CASH_TFR_IND, -1)                      
  ,coalesce(c.DB_CR_IND, -1)
 --VALUE(case when substr(d.MCHNT_AC_AR_ID,2,1)='1' then 1 else 0 end,-1)       --CNSPN_TXN_F,
  ,VALUE(case when (substr(d.MCHNT_AC_AR_ID,2,1)='1' and d.ALS_CNL='CP') then 1 else 0 end,-1)       --CNSPN_TXN_F,
  ,CRD.CCY                       
  ,SMY_DATE                                          
  ,CUR_YEAR                                          
  ,CUR_MONTH                                         
  ,sum(coalesce(c.TOT_COUNT, 0))                     
  ,sum(coalesce(c.TOT_AMT, 0))
FROM SMY.DB_CRD_SMY CRD
      left join TMP c on CRD.CRD_NO=c.VCHR_NO and CRD.CCY = c.DNMN_CCY_ID
      ----------------------Start on 20100121-------------------------
      --left join TMP_STMT_DEP_AC_RGST d on CRD.AC_AR_ID = d.AC_AR_ID AND c.DNMN_CCY_ID = d.DNMN_CCY_ID and c.ORIG_TXN_RUN_NBR= d.ORG_TXN_RUN_NBR AND c.SUB_TXN_RUN_NBR=d.SUB_TXN_RUN_NBR 
      --Remove the original transaction number joining condition
      left join TMP_STMT_DEP_AC_RGST d on CRD.AC_AR_ID = d.AC_AR_ID AND c.DNMN_CCY_ID = d.DNMN_CCY_ID and c.ORIG_TXN_RUN_NBR= d.ORG_TXN_RUN_NBR
      ----------------------End on 20100121-------------------------
------------------Start on 20100114-----------------
--where CRD_LCS_TP_ID = 11920001       --正常
where CRD_LCS_TP_ID in (
                          11920001       --正常
                         ,11920008	     --已换卡
                        )
------------------End on 20100114-----------------                      
-------------------------Start on 20100104-------------------------------------------------
    or CRD.END_DT = SMY_DATE
-------------------------End on 20100104-------------------------------------------------

group by 
   coalesce(CRD.OU_ID, '')                                   
  ,CRD.DB_CRD_TP_ID
  ,CRD.PSBK_RLTD_F    
  ----------Start on 20100113--------------
  --,CRD.PSBK_RLTD_F
  ,CRD.IS_NONGXIN_CRD_F
  ----------End on 20100113--------------
  ,CRD.ENT_IDV_IND
 --coalesce(c.TXN_CNL_TP_ID, '')                                             --20120227old_version modified by zhengbin
  ,VALUE(case when d.CNL_TP=21690010 then 'PT' else c.TXN_CNL_TP_ID end,'')  --20120227new_version modified by zhengbin
  ,coalesce(c.CASH_TFR_IND, -1)                      
  ,coalesce(c.DB_CR_IND, -1)
 --VALUE(case when substr(d.MCHNT_AC_AR_ID,2,1)='1' then 1 else 0 end,-1)       --CNSPN_TXN_F 20120227old_version modified by zhengbin
  ,VALUE(case when (substr(d.MCHNT_AC_AR_ID,2,1)='1' and d.ALS_CNL='CP') then 1 else 0 end,-1)       --CNSPN_TXN_F 20120227new_version modified by zhengbin
  ,CRD.CCY     ;--
*/

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

SET SMY_STEPNUM = SMY_STEPNUM+1;--
IF CUR_DAY = 1 THEN
   IF CUR_MONTH = 1 THEN
      SET SMY_STEPDESC = '插入年初数据'; --
      INSERT INTO SMY.DB_CRD_DEP_TXN_DLY_SMY
        (OU_ID,
	      CRD_TP_ID,
	      PSBK_RLTD_F,
	      IS_NONGXIN_CRD_F, 
	      ENT_IDV_IND,
	      TXN_CNL_TP_CD,
	      CASH_TFR_IND,
	      DB_CR_IND,
	      CNSPN_TXN_F,
	      CCY,
	      ACG_DT,
	      CDR_YR,
	      CDR_MTH,
	      NBR_TXN,
	      TXN_AMT,
        TOT_MTD_TXN_AMT,
        TOT_MTD_NBR_TXN,
        TOT_QTD_TXN_AMT,
        TOT_QTD_NBR_TXN,
        TOT_YTD_TXN_AMT,
        TOT_YTD_NBR_TXN)
	    SELECT 
	       OU_ID,
	       CRD_TP_ID,
	       PSBK_RLTD_F,
	       IS_NONGXIN_CRD_F, 
	       ENT_IDV_IND,
	       TXN_CNL_TP_CD,
	       CASH_TFR_IND,
	       DB_CR_IND,
	       CNSPN_TXN_F,
	       CCY,
	       ACG_DT,
	       CDR_YR,
	       CDR_MTH,
	       NBR_TXN,
	       TXN_AMT,
	       TXN_AMT,
	       NBR_TXN,
	       TXN_AMT,
	       NBR_TXN,
	       TXN_AMT,
	       NBR_TXN
       FROM SESSION.TMP_DB_CRD_DEP_TXN_DLY_SMY
;--
	       
  ELSEIF CUR_MONTH IN (4, 7, 10) THEN
      SET SMY_STEPDESC = '插入季初数据';--
      INSERT INTO SMY.DB_CRD_DEP_TXN_DLY_SMY
        (OU_ID,
	      CRD_TP_ID,
	      PSBK_RLTD_F,
	      IS_NONGXIN_CRD_F, 
	      ENT_IDV_IND,
	      TXN_CNL_TP_CD,
	      CASH_TFR_IND,
	      DB_CR_IND,
	      CNSPN_TXN_F,
	      CCY,
	      ACG_DT,
	      CDR_YR,
	      CDR_MTH,
	      NBR_TXN,
	      TXN_AMT,
        TOT_MTD_TXN_AMT,
        TOT_MTD_NBR_TXN,
        TOT_QTD_TXN_AMT,
        TOT_QTD_NBR_TXN,
        TOT_YTD_TXN_AMT,
        TOT_YTD_NBR_TXN)
      SELECT 
	       a.OU_ID,
	       a.CRD_TP_ID,
	       a.PSBK_RLTD_F,
	       a.IS_NONGXIN_CRD_F, 
	       a.ENT_IDV_IND,
	       a.TXN_CNL_TP_CD,
	       a.CASH_TFR_IND,
	       a.DB_CR_IND,
	       a.CNSPN_TXN_F,
	       a.CCY,
	       a.ACG_DT,
	       a.CDR_YR,
	       a.CDR_MTH,
	       a.NBR_TXN,
	       a.TXN_AMT,
	       a.TXN_AMT,
	       a.NBR_TXN,
	       a.TXN_AMT,
	       a.NBR_TXN,
	       COALESCE(b.TOT_YTD_TXN_AMT, 0.00) + a.TXN_AMT,
	       COALESCE(b.TOT_YTD_NBR_TXN, 0) + a.NBR_TXN
       FROM SESSION.TMP_DB_CRD_DEP_TXN_DLY_SMY a LEFT JOIN SMY.DB_CRD_DEP_TXN_DLY_SMY b
			 ON		a.OU_ID            = b.OU_ID AND
						a.CRD_TP_ID        = b.CRD_TP_ID AND
						a.PSBK_RLTD_F      = b.PSBK_RLTD_F AND
						a.IS_NONGXIN_CRD_F = b.IS_NONGXIN_CRD_F AND
						a.ENT_IDV_IND      = b.ENT_IDV_IND AND
						a.TXN_CNL_TP_CD    = b.TXN_CNL_TP_CD AND
						a.CASH_TFR_IND     = b.CASH_TFR_IND AND
						a.DB_CR_IND        = b.DB_CR_IND AND
						a.CCY              = b.CCY AND
						a.ACG_DT - 1 DAY   = b.ACG_DT AND
						a.CNSPN_TXN_F      = b.CNSPN_TXN_F;--

    ----------------------------Start on 20100113---------------------------------------------
    INSERT INTO SMY.DB_CRD_DEP_TXN_DLY_SMY
        (OU_ID,
	      CRD_TP_ID,
	      PSBK_RLTD_F,
	      IS_NONGXIN_CRD_F, 
	      ENT_IDV_IND,
	      TXN_CNL_TP_CD,
	      CASH_TFR_IND,
	      DB_CR_IND,
	      CNSPN_TXN_F,
	      CCY,
	      ACG_DT,
	      CDR_YR,
	      CDR_MTH,
	      NBR_TXN,
	      TXN_AMT,
        TOT_MTD_TXN_AMT,
        TOT_MTD_NBR_TXN,
        TOT_QTD_TXN_AMT,
        TOT_QTD_NBR_TXN,
        TOT_YTD_TXN_AMT,
        TOT_YTD_NBR_TXN)
      SELECT 
	       a.OU_ID,
	       a.CRD_TP_ID,
	       a.PSBK_RLTD_F,
	       a.IS_NONGXIN_CRD_F, 
	       a.ENT_IDV_IND,
	       a.TXN_CNL_TP_CD,
	       a.CASH_TFR_IND,
	       a.DB_CR_IND,
	       a.CNSPN_TXN_F,
	       a.CCY,
	       ACCOUNTING_DATE,
	       CUR_YEAR,
	       CUR_MONTH,
	       0,
	       0,
	       0,
	       0,
	       0,
	       0,
	       a.TOT_YTD_TXN_AMT,
	       a.TOT_YTD_NBR_TXN
       FROM SMY.DB_CRD_DEP_TXN_DLY_SMY a 
       where not exists(
            select 1 from session.TMP_DB_CRD_DEP_TXN_DLY_SMY b
            where a.OU_ID            = b.OU_ID AND
						a.CRD_TP_ID        = b.CRD_TP_ID AND
						a.PSBK_RLTD_F      = b.PSBK_RLTD_F AND
						a.IS_NONGXIN_CRD_F = b.IS_NONGXIN_CRD_F AND
						a.ENT_IDV_IND      = b.ENT_IDV_IND AND
						a.TXN_CNL_TP_CD    = b.TXN_CNL_TP_CD AND
						a.CASH_TFR_IND     = b.CASH_TFR_IND AND
						a.DB_CR_IND        = b.DB_CR_IND AND
						a.CCY              = b.CCY AND
						b.ACG_DT - 1 DAY   = a.ACG_DT AND
						a.CNSPN_TXN_F      = b.CNSPN_TXN_F
       )and ACG_DT = DATE(ACCOUNTING_DATE) - 1 DAYS
       ;--
       
    ----------------------------End on 20100113---------------------------------------------

    ELSE
    	SET SMY_STEPDESC = '插入非年初或季初的月初数据';--
      INSERT INTO SMY.DB_CRD_DEP_TXN_DLY_SMY
        (OU_ID,
	      CRD_TP_ID,
	      PSBK_RLTD_F,
	      IS_NONGXIN_CRD_F, 
	      ENT_IDV_IND,
	      TXN_CNL_TP_CD,
	      CASH_TFR_IND,
	      DB_CR_IND,
	      CNSPN_TXN_F,
	      CCY,
	      ACG_DT,
	      CDR_YR,
	      CDR_MTH,
	      NBR_TXN,
	      TXN_AMT,
        TOT_MTD_TXN_AMT,
        TOT_MTD_NBR_TXN,
        TOT_QTD_TXN_AMT,
        TOT_QTD_NBR_TXN,
        TOT_YTD_TXN_AMT,
        TOT_YTD_NBR_TXN)
      SELECT 
	       a.OU_ID,
	       a.CRD_TP_ID,
	       a.PSBK_RLTD_F,
	       a.IS_NONGXIN_CRD_F, 
	       a.ENT_IDV_IND,
	       a.TXN_CNL_TP_CD,
	       a.CASH_TFR_IND,
	       a.DB_CR_IND,
	       a.CNSPN_TXN_F,
	       a.CCY,
	       a.ACG_DT,
	       a.CDR_YR,
	       a.CDR_MTH,
	       a.NBR_TXN,
	       a.TXN_AMT,
	       a.TXN_AMT,
	       a.NBR_TXN,
	       COALESCE(b.TOT_QTD_TXN_AMT, 0.00) + a.TXN_AMT,
	       COALESCE(b.TOT_QTD_NBR_TXN, 0) + a.NBR_TXN,
	       COALESCE(b.TOT_YTD_TXN_AMT, 0.00) + a.TXN_AMT,
	       COALESCE(b.TOT_YTD_NBR_TXN, 0) + a.NBR_TXN
       FROM SESSION.TMP_DB_CRD_DEP_TXN_DLY_SMY a LEFT JOIN SMY.DB_CRD_DEP_TXN_DLY_SMY b
			 ON		a.OU_ID            = b.OU_ID AND
						a.CRD_TP_ID        = b.CRD_TP_ID AND
						a.PSBK_RLTD_F      = b.PSBK_RLTD_F AND
						a.IS_NONGXIN_CRD_F = b.IS_NONGXIN_CRD_F AND
						a.ENT_IDV_IND      = b.ENT_IDV_IND AND
						a.TXN_CNL_TP_CD    = b.TXN_CNL_TP_CD AND
						a.CASH_TFR_IND     = b.CASH_TFR_IND AND
						a.DB_CR_IND        = b.DB_CR_IND AND
						a.CCY              = b.CCY AND
						a.ACG_DT - 1 DAY   = b.ACG_DT AND
						a.CNSPN_TXN_F      = b.CNSPN_TXN_F;--

    ----------------------------Start on 20100113---------------------------------------------
    INSERT INTO SMY.DB_CRD_DEP_TXN_DLY_SMY
        (OU_ID,
	      CRD_TP_ID,
	      PSBK_RLTD_F,
	      IS_NONGXIN_CRD_F, 
	      ENT_IDV_IND,
	      TXN_CNL_TP_CD,
	      CASH_TFR_IND,
	      DB_CR_IND,
	      CNSPN_TXN_F,
	      CCY,
	      ACG_DT,
	      CDR_YR,
	      CDR_MTH,
	      NBR_TXN,
	      TXN_AMT,
        TOT_MTD_TXN_AMT,
        TOT_MTD_NBR_TXN,
        TOT_QTD_TXN_AMT,
        TOT_QTD_NBR_TXN,
        TOT_YTD_TXN_AMT,
        TOT_YTD_NBR_TXN)
      SELECT 
	       a.OU_ID,
	       a.CRD_TP_ID,
	       a.PSBK_RLTD_F,
	       a.IS_NONGXIN_CRD_F, 
	       a.ENT_IDV_IND,
	       a.TXN_CNL_TP_CD,
	       a.CASH_TFR_IND,
	       a.DB_CR_IND,
	       a.CNSPN_TXN_F,
	       a.CCY,
	       ACCOUNTING_DATE,
	       CUR_YEAR,
	       CUR_MONTH,
	       0,
	       0,
	       0,
	       0,
	       a.TOT_QTD_TXN_AMT,
	       a.TOT_QTD_NBR_TXN,
	       a.TOT_YTD_TXN_AMT,
	       a.TOT_YTD_NBR_TXN
       FROM SMY.DB_CRD_DEP_TXN_DLY_SMY a 
       where not exists(
            select 1 from session.TMP_DB_CRD_DEP_TXN_DLY_SMY b
            where a.OU_ID            = b.OU_ID AND
						a.CRD_TP_ID        = b.CRD_TP_ID AND
						a.PSBK_RLTD_F      = b.PSBK_RLTD_F AND
						a.IS_NONGXIN_CRD_F = b.IS_NONGXIN_CRD_F AND
						a.ENT_IDV_IND      = b.ENT_IDV_IND AND
						a.TXN_CNL_TP_CD    = b.TXN_CNL_TP_CD AND
						a.CASH_TFR_IND     = b.CASH_TFR_IND AND
						a.DB_CR_IND        = b.DB_CR_IND AND
						a.CCY              = b.CCY AND
						b.ACG_DT - 1 DAY   = a.ACG_DT AND
						a.CNSPN_TXN_F      = b.CNSPN_TXN_F
       )and ACG_DT = DATE(ACCOUNTING_DATE) - 1 DAYS
       ;--
       
    ----------------------------End on 20100113---------------------------------------------


   END IF;--
ELSE

	SET SMY_STEPDESC = 'merge 非月初数据';--

      INSERT INTO SMY.DB_CRD_DEP_TXN_DLY_SMY
        (OU_ID,
	      CRD_TP_ID,
	      PSBK_RLTD_F,
	      IS_NONGXIN_CRD_F, 
	      ENT_IDV_IND,
	      TXN_CNL_TP_CD,
	      CASH_TFR_IND,
	      DB_CR_IND,
	      CNSPN_TXN_F,
	      CCY,
	      ACG_DT,
	      CDR_YR,
	      CDR_MTH,
	      NBR_TXN,
	      TXN_AMT,
        TOT_MTD_TXN_AMT,
        TOT_MTD_NBR_TXN,
        TOT_QTD_TXN_AMT,
        TOT_QTD_NBR_TXN,
        TOT_YTD_TXN_AMT,
        TOT_YTD_NBR_TXN)
      SELECT 
	       a.OU_ID,
	       a.CRD_TP_ID,
	       a.PSBK_RLTD_F,
	       a.IS_NONGXIN_CRD_F, 
	       a.ENT_IDV_IND,
	       a.TXN_CNL_TP_CD,
	       a.CASH_TFR_IND,
	       a.DB_CR_IND,
	       a.CNSPN_TXN_F,
	       a.CCY,
	       a.ACG_DT,
	       a.CDR_YR,
	       a.CDR_MTH,
	       a.NBR_TXN,
	       a.TXN_AMT,
	       COALESCE(b.TOT_MTD_TXN_AMT, 0.00) + a.TXN_AMT,
	       COALESCE(b.TOT_MTD_NBR_TXN, 0) + a.NBR_TXN,
	       COALESCE(b.TOT_QTD_TXN_AMT, 0.00) + a.TXN_AMT,
	       COALESCE(b.TOT_QTD_NBR_TXN, 0) + a.NBR_TXN,
	       COALESCE(b.TOT_YTD_TXN_AMT, 0.00) + a.TXN_AMT,
	       COALESCE(b.TOT_YTD_NBR_TXN, 0) + a.NBR_TXN
       FROM SESSION.TMP_DB_CRD_DEP_TXN_DLY_SMY a LEFT JOIN SMY.DB_CRD_DEP_TXN_DLY_SMY b
			 ON		a.OU_ID            = b.OU_ID AND
						a.CRD_TP_ID        = b.CRD_TP_ID AND
						a.PSBK_RLTD_F      = b.PSBK_RLTD_F AND
						a.IS_NONGXIN_CRD_F = b.IS_NONGXIN_CRD_F AND
						a.ENT_IDV_IND      = b.ENT_IDV_IND AND
						a.TXN_CNL_TP_CD    = b.TXN_CNL_TP_CD AND
						a.CASH_TFR_IND     = b.CASH_TFR_IND AND
						a.DB_CR_IND        = b.DB_CR_IND AND
						a.CCY              = b.CCY AND
						a.ACG_DT - 1 DAY   = b.ACG_DT AND
						a.CNSPN_TXN_F      = b.CNSPN_TXN_F;--

    ----------------------------Start on 20100113---------------------------------------------
    INSERT INTO SMY.DB_CRD_DEP_TXN_DLY_SMY
        (OU_ID,
	      CRD_TP_ID,
	      PSBK_RLTD_F,
	      IS_NONGXIN_CRD_F, 
	      ENT_IDV_IND,
	      TXN_CNL_TP_CD,
	      CASH_TFR_IND,
	      DB_CR_IND,
	      CNSPN_TXN_F,
	      CCY,
	      ACG_DT,
	      CDR_YR,
	      CDR_MTH,
	      NBR_TXN,
	      TXN_AMT,
        TOT_MTD_TXN_AMT,
        TOT_MTD_NBR_TXN,
        TOT_QTD_TXN_AMT,
        TOT_QTD_NBR_TXN,
        TOT_YTD_TXN_AMT,
        TOT_YTD_NBR_TXN)
      SELECT 
	       a.OU_ID,
	       a.CRD_TP_ID,
	       a.PSBK_RLTD_F,
	       a.IS_NONGXIN_CRD_F, 
	       a.ENT_IDV_IND,
	       a.TXN_CNL_TP_CD,
	       a.CASH_TFR_IND,
	       a.DB_CR_IND,
	       a.CNSPN_TXN_F,
	       a.CCY,
	       ACCOUNTING_DATE,
	       CUR_YEAR,
	       CUR_MONTH,
	       0,
	       0,
	       a.TOT_MTD_TXN_AMT,
	       a.TOT_MTD_NBR_TXN,
	       a.TOT_QTD_TXN_AMT,
	       a.TOT_QTD_NBR_TXN,
	       a.TOT_YTD_TXN_AMT,
	       a.TOT_YTD_NBR_TXN
       FROM SMY.DB_CRD_DEP_TXN_DLY_SMY a 
       where not exists(
            select 1 from session.TMP_DB_CRD_DEP_TXN_DLY_SMY b
            where a.OU_ID            = b.OU_ID AND
						a.CRD_TP_ID        = b.CRD_TP_ID AND
						a.PSBK_RLTD_F      = b.PSBK_RLTD_F AND
						a.IS_NONGXIN_CRD_F = b.IS_NONGXIN_CRD_F AND
						a.ENT_IDV_IND      = b.ENT_IDV_IND AND
						a.TXN_CNL_TP_CD    = b.TXN_CNL_TP_CD AND
						a.CASH_TFR_IND     = b.CASH_TFR_IND AND
						a.DB_CR_IND        = b.DB_CR_IND AND
						a.CCY              = b.CCY AND
						b.ACG_DT - 1 DAY   = a.ACG_DT AND
						a.CNSPN_TXN_F      = b.CNSPN_TXN_F
       )and ACG_DT = DATE(ACCOUNTING_DATE) - 1 DAYS
       ;--
       
    ----------------------------End on 20100113---------------------------------------------

END IF;--

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

IF DAY(SMY_DATE + 1 DAY) = 1 THEN

  SET SMY_STEPNUM = SMY_STEPNUM+1;--
  SET SMY_STEPDESC = '插入月底数据';--
 DELETE FROM SMY.DB_CRD_DEP_TXN_MTHLY_SMY WHERE CDR_MTH = CUR_MONTH AND CDR_YR = CUR_YEAR;--
	INSERT INTO SMY.DB_CRD_DEP_TXN_MTHLY_SMY
		(OU_ID
		,CRD_TP_ID
		,PSBK_RLTD_F
		,IS_NONGXIN_CRD_F
		,ENT_IDV_IND
		,TXN_CNL_TP_CD
		,CASH_TFR_IND
		,DB_CR_IND
		,CNSPN_TXN_F
		,CCY
		,CDR_MTH
		,CDR_YR
		,ACG_DT
		,TOT_MTD_NBR_TXN
		,TOT_QTD_TXN_AMT
		,TOT_QTD_NBR_TXN
		,TOT_YTD_TXN_AMT
		,TOT_YTD_NBR_TXN
		,TOT_MTD_TXN_AMT  )
	SELECT 
		OU_ID
		,CRD_TP_ID
		,PSBK_RLTD_F
		,IS_NONGXIN_CRD_F
		,ENT_IDV_IND
		,TXN_CNL_TP_CD
		,CASH_TFR_IND
		,DB_CR_IND
		,CNSPN_TXN_F
		,CCY
		,CDR_MTH
		,CDR_YR
		,ACG_DT
		,TOT_MTD_NBR_TXN
		,TOT_QTD_TXN_AMT
		,TOT_QTD_NBR_TXN
		,TOT_YTD_TXN_AMT
		,TOT_YTD_NBR_TXN
		,TOT_MTD_TXN_AMT
	FROM SMY.DB_CRD_DEP_TXN_DLY_SMY WHERE ACG_DT = SMY_DATE
;--
GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

END IF;  --

END@