CREATE PROCEDURE SMY.PROC_POS_TXN_VOL_MTHLY_SMY(ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and NUOQI <date>
--
-- File name:           SMY.PROC_POS_TXN_VOL_MTHLY_SMY.sql
-- Procedure name: 			SMY.PROC_POS_TXN_VOL_MTHLY_SMY
-- Source Table:				SOR.STMT_DEP_AC_RGST
-- Target Table: 				SMY.POS_TXN_VOL_MTHLY_SMY
-- Project:             ZJ RCCB EDW
--
-- Purpose:
--
--=============================================================================
-- Creation Date:       2010.06.08
-- Origin Author:       Fang Yihua
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2010-06-08   Fang Yihua      Create SP File   
-- 2011-08-03   ZN              Modified  RCU_CRD_F column, join with SOR.CRD,match set 1,not match set 0	
-------------------------------------------------------------------------------
LANGUAGE SQL
BEGIN
/*声明异常处理使用变量*/
DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
DECLARE SMY_STEPNUM INT DEFAULT 1;                     --过程内部位置标记
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
DECLARE MAX_ACG_DT DATE;                               --最大会计日期
DECLARE DELETE_SQL VARCHAR(200);                       --删除历史表动态SQL

/*1.定义针对SQL异常情况的句柄(EXIT方式).
  2.将出现SQL异常时在存储过程中的位置(SMY_STEPNUM),位置描述(SMY_STEPDESC)，SQLCODE(SMY_SQLCODE)记入表SMY_LOG中作调试用.
  3.调用RESIGNAL重新引发异常,跳出存储过程执行体,对引发SQL异常之前存储过程体中所完成的操作进行回滚.*/
DECLARE CONTINUE HANDLER FOR NOT FOUND
SET at_end=1;--
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
SET SMY_PROCNM = 'PROC_POS_TXN_VOL_MTHLY_SMY';--
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
SELECT MAX(ACG_DT) INTO MAX_ACG_DT FROM SMY.POS_TXN_VOL_MTHLY_SMY;--
SET DELETE_SQL='ALTER TABLE HIS.POS_TXN_VOL_MTHLY_SMY ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE';--

/*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE,并插入新的起始标志*/
DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
COMMIT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, 'PROCEDURE START.', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

/*数据恢复与备份*/
IF MAX_ACG_DT=ACCOUNTING_DATE THEN
   DELETE FROM SMY.POS_TXN_VOL_MTHLY_SMY WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;--
   COMMIT;--
   IF CUR_DAY<>1 THEN
      INSERT INTO SMY.POS_TXN_VOL_MTHLY_SMY SELECT * FROM HIS.POS_TXN_VOL_MTHLY_SMY;--
      COMMIT;--
   END IF;--
ELSE
   EXECUTE IMMEDIATE DELETE_SQL;--
   INSERT INTO HIS.POS_TXN_VOL_MTHLY_SMY SELECT * FROM SMY.POS_TXN_VOL_MTHLY_SMY WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;--
   COMMIT;--
END IF;--

SET SMY_STEPNUM = SMY_STEPNUM+1;--
SET SMY_STEPDESC = '定义系统临时表,按照维度汇总,临时存放当日交易数据.';--

DECLARE GLOBAL TEMPORARY TABLE TMP(POS_SEQ_NO CHAR(20),
                                   CCY CHAR(3),
                                   CASH_TFR_IND INTEGER,
                                   DB_CR_IND INTEGER,
                                   AC_OU_ID CHARACTER(18),
                                   RCU_CRD_F SMALLINT,
                                   CNL_TP integer,
                                   CDR_YR SMALLINT,
                                   CDR_MTH SMALLINT,
                                   ACG_DT DATE,
                                   OU_ID CHARACTER(18),
                                   CUR_Day_NBR_TXN INTEGER,
                                   CUR_Day_AMT DECIMAL(17,2),
                                   MCHNT_AC_AR_ID CHARACTER(20),
                                   MCHNT_SEQ_NBR CHARACTER(15),
                                   XFT_CRD_F SMALLINT,
                                   POS_RECPT_OU_IP_ID CHARACTER(18),
                                   CTPT_AR_ID CHARACTER(20),
                                   RPRG_OU_IP_ID CHARACTER(18))
ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE   IN TS_USR_TMP32K  
 PARTITIONING KEY(POS_SEQ_NO);--
INSERT INTO SESSION.TMP
with tmp0 as(
select
distinct POS_RECPT_OU_IP_ID,DEB_CRD_NO
from SOR.POS_TML
where END_DT='9999-12-31'
)
SELECT A.TML_SEQ_NO AS POS_SEQ_NO,
       A.DNMN_CCY_ID AS CCY,
       A.CASH_TFR_IND,                                                    --转取标识
       A.DB_CR_IND,                                                       --借贷标识
       A.AC_RPRG_OU_IP_ID AS AC_OU_ID,                                    --卡账户归属机构号
--     (CASE WHEN substr(A.CC_AC_AR_ID,1,6) in ('622858','621058','622288','940036') THEN 1 ELSE 0 END) AS RCU_CRD_F, --本行卡标识 1-本行 0-他行
			( CASE WHEN E.CRD_NO is not null then 1 ELSE 0 END) AS RCU_CRD_F,   --本行卡标识 1-本行 0-他行
       A.CNL_TP,                                                          --渠道类别
       CUR_YEAR CDR_YR,
       CUR_MONTH CDR_MTH,
       ACCOUNTING_DATE ACG_DT,
       COALESCE(B.RPRG_OU_IP_ID,' ') AS OU_ID,                            --机构号 本代本机构号不为空，他代本机构号为空
       COUNT(1) AS CUR_Day_NBR_TXN,
       SUM(TXN_AMT) AS CUR_Day_AMT,
       COALESCE(A.MCHNT_AC_AR_ID,' ') as MCHNT_AC_AR_ID,                                                  --单位账户1
       COALESCE(A.MCHNT_SEQ_NBR,' ') as MCHNT_SEQ_NBR,                    --商户号
       (case when POS_RECPT_OU_IP_ID<>'' then 1 else 0 end) as XFT_CRD_F, --信付通标识 1-信付通 0-不是信付通
       COALESCE(POS_RECPT_OU_IP_ID,' ') as  POS_RECPT_OU_IP_ID,           --信付通对方账户绑定机构 
       COALESCE(A.CTPT_AR_ID,' ') CTPT_AR_ID,
       COALESCE(d.RPRG_OU_IP_ID,' ') RPRG_OU_IP_ID
FROM SOR.STMT_DEP_AC_RGST A
left join SOR.MCHNT B ON A.MCHNT_SEQ_NBR=B.MCHNT_SEQ_NO
left join tmp0 C ON A.CTPT_AR_ID=C.DEB_CRD_NO
left join sor.CRD d on a.CTPT_AR_ID=d.CRD_NO
left join sor.CRD E on A.CC_AC_AR_ID=E.CRD_NO
where (A.CNL_TP =21690004 or (A.CNL_TP in (21690003,21690008) and substr(MCHNT_AC_AR_ID,2,1)='1'))      --交易渠道为POS       
AND TXN_DT=ACCOUNTING_DATE
AND TXN_TP_ID=20460007              --正常
And A.DEL_F <> 1                    --未删除
GROUP BY A.TML_SEQ_NO,
         A.DNMN_CCY_ID,
         A.CASH_TFR_IND, 
         A.DB_CR_IND,
         A.CNL_TP,
         COALESCE(A.MCHNT_AC_AR_ID,' '),
         COALESCE(A.MCHNT_SEQ_NBR,' '),
         A.AC_RPRG_OU_IP_ID,
         (CASE WHEN E.CRD_NO is not null then 1 ELSE 0 END),
         (case when POS_RECPT_OU_IP_ID<>'' then 1 else 0 end),
         COALESCE(POS_RECPT_OU_IP_ID,' '),
         COALESCE(B.RPRG_OU_IP_ID,' '),         
         COALESCE(A.CTPT_AR_ID,' '),
         COALESCE(d.RPRG_OU_IP_ID,' ')
;--

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
SET SMY_STEPDESC = '使用当日交易数据更新汇总表.';--

IF CUR_DAY=1 THEN                                                                     --月初
   IF CUR_MONTH=1 THEN                                                              --年初
         INSERT INTO SMY.POS_TXN_VOL_MTHLY_SMY(POS_SEQ_NO,
                                                CCY,
                                                CASH_TFR_IND,
                                                DB_CR_IND,
                                                AC_OU_ID,
                                                RCU_CRD_F,
                                                CNL_TP,
                                                CDR_YR,
                                                CDR_MTH,
                                                ACG_DT,
                                                OU_ID,
                                                TOT_MTD_NBR_TXN,
                                                TOT_MTD_AMT,
                                                TOT_QTD_NBR_TXN,
                                                TOT_QTD_AMT,
                                                TOT_YTD_NBR_TXN,
                                                TOT_YTD_AMT,
                                                MCHNT_AC_AR_ID,
                                                MCHNT_SEQ_NBR,
                                                XFT_CRD_F,
                                                POS_RECPT_OU_IP_ID,
                                                CTPT_AR_ID,
                                                RPRG_OU_IP_ID)
          SELECT S.POS_SEQ_NO,
                 S.CCY,
                 S.CASH_TFR_IND,
                 S.DB_CR_IND,
                 S.AC_OU_ID,
                 S.RCU_CRD_F,
                 S.CNL_TP,
                 S.CDR_YR,
                 S.CDR_MTH,
                 S.ACG_DT,
                 S.OU_ID,
                 S.CUR_Day_NBR_TXN,
                 S.CUR_Day_AMT,
                 S.CUR_Day_NBR_TXN,
                 S.CUR_Day_AMT,
                 S.CUR_Day_NBR_TXN,
                 S.CUR_Day_AMT,
                 S.MCHNT_AC_AR_ID,
                 S.MCHNT_SEQ_NBR,
                 S.XFT_CRD_F,
                 S.POS_RECPT_OU_IP_ID,
                 S.CTPT_AR_ID,
                 S.RPRG_OU_IP_ID
          FROM SESSION.TMP S;--

      GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
      COMMIT;--
   ELSE  -- CUR_MONTH<>1 AND CUR_DAY=1
   	INSERT INTO SMY.POS_TXN_VOL_MTHLY_SMY(
   					POS_SEQ_NO,
            CCY,
            CASH_TFR_IND,
            DB_CR_IND,
            AC_OU_ID,
            RCU_CRD_F,
            CNL_TP,
            CDR_YR,
            CDR_MTH,
            ACG_DT,
            OU_ID,
            TOT_MTD_NBR_TXN,
            TOT_MTD_AMT,
            TOT_QTD_NBR_TXN,
            TOT_QTD_AMT,
            TOT_YTD_NBR_TXN,
            TOT_YTD_AMT,
            MCHNT_AC_AR_ID,
            MCHNT_SEQ_NBR,
            XFT_CRD_F,
            POS_RECPT_OU_IP_ID,
            CTPT_AR_ID,
            RPRG_OU_IP_ID
     )SELECT 
      			 S.POS_SEQ_NO
	          ,S.CCY
	          ,S.CASH_TFR_IND
	          ,S.DB_CR_IND
	          ,S.AC_OU_ID
	          ,S.RCU_CRD_F
	          ,S.CNL_TP
	          ,CUR_YEAR
	          ,CUR_MONTH
	          ,SMY_DATE
	          ,S.OU_ID
	          ,0                --TOT_MTD_NBR_TXN
	          ,0                --TOT_MTD_AMT
	          ,case when CUR_MONTH in (4,7,10) then 0 else S.TOT_QTD_NBR_TXN end  --TOT_QTD_NBR_TXN
	          ,case when CUR_MONTH in (4,7,10) then 0 else S.TOT_QTD_AMT end		  --TOT_QTD_AMT        
	          ,S.TOT_YTD_NBR_TXN
	          ,S.TOT_YTD_AMT
	          ,S.MCHNT_AC_AR_ID
	          ,S.MCHNT_SEQ_NBR
	          ,S.XFT_CRD_F
	          ,S.POS_RECPT_OU_IP_ID
	          ,S.CTPT_AR_ID
	          ,S.RPRG_OU_IP_ID
		      FROM SMY.POS_TXN_VOL_MTHLY_SMY S
		      WHERE 
		         S.CDR_MTH = CUR_MONTH -1
		         and
		         S.CDR_YR = CUR_YEAR
		         and
		         not exists(
		            select 1 from SESSION.TMP T
		            where T.POS_SEQ_NO = S.POS_SEQ_NO		                  
											AND T.CASH_TFR_IND = S.CASH_TFR_IND
											AND T.DB_CR_IND    = S.DB_CR_IND   
											AND T.CCY          = S.CCY         
											AND T.AC_OU_ID     = S.AC_OU_ID    
											AND T.RCU_CRD_F    = S.RCU_CRD_F
											AND T.CNL_TP       = S.CNL_TP
                      AND T.MCHNT_AC_AR_ID = S.MCHNT_AC_AR_ID
                      AND T.MCHNT_SEQ_NBR= S.MCHNT_SEQ_NBR
											AND T.CDR_YR       = S.CDR_YR      
											AND T.CDR_MTH -1   = S.CDR_MTH
											AND T.OU_ID        = S.OU_ID 
											AND T.XFT_CRD_F    = S.XFT_CRD_F
											AND T.POS_RECPT_OU_IP_ID=S.POS_RECPT_OU_IP_ID
											AND T.CTPT_AR_ID   = S.CTPT_AR_ID
											AND T.RPRG_OU_IP_ID= S.RPRG_OU_IP_ID     
		                  
		         )		        
		      ;--
   	IF CUR_MONTH IN (4,7,10) THEN                                                      --季初非年初
		      INSERT INTO SMY.POS_TXN_VOL_MTHLY_SMY(POS_SEQ_NO,
		                                            CCY,
		                                            CASH_TFR_IND,
		                                            DB_CR_IND,
		                                            AC_OU_ID,
		                                            RCU_CRD_F,
		                                            CNL_TP,
		                                            CDR_YR,
		                                            CDR_MTH,
		                                            ACG_DT,
		                                            OU_ID,
		                                            TOT_MTD_NBR_TXN,
		                                            TOT_MTD_AMT,
		                                            TOT_QTD_NBR_TXN,
		                                            TOT_QTD_AMT,
		                                            TOT_YTD_NBR_TXN,
		                                            TOT_YTD_AMT,
		                                            MCHNT_AC_AR_ID,
		                                            MCHNT_SEQ_NBR,
		                                            XFT_CRD_F,
		                                            POS_RECPT_OU_IP_ID,
		                                            CTPT_AR_ID,
		                                            RPRG_OU_IP_ID)
		      SELECT S.POS_SEQ_NO,
		             S.CCY,
		             S.CASH_TFR_IND,
		             S.DB_CR_IND,
		             S.AC_OU_ID,
		             S.RCU_CRD_F,
		             S.CNL_TP,
		             S.CDR_YR,
		             S.CDR_MTH,
		             S.ACG_DT,
		             S.OU_ID,
		             S.CUR_Day_NBR_TXN,
		             S.CUR_Day_AMT,
		             S.CUR_Day_NBR_TXN,
		             S.CUR_Day_AMT,
		             COALESCE(T.TOT_YTD_NBR_TXN+S.CUR_Day_NBR_TXN,S.CUR_Day_NBR_TXN),
		             COALESCE(T.TOT_YTD_AMT+S.CUR_Day_AMT,S.CUR_Day_AMT),
		             S.MCHNT_AC_AR_ID,
		             S.MCHNT_SEQ_NBR,
		             S.XFT_CRD_F,
		             S.POS_RECPT_OU_IP_ID,
		             S.CTPT_AR_ID,
		             S.RPRG_OU_IP_ID
		      FROM SESSION.TMP S
		      LEFT JOIN SMY.POS_TXN_VOL_MTHLY_SMY T
		      ON S.POS_SEQ_NO=T.POS_SEQ_NO
		      AND S.CCY=T.CCY
		      AND S.CASH_TFR_IND=T.CASH_TFR_IND
		      AND S.DB_CR_IND=T.DB_CR_IND
		      AND S.AC_OU_ID=T.AC_OU_ID
		      AND S.RCU_CRD_F=T.RCU_CRD_F
		      AND S.CNL_TP=T.CNL_TP
          AND S.MCHNT_AC_AR_ID=T.MCHNT_AC_AR_ID
          AND S.MCHNT_SEQ_NBR=T.MCHNT_SEQ_NBR
		      AND S.CDR_YR=T.CDR_YR
		      AND S.CDR_MTH-1=T.CDR_MTH
		      AND S.OU_ID=T.OU_ID
		      AND S.XFT_CRD_F=T.XFT_CRD_F
		      AND S.POS_RECPT_OU_IP_ID=T.POS_RECPT_OU_IP_ID
		      AND S.CTPT_AR_ID=T.CTPT_AR_ID
		      AND S.RPRG_OU_IP_ID=T.RPRG_OU_IP_ID;--
		   

          GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
          INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
          COMMIT;--
        ELSE                                                                             --月初非季初非年初
          INSERT INTO SMY.POS_TXN_VOL_MTHLY_SMY(POS_SEQ_NO,
                                                CCY,
                                                CASH_TFR_IND,
                                                DB_CR_IND,
                                                AC_OU_ID,
                                                RCU_CRD_F,
                                                CNL_TP,
                                                CDR_YR,
                                                CDR_MTH,
                                                ACG_DT,
                                                OU_ID,
                                                TOT_MTD_NBR_TXN,
                                                TOT_MTD_AMT,
                                                TOT_QTD_NBR_TXN,
                                                TOT_QTD_AMT,
                                                TOT_YTD_NBR_TXN,
                                                TOT_YTD_AMT,
                                                MCHNT_AC_AR_ID,
                                                MCHNT_SEQ_NBR,
                                                XFT_CRD_F,
                                                POS_RECPT_OU_IP_ID,
                                                CTPT_AR_ID,
                                                RPRG_OU_IP_ID)
          SELECT S.POS_SEQ_NO,
                 S.CCY,
                 S.CASH_TFR_IND,
                 S.DB_CR_IND,
                 S.AC_OU_ID,
                 S.RCU_CRD_F,
                 S.CNL_TP,
                 S.CDR_YR,
                 S.CDR_MTH,
                 S.ACG_DT,
                 S.OU_ID,
                 S.CUR_Day_NBR_TXN,
                 S.CUR_Day_AMT,
                 COALESCE(T.TOT_QTD_NBR_TXN+S.CUR_Day_NBR_TXN,S.CUR_Day_NBR_TXN),
                 COALESCE(T.TOT_QTD_AMT+S.CUR_Day_AMT,S.CUR_Day_AMT),
                 COALESCE(T.TOT_YTD_NBR_TXN+S.CUR_Day_NBR_TXN,S.CUR_Day_NBR_TXN),
                 COALESCE(T.TOT_YTD_AMT+S.CUR_Day_AMT,S.CUR_Day_AMT),
                 S.MCHNT_AC_AR_ID,
                 S.MCHNT_SEQ_NBR,
                 S.XFT_CRD_F,
                 S.POS_RECPT_OU_IP_ID,
                 S.CTPT_AR_ID,
                 S.RPRG_OU_IP_ID
          FROM SESSION.TMP S
          LEFT JOIN SMY.POS_TXN_VOL_MTHLY_SMY T
          ON S.POS_SEQ_NO=T.POS_SEQ_NO
          AND S.CCY=T.CCY
          AND S.CASH_TFR_IND=T.CASH_TFR_IND
          AND S.DB_CR_IND=T.DB_CR_IND
          AND S.AC_OU_ID=T.AC_OU_ID
          AND S.RCU_CRD_F=T.RCU_CRD_F
          AND S.CNL_TP=T.CNL_TP
          AND S.MCHNT_AC_AR_ID=T.MCHNT_AC_AR_ID
          AND S.MCHNT_SEQ_NBR=T.MCHNT_SEQ_NBR
          AND S.CDR_YR=T.CDR_YR
          AND S.CDR_MTH-1=T.CDR_MTH
          AND S.OU_ID=T.OU_ID
          AND S.XFT_CRD_F=T.XFT_CRD_F
          AND S.POS_RECPT_OU_IP_ID=T.POS_RECPT_OU_IP_ID
          AND S.CTPT_AR_ID=T.CTPT_AR_ID
          AND S.RPRG_OU_IP_ID=T.RPRG_OU_IP_ID;--

          GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
          INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
          COMMIT;--
    END IF;    --CUR_MONTH IN (4,7,10)
   END IF;     --CUR_MONTH=1          
ELSE    --CUR_DAY<>1                                                                                      ---非月初
  MERGE INTO SMY.POS_TXN_VOL_MTHLY_SMY S
  USING SESSION.TMP T
  ON S.POS_SEQ_NO=T.POS_SEQ_NO
  AND S.CCY=T.CCY
  AND S.CASH_TFR_IND=T.CASH_TFR_IND
  AND S.DB_CR_IND=T.DB_CR_IND
  AND S.AC_OU_ID=T.AC_OU_ID
  AND S.RCU_CRD_F=T.RCU_CRD_F
  AND S.CNL_TP=T.CNL_TP
  AND S.MCHNT_AC_AR_ID=T.MCHNT_AC_AR_ID
  AND S.MCHNT_SEQ_NBR=T.MCHNT_SEQ_NBR
  AND S.CDR_YR=T.CDR_YR
  AND S.CDR_MTH=T.CDR_MTH
  AND S.OU_ID=T.OU_ID
  AND S.XFT_CRD_F=T.XFT_CRD_F
  AND S.POS_RECPT_OU_IP_ID=T.POS_RECPT_OU_IP_ID
  AND S.CTPT_AR_ID=T.CTPT_AR_ID
  AND S.RPRG_OU_IP_ID=T.RPRG_OU_IP_ID
  WHEN MATCHED
  THEN UPDATE SET(POS_SEQ_NO,
                  CCY,
                  CASH_TFR_IND,
                  DB_CR_IND,
                  AC_OU_ID,
                  RCU_CRD_F,
                  CNL_TP,
                  CDR_YR,
                  CDR_MTH,
                  ACG_DT,
                  OU_ID,
                  TOT_MTD_NBR_TXN,
                  TOT_MTD_AMT,
                  TOT_QTD_NBR_TXN,
                  TOT_QTD_AMT,
                  TOT_YTD_NBR_TXN,
                  TOT_YTD_AMT,
                  MCHNT_AC_AR_ID,
                  MCHNT_SEQ_NBR,
                  XFT_CRD_F,
                  POS_RECPT_OU_IP_ID,
                  CTPT_AR_ID,
                  RPRG_OU_IP_ID)
                =(T.POS_SEQ_NO,
                  T.CCY,
                  T.CASH_TFR_IND,
                  T.DB_CR_IND,
                  T.AC_OU_ID,
                  T.RCU_CRD_F,
                  T.CNL_TP,
                  T.CDR_YR,
                  T.CDR_MTH,
                  T.ACG_DT,
                  T.OU_ID,
                  S.TOT_MTD_NBR_TXN+T.CUR_Day_NBR_TXN,
                  S.TOT_MTD_AMT+T.CUR_Day_AMT,
                  S.TOT_QTD_NBR_TXN+T.CUR_Day_NBR_TXN,
                  S.TOT_QTD_AMT+T.CUR_Day_AMT,
                  S.TOT_YTD_NBR_TXN+T.CUR_Day_NBR_TXN,
                  S.TOT_YTD_AMT+T.CUR_Day_AMT,
                  T.MCHNT_AC_AR_ID,
                  T.MCHNT_SEQ_NBR,
                  T.XFT_CRD_F,
                  T.POS_RECPT_OU_IP_ID,
                  T.CTPT_AR_ID,
                  T.RPRG_OU_IP_ID)
  WHEN NOT MATCHED
  THEN INSERT(POS_SEQ_NO,
              CCY,
              CASH_TFR_IND,
              DB_CR_IND,
              AC_OU_ID,
              RCU_CRD_F,
              CNL_TP,
              CDR_YR,
              CDR_MTH,
              ACG_DT,
              OU_ID,
              TOT_MTD_NBR_TXN,
              TOT_MTD_AMT,
              TOT_QTD_NBR_TXN,
              TOT_QTD_AMT,
              TOT_YTD_NBR_TXN,
              TOT_YTD_AMT,
              MCHNT_AC_AR_ID,
              MCHNT_SEQ_NBR,
              XFT_CRD_F,
              POS_RECPT_OU_IP_ID,
              CTPT_AR_ID,
              RPRG_OU_IP_ID)
       VALUES(T.POS_SEQ_NO,
              T.CCY,
              T.CASH_TFR_IND,
              T.DB_CR_IND,
              T.AC_OU_ID,
              T.RCU_CRD_F,
              T.CNL_TP,
              T.CDR_YR,
              T.CDR_MTH,
              T.ACG_DT,
              T.OU_ID,
              T.CUR_Day_NBR_TXN,
              T.CUR_Day_AMT,
              T.CUR_Day_NBR_TXN,
              T.CUR_Day_AMT,
              T.CUR_Day_NBR_TXN,
              T.CUR_Day_AMT,
              T.MCHNT_AC_AR_ID,
              T.MCHNT_SEQ_NBR,
              T.XFT_CRD_F,
              T.POS_RECPT_OU_IP_ID,
              T.CTPT_AR_ID,
              T.RPRG_OU_IP_ID);--

  GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
  COMMIT;--
END IF;   --CUR_DAY=1

SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
SET SMY_STEPDESC = '更新会计日期.';--
UPDATE SMY.POS_TXN_VOL_MTHLY_SMY SET ACG_DT=ACCOUNTING_DATE WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;--
GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

SET SMY_STEPNUM=6 ;--
SET SMY_STEPDESC = '存储过程结束!';--

INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
VALUES(SMY_PROCNM, SMY_DATE, 0, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

END@