Create Procedure SMY.PROC_MTHLY_INR_AC_ACML_BAL_AMT (IN ACCOUNTING_DATE
DATE) 
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_MTHLY_INR_AC_ACML_BAL_AMT.sql
-- Procedure name: 			SMY.PROC_MTHLY_INR_AC_ACML_BAL_AMT
-- Source Table:				SOR.INR_AC_AR_TXN_DTL ,SOR.ON_BST_AC_AR
-- Target Table: 				SMY.MTHLY_INR_AC_ACML_BAL_AMT
-- Project     :        ZJ RCCB EDW
-- NOTES       :        
-- Purpose     :            
-- PROCESS METHOD      :  UPDATE EACH DAY ,INSERT IN THE PERIOD OF ONE MONTH
--=============================================================================
-- Creation Date:       2009.11.23
-- Origin Author:       JAMES SHANG IBM copyright 
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2009-11-23   JAMES SHANG     Create SP File	
-- 2010-08-11   Peng Yi tao     Modify the method of calendar days Calculating 
-------------------------------------------------------------------------------
	Specific SQL091218155214400 
	Dynamic Result Sets 0 
	Modifies SQL Data   
	Called on Null Input  
	Language SQL 
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
SET SMY_PROCNM = 'PROC_MTHLY_INR_AC_ACML_BAL_AMT';--
SET SMY_DATE=ACCOUNTING_DATE;--
SET CUR_YEAR=YEAR(ACCOUNTING_DATE);  --取当前年份
SET CUR_MONTH=MONTH(ACCOUNTING_DATE); --取当前月份
SET CUR_DAY=DAY(ACCOUNTING_DATE);     --取当日
SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');--
SET NEXT_YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR+1))||'-01-01');--
--------------------------------------------start on 20100811-------------------------------------------------------------
--SET YR_DAY=DAYS(NEXT_YR_FIRST_DAY)-DAYS(YR_FIRST_DAY);--
SET YR_DAY=DAYS(ACCOUNTING_DATE)-DAYS(YR_FIRST_DAY)+1;--
--------------------------------------------end on 20100811-------------------------------------------------------------

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
--------------------------------------------start on 20100811-------------------------------------------------------------
--SET QTR_DAY=DAYS(NEXT_QTR_FIRST_DAY)-DAYS(QTR_FIRST_DAY);--
SET QTR_DAY=DAYS(ACCOUNTING_DATE)-DAYS(QTR_FIRST_DAY)+1;--
--------------------------------------------end on 20100811-------------------------------------------------------------
SET MONTH_FIRST_DAY=DATE(TRIM(CHAR(TRIM(CHAR(CUR_YEAR))||'-'||RIGHT('0'||TRIM(CHAR(CUR_MONTH)),2)||'-01')));--
IF CUR_MONTH=12 THEN
   SET NEXT_MONTH_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR+1))||'-01-01');--
ELSE
   SET NEXT_MONTH_FIRST_DAY=DATE(TRIM(CHAR(TRIM(CHAR(CUR_YEAR))||'-'||RIGHT('0'||TRIM(CHAR(CUR_MONTH+1)),2)||'-01')));--
END IF;--
--------------------------------------------start on 20100811-------------------------------------------------------------
--SET MONTH_DAY=DAYS(NEXT_MONTH_FIRST_DAY)-DAYS(MONTH_FIRST_DAY);--
SET MONTH_DAY=DAYS(ACCOUNTING_DATE)-DAYS(MONTH_FIRST_DAY);--
--------------------------------------------end on 20100811-------------------------------------------------------------
SELECT MAX(ACG_DT) INTO MAX_ACG_DT FROM SMY.MTHLY_INR_AC_ACML_BAL_AMT;          --当前汇总表总存放数据的最大会计日期
SET DELETE_SQL='ALTER TABLE HIS.MTHLY_INR_AC_ACML_BAL_AMT ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE';--

/*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE,并插入新的起始标志*/
DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
COMMIT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, 'PROCEDURE START.', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

/*数据恢复与备份*/
IF MAX_ACG_DT=ACCOUNTING_DATE THEN
   DELETE FROM SMY.MTHLY_INR_AC_ACML_BAL_AMT WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;--
   COMMIT;--
   IF CUR_DAY<>1 THEN
      INSERT INTO SMY.MTHLY_INR_AC_ACML_BAL_AMT SELECT * FROM HIS.MTHLY_INR_AC_ACML_BAL_AMT;--
      COMMIT;--
   END IF;--
ELSE
   EXECUTE IMMEDIATE DELETE_SQL;--
   INSERT INTO HIS.MTHLY_INR_AC_ACML_BAL_AMT SELECT * FROM SMY.MTHLY_INR_AC_ACML_BAL_AMT WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;--
   COMMIT;--
END IF;--

SET SMY_STEPNUM = SMY_STEPNUM+1;--
SET SMY_STEPDESC = 'DEFINE SESSION.TMP.';--

DECLARE GLOBAL TEMPORARY TABLE TMP(INR_AC_AR_ID CHAR(20),
                                   DNMN_CCY_ID CHAR(3),
                                   CDR_YR SMALLINT,
                                   CDR_MTH SMALLINT,
                                   ACG_DT DATE,
                                   RPRG_OU_IP_ID CHAR(18),
                                   ACG_SBJ_ID CHAR(10),
                                   BAL_ACG_EFF_TP_ID INTEGER,
                                   NOD_In_MTH SMALLINT,
                                   NOCLD_In_MTH SMALLINT,
                                   NOCLD_IN_QTR SMALLINT,
                                   NOCLD_IN_YEAR SMALLINT,
                                   BAL_AMT DECIMAL(17,2),
                                   CUR_Day_CR_AMT DECIMAL(17,2),
                                   CUR_Day_DB_AMT DECIMAL(17,2),
                                   NBR_CUR_CR_TXN INTEGER,
                                   NBR_CUR_DB_TXN INTEGER) 
ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(INR_AC_AR_ID);--
INSERT INTO SESSION.TMP
WITH TMP0 AS (
Select INR_AC_AR_ID,
       DNMN_CCY_ID,
       SUM(CASE WHEN DB_CR_IND=14280002 THEN TXN_AMT ELSE 0 END) CUR_Day_CR_AMT,            --本日贷方发生额
       SUM(CASE WHEN DB_CR_IND=14280001 THEN TXN_AMT ELSE 0 END) CUR_Day_DB_AMT,            --本日借方发生额
       SUM(CASE WHEN DB_CR_IND=14280002 THEN 1 ELSE 0 END) NBR_CUR_CR_TXN,                --本日贷方发生笔数
       SUM(CASE WHEN DB_CR_IND=14280001 THEN 1 ELSE 0 END) NBR_CUR_DB_TXN                 --本日借方发生笔数
from SOR.INR_AC_AR_TXN_DTL 
WHERE DEL_F=0                        --未删除
AND TXN_OCCR_DT=ACCOUNTING_DATE 
AND DB_CR_IND in (14280001,14280002) --14280001,借  14280002,贷  -1源表中代码为空或空格
GROUP BY INR_AC_AR_ID,
         DNMN_CCY_ID
)
SELECT A.INR_AC_AR_ID,                           --账号
       A.DNMN_CCY_ID,                            --币种
       CUR_YEAR CDR_YR,                          --当前年份
       CUR_MONTH CDR_MTH,                        --当前月份
       ACCOUNTING_DATE ACG_DT,                   --会计日期
       A.RPRG_OU_IP_ID,
       A.ACG_SBJ_ID,
       A.BAL_ACG_EFF_TP_ID,                      --余额方向
       1 NOD_In_MTH,                             --本日有效
       CUR_DAY NOCLD_In_MTH,                     --本月第几天
       QTR_DAY NOCLD_IN_QTR,                     --当季第几天
       YR_DAY NOCLD_IN_YEAR,                     --当年第几天
       SUM(BAL_AMT) BAL_AMT,                     --本日余额
       SUM(COALESCE(B.CUR_Day_CR_AMT,0)) CUR_Day_CR_AMT,          --本日贷方发生额
       SUM(COALESCE(B.CUR_Day_DB_AMT,0)) CUR_Day_DB_AMT,          --本日借方发生额
       SUM(COALESCE(B.NBR_CUR_CR_TXN,0)) NBR_CUR_CR_TXN,          --本日贷方发生笔数
       SUM(COALESCE(B.NBR_CUR_DB_TXN,0)) NBR_CUR_DB_TXN           --本日借方发生笔数
FROM (SELECT INR_AC_AR_ID,DNMN_CCY_ID,RPRG_OU_IP_ID,ACG_SBJ_ID,BAL_ACG_EFF_TP_ID,BAL_AMT FROM SOR.ON_BST_AC_AR 
      WHERE AR_LCS_TP_ID=20370007     --正常
      AND DEL_F=0                     --未删除
      UNION ALL
      -------------------------------------Start on 20091214---------------------------------------------------------------
      --SELECT INR_AC_AR_ID,DNMN_CCY_ID,RPRG_OU_IP_ID,ACG_SBJ_ID,BAL_ACG_EFF_TP_ID,BAL_AMT FROM SOR.ON_BST_AC_AR
      SELECT INR_AC_AR_ID,DNMN_CCY_ID,RPRG_OU_IP_ID,ACG_SBJ_ID,BAL_ACG_EFF_TP_ID,BAL_AMT FROM SOR.OFF_BST_AC_AR
      -------------------------------------End on 20091214---------------------------------------------------------------
      WHERE AR_LCS_TP_ID=20370007     --正常
      AND DEL_F=0                     --未删除
      ) A
LEFT JOIN TMP0 B
ON A.INR_AC_AR_ID=B.INR_AC_AR_ID AND A.DNMN_CCY_ID=B.DNMN_CCY_ID
GROUP BY A.INR_AC_AR_ID,A.DNMN_CCY_ID,A.RPRG_OU_IP_ID,A.ACG_SBJ_ID,A.BAL_ACG_EFF_TP_ID;--

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
SET SMY_STEPDESC = 'UPDATE SMY.MTHLY_INR_AC_ACML_BAL_AMT.';--

IF CUR_DAY=1 THEN                                                                     --月初
   IF CUR_MONTH IN (4,7,10) THEN                                                      --季初非年初
      INSERT INTO SMY.MTHLY_INR_AC_ACML_BAL_AMT(AC_AR_ID,
                                                 CCY,
                                                 CDR_YR,
                                                 CDR_MTH,
                                                 ACG_DT,
                                                 RPRG_OU_IP_ID,
                                                 ACG_SBJ_ID,
                                                 BAL_ACG_EFF_TP_ID,
                                                 NOD_In_MTH,
                                                 NOCLD_In_MTH,
                                                 NOD_IN_QTR,
                                                 NOCLD_IN_QTR,
                                                 NOD_IN_YEAR,
                                                 NOCLD_IN_YEAR,
                                                 BAL_AMT,
                                                 MTD_ACML_BAL_AMT,
                                                 QTD_ACML_BAL_AMT,
                                                 YTD_ACML_BAL_AMT,
                                                 CUR_Day_CR_AMT,
                                                 CUR_Day_DB_AMT,
                                                 NBR_CUR_CR_TXN,
                                                 NBR_CUR_DB_TXN,
                                                 TOT_MTD_CR_AMT,
                                                 TOT_MTD_DB_AMT,
                                                 TOT_MTD_NBR_CR_TXN,
                                                 TOT_MTD_NBR_DB_TXN,
                                                 TOT_QTD_CR_AMT,
                                                 TOT_QTD_DB_AMT,
                                                 TOT_QTD_NBR_CR_TXN,
                                                 TOT_QTD_NBR_DB_TXN,
                                                 TOT_YTD_CR_AMT,
                                                 TOT_YTD_DB_AMT,
                                                 TOT_YTD_NBR_CR_TXN,
                                                 TOT_YTD_NBR_DB_TXN)
      SELECT S.INR_AC_AR_ID,
             S.DNMN_CCY_ID,
             S.CDR_YR,
             S.CDR_MTH,
             S.ACG_DT,
             S.RPRG_OU_IP_ID,
             S.ACG_SBJ_ID,
             S.BAL_ACG_EFF_TP_ID,
             S.NOD_In_MTH,
             S.NOCLD_In_MTH,
             S.NOD_In_MTH,
             S.NOCLD_IN_QTR,
             COALESCE(T.NOD_IN_YEAR+S.NOD_In_MTH,S.NOD_In_MTH),
             S.NOCLD_IN_YEAR,
             S.BAL_AMT,
             S.BAL_AMT,
             S.BAL_AMT,
             CASE WHEN S.BAL_ACG_EFF_TP_ID=T.BAL_ACG_EFF_TP_ID THEN COALESCE(T.YTD_ACML_BAL_AMT+S.BAL_AMT,S.BAL_AMT) ELSE COALESCE(-1*T.YTD_ACML_BAL_AMT+S.BAL_AMT,S.BAL_AMT) END,
             S.CUR_Day_CR_AMT,
             S.CUR_Day_DB_AMT,
             S.NBR_CUR_CR_TXN,
             S.NBR_CUR_DB_TXN,
             S.CUR_Day_CR_AMT,
             S.CUR_Day_DB_AMT,
             S.NBR_CUR_CR_TXN,
             S.NBR_CUR_DB_TXN,
             S.CUR_Day_CR_AMT,
             S.CUR_Day_DB_AMT,
             S.NBR_CUR_CR_TXN,
             S.NBR_CUR_DB_TXN,
             COALESCE(T.TOT_YTD_CR_AMT+S.CUR_Day_CR_AMT,S.CUR_Day_CR_AMT),
             COALESCE(T.TOT_YTD_DB_AMT+S.CUR_Day_DB_AMT,S.CUR_Day_DB_AMT),
             COALESCE(T.TOT_YTD_NBR_CR_TXN+S.NBR_CUR_CR_TXN,S.NBR_CUR_CR_TXN),
             COALESCE(T.TOT_YTD_NBR_DB_TXN+S.NBR_CUR_DB_TXN,S.NBR_CUR_DB_TXN)
      FROM SESSION.TMP S
      LEFT JOIN SMY.MTHLY_INR_AC_ACML_BAL_AMT T
      ON S.INR_AC_AR_ID=T.AC_AR_ID
      AND S.DNMN_CCY_ID=T.CCY
      AND S.CDR_YR=T.CDR_YR
      AND S.CDR_MTH-1=T.CDR_MTH;--
            
      GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
      COMMIT;--
   ELSEIF CUR_MONTH=1 THEN                                                              --年初
          INSERT INTO SMY.MTHLY_INR_AC_ACML_BAL_AMT(AC_AR_ID,
                                                     CCY,
                                                     CDR_YR,
                                                     CDR_MTH,
                                                     ACG_DT,
                                                     RPRG_OU_IP_ID,
                                                     ACG_SBJ_ID,
                                                     BAL_ACG_EFF_TP_ID,
                                                     NOD_In_MTH,
                                                     NOCLD_In_MTH,
                                                     NOD_IN_QTR,
                                                     NOCLD_IN_QTR,
                                                     NOD_IN_YEAR,
                                                     NOCLD_IN_YEAR,
                                                     BAL_AMT,
                                                     MTD_ACML_BAL_AMT,
                                                     QTD_ACML_BAL_AMT,
                                                     YTD_ACML_BAL_AMT,
                                                     CUR_Day_CR_AMT,
                                                     CUR_Day_DB_AMT,
                                                     NBR_CUR_CR_TXN,
                                                     NBR_CUR_DB_TXN,
                                                     TOT_MTD_CR_AMT,
                                                     TOT_MTD_DB_AMT,
                                                     TOT_MTD_NBR_CR_TXN,
                                                     TOT_MTD_NBR_DB_TXN,
                                                     TOT_QTD_CR_AMT,
                                                     TOT_QTD_DB_AMT,
                                                     TOT_QTD_NBR_CR_TXN,
                                                     TOT_QTD_NBR_DB_TXN,
                                                     TOT_YTD_CR_AMT,
                                                     TOT_YTD_DB_AMT,
                                                     TOT_YTD_NBR_CR_TXN,
                                                     TOT_YTD_NBR_DB_TXN)
          SELECT S.INR_AC_AR_ID,
                 S.DNMN_CCY_ID,
                 S.CDR_YR,
                 S.CDR_MTH,
                 S.ACG_DT,
                 S.RPRG_OU_IP_ID,
                 S.ACG_SBJ_ID,
                 S.BAL_ACG_EFF_TP_ID,
                 S.NOD_In_MTH,
                 S.NOCLD_In_MTH,
                 S.NOD_In_MTH,
                 S.NOCLD_IN_QTR,
                 S.NOD_In_MTH,
                 S.NOCLD_IN_YEAR,
                 S.BAL_AMT,
                 S.BAL_AMT,
                 S.BAL_AMT,
                 S.BAL_AMT,
                 S.CUR_Day_CR_AMT,
                 S.CUR_Day_DB_AMT,
                 S.NBR_CUR_CR_TXN,
                 S.NBR_CUR_DB_TXN,
                 S.CUR_Day_CR_AMT,
                 S.CUR_Day_DB_AMT,
                 S.NBR_CUR_CR_TXN,
                 S.NBR_CUR_DB_TXN,
                 S.CUR_Day_CR_AMT,
                 S.CUR_Day_DB_AMT,
                 S.NBR_CUR_CR_TXN,
                 S.NBR_CUR_DB_TXN,
                 S.CUR_Day_CR_AMT,
                 S.CUR_Day_DB_AMT,
                 S.NBR_CUR_CR_TXN,
                 S.NBR_CUR_DB_TXN
          FROM SESSION.TMP S;--
                
          GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
          INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
          COMMIT;--
        ELSE                                                                             --月初非季初非年初
          INSERT INTO SMY.MTHLY_INR_AC_ACML_BAL_AMT(AC_AR_ID,
                                                     CCY,
                                                     CDR_YR,
                                                     CDR_MTH,
                                                     ACG_DT,
                                                     RPRG_OU_IP_ID,
                                                     ACG_SBJ_ID,
                                                     BAL_ACG_EFF_TP_ID,
                                                     NOD_In_MTH,
                                                     NOCLD_In_MTH,
                                                     NOD_IN_QTR,
                                                     NOCLD_IN_QTR,
                                                     NOD_IN_YEAR,
                                                     NOCLD_IN_YEAR,
                                                     BAL_AMT,
                                                     MTD_ACML_BAL_AMT,
                                                     QTD_ACML_BAL_AMT,
                                                     YTD_ACML_BAL_AMT,
                                                     CUR_Day_CR_AMT,
                                                     CUR_Day_DB_AMT,
                                                     NBR_CUR_CR_TXN,
                                                     NBR_CUR_DB_TXN,
                                                     TOT_MTD_CR_AMT,
                                                     TOT_MTD_DB_AMT,
                                                     TOT_MTD_NBR_CR_TXN,
                                                     TOT_MTD_NBR_DB_TXN,
                                                     TOT_QTD_CR_AMT,
                                                     TOT_QTD_DB_AMT,
                                                     TOT_QTD_NBR_CR_TXN,
                                                     TOT_QTD_NBR_DB_TXN,
                                                     TOT_YTD_CR_AMT,
                                                     TOT_YTD_DB_AMT,
                                                     TOT_YTD_NBR_CR_TXN,
                                                     TOT_YTD_NBR_DB_TXN)
          SELECT S.INR_AC_AR_ID,
                 S.DNMN_CCY_ID,
                 S.CDR_YR,
                 S.CDR_MTH,
                 S.ACG_DT,
                 S.RPRG_OU_IP_ID,
                 S.ACG_SBJ_ID,
                 S.BAL_ACG_EFF_TP_ID,
                 S.NOD_In_MTH,
                 S.NOCLD_In_MTH,
                 COALESCE(T.NOD_IN_QTR+S.NOD_In_MTH,S.NOD_In_MTH),
                 S.NOCLD_IN_QTR,
                 COALESCE(T.NOD_IN_YEAR+S.NOD_In_MTH,S.NOD_In_MTH),
                 S.NOCLD_IN_YEAR,
                 S.BAL_AMT,
                 S.BAL_AMT,
                 CASE WHEN S.BAL_ACG_EFF_TP_ID=T.BAL_ACG_EFF_TP_ID THEN COALESCE(T.QTD_ACML_BAL_AMT+S.BAL_AMT,S.BAL_AMT) ELSE COALESCE(-1*T.QTD_ACML_BAL_AMT+S.BAL_AMT,S.BAL_AMT) END,
                 CASE WHEN S.BAL_ACG_EFF_TP_ID=T.BAL_ACG_EFF_TP_ID THEN COALESCE(T.YTD_ACML_BAL_AMT+S.BAL_AMT,S.BAL_AMT) ELSE COALESCE(-1*T.YTD_ACML_BAL_AMT+S.BAL_AMT,S.BAL_AMT) END,
                 S.CUR_Day_CR_AMT,
                 S.CUR_Day_DB_AMT,
                 S.NBR_CUR_CR_TXN,
                 S.NBR_CUR_DB_TXN,
                 S.CUR_Day_CR_AMT,
                 S.CUR_Day_DB_AMT,
                 S.NBR_CUR_CR_TXN,
                 S.NBR_CUR_DB_TXN,
                 COALESCE(T.TOT_QTD_CR_AMT+S.CUR_Day_CR_AMT,S.CUR_Day_CR_AMT),
                 COALESCE(T.TOT_QTD_DB_AMT+S.CUR_Day_DB_AMT,S.CUR_Day_DB_AMT),
                 COALESCE(T.TOT_QTD_NBR_CR_TXN+S.NBR_CUR_CR_TXN,S.NBR_CUR_CR_TXN),
                 COALESCE(T.TOT_QTD_NBR_DB_TXN+S.NBR_CUR_DB_TXN,S.NBR_CUR_DB_TXN),
                 COALESCE(T.TOT_YTD_CR_AMT+S.CUR_Day_CR_AMT,S.CUR_Day_CR_AMT),
                 COALESCE(T.TOT_YTD_DB_AMT+S.CUR_Day_DB_AMT,S.CUR_Day_DB_AMT),
                 COALESCE(T.TOT_YTD_NBR_CR_TXN+S.NBR_CUR_CR_TXN,S.NBR_CUR_CR_TXN),
                 COALESCE(T.TOT_YTD_NBR_DB_TXN+S.NBR_CUR_DB_TXN,S.NBR_CUR_DB_TXN)
          FROM SESSION.TMP S
          LEFT JOIN SMY.MTHLY_INR_AC_ACML_BAL_AMT T
          ON S.INR_AC_AR_ID=T.AC_AR_ID
          AND S.DNMN_CCY_ID=T.CCY
          AND S.CDR_YR=T.CDR_YR
          AND S.CDR_MTH-1=T.CDR_MTH;--
                
          GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
          INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);                      --
          COMMIT;--
   END IF;--
ELSE                                                                                          ---非月初
  MERGE INTO SMY.MTHLY_INR_AC_ACML_BAL_AMT S
  USING SESSION.TMP T
  ON S.AC_AR_ID=T.INR_AC_AR_ID 
  AND S.CCY=T.DNMN_CCY_ID
  AND S.CDR_YR=T.CDR_YR
  AND S.CDR_MTH=T.CDR_MTH
  WHEN MATCHED 
  THEN UPDATE SET(AC_AR_ID,
                  CCY,
                  CDR_YR,
                  CDR_MTH,
                  ACG_DT,
                  RPRG_OU_IP_ID,
                  ACG_SBJ_ID,
                  BAL_ACG_EFF_TP_ID,
                  NOD_In_MTH,
                  NOCLD_In_MTH,
                  NOD_IN_QTR,
                  NOCLD_IN_QTR,
                  NOD_IN_YEAR,
                  NOCLD_IN_YEAR,
                  BAL_AMT,
                  MTD_ACML_BAL_AMT,
                  QTD_ACML_BAL_AMT,
                  YTD_ACML_BAL_AMT,
                  CUR_Day_CR_AMT,
                  CUR_Day_DB_AMT,
                  NBR_CUR_CR_TXN,
                  NBR_CUR_DB_TXN,
                  TOT_MTD_CR_AMT,
                  TOT_MTD_DB_AMT,
                  TOT_MTD_NBR_CR_TXN,
                  TOT_MTD_NBR_DB_TXN,
                  TOT_QTD_CR_AMT,
                  TOT_QTD_DB_AMT,
                  TOT_QTD_NBR_CR_TXN,
                  TOT_QTD_NBR_DB_TXN,
                  TOT_YTD_CR_AMT,
                  TOT_YTD_DB_AMT,
                  TOT_YTD_NBR_CR_TXN,
                  TOT_YTD_NBR_DB_TXN)
                =(T.INR_AC_AR_ID,
                  T.DNMN_CCY_ID,
                  T.CDR_YR,
                  T.CDR_MTH,
                  T.ACG_DT,
                  T.RPRG_OU_IP_ID,
                  T.ACG_SBJ_ID,
                  T.BAL_ACG_EFF_TP_ID,
                  S.NOD_In_MTH+T.NOD_In_MTH,
                  T.NOCLD_In_MTH,
                  S.NOD_IN_QTR+T.NOD_In_MTH,
                  T.NOCLD_IN_QTR,
                  S.NOD_IN_YEAR+T.NOD_In_MTH,
                  T.NOCLD_IN_YEAR,                        
                  T.BAL_AMT,
                  CASE WHEN S.BAL_ACG_EFF_TP_ID=T.BAL_ACG_EFF_TP_ID THEN S.MTD_ACML_BAL_AMT+T.BAL_AMT ELSE -1*S.MTD_ACML_BAL_AMT+T.BAL_AMT END,
                  CASE WHEN S.BAL_ACG_EFF_TP_ID=T.BAL_ACG_EFF_TP_ID THEN S.QTD_ACML_BAL_AMT+T.BAL_AMT ELSE -1*S.QTD_ACML_BAL_AMT+T.BAL_AMT END,
                  CASE WHEN S.BAL_ACG_EFF_TP_ID=T.BAL_ACG_EFF_TP_ID THEN S.YTD_ACML_BAL_AMT+T.BAL_AMT ELSE -1*S.YTD_ACML_BAL_AMT+T.BAL_AMT END,
                  T.CUR_Day_CR_AMT,
                  T.CUR_Day_DB_AMT,
                  T.NBR_CUR_CR_TXN,
                  T.NBR_CUR_DB_TXN,
                  S.TOT_MTD_CR_AMT+T.CUR_Day_CR_AMT,
                  S.TOT_MTD_DB_AMT+T.CUR_Day_DB_AMT,
                  S.TOT_MTD_NBR_CR_TXN+T.NBR_CUR_CR_TXN,
                  S.TOT_MTD_NBR_DB_TXN+T.NBR_CUR_DB_TXN,
                  S.TOT_QTD_CR_AMT+T.CUR_Day_CR_AMT,
                  S.TOT_QTD_DB_AMT+T.CUR_Day_DB_AMT,
                  S.TOT_QTD_NBR_CR_TXN+T.NBR_CUR_CR_TXN,
                  S.TOT_QTD_NBR_DB_TXN+T.NBR_CUR_DB_TXN,
                  S.TOT_YTD_CR_AMT+T.CUR_Day_CR_AMT,
                  S.TOT_YTD_DB_AMT+T.CUR_Day_DB_AMT,
                  S.TOT_YTD_NBR_CR_TXN+T.NBR_CUR_CR_TXN,
                  S.TOT_YTD_NBR_DB_TXN+T.NBR_CUR_DB_TXN)
  WHEN NOT MATCHED
  THEN INSERT(AC_AR_ID,
              CCY,
              CDR_YR,
              CDR_MTH,
              ACG_DT,
              RPRG_OU_IP_ID,
              ACG_SBJ_ID,
              BAL_ACG_EFF_TP_ID,
              NOD_In_MTH,
              NOCLD_In_MTH,
              NOD_IN_QTR,
              NOCLD_IN_QTR,
              NOD_IN_YEAR,
              NOCLD_IN_YEAR,
              BAL_AMT,
              MTD_ACML_BAL_AMT,
              QTD_ACML_BAL_AMT,
              YTD_ACML_BAL_AMT,
              CUR_Day_CR_AMT,
              CUR_Day_DB_AMT,
              NBR_CUR_CR_TXN,
              NBR_CUR_DB_TXN,
              TOT_MTD_CR_AMT,
              TOT_MTD_DB_AMT,
              TOT_MTD_NBR_CR_TXN,
              TOT_MTD_NBR_DB_TXN,
              TOT_QTD_CR_AMT,
              TOT_QTD_DB_AMT,
              TOT_QTD_NBR_CR_TXN,
              TOT_QTD_NBR_DB_TXN,
              TOT_YTD_CR_AMT,
              TOT_YTD_DB_AMT,
              TOT_YTD_NBR_CR_TXN,
              TOT_YTD_NBR_DB_TXN)
       VALUES(T.INR_AC_AR_ID,
              T.DNMN_CCY_ID,
              T.CDR_YR,
              T.CDR_MTH,
              T.ACG_DT,
              T.RPRG_OU_IP_ID,
              T.ACG_SBJ_ID,
              T.BAL_ACG_EFF_TP_ID,
              T.NOD_In_MTH,
              T.NOCLD_In_MTH,
              T.NOD_In_MTH,
              T.NOCLD_IN_QTR,
              T.NOD_In_MTH,
              T.NOCLD_IN_YEAR,
              T.BAL_AMT,
              T.BAL_AMT,
              T.BAL_AMT,
              T.BAL_AMT,
              T.CUR_Day_CR_AMT,
              T.CUR_Day_DB_AMT,
              T.NBR_CUR_CR_TXN,
              T.NBR_CUR_DB_TXN,
              T.CUR_Day_CR_AMT,
              T.CUR_Day_DB_AMT,
              T.NBR_CUR_CR_TXN,
              T.NBR_CUR_DB_TXN,
              T.CUR_Day_CR_AMT,
              T.CUR_Day_DB_AMT,
              T.NBR_CUR_CR_TXN,
              T.NBR_CUR_DB_TXN,
              T.CUR_Day_CR_AMT,
              T.CUR_Day_DB_AMT,
              T.NBR_CUR_CR_TXN,
              T.NBR_CUR_DB_TXN);--
  
  GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);   --
  COMMIT;--
END IF;--

SET SMY_STEPNUM=6 ;--
SET SMY_STEPDESC = '存储过程结束!';--

INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
VALUES(SMY_PROCNM, SMY_DATE, 0, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

END@