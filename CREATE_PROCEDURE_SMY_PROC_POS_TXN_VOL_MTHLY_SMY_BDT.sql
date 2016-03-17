CREATE PROCEDURE SMY.PROC_POS_TXN_VOL_MTHLY_SMY_BDT(ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and NUOQI <date>
--
-- File name:           SMY.PROC_POS_TXN_VOL_MTHLY_SMY_BDT.sql
-- Procedure name: 			SMY.PROC_POS_TXN_VOL_MTHLY_SMY_BDT
-- Source Table:				SOR.ATM_POS_FEE_TXN
-- Target Table: 				SMY.POS_TXN_VOL_MTHLY_SMY_BDT
-- Project:             ZJ RCCB EDW
--
-- Purpose:
--
--=============================================================================
-- Creation Date:       2010.07.07
-- Origin Author:       Fang Yihua
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2010-07-07   Fang Yihua      Create SP File    		
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
SET SMY_PROCNM = 'PROC_POS_TXN_VOL_MTHLY_SMY_BDT';--
SET SMY_DATE=ACCOUNTING_DATE - 1 day;--
SET CUR_YEAR=YEAR(ACCOUNTING_DATE - 1 day);  --取当前年份
SET CUR_MONTH=MONTH(ACCOUNTING_DATE - 1 day); --取当前月份
SET CUR_DAY=DAY(ACCOUNTING_DATE - 1 day);     --取当日
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
SELECT MAX(ACG_DT) INTO MAX_ACG_DT FROM SMY.POS_TXN_VOL_MTHLY_SMY_BDT;--
SET DELETE_SQL='ALTER TABLE HIS.POS_TXN_VOL_MTHLY_SMY_BDT ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE';--

/*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE - 1 day,并插入新的起始标志*/
DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
COMMIT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, 'PROCEDURE START.', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

/*数据恢复与备份*/
IF MAX_ACG_DT=ACCOUNTING_DATE - 1 day THEN
   DELETE FROM SMY.POS_TXN_VOL_MTHLY_SMY_BDT WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;--
   COMMIT;--
   IF CUR_DAY<>1 THEN
      INSERT INTO SMY.POS_TXN_VOL_MTHLY_SMY_BDT SELECT * FROM HIS.POS_TXN_VOL_MTHLY_SMY_BDT;--
      COMMIT;--
   END IF;--
ELSE
   EXECUTE IMMEDIATE DELETE_SQL;--
   INSERT INTO HIS.POS_TXN_VOL_MTHLY_SMY_BDT SELECT * FROM SMY.POS_TXN_VOL_MTHLY_SMY_BDT WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;--
   COMMIT;--
END IF;--

SET SMY_STEPNUM = SMY_STEPNUM+1;--
SET SMY_STEPDESC = '定义系统临时表,按照维度汇总,临时存放当日交易数据.';--

DECLARE GLOBAL TEMPORARY TABLE TMP(CDR_YR SMALLINT,
                                   CDR_MTH SMALLINT,
                                   ACG_DT DATE,
                                   OU_ID CHARACTER(18),
                                   CUR_Day_NBR_TXN INTEGER,
                                   CUR_Day_AMT DECIMAL(17,2))
ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE   IN TS_USR_TMP32K  
 PARTITIONING KEY(OU_ID);--
INSERT INTO SESSION.TMP
SELECT CUR_YEAR CDR_YR,
       CUR_MONTH CDR_MTH,
       ACCOUNTING_DATE - 1 day ACG_DT,
       COALESCE(B.RPRG_OU_IP_ID,' ') AS OU_ID,      --机构号 本代本机构号不为空，他代本机构号为空
       COUNT(1) AS CUR_Day_NBR_TXN,
       SUM(TXN_AMT) AS CUR_Day_AMT
from sor.ATM_POS_FEE_TXN A inner join SOR.MCHNT B on A.MCHNT_SEQ_NBR=B.MCHNT_SEQ_NO
where CUP_CLR_DT=ACCOUNTING_DATE - 1 day and TXN_CGY_TP_ID in (45000005,45000006) 
and (TM_SEQ_NO,TXN_RUN_NBR) not in (select TM_SEQ_NO,ORIG_TXN_RUN_NBR from sor.ATM_POS_FEE_TXN where CUP_CLR_DT=ACCOUNTING_DATE - 1 day and TXN_CGY_TP_ID in (45000005,45000006))
and ORIG_TXN_RUN_NBR=0
group by COALESCE(B.RPRG_OU_IP_ID,' ')
;--

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
SET SMY_STEPDESC = '使用当日交易数据更新汇总表.';--

IF CUR_DAY=1 THEN                                                                     --月初
   IF CUR_MONTH=1 THEN                                                              --年初
         INSERT INTO SMY.POS_TXN_VOL_MTHLY_SMY_BDT(
                                                CDR_YR,
                                                CDR_MTH,
                                                ACG_DT,
                                                OU_ID,
                                                TOT_MTD_NBR_TXN,
                                                TOT_MTD_AMT,
                                                TOT_QTD_NBR_TXN,
                                                TOT_QTD_AMT,
                                                TOT_YTD_NBR_TXN,
                                                TOT_YTD_AMT)
          SELECT S.CDR_YR,
                 S.CDR_MTH,
                 S.ACG_DT,
                 S.OU_ID,
                 S.CUR_Day_NBR_TXN,
                 S.CUR_Day_AMT,
                 S.CUR_Day_NBR_TXN,
                 S.CUR_Day_AMT,
                 S.CUR_Day_NBR_TXN,
                 S.CUR_Day_AMT
          FROM SESSION.TMP S;--

      GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
      COMMIT;--
   ELSE  -- CUR_MONTH<>1 AND CUR_DAY=1
   	INSERT INTO SMY.POS_TXN_VOL_MTHLY_SMY_BDT(
   					CDR_YR,
            CDR_MTH,
            ACG_DT,
            OU_ID,
            TOT_MTD_NBR_TXN,
            TOT_MTD_AMT,
            TOT_QTD_NBR_TXN,
            TOT_QTD_AMT,
            TOT_YTD_NBR_TXN,
            TOT_YTD_AMT
     )SELECT 
      			 CUR_YEAR
	          ,CUR_MONTH
	          ,SMY_DATE
	          ,S.OU_ID
	          ,0                --TOT_MTD_NBR_TXN
	          ,0                --TOT_MTD_AMT
	          ,case when CUR_MONTH in (4,7,10) then 0 else S.TOT_QTD_NBR_TXN end  --TOT_QTD_NBR_TXN
	          ,case when CUR_MONTH in (4,7,10) then 0 else S.TOT_QTD_AMT end		  --TOT_QTD_AMT        
	          ,S.TOT_YTD_NBR_TXN
	          ,S.TOT_YTD_AMT
	        FROM SMY.POS_TXN_VOL_MTHLY_SMY_BDT S
		      WHERE 
		         S.CDR_MTH = CUR_MONTH -1
		         and
		         S.CDR_YR = CUR_YEAR
		         and
		         not exists(
		            select 1 from SESSION.TMP T
		            where T.CDR_YR       = S.CDR_YR      
											AND T.CDR_MTH -1   = S.CDR_MTH
											AND T.OU_ID        = S.OU_ID
		         )		        
		      ;--
   	IF CUR_MONTH IN (4,7,10) THEN                                                      --季初非年初
		      INSERT INTO SMY.POS_TXN_VOL_MTHLY_SMY_BDT(
		                                            CDR_YR,
		                                            CDR_MTH,
		                                            ACG_DT,
		                                            OU_ID,
		                                            TOT_MTD_NBR_TXN,
		                                            TOT_MTD_AMT,
		                                            TOT_QTD_NBR_TXN,
		                                            TOT_QTD_AMT,
		                                            TOT_YTD_NBR_TXN,
		                                            TOT_YTD_AMT)
		      SELECT 
		             S.CDR_YR,
		             S.CDR_MTH,
		             S.ACG_DT,
		             S.OU_ID,
		             S.CUR_Day_NBR_TXN,
		             S.CUR_Day_AMT,
		             S.CUR_Day_NBR_TXN,
		             S.CUR_Day_AMT,
		             COALESCE(T.TOT_YTD_NBR_TXN+S.CUR_Day_NBR_TXN,S.CUR_Day_NBR_TXN),
		             COALESCE(T.TOT_YTD_AMT+S.CUR_Day_AMT,S.CUR_Day_AMT)
		      FROM SESSION.TMP S
		      LEFT JOIN SMY.POS_TXN_VOL_MTHLY_SMY_BDT T
		      ON 
		      S.CDR_YR=T.CDR_YR
		      AND S.CDR_MTH-1=T.CDR_MTH
		      AND S.OU_ID=T.OU_ID;--
		   

          GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
          INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
          COMMIT;--
        ELSE                                                                             --月初非季初非年初
          INSERT INTO SMY.POS_TXN_VOL_MTHLY_SMY_BDT(
                                                CDR_YR,
                                                CDR_MTH,
                                                ACG_DT,
                                                OU_ID,
                                                TOT_MTD_NBR_TXN,
                                                TOT_MTD_AMT,
                                                TOT_QTD_NBR_TXN,
                                                TOT_QTD_AMT,
                                                TOT_YTD_NBR_TXN,
                                                TOT_YTD_AMT)
          SELECT 
                 S.CDR_YR,
                 S.CDR_MTH,
                 S.ACG_DT,
                 S.OU_ID,
                 S.CUR_Day_NBR_TXN,
                 S.CUR_Day_AMT,
                 COALESCE(T.TOT_QTD_NBR_TXN+S.CUR_Day_NBR_TXN,S.CUR_Day_NBR_TXN),
                 COALESCE(T.TOT_QTD_AMT+S.CUR_Day_AMT,S.CUR_Day_AMT),
                 COALESCE(T.TOT_YTD_NBR_TXN+S.CUR_Day_NBR_TXN,S.CUR_Day_NBR_TXN),
                 COALESCE(T.TOT_YTD_AMT+S.CUR_Day_AMT,S.CUR_Day_AMT)
          FROM SESSION.TMP S
          LEFT JOIN SMY.POS_TXN_VOL_MTHLY_SMY_BDT T
          ON S.CDR_YR=T.CDR_YR
          AND S.CDR_MTH-1=T.CDR_MTH
          AND S.OU_ID=T.OU_ID;--

          GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
          INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
          COMMIT;--
    END IF;    --CUR_MONTH IN (4,7,10)
   END IF;     --CUR_MONTH=1          
ELSE    --CUR_DAY<>1                                                                                      ---非月初
  MERGE INTO SMY.POS_TXN_VOL_MTHLY_SMY_BDT S
  USING SESSION.TMP T
  ON S.CDR_YR=T.CDR_YR
  AND S.CDR_MTH=T.CDR_MTH
  AND S.OU_ID=T.OU_ID
  WHEN MATCHED
  THEN UPDATE SET(
                  CDR_YR,
                  CDR_MTH,
                  ACG_DT,
                  OU_ID,
                  TOT_MTD_NBR_TXN,
                  TOT_MTD_AMT,
                  TOT_QTD_NBR_TXN,
                  TOT_QTD_AMT,
                  TOT_YTD_NBR_TXN,
                  TOT_YTD_AMT)
                =(
                  T.CDR_YR,
                  T.CDR_MTH,
                  T.ACG_DT,
                  T.OU_ID,
                  S.TOT_MTD_NBR_TXN+T.CUR_Day_NBR_TXN,
                  S.TOT_MTD_AMT+T.CUR_Day_AMT,
                  S.TOT_QTD_NBR_TXN+T.CUR_Day_NBR_TXN,
                  S.TOT_QTD_AMT+T.CUR_Day_AMT,
                  S.TOT_YTD_NBR_TXN+T.CUR_Day_NBR_TXN,
                  S.TOT_YTD_AMT+T.CUR_Day_AMT)
  WHEN NOT MATCHED
  THEN INSERT(
              CDR_YR,
              CDR_MTH,
              ACG_DT,
              OU_ID,
              TOT_MTD_NBR_TXN,
              TOT_MTD_AMT,
              TOT_QTD_NBR_TXN,
              TOT_QTD_AMT,
              TOT_YTD_NBR_TXN,
              TOT_YTD_AMT)
       VALUES(
              T.CDR_YR,
              T.CDR_MTH,
              T.ACG_DT,
              T.OU_ID,
              T.CUR_Day_NBR_TXN,
              T.CUR_Day_AMT,
              T.CUR_Day_NBR_TXN,
              T.CUR_Day_AMT,
              T.CUR_Day_NBR_TXN,
              T.CUR_Day_AMT);--

  GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
  COMMIT;--
END IF;   --CUR_DAY=1

SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
SET SMY_STEPDESC = '更新会计日期.';--
UPDATE SMY.POS_TXN_VOL_MTHLY_SMY_BDT SET ACG_DT=ACCOUNTING_DATE - 1 day WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;--
GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

SET SMY_STEPNUM=6 ;--
SET SMY_STEPDESC = '存储过程结束!';--

INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
VALUES(SMY_PROCNM, SMY_DATE, 0, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

END
@