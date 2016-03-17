CREATE PROCEDURE SMY.PROC_MTHLY_FT_DEP_ACML_BAL_AMT(ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_MTHLY_FT_DEP_ACML_BAL_AMT.sql
-- Procedure name: 			SMY.PROC_MTHLY_FT_DEP_ACML_BAL_AMT
-- Source Table:				SOR.FT_DEP_AR, SOR.ONLINE_TXN_RUN
-- Target Table: 				SMY.MTHLY_FT_DEP_ACML_BAL_AMT
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
-- 2009-11-09   Peng Jie     Create SP File		
-- 2009-12-04   Xu Yan       Rename the history table
-- 2009-12-15   Xu Yan       Changed the invalid target table name.
-- 2010-01-19   Xu Yan       Updated the conditional statement and NOCLD value
-- 2010-03-03   Xu Yan       Fixed the previous bug for MAX_ACG_DT checking
-- 2010-03-23   Xu Yan       Fixed previous bugs for 'TOT_MTD_NBR_DB_TXN'
-- 2012-02-27   Chen XiaoWen 将merge当中的match、not match逻辑拆分为match update和单独的insert语句。
-- 2012-04-17   Chen XiaoWen 增加字段BOY_BAL_AMT_年初余额
-------------------------------------------------------------------------------
LANGUAGE SQL
BEGIN
/*声明异常处理使用变量*/
DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
DECLARE SMY_STEPNUM INT DEFAULT 0;                     --过程内部位置标记
DECLARE SMY_STEPDESC VARCHAR(100) DEFAULT '';          --过程内部位置描述
DECLARE SMY_DATE DATE;                                 --临时日期变量
DECLARE SMY_RCOUNT INT;                                --DML语句作用记录数
DECLARE SMY_PROCNM VARCHAR(100);    --
DECLARE CUR_YEAR SMALLINT;--
DECLARE CUR_MONTH SMALLINT;--
DECLARE CUR_DAY INTEGER;--
DECLARE MAX_ACG_DT DATE;--
DECLARE YR_FIRST_DAY DATE;--
DECLARE QTR_FIRST_DAY DATE;--
DECLARE YR_DAY SMALLINT;--
DECLARE QTR_DAY SMALLINT;--
DECLARE MNH_DAY SMALLINT;--
DECLARE DELETE_SQL VARCHAR(200);                       --删除历史表动态SQL
DECLARE MTH_FIRST_DAY DATE;--
DECLARE MTH_LAST_DAY DATE;--
DECLARE LAST_YEAR SMALLINT;           --去年
DECLARE LAST_YEAR_MTH12_FIRSTDAY DATE;--去年的12月1日
DECLARE LAST_YEAR_MTH12_LASTDAY DATE; --去年的12月31日

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
	SET SMY_SQLCODE = SQLCODE;--
  ROLLBACK;--
  SET SMY_STEPNUM = SMY_STEPNUM+1;--
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, 99, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
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
SET SMY_PROCNM = 'PROC_MTHLY_FT_DEP_ACML_BAL_AMT';--
SET SMY_DATE=ACCOUNTING_DATE;--
SET CUR_YEAR=YEAR(ACCOUNTING_DATE);  --取当前年份
SET CUR_MONTH=MONTH(ACCOUNTING_DATE); --取当前月份
SET CUR_DAY=DAY(ACCOUNTING_DATE);     --取当日
SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');--
SET YR_DAY=DAYOFYEAR(SMY_DATE);--
IF CUR_MONTH IN (1,2,3) THEN 
   SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');   --
ELSEIF CUR_MONTH IN (4,5,6) THEN 
       SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-04-01');       --
    ELSEIF CUR_MONTH IN (7,8,9) THEN 
           SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-07-01');           --
        ELSE
            SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-10-01');           --
END IF;--

SET LAST_YEAR=CUR_YEAR-1;
SET LAST_YEAR_MTH12_FIRSTDAY=DATE(TRIM(CHAR(LAST_YEAR))||'-12-01');
SET LAST_YEAR_MTH12_LASTDAY=DATE(TRIM(CHAR(LAST_YEAR))||'-12-31');

SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,ISO),1,7)||'-01'); --取当月初日
VALUES(MTH_FIRST_DAY + 1 MONTH - 1 DAY) INTO MTH_LAST_DAY ;      --

SET MNH_DAY=DAY(ACCOUNTING_DATE); --

SET QTR_DAY=DAYS(SMY_DATE)-DAYS(QTR_FIRST_DAY) + 1;--
-------------------------------------Start on 20100303----------------------------------	
--SELECT MAX(ACG_DT) INTO MAX_ACG_DT FROM SMY.MTHLY_CR_CRD_AC_ACML_BAL_AMT;--
SELECT MAX(ACG_DT) INTO MAX_ACG_DT FROM SMY.MTHLY_FT_DEP_ACML_BAL_AMT;--
-------------------------------------End on 20100303----------------------------------

SET DELETE_SQL='ALTER TABLE HIS.MTHLY_FT_DEP_ACML_BAL_AMT ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE';--

/*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE,并插入新的起始标志*/
DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
----------------Start on 20100303--------------------
--COMMIT;--
SET SMY_STEPNUM = SMY_STEPNUM+1;--
----------------End on 20100303--------------------
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, 'PROCEDURE START.', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

/*数据恢复与备份*/
IF MAX_ACG_DT=ACCOUNTING_DATE THEN
   ------------------------Start on 20100303--------------------
   DELETE FROM SMY.MTHLY_FT_DEP_ACML_BAL_AMT WHERE ACG_DT>=MTH_FIRST_DAY AND ACG_DT<=MTH_LAST_DAY;--
   --COMMIT;--
   IF CUR_DAY<>1 THEN
      INSERT INTO SMY.MTHLY_FT_DEP_ACML_BAL_AMT SELECT * FROM HIS.MTHLY_FT_DEP_ACML_BAL_AMT;--
      COMMIT;--
   END IF;--
   ------------------------End on 20100303--------------------
ELSE
   EXECUTE IMMEDIATE DELETE_SQL;--
   --COMMIT;--
   INSERT INTO HIS.MTHLY_FT_DEP_ACML_BAL_AMT SELECT * FROM SMY.MTHLY_FT_DEP_ACML_BAL_AMT WHERE ACG_DT>=MTH_FIRST_DAY AND ACG_DT<=MTH_LAST_DAY;--
   COMMIT;--
END IF;--

SET SMY_STEPNUM = SMY_STEPNUM+1;--

INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, '数据恢复与备份.', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

--SET SMY_STEPNUM = SMY_STEPNUM+1;--
SET SMY_STEPDESC = '创建临时表,并把当日数据插入';--

DECLARE GLOBAL TEMPORARY TABLE TMP_MTHLY_FT_DEP_ACML_BAL_AMT(AC_AR_ID             CHARACTER(20)  ,
                                                              CCY                  CHARACTER(3)   ,
                                                              BAL_AMT              DECIMAL(17, 2) ,
                                                              CUR_DAY_CR_AMT       DECIMAL(17, 2) ,
                                                              CUR_DAY_DB_AMT       DECIMAL(17, 2) ,
                                                              NBR_CUR_CR_TXN       INTEGER        ,
                                                              NBR_CUR_DB_TXN       INTEGER        
                                                             ,NOD_IN_MTH           SMALLINT
                                                             )
ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE PARTITIONING KEY(AC_AR_ID); 	--


INSERT INTO SESSION.TMP_MTHLY_FT_DEP_ACML_BAL_AMT(AC_AR_ID      
                                                   ,CCY           
                                                   ,BAL_AMT       
                                                   ,CUR_DAY_CR_AMT
                                                   ,CUR_DAY_DB_AMT
                                                   ,NBR_CUR_CR_TXN
                                                   ,NBR_CUR_DB_TXN
                                                   ,NOD_IN_MTH)
WITH TMP_FT_DEP_AR AS ( SELECT 
                          FT_DEP_AR_ID       ,
                          DNMN_CCY_ID        ,
                          max(case when AR_LCS_TP_ID = 20370007 
                            then 1 else 0 end ) NOD_IN_MTH
                          ,SUM(BAL_AMT) AS BAL_AMT                          
                        FROM SOR.FT_DEP_AR
                        ------------Start on 20100119-----------------
                        --WHERE DEL_F=0 AND AR_LCS_TP_ID = 20370007
                        WHERE DEL_F=0 
                              AND 
                              (END_DT >= YR_FIRST_DAY
                               OR                               
      												 END_DT = '1899-12-31'	
      												)
                        ------------Start on 20100119-----------------
                        GROUP BY 
                          FT_DEP_AR_ID      ,
                          DNMN_CCY_ID),
TMP_ONLINE_TXN_RUN_D AS (SELECT 
                               TXN_AR_ID         ,
                               TXN_DNMN_CCY_ID       ,
                               SUM(case when DB_CR_TP_ID = 14280002 then TXN_AMT else 0 end) AS CUR_DAY_CR_AMT   ,
                               SUM(CASE WHEN DB_CR_TP_ID = 14280002 THEN 1 ELSE 0 END)  AS NBR_CUR_CR_TXN
                              ,SUM(case when DB_CR_TP_ID = 14280001 then TXN_AMT else 0 end) AS CUR_DAY_DB_AMT 
                              ,SUM(CASE WHEN DB_CR_TP_ID = 14280001 THEN 1 ELSE 0 END) AS NBR_CUR_DB_TXN
                             FROM SOR.ONLINE_TXN_RUN
                             WHERE DB_CR_TP_ID in (
                              										 14280002
                                                  ,14280001
                                                  )
                                   AND TXN_DT = SMY_DATE 
                                   --AND DEL_F=0 
                                   AND ACG_TXN_RUN_TP_ID = 15150002 
                                   AND TXN_RED_BLUE_TP_ID = 15200001 
                                   AND PST_RVRSL_TP_ID = 15130001
                             GROUP BY
                               TXN_AR_ID         ,
                               TXN_DNMN_CCY_ID)
/*
,TMP_ONLINE_TXN_RUN_J AS (SELECT 
                               TXN_AR_ID         ,
                               TXN_DNMN_CCY_ID       ,
                               SUM(TXN_AMT) AS CUR_DAY_DB_AMT  ,
                               COUNT(*) AS NBR_CUR_DB_TXN
                             FROM SOR.ONLINE_TXN_RUN
                             WHERE DB_CR_TP_ID = 14280001 AND TXN_DT = SMY_DATE AND DEL_F=0 AND ACG_TXN_RUN_TP_ID = 15150002 AND TXN_RED_BLUE_TP_ID = 15200001 AND PST_RVRSL_TP_ID = 15130001
                             GROUP BY
                               TXN_AR_ID         ,
                               TXN_DNMN_CCY_ID)
*/                               
SELECT 
  a.FT_DEP_AR_ID         ,
  a.DNMN_CCY_ID          ,
  a.BAL_AMT              ,
  value(b.CUR_DAY_CR_AMT,0)       ,
  value(b.CUR_DAY_DB_AMT,0)       ,
  value(b.NBR_CUR_CR_TXN,0)       ,
  value(b.NBR_CUR_DB_TXN,0)
 ,NOD_IN_MTH  
FROM TMP_FT_DEP_AR a LEFT JOIN TMP_ONLINE_TXN_RUN_D b ON a.FT_DEP_AR_ID = b.TXN_AR_ID AND a.DNMN_CCY_ID = b.TXN_DNMN_CCY_ID
                   --  LEFT JOIN TMP_ONLINE_TXN_RUN_J c ON a.FT_DEP_AR_ID = c.TXN_AR_ID AND a.DNMN_CCY_ID = c.TXN_DNMN_CCY_ID
;--

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
SET SMY_STEPNUM = SMY_STEPNUM+1;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

CREATE INDEX SESSION.AC_AR_ID_CCY ON SESSION.TMP_MTHLY_FT_DEP_ACML_BAL_AMT(AC_AR_ID,CCY);

--SET SMY_STEPNUM = SMY_STEPNUM+1;--

IF CUR_DAY = 1 THEN
   IF CUR_MONTH = 1 THEN      
      INSERT INTO SMY.MTHLY_FT_DEP_ACML_BAL_AMT(AC_AR_ID
																								,CDR_YR
																								,CCY
																								,CDR_MTH
																								,NOCLD_IN_MTH
																								,NOD_IN_MTH
																								,NOCLD_IN_QTR
																								,NOD_IN_QTR
																								,NOCLD_IN_YEAR
																								,NOD_IN_YEAR
																								,ACG_DT
																								,BAL_AMT
																								,CUR_DAY_CR_AMT
																								,CUR_DAY_DB_AMT
																								,NBR_CUR_CR_TXN
																								,NBR_CUR_DB_TXN
																								,MTD_ACML_BAL_AMT
																								,TOT_MTD_CR_AMT
																								,TOT_MTD_DB_AMT
																								,TOT_MTD_NBR_CR_TXN
																								,TOT_MTD_NBR_DB_TXN
																								,QTD_ACML_BAL_AMT
																								,TOT_QTD_CR_AMT
																								,TOT_QTD_DB_AMT
																								,TOT_QTD_NBR_CR_TXN
																								,TOT_QTD_NBR_DB_TXN
																								,YTD_ACML_BAL_AMT
																								,TOT_YTD_CR_AMT
																								,TOT_YTD_DB_AMT
																								,TOT_YTD_NBR_CR_TXN
																								,TOT_YTD_NBR_DB_TXN
																								,BOY_BAL_AMT) 	                                     
      SELECT 
        S.AC_AR_ID                           ,
        CUR_YEAR                           ,
        S.CCY                                ,
        CUR_MONTH                          ,
        MNH_DAY                            ,
        S.NOD_IN_MTH                            ,
        QTR_DAY                            ,
        S.NOD_IN_MTH                            ,
        YR_DAY                             ,
        S.NOD_IN_MTH                            ,
        SMY_DATE                           ,
        S.BAL_AMT                          ,
        S.CUR_DAY_CR_AMT                   ,
        S.CUR_DAY_DB_AMT                   ,
        S.NBR_CUR_CR_TXN                   ,
        S.NBR_CUR_DB_TXN                   ,
   	    S.BAL_AMT                          ,
        S.CUR_DAY_CR_AMT                   ,
        S.CUR_DAY_DB_AMT                   ,
        S.NBR_CUR_CR_TXN                   ,
        S.NBR_CUR_DB_TXN                   ,
   	    S.BAL_AMT                          ,
        S.CUR_DAY_CR_AMT                   ,
        S.CUR_DAY_DB_AMT                   ,
        S.NBR_CUR_CR_TXN                   ,
        S.NBR_CUR_DB_TXN                   ,
   	    S.BAL_AMT                          ,
        S.CUR_DAY_CR_AMT                   ,
        S.CUR_DAY_DB_AMT                   ,
        S.NBR_CUR_CR_TXN                   ,
        S.NBR_CUR_DB_TXN                   ,
        COALESCE(T.BAL_AMT, 0.00)          --上年末余额
      FROM SESSION.TMP_MTHLY_FT_DEP_ACML_BAL_AMT S 
      LEFT JOIN (SELECT AC_AR_ID,CCY,BAL_AMT 
                 FROM SMY.MTHLY_FT_DEP_ACML_BAL_AMT 
                 WHERE ACG_DT>=LAST_YEAR_MTH12_FIRSTDAY and ACG_DT<=LAST_YEAR_MTH12_LASTDAY) T 
      ON S.AC_AR_ID=T.AC_AR_ID and S.CCY=T.CCY
      ;


  ELSEIF CUR_MONTH IN (4, 7, 10) THEN
      SET SMY_STEPDESC = '插入季初数据';--
      
      INSERT INTO SMY.MTHLY_FT_DEP_ACML_BAL_AMT(AC_AR_ID
																								,CDR_YR
																								,CCY
																								,CDR_MTH
																								,NOCLD_IN_MTH
																								,NOD_IN_MTH
																								,NOCLD_IN_QTR
																								,NOD_IN_QTR
																								,NOCLD_IN_YEAR
																								,NOD_IN_YEAR
																								,ACG_DT
																								,BAL_AMT
																								,CUR_DAY_CR_AMT
																								,CUR_DAY_DB_AMT
																								,NBR_CUR_CR_TXN
																								,NBR_CUR_DB_TXN
																								,MTD_ACML_BAL_AMT
																								,TOT_MTD_CR_AMT
																								,TOT_MTD_DB_AMT
																								,TOT_MTD_NBR_CR_TXN
																								,TOT_MTD_NBR_DB_TXN
																								,QTD_ACML_BAL_AMT
																								,TOT_QTD_CR_AMT
																								,TOT_QTD_DB_AMT
																								,TOT_QTD_NBR_CR_TXN
																								,TOT_QTD_NBR_DB_TXN
																								,YTD_ACML_BAL_AMT
																								,TOT_YTD_CR_AMT
																								,TOT_YTD_DB_AMT
																								,TOT_YTD_NBR_CR_TXN
																								,TOT_YTD_NBR_DB_TXN
																								,BOY_BAL_AMT)
      SELECT 
        a.AC_AR_ID                           ,
        CUR_YEAR                           ,
        a.CCY                                ,
        CUR_MONTH                          ,
        MNH_DAY                            ,
        a.NOD_IN_MTH                            ,
        QTR_DAY                            ,
        a.NOD_IN_MTH                   ,
        YR_DAY                             ,
        value(b.NOD_IN_YEAR,0) + a.NOD_IN_MTH                  ,
        SMY_DATE                           ,
        a.BAL_AMT                          ,
        a.CUR_DAY_CR_AMT                   ,
        a.CUR_DAY_DB_AMT                   ,
        a.NBR_CUR_CR_TXN                   ,
        -------------Start on 20100323---------------
        --a.CUR_DAY_DB_AMT                   ,
        a.NBR_CUR_DB_TXN                   ,
        -------------End on 20100323-----------------
   	    a.BAL_AMT                          ,
        a.CUR_DAY_CR_AMT                   ,
        a.CUR_DAY_DB_AMT                   ,
        a.NBR_CUR_CR_TXN                   ,
        -------------Start on 20100323---------------
        --a.CUR_DAY_DB_AMT                   ,
        a.NBR_CUR_DB_TXN                   ,
        -------------End on 20100323-----------------
   	    a.BAL_AMT                          ,
        a.CUR_DAY_CR_AMT                   ,
        a.CUR_DAY_DB_AMT                   ,
        a.NBR_CUR_CR_TXN                   ,
        -------------Start on 20100323---------------
        --a.CUR_DAY_DB_AMT                   ,
        a.NBR_CUR_DB_TXN                   ,
        -------------End on 20100323----------------- 
   	    COALESCE(b.YTD_ACML_BAL_AMT, 0.00) + a.BAL_AMT                          ,
        COALESCE(b.TOT_YTD_CR_AMT, 0.00) + a.CUR_DAY_CR_AMT                   ,
        COALESCE(b.TOT_YTD_DB_AMT, 0.00) + a.CUR_DAY_DB_AMT                   ,
        COALESCE(b.TOT_YTD_NBR_CR_TXN, 0.00) + a.NBR_CUR_CR_TXN                   ,
        COALESCE(b.TOT_YTD_NBR_DB_TXN, 0.00) + a.NBR_CUR_DB_TXN               ,
        COALESCE(b.BOY_BAL_AMT, 0.00)   --上年末余额
      FROM SESSION.TMP_MTHLY_FT_DEP_ACML_BAL_AMT a LEFT JOIN SMY.MTHLY_FT_DEP_ACML_BAL_AMT b
      ON a.AC_AR_ID = b.AC_AR_ID AND
				 CUR_YEAR   = b.CDR_YR AND
				 a.CCY      = b.CCY AND
				 CUR_MONTH -1   = b.CDR_MTH ;--
    
    ELSE
    	SET SMY_STEPDESC = '插入非年初或季初的月初数据';--
      
      INSERT INTO SMY.MTHLY_FT_DEP_ACML_BAL_AMT(AC_AR_ID
																								,CDR_YR
																								,CCY
																								,CDR_MTH
																								,NOCLD_IN_MTH
																								,NOD_IN_MTH
																								,NOCLD_IN_QTR
																								,NOD_IN_QTR
																								,NOCLD_IN_YEAR
																								,NOD_IN_YEAR
																								,ACG_DT
																								,BAL_AMT
																								,CUR_DAY_CR_AMT
																								,CUR_DAY_DB_AMT
																								,NBR_CUR_CR_TXN
																								,NBR_CUR_DB_TXN
																								,MTD_ACML_BAL_AMT
																								,TOT_MTD_CR_AMT
																								,TOT_MTD_DB_AMT
																								,TOT_MTD_NBR_CR_TXN
																								,TOT_MTD_NBR_DB_TXN
																								,QTD_ACML_BAL_AMT
																								,TOT_QTD_CR_AMT
																								,TOT_QTD_DB_AMT
																								,TOT_QTD_NBR_CR_TXN
																								,TOT_QTD_NBR_DB_TXN
																								,YTD_ACML_BAL_AMT
																								,TOT_YTD_CR_AMT
																								,TOT_YTD_DB_AMT
																								,TOT_YTD_NBR_CR_TXN
																								,TOT_YTD_NBR_DB_TXN
																								,BOY_BAL_AMT)
      SELECT 
        a.AC_AR_ID                           ,
        CUR_YEAR                           ,
        a.CCY                                ,
        CUR_MONTH                          ,
        MNH_DAY                            ,
        a.NOD_IN_MTH                            ,
        QTR_DAY                            ,
        value(b.NOD_IN_QTR,0) + a.NOD_IN_MTH                   ,
        YR_DAY                             ,
        value(b.NOD_IN_YEAR,0) + a.NOD_IN_MTH                  ,
        SMY_DATE                           ,
        a.BAL_AMT                          ,
        a.CUR_DAY_CR_AMT                   ,
        a.CUR_DAY_DB_AMT                   ,
        a.NBR_CUR_CR_TXN                   ,
        a.NBR_CUR_DB_TXN                   ,
   	    a.BAL_AMT                          ,
        a.CUR_DAY_CR_AMT                   ,
        a.CUR_DAY_DB_AMT                   ,
        a.NBR_CUR_CR_TXN                   ,
        a.NBR_CUR_DB_TXN                   ,
   	    COALESCE(b.QTD_ACML_BAL_AMT, 0.00) + a.BAL_AMT                          ,
        COALESCE(b.TOT_QTD_CR_AMT, 0.00) + a.CUR_DAY_CR_AMT                   ,
        COALESCE(b.TOT_QTD_DB_AMT, 0.00) + a.CUR_DAY_DB_AMT                   ,
        COALESCE(b.TOT_QTD_NBR_CR_TXN, 0.00) + a.NBR_CUR_CR_TXN                   ,
        COALESCE(b.TOT_QTD_NBR_DB_TXN, 0.00) + a.NBR_CUR_DB_TXN                   , 
   	    COALESCE(b.YTD_ACML_BAL_AMT, 0.00) + a.BAL_AMT                          ,
        COALESCE(b.TOT_YTD_CR_AMT, 0.00) + a.CUR_DAY_CR_AMT                   ,
        COALESCE(b.TOT_YTD_DB_AMT, 0.00) + a.CUR_DAY_DB_AMT                   ,
        COALESCE(b.TOT_YTD_NBR_CR_TXN, 0.00) + a.NBR_CUR_CR_TXN                   ,
        COALESCE(b.TOT_YTD_NBR_DB_TXN, 0.00) + a.NBR_CUR_DB_TXN               ,
        COALESCE(b.BOY_BAL_AMT, 0.00)   --上年末余额
      FROM SESSION.TMP_MTHLY_FT_DEP_ACML_BAL_AMT a LEFT JOIN SMY.MTHLY_FT_DEP_ACML_BAL_AMT b
      ON a.AC_AR_ID = b.AC_AR_ID AND
				 CUR_YEAR   = b.CDR_YR AND
				 a.CCY      = b.CCY AND
				 CUR_MONTH -1 = b.CDR_MTH ;--
    

   END IF;--
ELSE
	SET SMY_STEPDESC = 'merge非月初数据';--
  
  MERGE INTO SMY.MTHLY_FT_DEP_ACML_BAL_AMT TAG
  USING SESSION.TMP_MTHLY_FT_DEP_ACML_BAL_AMT SOC	
  ON SOC.AC_AR_ID = TAG.AC_AR_ID AND 
     CUR_YEAR     = TAG.CDR_YR AND   
     SOC.CCY      = TAG.CCY AND      
     CUR_MONTH    = TAG.CDR_MTH 
	WHEN MATCHED THEN
	         UPDATE SET 
						(AC_AR_ID           
						,CDR_YR             
						,CCY                
						,CDR_MTH            
						,NOCLD_IN_MTH       
						,NOD_IN_MTH         
						,NOCLD_IN_QTR       
						,NOD_IN_QTR         
						,NOCLD_IN_YEAR      
						,NOD_IN_YEAR        
						,ACG_DT             
						,BAL_AMT            
						,CUR_DAY_CR_AMT     
						,CUR_DAY_DB_AMT     
						,NBR_CUR_CR_TXN     
						,NBR_CUR_DB_TXN     
						,MTD_ACML_BAL_AMT   
						,TOT_MTD_CR_AMT     
						,TOT_MTD_DB_AMT     
						,TOT_MTD_NBR_CR_TXN 
						,TOT_MTD_NBR_DB_TXN 
						,QTD_ACML_BAL_AMT   
						,TOT_QTD_CR_AMT     
						,TOT_QTD_DB_AMT     
						,TOT_QTD_NBR_CR_TXN 
						,TOT_QTD_NBR_DB_TXN 
						,YTD_ACML_BAL_AMT   
						,TOT_YTD_CR_AMT     
						,TOT_YTD_DB_AMT     
						,TOT_YTD_NBR_CR_TXN 
						,TOT_YTD_NBR_DB_TXN)
          =
						(TAG.AC_AR_ID           
						,TAG.CDR_YR             
						,TAG.CCY                
						,TAG.CDR_MTH            
						,MNH_DAY       
						,TAG.NOD_IN_MTH + SOC.NOD_IN_MTH
						,QTR_DAY       
						,TAG.NOD_IN_QTR + SOC.NOD_IN_MTH
						,YR_DAY      
						,TAG.NOD_IN_YEAR + SOC.NOD_IN_MTH
						,SMY_DATE             
						,SOC.BAL_AMT            
						,SOC.CUR_DAY_CR_AMT     
						,SOC.CUR_DAY_DB_AMT     
						,SOC.NBR_CUR_CR_TXN     
						,SOC.NBR_CUR_DB_TXN     
						,TAG.MTD_ACML_BAL_AMT + SOC.BAL_AMT
						,TAG.TOT_MTD_CR_AMT + SOC.CUR_DAY_CR_AMT
						,TAG.TOT_MTD_DB_AMT + SOC.CUR_DAY_DB_AMT
						,TAG.TOT_MTD_NBR_CR_TXN + SOC.NBR_CUR_CR_TXN
						,TAG.TOT_MTD_NBR_DB_TXN + SOC.NBR_CUR_DB_TXN
						,TAG.QTD_ACML_BAL_AMT + SOC.BAL_AMT
						,TAG.TOT_QTD_CR_AMT + SOC.CUR_DAY_CR_AMT
						,TAG.TOT_QTD_DB_AMT + SOC.CUR_DAY_DB_AMT
						,TAG.TOT_QTD_NBR_CR_TXN + SOC.NBR_CUR_CR_TXN
						,TAG.TOT_QTD_NBR_DB_TXN + SOC.NBR_CUR_DB_TXN
						,TAG.YTD_ACML_BAL_AMT + SOC.BAL_AMT
						,TAG.TOT_YTD_CR_AMT + SOC.CUR_DAY_CR_AMT
						,TAG.TOT_YTD_DB_AMT + SOC.CUR_DAY_DB_AMT
						,TAG.TOT_YTD_NBR_CR_TXN + SOC.NBR_CUR_CR_TXN
						,TAG.TOT_YTD_NBR_DB_TXN + SOC.NBR_CUR_DB_TXN);
/*  WHEN NOT MATCHED THEN
          INSERT 	
						(AC_AR_ID           
						,CDR_YR             
						,CCY                
						,CDR_MTH            
						,NOCLD_IN_MTH       
						,NOD_IN_MTH         
						,NOCLD_IN_QTR       
						,NOD_IN_QTR         
						,NOCLD_IN_YEAR      
						,NOD_IN_YEAR        
						,ACG_DT             
						,BAL_AMT            
						,CUR_DAY_CR_AMT     
						,CUR_DAY_DB_AMT     
						,NBR_CUR_CR_TXN     
						,NBR_CUR_DB_TXN     
						,MTD_ACML_BAL_AMT   
						,TOT_MTD_CR_AMT     
						,TOT_MTD_DB_AMT     
						,TOT_MTD_NBR_CR_TXN 
						,TOT_MTD_NBR_DB_TXN 
						,QTD_ACML_BAL_AMT   
						,TOT_QTD_CR_AMT     
						,TOT_QTD_DB_AMT     
						,TOT_QTD_NBR_CR_TXN 
						,TOT_QTD_NBR_DB_TXN 
						,YTD_ACML_BAL_AMT   
						,TOT_YTD_CR_AMT     
						,TOT_YTD_DB_AMT     
						,TOT_YTD_NBR_CR_TXN 
						,TOT_YTD_NBR_DB_TXN)
				VALUES
          (AC_AR_ID                           ,
          CUR_YEAR                           ,
          CCY                                ,
          CUR_MONTH                          ,
          MNH_DAY                            ,
          NOD_IN_MTH                            ,
          QTR_DAY                            ,
          NOD_IN_MTH                            ,
          YR_DAY                             ,
          NOD_IN_MTH                            ,
          SMY_DATE                           ,
          BAL_AMT                          ,
          CUR_DAY_CR_AMT                   ,
          CUR_DAY_DB_AMT                   ,
          NBR_CUR_CR_TXN                   ,
          -------------Start on 20100323---------------
          --CUR_DAY_DB_AMT                   ,
          NBR_CUR_DB_TXN                   ,
          -------------End on 20100323-----------------
   	      BAL_AMT                          ,
          CUR_DAY_CR_AMT                   ,
          CUR_DAY_DB_AMT                   ,
          NBR_CUR_CR_TXN                   ,
          -------------Start on 20100323---------------
          --CUR_DAY_DB_AMT                   ,
          NBR_CUR_DB_TXN                   ,
          -------------End on 20100323-----------------
   	      BAL_AMT                          ,
          CUR_DAY_CR_AMT                   ,
          CUR_DAY_DB_AMT                   ,
          NBR_CUR_CR_TXN                   ,
          -------------Start on 20100323---------------
          --CUR_DAY_DB_AMT                   ,
          NBR_CUR_DB_TXN                   ,
          -------------End on 20100323-----------------
   	      BAL_AMT                          ,
          CUR_DAY_CR_AMT                   ,
          CUR_DAY_DB_AMT                   ,
          NBR_CUR_CR_TXN                   ,
          -------------Start on 20100323---------------
          --CUR_DAY_DB_AMT                   ,
          NBR_CUR_DB_TXN                   
          -------------End on 20100323-----------------
          );--*/
  INSERT INTO SMY.MTHLY_FT_DEP_ACML_BAL_AMT
  (
          AC_AR_ID           
          ,CDR_YR             
          ,CCY                
          ,CDR_MTH            
          ,NOCLD_IN_MTH       
          ,NOD_IN_MTH         
          ,NOCLD_IN_QTR       
          ,NOD_IN_QTR         
          ,NOCLD_IN_YEAR      
          ,NOD_IN_YEAR        
          ,ACG_DT             
          ,BAL_AMT            
          ,CUR_DAY_CR_AMT     
          ,CUR_DAY_DB_AMT     
          ,NBR_CUR_CR_TXN     
          ,NBR_CUR_DB_TXN     
          ,MTD_ACML_BAL_AMT   
          ,TOT_MTD_CR_AMT     
          ,TOT_MTD_DB_AMT     
          ,TOT_MTD_NBR_CR_TXN 
          ,TOT_MTD_NBR_DB_TXN 
          ,QTD_ACML_BAL_AMT   
          ,TOT_QTD_CR_AMT     
          ,TOT_QTD_DB_AMT     
          ,TOT_QTD_NBR_CR_TXN 
          ,TOT_QTD_NBR_DB_TXN 
          ,YTD_ACML_BAL_AMT   
          ,TOT_YTD_CR_AMT     
          ,TOT_YTD_DB_AMT     
          ,TOT_YTD_NBR_CR_TXN 
          ,TOT_YTD_NBR_DB_TXN)
  SELECT
          SOC.AC_AR_ID,
          CUR_YEAR,
          SOC.CCY,
          CUR_MONTH,
          MNH_DAY,
          SOC.NOD_IN_MTH,
          QTR_DAY,
          SOC.NOD_IN_MTH,
          YR_DAY,
          SOC.NOD_IN_MTH,
          SMY_DATE,
          SOC.BAL_AMT,
          SOC.CUR_DAY_CR_AMT,
          SOC.CUR_DAY_DB_AMT,
          SOC.NBR_CUR_CR_TXN,
          SOC.NBR_CUR_DB_TXN,
          SOC.BAL_AMT,
          SOC.CUR_DAY_CR_AMT,
          SOC.CUR_DAY_DB_AMT,
          SOC.NBR_CUR_CR_TXN,
          SOC.NBR_CUR_DB_TXN,
          SOC.BAL_AMT,
          SOC.CUR_DAY_CR_AMT,
          SOC.CUR_DAY_DB_AMT,
          SOC.NBR_CUR_CR_TXN,
          SOC.NBR_CUR_DB_TXN,
          SOC.BAL_AMT,
          SOC.CUR_DAY_CR_AMT,
          SOC.CUR_DAY_DB_AMT,
          SOC.NBR_CUR_CR_TXN,
          SOC.NBR_CUR_DB_TXN
  FROM SESSION.TMP_MTHLY_FT_DEP_ACML_BAL_AMT as SOC
    WHERE NOT EXISTS (
         SELECT 1 FROM SMY.MTHLY_FT_DEP_ACML_BAL_AMT TAG
         WHERE SOC.AC_AR_ID = TAG.AC_AR_ID 
               AND CUR_YEAR = TAG.CDR_YR 
               AND SOC.CCY = TAG.CCY 
               AND CUR_MONTH = TAG.CDR_MTH 
    );

END IF;--

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
SET SMY_STEPNUM = SMY_STEPNUM+1;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
 


END@