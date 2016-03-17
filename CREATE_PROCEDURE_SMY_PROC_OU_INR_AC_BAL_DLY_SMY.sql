CREATE PROCEDURE SMY.PROC_OU_INR_AC_BAL_DLY_SMY(IN ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_OU_INR_AC_BAL_DLY_SMY.sql
-- Procedure name: 			SMY.PROC_OU_INR_AC_BAL_DLY_SMY
-- Source Table:				SMY.MTHLY_INR_AC_ACML_BAL_AMT
--                      SOR.ACG_SBJ_CODE_MAPPING
-- Target Table: 				SMY.OU_INR_AC_BAL_DLY_SMY
--                      SMY.OU_INR_AC_BAL_MTHLY_SMY 
-- Project     :        ZJ RCCB EDW
-- NOTES       :        
-- Purpose     :            
-- PROCESS METHOD      :  INSERT ONLY EACH DAY
--=============================================================================
-- Creation Date:       2009.11.11
-- Origin Author:       JAMES SHANG IBM copyright 
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2009-11-11   JAMES SHANG     Create SP File
-- 2010-08-10   Fang Yihua      Added three new columns 'NOCLD_IN_MTH','NOCLD_IN_QTR','NOCLD_IN_YEAR'
-- 2010-08-24		Feng Jia Qiang	Modify the condition which insert data into SMY.OU_INR_AC_BAL_MTHLY_SMY
-------------------------------------------------------------------------------
LANGUAGE SQL
BEGIN

/*声明异常处理使用变量*/
		DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
		DECLARE SMY_STEPNUM INT DEFAULT 1;                     --过程内部位置标记
		DECLARE SMY_STEPDESC VARCHAR(100) DEFAULT '';          --过程内部位置描述
		DECLARE SMY_DATE DATE;        --临时日期变量
		DECLARE SMY_RCOUNT INT;       --DML语句作用记录数
		DECLARE SMY_PROCNM VARCHAR(100);                        --存储过程名称

/*声明存储过程使用变量*/
		DECLARE CUR_YEAR SMALLINT;--
		DECLARE CUR_MONTH SMALLINT;--
		DECLARE CUR_DAY INTEGER;--
		DECLARE YR_FIRST_DAY DATE;--
		DECLARE QTR_FIRST_DAY DATE;--
		DECLARE YR_DAY SMALLINT;--
		DECLARE QTR_DAY SMALLINT;--
		DECLARE MAX_ACG_DT DATE;--
		DECLARE LAST_SMY_DATE DATE;--
		DECLARE MTH_FIRST_DAY DATE;--
		DECLARE V_T SMALLINT;--
    DECLARE C_YR_DAY SMALLINT;--
		DECLARE C_QTR_DAY SMALLINT;--
		DECLARE QTR_LAST_DAY DATE;--
		DECLARE C_MON_DAY SMALLINT;--
		DECLARE CUR_QTR SMALLINT;    --
		-- 账务日期月的最后一天
		DECLARE MTH_LAST_DAY DATE; 	--
/*
	1.定义针对SQL异常情况的句柄(EXIT方式).
  2.将出现SQL异常时在存储过程中的位置(SMY_STEPNUM),位置描述(SMY_STEPDESC),SQLCODE(SMY_SQLCODE)记入表SMY_LOG中作调试用.
  3.调用RESIGNAL重新引发异常,跳出存储过程执行体,对引发SQL异常之前存储过程体中所完成的操作进行回滚.
*/

		DECLARE CONTINUE HANDLER FOR NOT FOUND
		  SET V_T=0 ; --
		    
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
    SET SMY_PROCNM  ='PROC_OU_INR_AC_BAL_DLY_SMY';--
    SET SMY_DATE    =ACCOUNTING_DATE;    --
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --取当前年份
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --取当前月份
    SET CUR_DAY     =DAY(ACCOUNTING_DATE);     --取月第几日
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- 取年初日
    SET C_YR_DAY      =DAYOFYEAR(DATE(TRIM(CHAR(YEAR(ACCOUNTING_DATE)))||'-12-31')); --取当年第几日
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --当前季度
    SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,ISO),1,7)||'-01'); --取当月初日
    
    VALUES(ACCOUNTING_DATE - 1 DAY) INTO LAST_SMY_DATE;--
    VALUES(MTH_FIRST_DAY + 1 MONTH - 1 DAY) INTO MTH_LAST_DAY ;     --
    --计算月日历天数
    SET C_MON_DAY = DAYS(MTH_FIRST_DAY + 1 MONTH ) - DAYS(MTH_FIRST_DAY);    --
  
    --计算季度日历天数
    IF CUR_QTR = 1  
       THEN 
       	SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');--
       	SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-04-01') - 1 DAY ;--
    ELSEIF CUR_QTR = 2
       THEN 
       	SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-04-01');--
       	SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-07-01') - 1 DAY ;       	--
    ELSEIF CUR_QTR = 3
       THEN 
       	SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-07-01');--
       	SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-10-01') - 1 DAY ;       	--
    ELSE
       SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-10-01');--
       SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-12-31');       --
    END IF;--

------------------------------------Start on 2010-08-10 -------------------------------------------------
    SET YR_DAY      =DAYS(ACCOUNTING_DATE)-DAYS(YR_FIRST_DAY)+1;--
    SET QTR_DAY=DAYS(ACCOUNTING_DATE)-DAYS(QTR_FIRST_DAY)+1;-- 
------------------------------------End on 2010-08-10 ---------------------------------------------------

  /*取当季日历天数*/ 
  	SET C_QTR_DAY = DAYS(QTR_LAST_DAY) - DAYS(QTR_FIRST_DAY) + 1 ;--
		
		SELECT COALESCE(MAX(ACG_DT),'1900-01-01') INTO MAX_ACG_DT FROM SMY.OU_INR_AC_BAL_DLY_SMY;--

/*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE,并插入新的起始标志*/
		DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
			COMMIT;--
		
		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		
		SET SMY_STEPDESC = 	'存储过程开始运行' ;--
		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
				VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);		 --

/*数据恢复与备份*/
    IF MAX_ACG_DT = ACCOUNTING_DATE THEN
       DELETE FROM SMY.OU_INR_AC_BAL_DLY_SMY WHERE ACG_DT = ACCOUNTING_DATE ;--
       COMMIT;--
    END IF;--

/*月表的恢复*/
   IF ACCOUNTING_DATE = MTH_LAST_DAY THEN 
   		DELETE FROM SMY.OU_INR_AC_BAL_MTHLY_SMY WHERE ACG_DT = ACCOUNTING_DATE ;--
   		COMMIT;--
   	END IF;--

  SET SMY_STEPNUM = 2;
  SET SMY_STEPDESC = '声明用户临时表,存放昨日SMY数据';
  
DECLARE GLOBAL TEMPORARY TABLE PRE AS (
  SELECT 
      ACG_OU_IP_ID,                                          
      ACG_SBJ_ID,
      BAL_ACG_EFF_TP_ID,
      CCY,
      NOD_IN_MTH,
      NOD_IN_QTR,
      NOD_IN_YEAR,
      BAL_AMT,
      NBR_AC,
      CUR_CR_AMT,
      CUR_DB_AMT,
      MTD_ACML_BAL_AMT,
      QTD_ACML_BAL_AMT,
      YTD_ACML_BAL_AMT,
      TOT_MTD_DB_AMT,
      TOT_MTD_CR_AMT,
      TOT_QTD_CR_AMT,
      TOT_QTD_DB_AMT,
      TOT_YTD_DB_AMT,
      TOT_YTD_CR_AMT
  FROM SMY.OU_INR_AC_BAL_DLY_SMY
)DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(ACG_OU_IP_ID);
  
CREATE INDEX SESSION.PRE_IDX ON SESSION.PRE(ACG_OU_IP_ID,ACG_SBJ_ID,BAL_ACG_EFF_TP_ID,CCY);
  
/*如果是年第一日不需要插入*/
IF YR_FIRST_DAY <>  ACCOUNTING_DATE THEN 
  IF ACCOUNTING_DATE = QTR_FIRST_DAY THEN   --季初
      INSERT INTO SESSION.PRE
      (
			    ACG_OU_IP_ID,     
          ACG_SBJ_ID,       
          BAL_ACG_EFF_TP_ID,
          CCY,              
          NOD_IN_MTH,       
          NOD_IN_QTR,       
          NOD_IN_YEAR,      
          BAL_AMT,          
          NBR_AC,           
          CUR_CR_AMT,       
          CUR_DB_AMT,       
          MTD_ACML_BAL_AMT, 
          QTD_ACML_BAL_AMT, 
          YTD_ACML_BAL_AMT, 
          TOT_MTD_DB_AMT,   
          TOT_MTD_CR_AMT,   
          TOT_QTD_CR_AMT,   
          TOT_QTD_DB_AMT,   
          TOT_YTD_DB_AMT,   
          TOT_YTD_CR_AMT    
      )
      SELECT 
 			    ACG_OU_IP_ID,     
          ACG_SBJ_ID,       
          BAL_ACG_EFF_TP_ID,
          CCY,              
          0,                  --当月有效天数
          0,                  --当季有效天数
          NOD_IN_YEAR,        --当年有效天数
          BAL_AMT,          
          NBR_AC,           
          CUR_CR_AMT,       
          CUR_DB_AMT,       
          0,                  --月累计余额
          0,                  --季累计余额
          YTD_ACML_BAL_AMT,   --年累计余额
          0,                  --月累计借方发生额
          0,                  --月累计贷方发生额
          0,                  --季累计贷方发生额
          0,                  --季累计借方发生额
          TOT_YTD_DB_AMT,     --年累计借方发生额
          TOT_YTD_CR_AMT      --年累计贷方发生额
      FROM SMY.OU_INR_AC_BAL_DLY_SMY WHERE ACG_DT = LAST_SMY_DATE ;--
  ELSEIF ACCOUNTING_DATE=MTH_FIRST_DAY THEN   --月初
      INSERT INTO SESSION.PRE 
      (
			    ACG_OU_IP_ID,     
          ACG_SBJ_ID,       
          BAL_ACG_EFF_TP_ID,
          CCY,              
          NOD_IN_MTH,       
          NOD_IN_QTR,       
          NOD_IN_YEAR,      
          BAL_AMT,          
          NBR_AC,           
          CUR_CR_AMT,       
          CUR_DB_AMT,       
          MTD_ACML_BAL_AMT, 
          QTD_ACML_BAL_AMT, 
          YTD_ACML_BAL_AMT, 
          TOT_MTD_DB_AMT,   
          TOT_MTD_CR_AMT,   
          TOT_QTD_CR_AMT,   
          TOT_QTD_DB_AMT,   
          TOT_YTD_DB_AMT,   
          TOT_YTD_CR_AMT    
      )
      SELECT 
 			    ACG_OU_IP_ID,     
          ACG_SBJ_ID,       
          BAL_ACG_EFF_TP_ID,
          CCY,              
          0,                  --当月有效天数
          NOD_IN_QTR,         --当季有效天数
          NOD_IN_YEAR,        --当年有效天数
          BAL_AMT,          
          NBR_AC,           
          CUR_CR_AMT,       
          CUR_DB_AMT,       
          0,                  --月累计余额
          QTD_ACML_BAL_AMT,   --季累计余额
          YTD_ACML_BAL_AMT,   --年累计余额
          0,                  --月累计借方发生额
          0,                  --月累计贷方发生额
          TOT_QTD_CR_AMT,     --季累计贷方发生额
          TOT_QTD_DB_AMT,     --季累计借方发生额
          TOT_YTD_DB_AMT,     --年累计借方发生额
          TOT_YTD_CR_AMT      --年累计贷方发生额
      FROM SMY.OU_INR_AC_BAL_DLY_SMY WHERE ACG_DT = LAST_SMY_DATE ;--
  ELSE
      INSERT INTO SESSION.PRE 
      (
			    ACG_OU_IP_ID,     
          ACG_SBJ_ID,       
          BAL_ACG_EFF_TP_ID,
          CCY,              
          NOD_IN_MTH,       
          NOD_IN_QTR,       
          NOD_IN_YEAR,      
          BAL_AMT,          
          NBR_AC,           
          CUR_CR_AMT,       
          CUR_DB_AMT,       
          MTD_ACML_BAL_AMT, 
          QTD_ACML_BAL_AMT, 
          YTD_ACML_BAL_AMT, 
          TOT_MTD_DB_AMT,   
          TOT_MTD_CR_AMT,   
          TOT_QTD_CR_AMT,   
          TOT_QTD_DB_AMT,   
          TOT_YTD_DB_AMT,   
          TOT_YTD_CR_AMT    
      ) 
      SELECT 
 			    ACG_OU_IP_ID,     
          ACG_SBJ_ID,       
          BAL_ACG_EFF_TP_ID,
          CCY,              
          NOD_IN_MTH,         --当月有效天数
          NOD_IN_QTR,         --当季有效天数
          NOD_IN_YEAR,        --当年有效天数
          BAL_AMT,          
          NBR_AC,           
          CUR_CR_AMT,       
          CUR_DB_AMT,       
          MTD_ACML_BAL_AMT,   --月累计余额
          QTD_ACML_BAL_AMT,   --季累计余额
          YTD_ACML_BAL_AMT,   --年累计余额
          TOT_MTD_DB_AMT,     --月累计借方发生额
          TOT_MTD_CR_AMT,     --月累计贷方发生额
          TOT_QTD_CR_AMT,     --季累计贷方发生额
          TOT_QTD_DB_AMT,     --季累计借方发生额
          TOT_YTD_DB_AMT,     --年累计借方发生额
          TOT_YTD_CR_AMT      --年累计贷方发生额
      FROM SMY.OU_INR_AC_BAL_DLY_SMY WHERE ACG_DT = LAST_SMY_DATE ;--
  END IF;
END IF;

  /** 收集操作信息 */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	           --
	
  SET SMY_STEPNUM = 3;
  SET SMY_STEPDESC = '声明用户临时表,存放当日数据';
  
DECLARE GLOBAL TEMPORARY TABLE CUR AS (
  SELECT
      RPRG_OU_IP_ID AS ACG_OU_IP_ID,                         --核算机构        
      INR.ACG_SBJ_ID AS ACG_SBJ_ID,                          --科目核算码      
      BAL_ACG_EFF_TP_ID AS BAL_ACG_EFF_TP_ID,                --余额方向        
      CCY AS CCY,                                            --币种            
      '1899-12-31' AS ACG_DT,                                --日期            
      12 AS CDR_YR,                                          --年份            
      31 AS CDR_MTH,                                         --月份            
      COALESCE(MAP.NEW_ACG_SBJ_ID,'-1') AS NEW_ACG_SBJ_ID,   --新科目          
      1 AS NOD_IN_MTH,                                       --当月有效天数    
      1 AS NOD_IN_QTR,                                       --当季有效天数    
      1 AS NOD_IN_YEAR,                                      --当年有效天数         
      SUM(BAL_AMT) AS BAL_AMT,                               --余额            
      COUNT(DISTINCT AC_AR_ID) AS NBR_AC,                    --账户数          
      SUM(CUR_DAY_CR_AMT) AS CUR_CR_AMT,                     --贷方发生额      
      SUM(CUR_DAY_DB_AMT) AS CUR_DB_AMT                      --借方发生额      
  FROM SMY.MTHLY_INR_AC_ACML_BAL_AMT INR
  LEFT JOIN SOR.ACG_SBJ_CODE_MAPPING MAP ON INR.ACG_SBJ_ID = MAP.ACG_SBJ_ID AND MAP.END_DT = '9999-12-31'
  GROUP BY 
      RPRG_OU_IP_ID,                      --核算机构
      INR.ACG_SBJ_ID,                     --科目核算码
      BAL_ACG_EFF_TP_ID,                  --余额方向
      CCY,                                --币种
      COALESCE(MAP.NEW_ACG_SBJ_ID,'-1')   --新科目
) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(ACG_OU_IP_ID);
  
CREATE INDEX SESSION.CUR_IDX ON SESSION.CUR(ACG_OU_IP_ID,ACG_SBJ_ID,BAL_ACG_EFF_TP_ID,CCY);

INSERT INTO SESSION.CUR
SELECT
    RPRG_OU_IP_ID AS ACG_OU_IP_ID,                         --核算机构        
    INR.ACG_SBJ_ID AS ACG_SBJ_ID,                          --科目核算码      
    BAL_ACG_EFF_TP_ID AS BAL_ACG_EFF_TP_ID,                --余额方向        
    CCY AS CCY,                                            --币种            
    ACCOUNTING_DATE AS ACG_DT,                             --日期            
    CUR_YEAR AS CDR_YR,                                    --年份            
    CUR_MONTH AS CDR_MTH,                                  --月份            
    COALESCE(MAP.NEW_ACG_SBJ_ID,'-1') AS NEW_ACG_SBJ_ID,   --新科目          
    1 AS NOD_IN_MTH,                                       --当月有效天数    
    1 AS NOD_IN_QTR,                                       --当季有效天数    
    1 AS NOD_IN_YEAR,                                      --当年有效天数         
    SUM(BAL_AMT) AS BAL_AMT,                               --余额            
    COUNT(DISTINCT AC_AR_ID) AS NBR_AC,                    --账户数          
    SUM(CUR_DAY_CR_AMT) AS CUR_CR_AMT,                     --贷方发生额      
    SUM(CUR_DAY_DB_AMT) AS CUR_DB_AMT                      --借方发生额        
FROM SMY.MTHLY_INR_AC_ACML_BAL_AMT INR
LEFT JOIN SOR.ACG_SBJ_CODE_MAPPING MAP ON INR.ACG_SBJ_ID = MAP.ACG_SBJ_ID AND MAP.END_DT = '9999-12-31'
WHERE ACG_DT = ACCOUNTING_DATE
GROUP BY 
    RPRG_OU_IP_ID,                      --核算机构
    INR.ACG_SBJ_ID,                     --科目核算码
    BAL_ACG_EFF_TP_ID,                  --余额方向
    CCY,                                --币种
    COALESCE(MAP.NEW_ACG_SBJ_ID,'-1')   --新科目
;

  /** 收集操作信息 */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	           --
	
  SET SMY_STEPNUM = 4;
  SET SMY_STEPDESC = '将今日数据和昨日数据关联后插入SMY.OU_INR_AC_BAL_DLY_SMY';
  
  INSERT INTO SMY.OU_INR_AC_BAL_DLY_SMY
  (
       ACG_OU_IP_ID        --核算机构
      ,ACG_SBJ_ID          --科目核算码
      ,BAL_ACG_EFF_TP_ID   --余额方向
      ,CCY                 --币种
      ,ACG_DT              --日期
      ,CDR_YR              --年份
      ,CDR_MTH             --月份
      ,NEW_ACG_SBJ_ID      --新科目
      ,NOD_IN_MTH          --当月有效天数
      ,NOD_IN_QTR          --当季有效天数
      ,NOD_IN_YEAR         --当年有效天数
      ,BAL_AMT             --余额
      ,NBR_AC              --账户数
      ,CUR_CR_AMT          --贷方发生额
      ,CUR_DB_AMT          --借方发生额
      ,MTD_ACML_BAL_AMT    --月累计余额
      ,QTD_ACML_BAL_AMT    --季累计余额
      ,YTD_ACML_BAL_AMT    --年累计余额
      ,TOT_MTD_DB_AMT      --月累计借方发生额
      ,TOT_MTD_CR_AMT      --月累计贷方发生额
      ,TOT_QTD_CR_AMT      --季累计贷方发生额
      ,TOT_QTD_DB_AMT      --季累计借方发生额
      ,TOT_YTD_DB_AMT      --年累计借方发生额
      ,TOT_YTD_CR_AMT      --年累计贷方发生额
      ,NOCLD_IN_MTH        --当月日历天数
      ,NOCLD_IN_QTR        --当季日历天数
      ,NOCLD_IN_YEAR       --当年日历天数
  )
  SELECT 
       CUR.ACG_OU_IP_ID                                   --核算机构
      ,CUR.ACG_SBJ_ID                                     --科目核算码
      ,CUR.BAL_ACG_EFF_TP_ID                              --余额方向
      ,CUR.CCY                                            --币种
      ,CUR.ACG_DT                                         --日期
      ,CUR.CDR_YR                                         --年份
      ,CUR.CDR_MTH                                        --月份
      ,CUR.NEW_ACG_SBJ_ID                                 --新科目
      ,COALESCE(PRE.NOD_IN_MTH ,0) + CUR.NOD_IN_MTH       --当月有效天数
      ,COALESCE(PRE.NOD_IN_QTR ,0) + CUR.NOD_IN_QTR       --当季有效天数
      ,COALESCE(PRE.NOD_IN_YEAR,0) + CUR.NOD_IN_YEAR      --当年有效天数
      ,CUR.BAL_AMT                                        --余额
      ,CUR.NBR_AC                                         --账户数
      ,CUR.CUR_CR_AMT                                     --贷方发生额
      ,CUR.CUR_DB_AMT                                     --借方发生额
      ,COALESCE(PRE.MTD_ACML_BAL_AMT ,0) + CUR.BAL_AMT    --月累计余额
      ,COALESCE(PRE.QTD_ACML_BAL_AMT ,0) + CUR.BAL_AMT    --季累计余额
      ,COALESCE(PRE.YTD_ACML_BAL_AMT ,0) + CUR.BAL_AMT    --年累计余额
      ,COALESCE(PRE.TOT_MTD_DB_AMT ,0) + CUR.CUR_DB_AMT   --月累计借方发生额
      ,COALESCE(PRE.TOT_MTD_CR_AMT ,0) + CUR.CUR_CR_AMT   --月累计贷方发生额
      ,COALESCE(PRE.TOT_QTD_CR_AMT ,0) + CUR.CUR_CR_AMT   --季累计贷方发生额
      ,COALESCE(PRE.TOT_QTD_DB_AMT ,0) + CUR.CUR_DB_AMT   --季累计借方发生额
      ,COALESCE(PRE.TOT_YTD_DB_AMT ,0) + CUR.CUR_DB_AMT   --年累计借方发生额
      ,COALESCE(PRE.TOT_YTD_CR_AMT ,0) + CUR.CUR_CR_AMT   --年累计贷方发生额
      ,CUR_DAY                                            --当月日历天数
      ,QTR_DAY                                            --当季日历天数
      ,YR_DAY                                             --当年日历天数
  FROM SESSION.CUR AS CUR LEFT OUTER JOIN SESSION.PRE AS PRE
      ON CUR.ACG_OU_IP_ID         = PRE.ACG_OU_IP_ID   
		  AND CUR.ACG_SBJ_ID        = PRE.ACG_SBJ_ID     
		  AND CUR.BAL_ACG_EFF_TP_ID = PRE.BAL_ACG_EFF_TP_ID
		  AND CUR.CCY               = PRE.CCY
;

  /** 收集操作信息 */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	           --
	
	/*如果是年第一日不需要回插*/
	IF YR_FIRST_DAY <>  ACCOUNTING_DATE THEN 
    SET SMY_STEPNUM = 5;
    SET SMY_STEPDESC = '回插机构拆并的数据';
    
    INSERT INTO SMY.OU_INR_AC_BAL_DLY_SMY
    (
         ACG_OU_IP_ID        --核算机构
        ,ACG_SBJ_ID          --科目核算码
        ,BAL_ACG_EFF_TP_ID   --余额方向
        ,CCY                 --币种
        ,ACG_DT              --日期
        ,CDR_YR              --年份
        ,CDR_MTH             --月份
        ,NEW_ACG_SBJ_ID      --新科目
        ,NOD_IN_MTH          --当月有效天数
        ,NOD_IN_QTR          --当季有效天数
        ,NOD_IN_YEAR         --当年有效天数
        ,BAL_AMT             --余额
        ,NBR_AC              --账户数
        ,CUR_CR_AMT          --贷方发生额
        ,CUR_DB_AMT          --借方发生额
        ,MTD_ACML_BAL_AMT    --月累计余额
        ,QTD_ACML_BAL_AMT    --季累计余额
        ,YTD_ACML_BAL_AMT    --年累计余额
        ,TOT_MTD_DB_AMT      --月累计借方发生额
        ,TOT_MTD_CR_AMT      --月累计贷方发生额
        ,TOT_QTD_CR_AMT      --季累计贷方发生额
        ,TOT_QTD_DB_AMT      --季累计借方发生额
        ,TOT_YTD_DB_AMT      --年累计借方发生额
        ,TOT_YTD_CR_AMT      --年累计贷方发生额
        ,NOCLD_IN_MTH        --当月日历天数
        ,NOCLD_IN_QTR        --当季日历天数
        ,NOCLD_IN_YEAR       --当年日历天数
    )
    SELECT 
         ACG_OU_IP_ID        --核算机构
        ,ACG_SBJ_ID          --科目核算码
        ,BAL_ACG_EFF_TP_ID   --余额方向
        ,CCY                 --币种
        ,ACCOUNTING_DATE     --日期
        ,CUR_YEAR            --年份
        ,CUR_MONTH           --月份
        ,NEW_ACG_SBJ_ID      --新科目
        ,NOD_IN_MTH          --当月有效天数
        ,NOD_IN_QTR          --当季有效天数
        ,NOD_IN_YEAR         --当年有效天数
        ,0                   --余额
        ,0                   --账户数
        ,0                   --贷方发生额
        ,0                   --借方发生额
        ,case when CUR_DAY=1 then 0 else MTD_ACML_BAL_AMT end   --月累计余额
        ,case when CUR_DAY=1 and CUR_MONTH in (4,7,10) then 0 else QTD_ACML_BAL_AMT end   --季累计余额
        ,YTD_ACML_BAL_AMT    --年累计余额
        ,case when CUR_DAY=1 then 0 else TOT_MTD_DB_AMT end     --月累计借方发生额
        ,case when CUR_DAY=1 then 0 else TOT_MTD_CR_AMT end     --月累计贷方发生额
        ,case when CUR_DAY=1 and CUR_MONTH in (4,7,10) then 0 else TOT_QTD_CR_AMT end     --季累计贷方发生额
        ,case when CUR_DAY=1 and CUR_MONTH in (4,7,10) then 0 else TOT_QTD_DB_AMT end     --季累计借方发生额
        ,TOT_YTD_DB_AMT      --年累计借方发生额
        ,TOT_YTD_CR_AMT      --年累计贷方发生额
        ,NOCLD_IN_MTH        --当月日历天数
        ,NOCLD_IN_QTR        --当季日历天数
        ,NOCLD_IN_YEAR       --当年日历天数
    FROM SMY.OU_INR_AC_BAL_DLY_SMY PRE
    WHERE ACG_DT = LAST_SMY_DATE
      AND NOT EXISTS(
        SELECT 1 FROM SMY.OU_INR_AC_BAL_DLY_SMY CUR
        WHERE CUR.ACG_OU_IP_ID=PRE.ACG_OU_IP_ID
          AND CUR.ACG_SBJ_ID=PRE.ACG_SBJ_ID
          AND CUR.BAL_ACG_EFF_TP_ID=PRE.BAL_ACG_EFF_TP_ID
          AND CUR.CCY=PRE.CCY
          AND CUR.ACG_DT=ACCOUNTING_DATE)
    ;
    
    /** 收集操作信息 */
    GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
    INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS) 
      VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	 --
	END IF;
	

/*月表的插入*/
   IF ACCOUNTING_DATE = MTH_LAST_DAY THEN 
  		SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
		  SET SMY_STEPDESC = '本账务日期为该月最后一天,往月表SMY.OU_INR_AC_BAL_MTHLY_SMY 中插入数据';   	--
	INSERT INTO SMY.OU_INR_AC_BAL_MTHLY_SMY
 (
          ACG_OU_IP_ID        --核算机构
         ,ACG_SBJ_ID          --科目核算码
         ,BAL_ACG_EFF_TP_ID   --余额方向
         ,CCY                 --币种
         ,ACG_DT              --日期
         ,CDR_YR              --年份
         ,CDR_MTH             --月份
         ,NEW_ACG_SBJ_ID      --新科目
         ,NOD_IN_MTH          --当月有效天数
         ,NOD_IN_QTR          --当季有效天数
         ,NOD_IN_YEAR         --当年有效天数
         ,BAL_AMT             --余额
         ,NBR_AC              --账户数
         ,CUR_CR_AMT          --贷方发生额
         ,CUR_DB_AMT          --借方发生额
         ,MTD_ACML_BAL_AMT    --月累计余额
         ,QTD_ACML_BAL_AMT    --季累计余额
         ,YTD_ACML_BAL_AMT    --年累计余额
         ,TOT_MTD_DB_AMT      --月累计借方发生额
         ,TOT_MTD_CR_AMT      --月累计贷方发生额
         ,TOT_QTD_CR_AMT      --季累计贷方发生额
         ,TOT_QTD_DB_AMT      --季累计借方发生额
         ,TOT_YTD_DB_AMT      --年累计借方发生额
         ,TOT_YTD_CR_AMT      --年累计贷方发生额
------------------------------------Start on 2010-08-10 -------------------------------------------------
         ,NOCLD_IN_MTH        --当月日历天数
         ,NOCLD_IN_QTR        --当季日历天数
         ,NOCLD_IN_YEAR       --当年日历天数
------------------------------------End on 2010-08-10 ---------------------------------------------------

            )  
            
SELECT
          ACG_OU_IP_ID        --核算机构
         ,ACG_SBJ_ID          --科目核算码
         ,BAL_ACG_EFF_TP_ID   --余额方向
         ,CCY                 --币种
         ,ACG_DT              --日期
         ,CDR_YR              --年份
         ,CDR_MTH             --月份
         ,NEW_ACG_SBJ_ID      --新科目
         ,NOD_IN_MTH          --当月有效天数
         ,NOD_IN_QTR          --当季有效天数
         ,NOD_IN_YEAR         --当年有效天数
         ,BAL_AMT             --余额
         ,NBR_AC              --账户数
         ,CUR_CR_AMT          --贷方发生额
         ,CUR_DB_AMT          --借方发生额
         ,MTD_ACML_BAL_AMT    --月累计余额
         ,QTD_ACML_BAL_AMT    --季累计余额
         ,YTD_ACML_BAL_AMT    --年累计余额
         ,TOT_MTD_DB_AMT      --月累计借方发生额
         ,TOT_MTD_CR_AMT      --月累计贷方发生额
         ,TOT_QTD_CR_AMT      --季累计贷方发生额
         ,TOT_QTD_DB_AMT      --季累计借方发生额
         ,TOT_YTD_DB_AMT      --年累计借方发生额
         ,TOT_YTD_CR_AMT      --年累计贷方发生额
------------------------------------Start on 2010-08-10 -------------------------------------------------
         ,NOCLD_IN_MTH        --当月日历天数
         ,NOCLD_IN_QTR        --当季日历天数
         ,NOCLD_IN_YEAR       --当年日历天数
------------------------------------End on 2010-08-10 ---------------------------------------------------

    -- FROM SMY.OU_INR_AC_BAL_DLY_SMY WHERE  ACG_DT = LAST_SMY_DATE ; --deleted by Feng Jia Qiang 2010-08-24
  FROM SMY.OU_INR_AC_BAL_DLY_SMY WHERE  ACG_DT = MTH_LAST_DAY ; --added by Feng Jia Qiang 2010-08-24
   
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	        		  --
		  
	 END IF;--


	 COMMIT;--
END@