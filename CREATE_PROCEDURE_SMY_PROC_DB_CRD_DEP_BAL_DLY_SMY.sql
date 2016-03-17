CREATE PROCEDURE SMY.PROC_DB_CRD_DEP_BAL_DLY_SMY(IN ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_DB_CRD_DEP_BAL_DLY_SMY.sql
-- Procedure name: 			SMY.PROC_DB_CRD_DEP_BAL_DLY_SMY
-- Source Table:				SOR.DB_CRD , SOR.DMD_DEP_SUB_AR,SOR.CRD
-- Target Table: 				SMY.DB_CRD_DEP_BAL_DLY_SMY
--                      SMY.DB_CRD_DEP_BAL_MTHLY_SMY
-- Project     :        ZJ RCCB EDW
-- NOTES       :        
-- Purpose     :            
-- PROCESS METHOD      :  INSERT ONLY EACH DAY
--=============================================================================
-- Creation Date:       2009.11.10
-- Origin Author:       JAMES SHANG IBM copyright 
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2009-11-10   JAMES SHANG     Create SP File
-- 2009-11-24   JAMES SHANG     添加往月表插入数据的处理
-- 2009-12-03   Xu Yan          Used the SMY table improve the performance
-- 2009-12-17   Xu Yan          Updated the WHERE condition to filter in only normal account
-- 2010-01-19   Xu Yan          Updated the accumualted value getting logic
-- 2010-01-21   Xu Yan          Dealed with the cards which have duplicated accounts.
-- 2010-02-01   Xu Yan          Removed the inactive cards and accumulated amount
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
		DECLARE CUR_YEAR SMALLINT;
		DECLARE CUR_MONTH SMALLINT;
		DECLARE CUR_DAY INTEGER;
		DECLARE YR_FIRST_DAY DATE;
		DECLARE QTR_FIRST_DAY DATE;
		DECLARE YR_DAY SMALLINT;
		DECLARE QTR_DAY SMALLINT;
		DECLARE MAX_ACG_DT DATE;
		DECLARE LAST_SMY_DATE DATE;
		DECLARE MTH_FIRST_DAY DATE;
		DECLARE V_T SMALLINT;
    DECLARE C_YR_DAY SMALLINT;
		DECLARE C_QTR_DAY SMALLINT;
		DECLARE QTR_LAST_DAY DATE;
		DECLARE C_MON_DAY SMALLINT;
		DECLARE CUR_QTR SMALLINT;
		-- 账务日期月的最后一天
		DECLARE MTH_LAST_DAY DATE; 

/*
	1.定义针对SQL异常情况的句柄(EXIT方式).
  2.将出现SQL异常时在存储过程中的位置(SMY_STEPNUM),位置描述(SMY_STEPDESC),SQLCODE(SMY_SQLCODE)记入表SMY_LOG中作调试用.
  3.调用RESIGNAL重新引发异常,跳出存储过程执行体,对引发SQL异常之前存储过程体中所完成的操作进行回滚.
*/

		DECLARE CONTINUE HANDLER FOR NOT FOUND
		  SET V_T=0 ; 
		    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
    	SET SMY_SQLCODE = SQLCODE;
      ROLLBACK;
      set SMY_STEPNUM = SMY_STEPNUM +1;
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);
      COMMIT;
      RESIGNAL;
    END;
    
    DECLARE CONTINUE HANDLER FOR SQLWARNING
    BEGIN
    	SET SMY_STEPNUM = SMY_STEPNUM + 1;
      SET SMY_SQLCODE = SQLCODE;
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, 'SQL 存在Warning 信息', SMY_SQLCODE, NULL, CURRENT TIMESTAMP);
      COMMIT;
    END;

   /*变量赋值*/
    SET SMY_PROCNM  ='PROC_DB_CRD_DEP_BAL_DLY_SMY';
    SET SMY_DATE    =ACCOUNTING_DATE;    
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --取当前年份
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --取当前月份
    SET CUR_DAY     =DAY(ACCOUNTING_DATE);     --取月第几日
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- 取年初日
    SET C_YR_DAY      =DAYOFYEAR(ACCOUNTING_DATE); --取当年第几日
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --当前季度
    SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,ISO),1,7)||'-01'); --取当月初日
    
    VALUES(ACCOUNTING_DATE - 1 DAY) INTO LAST_SMY_DATE;
    VALUES(MTH_FIRST_DAY + 1 MONTH - 1 DAY) INTO MTH_LAST_DAY ;
    --计算月日历天数
    SET C_MON_DAY = CUR_DAY;    
    
    --计算季度日历天数
    IF CUR_QTR = 1  
       THEN 
       	SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');
       	SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-04-01') - 1 DAY ;
    ELSEIF CUR_QTR = 2
       THEN 
       	SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-04-01');
       	SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-07-01') - 1 DAY ;       	
    ELSEIF CUR_QTR = 3
       THEN 
       	SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-07-01');
       	SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-10-01') - 1 DAY ;       	
    ELSE
       SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-10-01');
       SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-12-31');       
    END IF;

  /*取当季日历天数*/ 
  	SET C_QTR_DAY = DAYS(SMY_DATE) - DAYS(QTR_FIRST_DAY) + 1 ;
		
		SELECT COALESCE(MAX(ACG_DT),'1900-01-01') INTO MAX_ACG_DT FROM SMY.DB_CRD_DEP_BAL_DLY_SMY;

/*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE,并插入新的起始标志*/
		DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;
			COMMIT;
		
		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
		
		SET SMY_STEPDESC = 	'存储过程开始运行' ;
		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
				VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);		 

/*数据恢复与备份*/
    IF MAX_ACG_DT = ACCOUNTING_DATE THEN
       DELETE FROM SMY.DB_CRD_DEP_BAL_DLY_SMY WHERE ACG_DT = ACCOUNTING_DATE ;
       COMMIT;
    END IF;

/*月表的恢复*/
   IF ACCOUNTING_DATE = MTH_LAST_DAY THEN 
   		DELETE FROM SMY.DB_CRD_DEP_BAL_MTHLY_SMY WHERE ACG_DT = ACCOUNTING_DATE ;
   		COMMIT;
   	END IF;

SET SMY_STEPDESC = '声明用户临时表,存放DB_CRD_SMY数据';

	/*声明用户临时表*/
	------------------------Start on 20100121--------------------------------------------------------------
	DECLARE GLOBAL TEMPORARY TABLE TMP_DB_CRD
		LIKE SMY.DB_CRD_SMY
	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(CRD_NO);
 
 create index session.IDX_AC_AR on session.TMP_DB_CRD(AC_AR_ID);
 
 insert into SESSION.TMP_DB_CRD
    select * from SMY.DB_CRD_SMY where CRD_LCS_TP_ID = 11920001   --正常	
    ;
 --------------Start on 20100201--------------------
 /*
 insert into SESSION.TMP_DB_CRD 
    select * from SMY.DB_CRD_SMY S
			 where not exists (
			     select 1 
			     from SESSION.TMP_DB_CRD T
			     where  
			       T.AC_AR_ID = S.AC_AR_ID
			 );
	*/		 
 --------------End on 20100201--------------------
 ------------------------End on 20100121--------------------------------------------------------------
      
 /** 收集操作信息 */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
 	set SMY_STEPNUM = SMY_STEPNUM +1;
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	           


		SET SMY_STEPDESC = '往表SMY.DB_CRD_DEP_BAL_DLY_SMY 中插入会计日期为当天的数据';
		

	INSERT INTO SMY.DB_CRD_DEP_BAL_DLY_SMY
 (
     OU_ID                  --受理机构号
    ,CRD_TP_ID              --卡类型
    ,PSBK_RLTD_F            --卡折相关标识
    ,IS_NONGXIN_CRD_F       --丰收卡/农信卡标识
    ,ENT_IDV_IND            --卡对象
    ,CCY                    --币种
    ,ACG_DT                 --日期YYYY-MM-DD
    ,CDR_YR                 --年份YYYY
    ,CDR_MTH                --月份MM
    ,NOD_In_MTH           --当月天数
    ,NOD_In_QTR           --当季日历日天数
    ,NOD_In_Year          --当年日历日天数
    ,DEP_BAL_CRD            --银行卡存款余额
    ,MTD_ACML_BAL_AMT       --累计余额
    ,QTD_ACML_DEP_BAL_AMT   --累计存款余额
    ,YTD_ACML_DEP_BAL_AMT   --累计存款余额	
            )
	WITH TMP_CUR AS 
	( 
		SELECT 
		     OU_ID     AS OU_ID       
        ,DB_CRD_TP_ID            AS CRD_TP_ID  
        ,PSBK_RLTD_F       AS PSBK_RLTD_F
        ,IS_NONGXIN_CRD_F
        ,ENT_IDV_IND     AS ENT_IDV_IND     
        ,DB_CRD.CCY      AS CCY             
        ,ACCOUNTING_DATE         AS ACG_DT          
        ,CUR_YEAR                AS CDR_YR          
        ,CUR_MONTH               AS CDR_MTH         
        ,C_MON_DAY               AS NOD_In_MTH    
        ,C_QTR_DAY               AS NOD_In_QTR    
        ,C_YR_DAY               AS NOD_In_Year   
        ,SUM(COALESCE(AC.BAL_AMT,0))  AS DEP_BAL_CRD  
        ,SUM(COALESCE(AC.MTD_ACML_BAL_AMT, 0)) AS MTD_ACML_BAL_AMT
    		,SUM(COALESCE(AC.QTD_ACML_BAL_AMT,0))  AS QTD_ACML_DEP_BAL_AMT --累计存款余额
    		,SUM(COALESCE(AC.YTD_ACML_BAL_AMT,0))  AS YTD_ACML_DEP_BAL_AMT --累计存款余额	                  
		FROM SESSION.TMP_DB_CRD  AS DB_CRD 	
		     LEFT JOIN SMY.MTHLY_DMD_DEP_ACML_BAL_AMT AS AC
		     ON DB_CRD.AC_AR_ID = AC.AC_AR_ID
		        AND
		        DB_CRD.CCY = AC.CCY
		        AND
		        AC.CDR_YR = CUR_YEAR
		        AND
		        AC.CDR_MTH = CUR_MONTH
		--where CRD_LCS_TP_ID = 11920001   --正常	
	where DB_CRD.END_DT >= YR_FIRST_DAY
	      OR
	      DB_CRD.END_DT = '1899-12-31'
		GROUP BY 
		     OU_ID       
        ,DB_CRD_TP_ID
        ,PSBK_RLTD_F
        ,IS_NONGXIN_CRD_F
        ,ENT_IDV_IND
        ,DB_CRD.CCY	
       )            
SELECT
     CUR.OU_ID                
    ,CUR.CRD_TP_ID            
    ,CUR.PSBK_RLTD_F          
    ,CUR.IS_NONGXIN_CRD_F     
    ,CUR.ENT_IDV_IND          
    ,CUR.CCY                  
    ,CUR.ACG_DT               
    ,CUR.CDR_YR               
    ,CUR.CDR_MTH              
    ,CUR.NOD_In_MTH 
    ,CUR.NOD_In_QTR 
    ,CUR.NOD_In_Year
    ,CUR.DEP_BAL_CRD
    ,MTD_ACML_BAL_AMT    
    ,QTD_ACML_DEP_BAL_AMT
    ,YTD_ACML_DEP_BAL_AMT
           
FROM  TMP_CUR AS CUR  
;
 /** 收集操作信息 */	                          
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
 	set SMY_STEPNUM = SMY_STEPNUM +1;
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	 


  IF ACCOUNTING_DATE = MTH_LAST_DAY THEN 
  	  		
		  SET SMY_STEPDESC = '本账务日期为该月最后一天,往月表SMY.DB_CRD_DEP_BAL_MTHLY_SMY 中插入数据';

	INSERT INTO SMY.DB_CRD_DEP_BAL_MTHLY_SMY
  (
     OU_ID                  --受理机构号
    ,CRD_TP_ID              --卡类型
    ,PSBK_RLTD_F            --卡折相关标识
    ,IS_NONGXIN_CRD_F       --丰收卡/农信卡标识
    ,ENT_IDV_IND            --卡对象
    ,CCY                    --币种
    ,ACG_DT                 --日期YYYY-MM-DD
    ,CDR_YR                 --年份YYYY
    ,CDR_MTH                --月份MM
    ,NOD_In_MTH           --当月天数
    ,NOD_In_QTR           --当季日历日天数
    ,NOD_In_Year          --当年日历日天数
    ,DEP_BAL_CRD            --银行卡存款余额
    ,MTD_ACML_BAL_AMT       --累计余额
    ,QTD_ACML_DEP_BAL_AMT   --累计存款余额
    ,YTD_ACML_DEP_BAL_AMT   --累计存款余额	
            )
  SELECT 
     OU_ID                  --受理机构号
    ,CRD_TP_ID              --卡类型
    ,PSBK_RLTD_F            --卡折相关标识
    ,IS_NONGXIN_CRD_F       --丰收卡/农信卡标识
    ,ENT_IDV_IND            --卡对象
    ,CCY                    --币种
    ,ACG_DT                 --日期YYYY-MM-DD
    ,CDR_YR                 --年份YYYY
    ,CDR_MTH                --月份MM
    ,NOD_In_MTH           --当月天数
    ,NOD_In_QTR           --当季日历日天数
    ,NOD_In_Year          --当年日历日天数
    ,DEP_BAL_CRD            --银行卡存款余额
    ,MTD_ACML_BAL_AMT       --累计余额
    ,QTD_ACML_DEP_BAL_AMT   --累计存款余额
    ,YTD_ACML_DEP_BAL_AMT   --累计存款余额
  FROM   SMY.DB_CRD_DEP_BAL_DLY_SMY  WHERE ACG_DT=	ACCOUNTING_DATE;            
   
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
 	set SMY_STEPNUM = SMY_STEPNUM +1;
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	  		   
  END IF;

	 COMMIT;
END@