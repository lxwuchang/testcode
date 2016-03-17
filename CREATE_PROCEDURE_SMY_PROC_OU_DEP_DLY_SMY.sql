CREATE PROCEDURE SMY.PROC_OU_DEP_DLY_SMY(IN ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_OU_DEP_DLY_SMY.sql
-- Procedure name: 			SMY.PROC_OU_DEP_DLY_SMY
-- Source Table:				SMY.DEP_AR_SMY ,SOR.CST
--                      SMY.MTHLY_FT_DEP_ACML_BAL_AMT
--                      SMY.MTHLY_DMD_DEP_ACML_BAL_AMT
--                      SMY.MTHLY_INTRBNK_DEP_ACML_BAL_AMT
-- Target Table: 				SMY.OU_DEP_DLY_SMY
--                      SMY.OU_DEP_MTHLY_SMY 
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
-- 2009-11-24   JAMES SHANG     增加月表处理		
-- 2009-11-26   Xu Yan          Updated 'NEW_ACG_SBJ_ID' column
-- 2009-12-01   Xu Yan          Updated the joint table for 'NEW_ACG_SBJ_ID'
-- 2009-12-02   Xu Yan          Updated 'NEW_ACG_SBJ_ID' due to SOR change
-- 2009-12-18   Xu Yan          Updated to filter in only normal accounts
-- 2010-01-06   Xu Yan          Included the transactions on the account closing day.
-- 2010-01-19   Xu Yan          Updated the accumualted value getting logic,which is same as the LOAN
-- 2010-02-25   Xu Yan          Fixed a bug about the accumulated amount.
-- 2010-08-10   Fang Yihua      Added three new columns 'NOCLD_IN_MTH','NOCLD_IN_QTR','NOCLD_IN_YEAR'
-- 2010-09-02   Sheng Qibin     Added two TEMPORARY TABLE ,two INDEX, TMP_MTHLY_DMD_DEP_ACML_BAL_AMT,TMP_MTHLY_FT_DEP_ACML_BAL_AMT,IDX_TMP1,IDX_TMP_MTHLY_FT_DEP_ACML_BAL_AMT
-- 2012-02-06   Zheng Bin       Updated SESSION.TMP_CUR'data filter by the current year
-- 2012-02-27   Chen XiaoWen    1.调整部分临时表的索引
--                              2.导入数据到TMP_MTHLY_DMD_DEP_ACML_BAL_AMT、TMP_MTHLY_FT_DEP_ACML_BAL_AMT临时表时，修改原查询条件，使用ACG_DT分区键查询
--                              3.插入数据到TMP_CUR_AMT时，将三个UNION ALL组装起来的四部分数据修改为四个单独的insert语句。
-- 2012-03-16   Chen XiaoWen    1.增加临时表TMP_TMP，缓存中间结果后再进行group by
-- 2012-06-07   Chen XiaoWen    修改TM_MAT_SEG_ID字段的取数规则
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
		DECLARE CUR_QTR SMALLINT; --
		-- 账务日期月的最后一天
		DECLARE MTH_LAST_DAY DATE; 		   --
    DECLARE MAT_SEG_ID_1 INT DEFAULT 0;
    DECLARE MAT_SEG_ID_2 INT DEFAULT 0;
    DECLARE CNT INT DEFAULT 0;
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
      set SMY_STEPNUM = SMY_STEPNUM + 1;--
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
      COMMIT;--
      RESIGNAL;--
    END;--
--    DECLARE CONTINUE HANDLER FOR SQLWARNING
--    BEGIN
--      SET SMY_SQLCODE = SQLCODE;--
--      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
--      COMMIT;--
--    END;--
--

   /*变量赋值*/

    SET SMY_PROCNM  ='PROC_OU_DEP_DLY_SMY';--
    SET SMY_DATE    =ACCOUNTING_DATE;    --
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --取当前年份
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --取当前月份
    SET CUR_DAY     =DAY(ACCOUNTING_DATE);     --取月第几日
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- 取年初日
    SET C_YR_DAY      =DAYOFYEAR(ACCOUNTING_DATE); --取当年第几日
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --当前季度
    SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,ISO),1,7)||'-01'); --取当月初日

    VALUES(ACCOUNTING_DATE - 1 DAY) INTO LAST_SMY_DATE;--
    VALUES(MTH_FIRST_DAY + 1 MONTH - 1 DAY) INTO MTH_LAST_DAY ;  --

    --计算月日历天数

    SET C_MON_DAY = CUR_DAY;    --

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

  	SET C_QTR_DAY = DAYS(SMY_DATE) - DAYS(QTR_FIRST_DAY) + 1 ;--
		SELECT COALESCE(MAX(ACG_DT),'1900-01-01') INTO MAX_ACG_DT FROM SMY.OU_DEP_DLY_SMY;--
		
    SELECT MAT_SEG_ID INTO MAT_SEG_ID_1 FROM SMY.MAT_SEG WHERE LOW_VAL=-99999;
    SELECT MAT_SEG_ID INTO MAT_SEG_ID_2 FROM SMY.MAT_SEG WHERE MAX_VAL=99999;
    SELECT MAX(LOW_VAL)-1 INTO CNT FROM SMY.MAT_SEG;

/*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE,并插入新的起始标志*/

		DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--

			COMMIT;--

		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		
		SET SMY_STEPDESC = 	'存储过程开始运行' ;--

		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
				VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);		 --
				
/*数据恢复与备份*/

    IF MAX_ACG_DT = ACCOUNTING_DATE THEN
       DELETE FROM SMY.OU_DEP_DLY_SMY WHERE ACG_DT = ACCOUNTING_DATE ;--
       COMMIT;--
    END IF;--

/*月表的恢复*/

   IF ACCOUNTING_DATE = MTH_LAST_DAY THEN 
   		DELETE FROM SMY.OU_DEP_MTHLY_SMY WHERE ACG_DT = ACCOUNTING_DATE ;--
   		COMMIT;--
   	END IF;--
   	
 /** 收集操作信息 */		                             

 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--

 	set SMY_STEPNUM = SMY_STEPNUM + 1;--

 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	           --

   /** 统计借、贷方发生额

     来源表

     SMY.MTHLY_FT_DEP_ACML_BAL_AMT

     SMY.MTHLY_DMD_DEP_ACML_BAL_AMT

     SMY.MTHLY_INTRBNK_DEP_ACML_BAL_AMT

    **/

		--SET SMY_STEPNUM = 3 ;--

		SET SMY_STEPDESC = '声明用户临时表,统计借、贷方发生额';--
		
------add by sheng qibin start 20100902----------------                                                                                                        
	                                                                                                                           
	DECLARE GLOBAL TEMPORARY TABLE TMP_MTHLY_DMD_DEP_ACML_BAL_AMT                                                                                         
		LIKE SMY.MTHLY_DMD_DEP_ACML_BAL_AMT                                                                                               
	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(AC_AR_ID);--
	                                                                                                                                                                                                                               
	CREATE UNIQUE INDEX SESSION.IDX_TMP1 ON SESSION.TMP_MTHLY_DMD_DEP_ACML_BAL_AMT (AC_AR_ID,CCY);
   
   DECLARE GLOBAL TEMPORARY TABLE TMP_MTHLY_FT_DEP_ACML_BAL_AMT                                                                                         
		LIKE SMY.MTHLY_FT_DEP_ACML_BAL_AMT
	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(AC_AR_ID); --
	
	CREATE UNIQUE INDEX SESSION.IDX_TMP_MTHLY_FT_DEP_ACML_BAL_AMT ON SESSION.TMP_MTHLY_FT_DEP_ACML_BAL_AMT (AC_AR_ID,CCY);

	insert into SESSION.TMP_MTHLY_DMD_DEP_ACML_BAL_AMT
	select * from SMY.MTHLY_DMD_DEP_ACML_BAL_AMT where ACG_DT>=MTH_FIRST_DAY and ACG_DT<=MTH_LAST_DAY;
	
	insert into SESSION.TMP_MTHLY_FT_DEP_ACML_BAL_AMT
	select * FROM  SMY.MTHLY_FT_DEP_ACML_BAL_AMT where ACG_DT>=MTH_FIRST_DAY and ACG_DT<=MTH_LAST_DAY;

/*	
	insert into SESSION.TMP_MTHLY_DMD_DEP_ACML_BAL_AMT
	select * from SMY.MTHLY_DMD_DEP_ACML_BAL_AMT 
	WHERE CDR_YR = CUR_YEAR
			      AND
			      CDR_MTH = CUR_MONTH ;--
 
 insert into SESSION.TMP_MTHLY_FT_DEP_ACML_BAL_AMT
 select * FROM  SMY.MTHLY_FT_DEP_ACML_BAL_AMT  --
			WHERE CDR_YR = CUR_YEAR
			      AND
			      CDR_MTH = CUR_MONTH ;--
*/
	
------add by sheng qibin end 20100902----------------    

	DECLARE GLOBAL TEMPORARY TABLE TMP_CUR_AMT  AS 	
	(
	  	SELECT 
	  			AC_AR_ID
	  		 ,CCY
	  		 ,SUM(CUR_DAY_CR_AMT) AS CUR_DAY_CR_AMT
	  		 ,SUM(CUR_DAY_DB_AMT) AS CUR_DAY_DB_AMT
	  		 ,SUM(CUR_DAY_DB_AMT) as BAL_AMT
	  		 ,SUM(CUR_DAY_DB_AMT) as MTD_ACML_BAL_AMT
	  		 ,SUM(CUR_DAY_DB_AMT) as QTD_ACML_BAL_AMT
	  		 ,SUM(CUR_DAY_DB_AMT) as YTD_ACML_BAL_AMT
	  		 ,SUM(CUR_DAY_DB_AMT) as TOT_MTD_CR_AMT
         ,SUM(CUR_DAY_DB_AMT) as TOT_MTD_DB_AMT
         ,SUM(CUR_DAY_DB_AMT) as TOT_QTD_CR_AMT
				 ,SUM(CUR_DAY_DB_AMT) as TOT_QTD_DB_AMT
				 ,SUM(CUR_DAY_DB_AMT) as TOT_YTD_CR_AMT
				 ,SUM(CUR_DAY_DB_AMT) as TOT_YTD_DB_AMT				 	  		 
			FROM  SMY.MTHLY_FT_DEP_ACML_BAL_AMT 
			GROUP BY 	
					AC_AR_ID	
				  ,CCY
	  )  DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(AC_AR_ID)
;		--

  CREATE INDEX SESSION.TMP_CUR_AMT_IDX ON SESSION.TMP_CUR_AMT (AC_AR_ID,CCY);
  
  INSERT INTO SESSION.TMP_CUR_AMT 
	  	SELECT 
	  			AC_AR_ID
	  		 ,CCY
	  		 ,SUM(COALESCE(CUR_DAY_CR_AMT,0))
	  		 ,SUM(COALESCE(CUR_DAY_DB_AMT,0))
	  		 ,SUM(COALESCE(BAL_AMT,0))
	  		 ,SUM(COALESCE(MTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(QTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(YTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(TOT_MTD_CR_AMT  ,0))
         ,SUM(COALESCE(TOT_MTD_DB_AMT  ,0))
         ,SUM(COALESCE(TOT_QTD_CR_AMT  ,0))
				 ,SUM(COALESCE(TOT_QTD_DB_AMT  ,0))
				 ,SUM(COALESCE(TOT_YTD_CR_AMT  ,0))
				 ,SUM(COALESCE(TOT_YTD_DB_AMT	,0))  
		--- delete by sheng qibin start 20100902------- 	
		--	FROM  SMY.MTHLY_FT_DEP_ACML_BAL_AMT  --
		--	WHERE CDR_YR = CUR_YEAR
		--	      AND
		--	      CDR_MTH = CUR_MONTH
		---delete by sheng qibin start 20100902-------
			--add by   sheng qibin start 20100902-------	 
			FROM  SESSION.TMP_MTHLY_FT_DEP_ACML_BAL_AMT
			--add by   sheng qibin end 20100902-------	 
			GROUP BY 	
					AC_AR_ID	
				 ,CCY;
	--UNION ALL
	INSERT INTO SESSION.TMP_CUR_AMT
	  	SELECT 
	  			AC_AR_ID
	  		 ,CCY
	  		 ,SUM(CUR_DAY_CR_AMT)
	  		 ,SUM(CUR_DAY_DB_AMT)
	  		 ,SUM(COALESCE(BAL_AMT,0))
	  		 ,SUM(COALESCE(MTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(QTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(YTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(TOT_MTD_CR_AMT  ,0))
         ,SUM(COALESCE(TOT_MTD_DB_AMT  ,0))
         ,SUM(COALESCE(TOT_QTD_CR_AMT  ,0))
				 ,SUM(COALESCE(TOT_QTD_DB_AMT  ,0))
				 ,SUM(COALESCE(TOT_YTD_CR_AMT  ,0))
				 ,SUM(COALESCE(TOT_YTD_DB_AMT	,0))
			--delete by sheng qibin start 20100902 ---- 
		 --FROM  SMY.MTHLY_DMD_DEP_ACML_BAL_AMT --
		 --	WHERE CDR_YR = CUR_YEAR
		--	      AND
		--	      CDR_MTH = CUR_MONTH  	  
		--delete by sheng qibin end 20100902 ---- 	
		--add by sheng qibin start 20100902 ---- 	 
			FROM  SESSION.TMP_MTHLY_DMD_DEP_ACML_BAL_AMT
		--add by sheng qibin end 20100902 ---- 	 
			GROUP BY 	
					AC_AR_ID	
				 ,CCY;
	--UNION ALL
	INSERT INTO SESSION.TMP_CUR_AMT
	  	SELECT 
	  			AC_AR_ID
	  		 ,CCY
	  		 ,SUM(CUR_DAY_CR_AMT)
	  		 ,SUM(CUR_DAY_DB_AMT)
	  		 ,SUM(COALESCE(BAL_AMT,0))
	  		 ,SUM(COALESCE(MTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(QTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(YTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(TOT_MTD_CR_AMT  ,0))
         ,SUM(COALESCE(TOT_MTD_DB_AMT  ,0))
         ,SUM(COALESCE(TOT_QTD_CR_AMT  ,0))
				 ,SUM(COALESCE(TOT_QTD_DB_AMT  ,0))
				 ,SUM(COALESCE(TOT_YTD_CR_AMT  ,0))
				 ,SUM(COALESCE(TOT_YTD_DB_AMT	,0))  	
			FROM  SMY.MTHLY_INTRBNK_DEP_ACML_BAL_AMT --
			WHERE CDR_YR = CUR_YEAR
			      AND
			      CDR_MTH = CUR_MONTH			
			GROUP BY 	
					AC_AR_ID
				 ,CCY;
	--UNION ALL 
	INSERT INTO SESSION.TMP_CUR_AMT
	  	SELECT 
	  			AC_AR_ID
	  		 ,CCY
	  		 ,SUM(CUR_DAY_CR_AMT)
	  		 ,SUM(CUR_DAY_DB_AMT)
	  		 ,SUM(COALESCE(BAL_AMT,0))
	  		 ,SUM(COALESCE(MTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(QTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(YTD_ACML_BAL_AMT,0))
	  		 ,SUM(COALESCE(TOT_MTD_CR_AMT  ,0))
         ,SUM(COALESCE(TOT_MTD_DB_AMT  ,0))
         ,SUM(COALESCE(TOT_QTD_CR_AMT  ,0))
				 ,SUM(COALESCE(TOT_QTD_DB_AMT  ,0))
				 ,SUM(COALESCE(TOT_YTD_CR_AMT  ,0))
				 ,SUM(COALESCE(TOT_YTD_DB_AMT	,0))  	
			FROM  SMY.MTHLY_EQTY_AC_ACML_BAL_AMT --
			WHERE CDR_YR = CUR_YEAR
			      AND
			      CDR_MTH = CUR_MONTH			
			GROUP BY 	
					AC_AR_ID
				 ,CCY	
  	;--
 /*统计信息收集*/    

 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--

 	set SMY_STEPNUM = SMY_STEPNUM + 1;--

 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	       --

		--SET SMY_STEPNUM = 4 ;--

		SET SMY_STEPDESC = '声明用户新增客户临时表，从SOR.CST 中插入生效日期为当日的数据';--

	DECLARE GLOBAL TEMPORARY TABLE TMP_NEW_CST AS 	
	(
	  	SELECT
	  		CST_ID
	  		,EFF_CST_DT
	  	FROM SOR.CST		  
	  )  DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(CST_ID)
	  ;				--
	
	CREATE INDEX SESSION.TMP_NEW_CST_IDX ON SESSION.TMP_NEW_CST (CST_ID);
	
  INSERT INTO SESSION.TMP_NEW_CST
   		SELECT
	  		CST_ID
	  	 ,EFF_CST_DT
	  	FROM SOR.CST
	  	WHERE YEAR(EFF_CST_DT)=CUR_YEAR
	 ;--

 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--

 	set SMY_STEPNUM = SMY_STEPNUM + 1;--

 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	  --

		--SET SMY_STEPNUM = 5 ;--
		SET SMY_STEPDESC = '声明用户临时表,插入当日汇总临时数据';--

  DECLARE GLOBAL TEMPORARY TABLE TMP_CUR AS 
   (
 		SELECT
			    DEP_AR_SMY.RPRG_OU_IP_ID        AS  ACG_OU_IP_ID   
         ,DEP_AR_SMY.ACG_SBJ_ID           AS  DEP_ACG_SBJ_ID 
         ,DEP_AR_SMY.PD_GRP_CODE          AS  PD_GRP_CD      
         ,DEP_AR_SMY.PD_SUB_CODE          AS  PD_SUB_CD      
         ,DEP_AR_SMY.DEP_TP_ID            AS  DEP_TM_TP_ID   
         ,DEP_AR_SMY.TM_MAT_SEG_ID        AS  TM_MAT_SEG_ID
         ,DEP_AR_SMY.ENT_IDV_IND          AS  ENT_IDV_IND    
         ,DEP_AR_SMY.DNMN_CCY_ID          AS  CCY
         ,DATE('2009-10-16')      AS ACG_DT               --日期
         ,2009      AS CDR_YR               --年份
         ,9      AS CDR_MTH              --月份
         ,1      AS NOD_IN_MTH           --月有效天数
         ,1      AS NOD_IN_QTR           --季度有效天数
         ,1      AS NOD_IN_YEAR          --年有效天数	
         ,DEP_AR_SMY.ACG_SBJ_ID   AS NEW_ACG_SBJ_ID       --新科目
         ,SUM(DEP_AR_SMY.BAL_AMT)   AS BAL_AMT                 --余额                  
         ,COUNT(DISTINCT DEP_AR_SMY.CST_ID)          AS NBR_CST            --客户数
         ,COUNT(DISTINCT DEP_AR_SMY.DEP_AR_ID)        AS NBR_AC             --账户数
         ,1  AS NBR_NEW_AC         --新增账户数        
         ,SUM(CASE WHEN TMP_NEW_CST.CST_ID  IS NULL THEN 0  ELSE 1 END)       AS NBR_NEW_CST        --新增客户数
         ,1     AS NBR_AC_CLS   --当天销户账户数 20370008: 销户
         ,SUM(COALESCE(TMP_CUR_AMT.CUR_DAY_CR_AMT ,0))     AS CUR_CR_AMT         --贷方发生额
         ,SUM(COALESCE(TMP_CUR_AMT.CUR_DAY_DB_AMT ,0))     AS CUR_DB_AMT         --借方发生额
         ,DEP_AR_SMY.AC_CHRCTR_TP_ID	AS AC_CHRCTR_TP_ID     --账户性质           
	  		 ,SUM(VALUE(MTD_ACML_BAL_AMT,0)) AS MTD_ACML_BAL_AMT
	  		 ,SUM(VALUE(QTD_ACML_BAL_AMT,0)) AS QTD_ACML_BAL_AMT
	  		 ,SUM(VALUE(YTD_ACML_BAL_AMT,0)) AS  YTD_ACML_BAL_AMT
	  		 ,SUM(VALUE(TOT_MTD_CR_AMT  ,0)) AS  TOT_MTD_CR_AMT  
         ,SUM(VALUE(TOT_MTD_DB_AMT  ,0)) AS  TOT_MTD_DB_AMT  
         ,SUM(VALUE(TOT_QTD_CR_AMT  ,0)) AS  TOT_QTD_CR_AMT  
				 ,SUM(VALUE(TOT_QTD_DB_AMT  ,0)) AS  TOT_QTD_DB_AMT  
				 ,SUM(VALUE(TOT_YTD_CR_AMT  ,0)) AS  TOT_YTD_CR_AMT  
				 ,SUM(VALUE(TOT_YTD_DB_AMT  ,0)) AS  TOT_YTD_DB_AMT 
         ,1 TOT_MTD_NBR_NEW_AC  --月新增账户数
         ,1 TOT_QTD_NBR_NEW_AC  --季新增账户数
         ,1 TOT_YTD_NBR_NEW_AC  --年新增账户数
         ,1 TOT_MTD_NBR_NEW_CST  --月新增客户数
         ,1 TOT_QTD_NBR_NEW_CST  --季新增客户数
         ,1 TOT_YTD_NBR_NEW_CST  --年新增客户数
         ,1 TOT_MTD_NBR_AC_CLS  --月累计销户账户数
         ,1 TOT_QTD_NBR_AC_CLS  --季累计销户账户数
         ,1 TOT_YTD_NBR_AC_CLS  --年累计销户账户数				     
    FROM  SMY.DEP_AR_SMY AS DEP_AR_SMY  
    LEFT OUTER JOIN SESSION.TMP_CUR_AMT  AS TMP_CUR_AMT ON DEP_AR_SMY.DEP_AR_ID = TMP_CUR_AMT.AC_AR_ID
    LEFT OUTER JOIN SESSION.TMP_NEW_CST  AS TMP_NEW_CST ON DEP_AR_SMY.CST_ID=TMP_NEW_CST.CST_ID
    GROUP BY 
			    DEP_AR_SMY.RPRG_OU_IP_ID    
         ,DEP_AR_SMY.ACG_SBJ_ID       
         ,DEP_AR_SMY.PD_GRP_CODE      
         ,DEP_AR_SMY.PD_SUB_CODE      
         ,DEP_AR_SMY.DEP_TP_ID        
         ,DEP_AR_SMY.TM_MAT_SEG_ID    
         ,DEP_AR_SMY.ENT_IDV_IND      
         ,DEP_AR_SMY.DNMN_CCY_ID 
         ,DEP_AR_SMY.AC_CHRCTR_TP_ID             
 ) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(ACG_OU_IP_ID); --

DECLARE GLOBAL TEMPORARY TABLE TMP_TMP AS 
(
    SELECT
			    DEP_AR_SMY.RPRG_OU_IP_ID        AS  ACG_OU_IP_ID   
         ,DEP_AR_SMY.ACG_SBJ_ID           AS  DEP_ACG_SBJ_ID 
         ,DEP_AR_SMY.PD_GRP_CODE          AS  PD_GRP_CD      
         ,DEP_AR_SMY.PD_SUB_CODE          AS  PD_SUB_CD      
         ,DEP_AR_SMY.DEP_TP_ID            AS  DEP_TM_TP_ID   
         ,DEP_AR_SMY.TM_MAT_SEG_ID        AS  TM_MAT_SEG_ID
         ,DEP_AR_SMY.ENT_IDV_IND          AS  ENT_IDV_IND    
         ,DEP_AR_SMY.DNMN_CCY_ID          AS  CCY
         ,VALUE(ACG_MAP.NEW_ACG_SBJ_ID,'')  AS NEW_ACG_SBJ_ID
         ,DEP_AR_SMY.BAL_AMT   AS BAL_AMT
         ,DEP_AR_SMY.CST_ID          AS NBR_CST
         ,CASE WHEN AR_LCS_TP_ID <> 20370008 then 1 else 0 end        AS NBR_AC
         ,CASE WHEN DEP_AR_SMY.EFF_DT='2012-03-13' THEN 1 ELSE 0 END   AS NBR_NEW_AC
         ,CASE WHEN TMP_NEW_CST.EFF_CST_DT = '2012-03-13' THEN 1  ELSE 0 END       AS NBR_NEW_CST
         ,case when DEP_AR_SMY.AR_LCS_TP_ID = 20370008 and DEP_AR_SMY.END_DT = '2012-03-13' then 1 else 0 end   AS NBR_AC_CLS
         ,COALESCE(TMP_CUR_AMT.CUR_DAY_CR_AMT ,0)     AS CUR_CR_AMT
         ,COALESCE(TMP_CUR_AMT.CUR_DAY_DB_AMT ,0)     AS CUR_DB_AMT
         ,DEP_AR_SMY.AC_CHRCTR_TP_ID	AS AC_CHRCTR_TP_ID
	  		 ,VALUE(MTD_ACML_BAL_AMT,0) AS MTD_ACML_BAL_AMT
	  		 ,VALUE(QTD_ACML_BAL_AMT,0) AS QTD_ACML_BAL_AMT
	  		 ,VALUE(YTD_ACML_BAL_AMT,0) AS  YTD_ACML_BAL_AMT
	  		 ,VALUE(TOT_MTD_CR_AMT  ,0) AS  TOT_MTD_CR_AMT  
         ,VALUE(TOT_MTD_DB_AMT  ,0) AS  TOT_MTD_DB_AMT  
         ,VALUE(TOT_QTD_CR_AMT  ,0) AS  TOT_QTD_CR_AMT  
				 ,VALUE(TOT_QTD_DB_AMT  ,0) AS  TOT_QTD_DB_AMT  
				 ,VALUE(TOT_YTD_CR_AMT  ,0) AS  TOT_YTD_CR_AMT  
				 ,VALUE(TOT_YTD_DB_AMT  ,0) AS  TOT_YTD_DB_AMT   
         ,CASE WHEN YEAR(DEP_AR_SMY.EFF_DT) = 2012 AND MONTH(DEP_AR_SMY.EFF_DT) = 3 THEN 1 ELSE 0 END  TOT_MTD_NBR_NEW_AC
         ,CASE WHEN YEAR(DEP_AR_SMY.EFF_DT) = 2012 AND QUARTER(DEP_AR_SMY.EFF_DT) = 1 THEN 1 ELSE 0 END  TOT_QTD_NBR_NEW_AC
         ,CASE WHEN YEAR(DEP_AR_SMY.EFF_DT) = 2012 THEN 1 ELSE 0 END  TOT_YTD_NBR_NEW_AC
         ,CASE WHEN YEAR(TMP_NEW_CST.EFF_CST_DT) = 2012 AND MONTH(TMP_NEW_CST.EFF_CST_DT) = 3 THEN 1  ELSE 0 END  TOT_MTD_NBR_NEW_CST
         ,CASE WHEN YEAR(TMP_NEW_CST.EFF_CST_DT) = 2012 AND QUARTER(TMP_NEW_CST.EFF_CST_DT) = 1 THEN 1  ELSE 0 END  TOT_QTD_NBR_NEW_CST
         ,CASE WHEN YEAR(TMP_NEW_CST.EFF_CST_DT) = 2012 THEN 1 ELSE 0 END  TOT_YTD_NBR_NEW_CST
         ,case when DEP_AR_SMY.AR_LCS_TP_ID = 20370008 and year(DEP_AR_SMY.END_DT) = 2012 and month(DEP_AR_SMY.END_DT) = 3 then 1 else 0 end TOT_MTD_NBR_AC_CLS
         ,case when DEP_AR_SMY.AR_LCS_TP_ID = 20370008 and year(DEP_AR_SMY.END_DT) = 2012 and quarter(DEP_AR_SMY.END_DT) = 1 then 1 else 0 end TOT_QTD_NBR_AC_CLS
         ,case when DEP_AR_SMY.AR_LCS_TP_ID = 20370008 and year(DEP_AR_SMY.END_DT) = 2012 then 1 else 0 end TOT_YTD_NBR_AC_CLS
    FROM  SMY.DEP_AR_SMY AS DEP_AR_SMY  
    LEFT OUTER JOIN SESSION.TMP_CUR_AMT  AS TMP_CUR_AMT ON DEP_AR_SMY.DEP_AR_ID = TMP_CUR_AMT.AC_AR_ID AND DEP_AR_SMY.DNMN_CCY_ID = TMP_CUR_AMT.CCY
    LEFT OUTER JOIN SESSION.TMP_NEW_CST  AS TMP_NEW_CST ON DEP_AR_SMY.CST_ID=TMP_NEW_CST.CST_ID
    LEFT OUTER JOIN SOR.ACG_SBJ_CODE_MAPPING AS ACG_MAP ON ACG_MAP.ACG_SBJ_ID = DEP_AR_SMY.ACG_SBJ_ID and ACG_MAP.END_DT = '9999-12-31'
) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(ACG_OU_IP_ID);

CREATE INDEX SESSION.IDX_TMP_TMP ON SESSION.TMP_TMP(ACG_OU_IP_ID,DEP_ACG_SBJ_ID,PD_GRP_CD,PD_SUB_CD,DEP_TM_TP_ID,TM_MAT_SEG_ID,ENT_IDV_IND,CCY,AC_CHRCTR_TP_ID,NEW_ACG_SBJ_ID);

INSERT INTO SESSION.TMP_TMP
    SELECT
			    DEP_AR_SMY.RPRG_OU_IP_ID        AS  ACG_OU_IP_ID   
         ,DEP_AR_SMY.ACG_SBJ_ID           AS  DEP_ACG_SBJ_ID 
         ,DEP_AR_SMY.PD_GRP_CODE          AS  PD_GRP_CD      
         ,DEP_AR_SMY.PD_SUB_CODE          AS  PD_SUB_CD      
         ,DEP_AR_SMY.DEP_TP_ID            AS  DEP_TM_TP_ID   
         
         --,DEP_AR_SMY.TM_MAT_SEG_ID        AS  TM_MAT_SEG_ID
         ,CASE WHEN DEP_AR_SMY.DEP_TP_ID=21200012 THEN 
              CASE WHEN DEP_AR_SMY.DEP_PRD_NOM = 0 OR DEP_AR_SMY.DEP_PRD_NOM IS NULL THEN
                  CASE WHEN DEP_AR_SMY.MAT_DT<=SMY_DATE THEN MAT_SEG_ID_1
                       WHEN DEP_AR_SMY.MAT_DT>=SMY_DATE+CNT DAYS THEN MAT_SEG_ID_2
                  ELSE z.MAT_SEG_ID END
              ELSE -1 END
          WHEN DEP_AR_SMY.DEP_TP_ID=21200009 THEN
              CASE WHEN DEP_AR_SMY.MAT_DT<=SMY_DATE THEN MAT_SEG_ID_1
                   WHEN DEP_AR_SMY.MAT_DT>=SMY_DATE+CNT DAYS THEN MAT_SEG_ID_2
              ELSE z.MAT_SEG_ID END
          ELSE -1 END AS TM_MAT_SEG_ID
          
         ,DEP_AR_SMY.ENT_IDV_IND          AS  ENT_IDV_IND    
         ,DEP_AR_SMY.DNMN_CCY_ID          AS  CCY
         ,VALUE(ACG_MAP.NEW_ACG_SBJ_ID,'')  AS NEW_ACG_SBJ_ID
         ,DEP_AR_SMY.BAL_AMT   AS BAL_AMT
         ,DEP_AR_SMY.CST_ID          AS NBR_CST
         ,CASE WHEN AR_LCS_TP_ID <> 20370008 then 1 else 0 end        AS NBR_AC
         ,CASE WHEN DEP_AR_SMY.EFF_DT=ACCOUNTING_DATE THEN 1 ELSE 0 END   AS NBR_NEW_AC
         ,CASE WHEN TMP_NEW_CST.EFF_CST_DT = SMY_DATE THEN 1 ELSE 0 END       AS NBR_NEW_CST
         ,case when DEP_AR_SMY.AR_LCS_TP_ID = 20370008 and DEP_AR_SMY.END_DT = SMY_DATE then 1 else 0 end   AS NBR_AC_CLS
         ,COALESCE(TMP_CUR_AMT.CUR_DAY_CR_AMT ,0)     AS CUR_CR_AMT
         ,COALESCE(TMP_CUR_AMT.CUR_DAY_DB_AMT ,0)     AS CUR_DB_AMT
         ,DEP_AR_SMY.AC_CHRCTR_TP_ID	AS AC_CHRCTR_TP_ID
	  		 ,VALUE(MTD_ACML_BAL_AMT,0) AS MTD_ACML_BAL_AMT
	  		 ,VALUE(QTD_ACML_BAL_AMT,0) AS QTD_ACML_BAL_AMT
	  		 ,VALUE(YTD_ACML_BAL_AMT,0) AS  YTD_ACML_BAL_AMT
	  		 ,VALUE(TOT_MTD_CR_AMT  ,0) AS  TOT_MTD_CR_AMT  
         ,VALUE(TOT_MTD_DB_AMT  ,0) AS  TOT_MTD_DB_AMT  
         ,VALUE(TOT_QTD_CR_AMT  ,0) AS  TOT_QTD_CR_AMT  
				 ,VALUE(TOT_QTD_DB_AMT  ,0) AS  TOT_QTD_DB_AMT  
				 ,VALUE(TOT_YTD_CR_AMT  ,0) AS  TOT_YTD_CR_AMT  
				 ,VALUE(TOT_YTD_DB_AMT  ,0) AS  TOT_YTD_DB_AMT   
         ,CASE WHEN YEAR(DEP_AR_SMY.EFF_DT) = CUR_YEAR AND MONTH(DEP_AR_SMY.EFF_DT) = CUR_MONTH THEN 1 ELSE 0 END TOT_MTD_NBR_NEW_AC
         ,CASE WHEN YEAR(DEP_AR_SMY.EFF_DT) = CUR_YEAR AND QUARTER(DEP_AR_SMY.EFF_DT) = CUR_QTR THEN 1 ELSE 0 END TOT_QTD_NBR_NEW_AC
         ,CASE WHEN YEAR(DEP_AR_SMY.EFF_DT) = CUR_YEAR THEN 1 ELSE 0 END TOT_YTD_NBR_NEW_AC
         ,CASE WHEN YEAR(TMP_NEW_CST.EFF_CST_DT) = CUR_YEAR AND MONTH(TMP_NEW_CST.EFF_CST_DT) = CUR_MONTH THEN 1 ELSE 0 END TOT_MTD_NBR_NEW_CST
         ,CASE WHEN YEAR(TMP_NEW_CST.EFF_CST_DT) = CUR_YEAR AND QUARTER(TMP_NEW_CST.EFF_CST_DT) = CUR_QTR THEN 1 ELSE 0 END TOT_QTD_NBR_NEW_CST
         ,CASE WHEN YEAR(TMP_NEW_CST.EFF_CST_DT) = CUR_YEAR THEN 1 ELSE 0 END TOT_YTD_NBR_NEW_CST
         ,case when DEP_AR_SMY.AR_LCS_TP_ID = 20370008 and year(DEP_AR_SMY.END_DT) = cur_year and month(DEP_AR_SMY.END_DT) = cur_month then 1 else 0 end TOT_MTD_NBR_AC_CLS
         ,case when DEP_AR_SMY.AR_LCS_TP_ID = 20370008 and year(DEP_AR_SMY.END_DT) = cur_year and quarter(DEP_AR_SMY.END_DT) = cur_qtr then 1 else 0 end TOT_QTD_NBR_AC_CLS
         ,case when DEP_AR_SMY.AR_LCS_TP_ID = 20370008 and year(DEP_AR_SMY.END_DT) = cur_year then 1 else 0 end TOT_YTD_NBR_AC_CLS
    FROM  SMY.DEP_AR_SMY AS DEP_AR_SMY  
    LEFT OUTER JOIN SESSION.TMP_CUR_AMT  AS TMP_CUR_AMT ON DEP_AR_SMY.DEP_AR_ID = TMP_CUR_AMT.AC_AR_ID AND DEP_AR_SMY.DNMN_CCY_ID = TMP_CUR_AMT.CCY
    LEFT OUTER JOIN SESSION.TMP_NEW_CST  AS TMP_NEW_CST ON DEP_AR_SMY.CST_ID=TMP_NEW_CST.CST_ID
    LEFT OUTER JOIN SOR.ACG_SBJ_CODE_MAPPING AS ACG_MAP ON ACG_MAP.ACG_SBJ_ID = DEP_AR_SMY.ACG_SBJ_ID and ACG_MAP.END_DT = '9999-12-31'
    LEFT OUTER JOIN SMY.SMY_DT z ON DEP_AR_SMY.MAT_DT=z.SMY_DT
;

INSERT INTO SESSION.TMP_CUR 
    SELECT
			    ACG_OU_IP_ID   
         ,DEP_ACG_SBJ_ID 
         ,PD_GRP_CD      
         ,PD_SUB_CD      
         ,DEP_TM_TP_ID   
         ,TM_MAT_SEG_ID
         ,ENT_IDV_IND    
         ,CCY
         ,ACCOUNTING_DATE      AS ACG_DT                 --日期
         ,CUR_YEAR      AS CDR_YR                        --年份
         ,CUR_MONTH      AS CDR_MTH                      --月份
         ,C_MON_DAY      AS NOD_IN_MTH                   --月有效天数
         ,C_QTR_DAY      AS NOD_IN_QTR                   --季度有效天数
         ,C_YR_DAY      AS NOD_IN_YEAR                   --年有效天数	
         ,NEW_ACG_SBJ_ID                                 --新科目
         ,SUM(BAL_AMT)   AS BAL_AMT                      --余额                  
         ,COUNT(DISTINCT NBR_CST)          AS NBR_CST    --客户数
         ,SUM(NBR_AC)        AS NBR_AC                   --账户数
         ,SUM(NBR_NEW_AC)   AS NBR_NEW_AC                --新增账户数        
         ,SUM(NBR_NEW_CST)       AS NBR_NEW_CST          --新增客户数
         ,sum(NBR_AC_CLS)   AS NBR_AC_CLS                --当天销户账户数 20370008: 销户
         ,SUM(CUR_CR_AMT)     AS CUR_CR_AMT              --贷方发生额
         ,SUM(CUR_DB_AMT)     AS CUR_DB_AMT              --借方发生额
         ,AC_CHRCTR_TP_ID                                --账户性质           
	  		 ,SUM(MTD_ACML_BAL_AMT) AS MTD_ACML_BAL_AMT
	  		 ,SUM(QTD_ACML_BAL_AMT) AS QTD_ACML_BAL_AMT
	  		 ,SUM(YTD_ACML_BAL_AMT) AS  YTD_ACML_BAL_AMT
	  		 ,SUM(TOT_MTD_CR_AMT) AS  TOT_MTD_CR_AMT  
         ,SUM(TOT_MTD_DB_AMT) AS  TOT_MTD_DB_AMT  
         ,SUM(TOT_QTD_CR_AMT) AS  TOT_QTD_CR_AMT  
				 ,SUM(TOT_QTD_DB_AMT) AS  TOT_QTD_DB_AMT  
				 ,SUM(TOT_YTD_CR_AMT) AS  TOT_YTD_CR_AMT  
				 ,SUM(TOT_YTD_DB_AMT) AS  TOT_YTD_DB_AMT   
         ,SUM(TOT_MTD_NBR_NEW_AC)  TOT_MTD_NBR_NEW_AC    --月新增账户数
         ,SUM(TOT_QTD_NBR_NEW_AC)  TOT_QTD_NBR_NEW_AC    --季新增账户数
         ,SUM(TOT_YTD_NBR_NEW_AC)  TOT_YTD_NBR_NEW_AC    --年新增账户数
         ,SUM(TOT_MTD_NBR_NEW_CST)  TOT_MTD_NBR_NEW_CST  --月新增客户数
         ,SUM(TOT_QTD_NBR_NEW_CST)  TOT_QTD_NBR_NEW_CST  --季新增客户数
         ,SUM(TOT_YTD_NBR_NEW_CST)  TOT_YTD_NBR_NEW_CST  --年新增客户数
         ,sum(TOT_MTD_NBR_AC_CLS)  TOT_MTD_NBR_AC_CLS    --月累计销户账户数
         ,sum(TOT_QTD_NBR_AC_CLS)  TOT_QTD_NBR_AC_CLS    --季累计销户账户数
         ,sum(TOT_YTD_NBR_AC_CLS)  TOT_YTD_NBR_AC_CLS    --年累计销户账户数					  
    FROM SESSION.TMP_TMP
    GROUP BY 
			    ACG_OU_IP_ID
         ,DEP_ACG_SBJ_ID
         ,PD_GRP_CD
         ,PD_SUB_CD
         ,DEP_TM_TP_ID
         ,TM_MAT_SEG_ID
         ,ENT_IDV_IND
         ,CCY
         ,AC_CHRCTR_TP_ID
         ,NEW_ACG_SBJ_ID
;

 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	
 	set SMY_STEPNUM = SMY_STEPNUM + 1;--

 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	  		            --

		--SET SMY_STEPNUM = 6 ;--
		SET SMY_STEPDESC = '插入当日汇总临时数据';--

	INSERT INTO SMY.OU_DEP_DLY_SMY
 (
            ACG_OU_IP_ID         --核算机构          
           ,DEP_ACG_SBJ_ID       --存款科目（核算码）
           ,PD_GRP_CD            --产品组代码        
           ,PD_SUB_CD            --产品字代码        
           ,DEP_TM_TP_ID         --存款期限类型      
           ,TM_MAT_SEG_ID        --到期期限类型      
           ,ENT_IDV_IND          --企业/个人标志     
           ,CCY                  --币种
           ,ACG_DT               --日期
           ,CDR_YR               --年份
           ,CDR_MTH              --月份
           ,NOD_IN_MTH           --月有效天数
           ,NOD_IN_QTR           --季度有效天数
           ,NOD_IN_YEAR          --年有效天数
           ,NEW_ACG_SBJ_ID       --新科目
           ,BAL_AMT              --余额
           ,NBR_CST              --客户数
           ,NBR_AC               --账户数
           ,NBR_NEW_AC           --新增账户数        
           ,NBR_NEW_CST          --新增客户数
           ,NBR_AC_CLS           --当天销户账户数
           ,CUR_CR_AMT           --贷方发生额
           ,CUR_DB_AMT           --借方发生额
           ,TOT_MTD_NBR_NEW_AC   --月新增账户数
           ,TOT_QTD_NBR_NEW_AC   --季新增账户数
           ,TOT_YTD_NBR_NEW_AC   --年新增账户数
           ,TOT_MTD_NBR_NEW_CST  --月新增客户数
           ,TOT_QTD_NBR_NEW_CST  --季新增客户数
           ,TOT_YTD_NBR_NEW_CST  --年新增客户数
           ,TOT_MTD_NBR_AC_CLS   --月累计销户账户数
           ,TOT_QTD_NBR_AC_CLS   --季累计销户账户数
           ,TOT_YTD_NBR_AC_CLS   --年累计销户账户数
           ,TOT_MTD_CR_AMT       --月贷方发生额
           ,TOT_QTD_CR_AMT       --季贷方发生额
           ,TOT_YTD_CR_AMT       --年贷方发生额
           ,TOT_MTD_DB_AMT       --月借方发生额
           ,TOT_QTD_DB_AMT       --季借方发生额
           ,TOT_YTD_DB_AMT       --年借方发生额
           ,MTD_ACML_BAL_AMT     --月累计余额
           ,QTD_ACML_BAL_AMT     --季累计余额
           ,YTD_ACML_BAL_AMT     --年累计余额           
           ,AC_CHRCTR_TP_ID	--账户性质            
------------------------------------Start on 2010-08-10 -------------------------------------------------
         ,NOCLD_IN_MTH        --当月日历天数
         ,NOCLD_IN_QTR        --当季日历天数
         ,NOCLD_IN_YEAR       --当年日历天数
------------------------------------End on 2010-08-10 ---------------------------------------------------
            )    
SELECT
            CUR.ACG_OU_IP_ID         --核算机构          
           ,CUR.DEP_ACG_SBJ_ID       --存款科目（核算码）
           ,COALESCE(CUR.PD_GRP_CD,'')            --产品组代码        
           ,COALESCE(CUR.PD_SUB_CD,'')            --产品字代码        
           ,CUR.DEP_TM_TP_ID         --存款期限类型      
           ,COALESCE(CUR.TM_MAT_SEG_ID,-1)        --到期期限类型      
           ,CUR.ENT_IDV_IND          --企业/个人标志     
           ,CUR.CCY                  --币种
           ,CUR.ACG_DT               --日期
           ,CUR.CDR_YR               --年份
           ,CUR.CDR_MTH              --月份
           ,CUR.NOD_IN_MTH           --月有效天数
           ,CUR.NOD_IN_QTR           --季度有效天数
           ,CUR.NOD_IN_YEAR          --年有效天数
           ,CUR.NEW_ACG_SBJ_ID       --新科目
           ,CUR.BAL_AMT              --余额
           ,case when NBR_AC = 0 then 0 else CUR.NBR_CST end             --客户数
           ,CUR.NBR_AC               --账户数
           ,CUR.NBR_NEW_AC           --新增账户数        
           ,CUR.NBR_NEW_CST          --新增客户数
           ,CUR.NBR_AC_CLS           --当天销户账户数
           ,CUR.CUR_CR_AMT           --贷方发生额
           ,CUR.CUR_DB_AMT           --借方发生额
           ----------------------------------Start on 20100225----------------------------------------------
           /*

           ,CUR.NBR_NEW_AC --月新增账户数

           ,CUR.NBR_NEW_AC --季新增账户数

           ,CUR.NBR_NEW_AC --年新增账户数

           ,CUR.NBR_NEW_CST --月新增客户数

           ,CUR.NBR_NEW_CST --季新增客户数

           ,CUR.NBR_NEW_CST --年新增客户数

           ,CUR.NBR_AC_CLS --月累计销户账户数

           ,CUR.NBR_AC_CLS --季累计销户账户数

           ,CUR.NBR_AC_CLS --年累计销户账户数

           ,CUR.CUR_CR_AMT  --月贷方发生额

           ,CUR.CUR_CR_AMT  --季贷方发生额

           ,CUR.CUR_CR_AMT  --年贷方发生额

           ,CUR.CUR_DB_AMT --月借方发生额

           ,CUR.CUR_DB_AMT --季借方发生额

           ,CUR.CUR_DB_AMT --年借方发生额

           ,CUR.BAL_AMT --月累计余额

           ,CUR.BAL_AMT --季累计余额

           ,CUR.BAL_AMT --年累计余额

           */
           ,TOT_MTD_NBR_NEW_AC   --月新增账户数
           ,TOT_QTD_NBR_NEW_AC   --季新增账户数
           ,TOT_YTD_NBR_NEW_AC   --年新增账户数
           ,TOT_MTD_NBR_NEW_CST  --月新增客户数
           ,TOT_QTD_NBR_NEW_CST  --季新增客户数
           ,TOT_YTD_NBR_NEW_CST  --年新增客户数
           ,TOT_MTD_NBR_AC_CLS   --月累计销户账户数
           ,TOT_QTD_NBR_AC_CLS   --季累计销户账户数
           ,TOT_YTD_NBR_AC_CLS   --年累计销户账户数
           ,TOT_MTD_CR_AMT       --月贷方发生额
           ,TOT_QTD_CR_AMT       --季贷方发生额
           ,TOT_YTD_CR_AMT       --年贷方发生额
           ,TOT_MTD_DB_AMT       --月借方发生额
           ,TOT_QTD_DB_AMT       --季借方发生额
           ,TOT_YTD_DB_AMT       --年借方发生额
           ,MTD_ACML_BAL_AMT     --月累计余额
           ,QTD_ACML_BAL_AMT     --季累计余额
           ,YTD_ACML_BAL_AMT     --年累计余额  
           ----------------------------------End on 20100225----------------------------------------------
           ,CUR.AC_CHRCTR_TP_ID	--账户性质                          	     
------------------------------------Start on 2010-08-10 -------------------------------------------------
         ,CUR_DAY                 --当月日历天数
         ,QTR_DAY                 --当季日历天数
         ,YR_DAY                  --当年日历天数
------------------------------------End on 2010-08-10 ---------------------------------------------------
	 FROM SESSION.TMP_CUR  AS CUR
;--
 /** 收集操作信息 */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--

  SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	 --

/*月表的插入*/
   IF ACCOUNTING_DATE = MTH_LAST_DAY THEN 
  		SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
		  SET SMY_STEPDESC = '本账务日期为该月最后一天,往月表SMY.OU_DEP_MTHLY_SMY 中插入数据';   	--
		  
	INSERT INTO SMY.OU_DEP_MTHLY_SMY
 (
      ACG_OU_IP_ID         --核算机构          
     ,DEP_ACG_SBJ_ID       --存款科目（核算码）
     ,PD_GRP_CD            --产品组代码        
     ,PD_SUB_CD            --产品字代码        
     ,DEP_TM_TP_ID         --存款期限类型      
     ,TM_MAT_SEG_ID        --到期期限类型      
     ,ENT_IDV_IND          --企业/个人标志     
     ,CCY                  --币种
     ,ACG_DT               --日期
     ,CDR_YR               --年份
     ,CDR_MTH              --月份
     ,NOD_IN_MTH           --月有效天数
     ,NOD_IN_QTR           --季度有效天数
     ,NOD_IN_YEAR          --年有效天数
     ,NEW_ACG_SBJ_ID       --新科目
     ,BAL_AMT              --余额
     ,NBR_CST              --客户数
     ,NBR_AC               --账户数
     ,NBR_NEW_AC           --新增账户数        
     ,NBR_NEW_CST          --新增客户数
     ,NBR_AC_CLS           --当天销户账户数
     ,CUR_CR_AMT           --贷方发生额
     ,CUR_DB_AMT           --借方发生额
     ,TOT_MTD_NBR_NEW_AC   --月新增账户数
     ,TOT_QTD_NBR_NEW_AC   --季新增账户数
     ,TOT_YTD_NBR_NEW_AC   --年新增账户数
     ,TOT_MTD_NBR_NEW_CST  --月新增客户数
     ,TOT_QTD_NBR_NEW_CST  --季新增客户数
     ,TOT_YTD_NBR_NEW_CST  --年新增客户数
     ,TOT_MTD_NBR_AC_CLS   --月累计销户账户数
     ,TOT_QTD_NBR_AC_CLS   --季累计销户账户数
     ,TOT_YTD_NBR_AC_CLS   --年累计销户账户数
     ,TOT_MTD_CR_AMT       --月贷方发生额
     ,TOT_QTD_CR_AMT       --季贷方发生额
     ,TOT_YTD_CR_AMT       --年贷方发生额
     ,TOT_MTD_DB_AMT       --月借方发生额
     ,TOT_QTD_DB_AMT       --季借方发生额
     ,TOT_YTD_DB_AMT       --年借方发生额
     ,MTD_ACML_BAL_AMT     --月累计余额
     ,QTD_ACML_BAL_AMT     --季累计余额
     ,YTD_ACML_BAL_AMT     --年累计余额
     ,AC_CHRCTR_TP_ID	--账户性质            
------------------------------------Start on 2010-08-10 -------------------------------------------------
         ,NOCLD_IN_MTH        --当月日历天数
         ,NOCLD_IN_QTR        --当季日历天数
         ,NOCLD_IN_YEAR       --当年日历天数
------------------------------------End on 2010-08-10 ---------------------------------------------------
            )
   SELECT 
      ACG_OU_IP_ID         --核算机构          
     ,DEP_ACG_SBJ_ID       --存款科目（核算码）
     ,COALESCE(PD_GRP_CD,'')            --产品组代码        
     ,COALESCE(PD_SUB_CD,'')            --产品字代码        
     ,DEP_TM_TP_ID         --存款期限类型      
     ,TM_MAT_SEG_ID        --到期期限类型      
     ,ENT_IDV_IND          --企业/个人标志     
     ,CCY                  --币种
     ,ACG_DT               --日期
     ,CDR_YR               --年份
     ,CDR_MTH              --月份
     ,NOD_IN_MTH           --月有效天数
     ,NOD_IN_QTR           --季度有效天数
     ,NOD_IN_YEAR          --年有效天数
     ,NEW_ACG_SBJ_ID       --新科目
     ,BAL_AMT              --余额
     ,NBR_CST              --客户数
     ,NBR_AC               --账户数
     ,NBR_NEW_AC           --新增账户数        
     ,NBR_NEW_CST          --新增客户数
     ,NBR_AC_CLS           --当天销户账户数
     ,CUR_CR_AMT           --贷方发生额
     ,CUR_DB_AMT           --借方发生额
     ,TOT_MTD_NBR_NEW_AC   --月新增账户数
     ,TOT_QTD_NBR_NEW_AC   --季新增账户数
     ,TOT_YTD_NBR_NEW_AC   --年新增账户数
     ,TOT_MTD_NBR_NEW_CST  --月新增客户数
     ,TOT_QTD_NBR_NEW_CST  --季新增客户数
     ,TOT_YTD_NBR_NEW_CST  --年新增客户数
     ,TOT_MTD_NBR_AC_CLS   --月累计销户账户数
     ,TOT_QTD_NBR_AC_CLS   --季累计销户账户数
     ,TOT_YTD_NBR_AC_CLS   --年累计销户账户数
     ,TOT_MTD_CR_AMT       --月贷方发生额
     ,TOT_QTD_CR_AMT       --季贷方发生额
     ,TOT_YTD_CR_AMT       --年贷方发生额
     ,TOT_MTD_DB_AMT       --月借方发生额
     ,TOT_QTD_DB_AMT       --季借方发生额
     ,TOT_YTD_DB_AMT       --年借方发生额
     ,MTD_ACML_BAL_AMT     --月累计余额
     ,QTD_ACML_BAL_AMT     --季累计余额
     ,YTD_ACML_BAL_AMT     --年累计余额
     ,AC_CHRCTR_TP_ID	--账户性质  
------------------------------------Start on 2010-08-10 -------------------------------------------------
         ,NOCLD_IN_MTH        --当月日历天数
         ,NOCLD_IN_QTR        --当季日历天数
         ,NOCLD_IN_YEAR       --当年日历天数
------------------------------------End on 2010-08-10 ---------------------------------------------------
   FROM SMY.OU_DEP_DLY_SMY WHERE ACG_DT=ACCOUNTING_DATE ;--

 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	     --
  END IF;--

	COMMIT;--
END@