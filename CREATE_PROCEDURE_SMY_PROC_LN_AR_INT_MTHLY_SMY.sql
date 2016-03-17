CREATE PROCEDURE SMY.PROC_LN_AR_INT_MTHLY_SMY(IN ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_LN_AR_INT_MTHLY_SMY.sql
-- Procedure name: 			SMY.PROC_LN_AR_INT_MTHLY_SMY
-- Source Table:				SMY.LOAN_AR_SMY, SOR.LN_TXN_DTL_INF,SOR.LN_INT_INF
-- Target Table: 				SMY.LN_AR_INT_MTHLY_SMY
-- Project     :        ZJ RCCB EDW
-- NOTES       :        DEPENDENCY  SMY.LOAN_AR_SMY
-- Purpose     :            
-- PROCESS METHOD      :  Update each day in one period of month, insert in one month.
--=============================================================================
-- Creation Date:       2009.11.09
-- Origin Author:       JAMES SHANG IBM copyright 
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2009-11-09   JAMES SHANG     Create SP File
-- 2009-11-18   Xu Yan          Updated some condition statements
-- 2009-11-23   SHANG						修改利息收入,表内实收利息的过滤条件
-- 2009-12-16   Xu Yan          Updated 'NBR_LN_DRDWNTXN','NBR_LN_RCVD_TXN'
-- 2009-12-16   Xu Yan          Fixed the bug for rerunning.
-- 2010-01-06   Xu Yan          Included the related transactions on the account closing day
-- 2010-01-19   Xu Yan          Removed all the conditional statements of the AR life cycle status and modify the calendar days
-- 2011-08-09		Li ShuWen				对性能进行优化。判断是否月初，是则用insert代替merge
-- 2011-08-11   Li ShenYu       摘要码改造
-- 2012-02-29   Chen XiaoWen    1、修改SMY.LN_AR_INT_MTHLY_SMY的查询条件为使用ACG_DT分区键
--                              2、将merge using的子查询拆分为临时表TMP_TMP
--                              3、将merge当中的match、not match逻辑拆分为match update和单独的insert语句。
-------------------------------------------------------------------------------
LANGUAGE SQL

 BEGIN 
    /*声明异常处理使用变量*/
		DECLARE SQLCODE INT DEFAULT 0; --
		DECLARE SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
		DECLARE SMY_STEPNUM INT DEFAULT 1;                     --过程内部位置标记
		DECLARE SMY_STEPDESC VARCHAR(100) DEFAULT '';          --过程内部位置描述
		DECLARE SMY_DATE DATE;        --临时日期变量
		DECLARE SMY_RCOUNT INT;       --DML语句作用记录数
		DECLARE SMY_PROCNM VARCHAR(100);                        --存储过程名称
		
/*声明存储过程使用变量*/
		DECLARE CUR_YEAR SMALLINT;--
		DECLARE CUR_MONTH SMALLINT;--
		DECLARE MON_DAY INTEGER;--
		DECLARE YR_FIRST_DAY DATE;--
		DECLARE QTR_FIRST_DAY DATE;--

		DECLARE MAX_ACG_DT DATE;--
		DECLARE LAST_SMY_DATE DATE;--
		DECLARE MTH_FIRST_DAY DATE;--
		DECLARE EMP_SQL VARCHAR(200);--
		DECLARE LAST_MONTH SMALLINT;--
		DECLARE CUR_QTR SMALLINT;--
		DECLARE V_T SMALLINT;--
		DECLARE C_MON_DAY SMALLINT;--
		DECLARE C_YR_DAY SMALLINT;--
		DECLARE C_QTR_DAY SMALLINT;--
		DECLARE QTR_LAST_DAY DATE;--
		DECLARE MTH_LAST_DAY DATE;
		DECLARE LASTMTH_FIRST_DAY DATE;--上月初日

/*
	1.定义针对SQL异常情况的句柄(EXIT方式).
  2.将出现SQL异常时在存储过程中的位置(SMY_STEPNUM),位置描述(SMY_STEPDESC)，SQLCODE(SMY_SQLCODE)记入表SMY_LOG中作调试用.
  3.调用RESIGNAL重新引发异常,跳出存储过程执行体,对引发SQL异常之前存储过程体中所完成的操作进行回滚.
*/
  
		DECLARE CONTINUE HANDLER FOR NOT FOUND
		  SET V_T=0 ; --
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
    	SET SMY_SQLCODE = SQLCODE;    	--
      ROLLBACK;--
      SET SMY_STEPNUM = SMY_STEPNUM + 1;--
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
      COMMIT;--
      RESIGNAL;--
    END;--
   
   /* 
    DECLARE CONTINUE HANDLER FOR SQLWARNING
    BEGIN
      SET SMY_SQLCODE = SQLCODE;--
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
      COMMIT;--
    END;--
   */
   
   /*变量赋值*/   
    SET SMY_PROCNM  ='PROC_LN_AR_INT_MTHLY_SMY';--
    SET SMY_DATE    =ACCOUNTING_DATE;    --
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --取当前年份
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --取当前月份
    SET MON_DAY     =DAY(ACCOUNTING_DATE);     --取月第几日
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- 取年初日
    ----------------------Start on 20100119---------------------------------
    --SET C_YR_DAY      =DAYOFYEAR(DATE(TRIM(CHAR(YEAR(ACCOUNTING_DATE)))||'-12-31')); --取当年第几日
    SET C_YR_DAY      =DAYOFYEAR(ACCOUNTING_DATE); --取当年第几日
    ----------------------End on 20100119---------------------------------
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --当前季度
    SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,ISO),1,7)||'-01'); --取当月初日
    VALUES(MTH_FIRST_DAY + 1 MONTH - 1 DAY) INTO MTH_LAST_DAY ;
    VALUES(MTH_FIRST_DAY - 1 MONTH) INTO LASTMTH_FIRST_DAY ;
    
    --计算月日历天数
    ----------------------Start on 20100119---------------------------------
    --SET C_MON_DAY = DAYS(MTH_FIRST_DAY + 1 MONTH ) - DAYS(MTH_FIRST_DAY);--
    SET C_MON_DAY = MON_DAY;--
    ----------------------End on 20100119---------------------------------
     
    VALUES(ACCOUNTING_DATE - 1 DAY) INTO LAST_SMY_DATE;--
    
    SET LAST_MONTH = MONTH(LAST_SMY_DATE);  --
    
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

  /*取当季日历天数*/ 
    ----------------------Start on 20100119---------------------------------
  	--SET C_QTR_DAY = DAYS(QTR_LAST_DAY) - DAYS(QTR_FIRST_DAY) + 1 ;--
  	SET C_QTR_DAY = DAYS(ACCOUNTING_DATE) - DAYS(QTR_FIRST_DAY) + 1 ;--
  	----------------------End on 20100119---------------------------------
		
		SELECT COALESCE(MAX(ACG_DT),'1900-01-01') INTO MAX_ACG_DT FROM SMY.LN_AR_INT_MTHLY_SMY;--

/*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE,并插入新的起始标志*/
		DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM ;--
			COMMIT;--
		
		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		
		SET SMY_STEPDESC = 	'存储过程开始运行';--
		SET SMY_STEPNUM = SMY_STEPNUM + 1;--
		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
				VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);		 --

/*数据恢复与备份*/
    IF MAX_ACG_DT = ACCOUNTING_DATE THEN
       DELETE FROM SMY.LN_AR_INT_MTHLY_SMY WHERE ACG_DT = ACCOUNTING_DATE;--
    /**每月第一日不需要从历史表中恢复**/       
       IF MON_DAY <> 1 THEN
      	 INSERT INTO SMY.LN_AR_INT_MTHLY_SMY  SELECT * FROM HIS.LN_AR_INT_MTHLY_SMY hist 
               where  not exists ( select 1 from SMY.LN_AR_INT_MTHLY_SMY cur
                                    where
				    hist.CTR_AR_ID = cur.CTR_AR_ID 
			            and
				    hist.CTR_ITM_ORDR_ID = cur.CTR_ITM_ORDR_ID
				    and
				    hist.CDR_YR = cur.CDR_YR
				    and
			            hist.CDR_MTH = cur.CDR_MTH
				);--
       END IF;--
     ELSE
  		/** 清空hist 备份表 **/

	    SET EMP_SQL= 'Alter TABLE HIS.LN_AR_INT_MTHLY_SMY ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE' ;		--
		  EXECUTE IMMEDIATE EMP_SQL;             --
      COMMIT;--
      
      /**backup 昨日数据 **/
  		INSERT INTO HIS.LN_AR_INT_MTHLY_SMY SELECT * FROM SMY.LN_AR_INT_MTHLY_SMY WHERE ACG_DT = LAST_SMY_DATE;--
      
    END IF;--

SET SMY_STEPDESC = '声明用户临时表,存放昨日SMY数据';--

	/*声明用户临时表*/
	
	DECLARE GLOBAL TEMPORARY TABLE TMP 
		LIKE SMY.LN_AR_INT_MTHLY_SMY
	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(CTR_AR_ID,CTR_ITM_ORDR_ID);--
----add by wuzhansan begin
  CREATE INDEX SESSION.IDX_TMP ON SESSION.TMP(CTR_AR_ID,CTR_ITM_ORDR_ID);
----add by wuzhansan end

----modify by wuzhanshan begin
 /*存放昨日SMY数据*/
IF YR_FIRST_DAY <>  ACCOUNTING_DATE  THEN

 IF QTR_FIRST_DAY=ACCOUNTING_DATE THEN 
	INSERT INTO SESSION.TMP 
	(
           CTR_AR_ID                         --合同号            
          ,CTR_ITM_ORDR_ID                   --合同序号          
          ,CDR_YR                            --日历年            
          ,CDR_MTH                           --日历月            
          ,ACG_DT                            --会计日期          
          ,LN_AR_ID                          --账户号            
          ,DNMN_CCY_ID                       --币种ID            
          ,YTD_ON_BST_INT_AMT_RCVD           --表内实收利息      
          ,YTD_OFF_BST_INT_AMT_RCVD          --表外实收利息      
          ,ON_BST_INT_RCVB                   --表内应收未收利息  
          ,OFF_BST_INT_RCVB                  --表外应收未收利息  
          ,TOT_YTD_AMT_OF_INT_INCM           --利息收入          
          ,LN_DRDWN_AMT                      --当天累放金额      
          ,AMT_LN_REPYMT_RCVD                --当天累收金额      
          ,TOT_MTD_LN_DRDWN_AMT              --月贷款累计发放金额
          ,TOT_QTD_LN_DRDWN_AMT              --季贷款累计发放金额
          ,TOT_YTD_LN_DRDWN_AMT              --年贷款累计发放金额
          ,TOT_MTD_AMT_LN_REPYMT_RCVD        --月累计收回贷款金额
          ,TOT_QTD_AMT_LN_RPYMT_RCVD         --季累计收回贷款金额
          ,TOT_YTD_AMT_LN_REPYMT_RCVD        --年累计收回贷款金额
          ,TOT_MTD_NBR_LN_RCVD_TXN           --月累计收回贷款笔数
          ,TOT_QTD_NBR_LN_RCVD_TXN           --季累计收回贷款笔数
          ,TOT_YTD_NBR_LN_RCVD_TXN           --年累计收回贷款笔数
          ,TOT_MTD_NBR_LN_DRDWNTXN           --月累计发放贷款笔数
          ,TOT_QTD_NBR_LN_DRDWN_TXN          --季累计发放贷款笔数
          ,TOT_YTD_NBR_LN_DRDWN_TXN          --年累计发放贷款笔数
          ,CUR_CR_AMT                        --贷方发生额        
          ,CUR_DB_AMT                        --借方发生额        
          ,TOT_MTD_CR_AMT                    --月累计贷方发生额  
          ,TOT_MTD_DB_AMT                    --月累计借方发生额  
          ,TOT_QTD_DB_AMT                    --季累计贷方发生额  
          ,TOT_QTD_CR_AMT                    --季累计借方发生额  
          ,TOT_YTD_CR_AMT                    --年累计贷方发生额  
          ,TOT_YTD_DB_AMT                    --年累计借方发生额
          ,OFF_BST_INT_RCVB_WRTOF            --表外应收利息核销金额
          ,OFF_BST_INT_RCVB_RPLC	           --表外应收利息置换金额
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST		 --抵债资产抵债利息收入
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN	   --核销贷款收回利息        
          ,BAL_AMT	            --
          ,MTD_ACML_BAL_AMT		--		月累计余额
          ,QTD_ACML_BAL_AMT		--		季累计余额
          ,YTD_ACML_BAL_AMT		--		年累计余额
          ,NOCLD_In_MTH				--		月日历天数
          ,NOD_In_MTH					--		月有效天数
          ,NOCLD_In_QTR				--		季日历天数
          ,NOD_In_QTR					--		季有效天数
          ,NOCLD_In_Year				--		年日历天数
          ,NOD_In_Year					--		年有效天数
          ,CUR_WRTOF_AMT_RCVD            --当天收回核销金额
          ,CUR_AMT_RCVD_Of_AST_RPLC      --当天收回置换资产金额
          ,CUR_WRTOF_AMT                 --当天核销金额
          ,TOT_MTD_WRTOF_AMT_RCVD        --月累计收回核销金额
          ,TOT_MTD_AMT_RCVD_Of_AST_RPLC  --月累计收回置换资产金额
          ,TOT_MTD_WRTOF_AMT             --月累计核销金额
          ,TOT_QTD_WRTOF_AMT_RCVD        --季累计收回核销金额
          ,TOT_QTD_AMT_RCVD_Of_AST_RPLC  --季累计收回置换资产金额
          ,TOT_QTD_WRTOF_AMT             --季累计核销金额
          ,TOT_YTD_WRTOF_AMT_RCVD        --年累计收回核销金额
          ,TOT_YTD_AMT_RCVD_Of_AST_RPLC  --年累计置换资产金额
          ,TOT_YTD_WRTOF_AMT             --年累计核销金额                  
          ) 
    SELECT 
           CTR_AR_ID                         --合同号            
          ,CTR_ITM_ORDR_ID                   --合同序号          
          ,CDR_YR                            --日历年            
          ,CDR_MTH                           --日历月            
          ,ACG_DT                            --会计日期          
          ,LN_AR_ID                          --账户号            
          ,DNMN_CCY_ID                       --币种ID            
          ,YTD_ON_BST_INT_AMT_RCVD           --表内实收利息      
          ,YTD_OFF_BST_INT_AMT_RCVD          --表外实收利息      
          ,ON_BST_INT_RCVB                   --表内应收未收利息  
          ,OFF_BST_INT_RCVB                  --表外应收未收利息  
          ,TOT_YTD_AMT_OF_INT_INCM           --利息收入          
          ,LN_DRDWN_AMT                      --当天累放金额      
          ,AMT_LN_REPYMT_RCVD                --当天累收金额      
          ,0                                 --月贷款累计发放金额
          ,0                                 --季贷款累计发放金额
          ,TOT_YTD_LN_DRDWN_AMT              --年贷款累计发放金额
          ,0                                 --月累计收回贷款金额
          ,0                                 --季累计收回贷款金额
          ,TOT_YTD_AMT_LN_REPYMT_RCVD        --年累计收回贷款金额
          ,0                                 --月累计收回贷款笔数
          ,0                                 --季累计收回贷款笔数
          ,TOT_YTD_NBR_LN_RCVD_TXN           --年累计收回贷款笔数
          ,0                                 --月累计发放贷款笔数
          ,0                                 --季累计发放贷款笔数
          ,TOT_YTD_NBR_LN_DRDWN_TXN          --年累计发放贷款笔数
          ,CUR_CR_AMT                        --贷方发生额        
          ,CUR_DB_AMT                        --借方发生额        
          ,0                                 --月累计贷方发生额  
          ,0                                 --月累计借方发生额  
          ,0                                 --季累计贷方发生额  
          ,0                                 --季累计借方发生额  
          ,TOT_YTD_CR_AMT                    --年累计贷方发生额  
          ,TOT_YTD_DB_AMT                    --年累计借方发生额
          ,OFF_BST_INT_RCVB_WRTOF            --表外应收利息核销金额
          ,OFF_BST_INT_RCVB_RPLC	           --表外应收利息置换金额
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST		 --抵债资产抵债利息收入
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN	   --核销贷款收回利息
          ,BAL_AMT	                         --
          ,0		                             --		月累计余额
          ,0		                             --		季累计余额
          ,YTD_ACML_BAL_AMT		               --		年累计余额
          ,NOCLD_In_MTH				               --		月日历天数
          ,0					                       --		月有效天数
          ,NOCLD_In_QTR				               --		季日历天数
          ,0					                       --		季有效天数
          ,NOCLD_In_Year				             --		年日历天数
          ,NOD_In_Year					             --		年有效天数
          ,CUR_WRTOF_AMT_RCVD                --当天收回核销金额
          ,CUR_AMT_RCVD_Of_AST_RPLC          --当天收回置换资产金额
          ,CUR_WRTOF_AMT                     --当天核销金额
          ,0                                 --月累计收回核销金额
          ,0                                 --月累计收回置换资产金额
          ,0                                 --月累计核销金额
          ,0                                 --季累计收回核销金额
          ,0                                 --季累计收回置换资产金额
          ,0                                 --季累计核销金额
          ,TOT_YTD_WRTOF_AMT_RCVD            --年累计收回核销金额
          ,TOT_YTD_AMT_RCVD_Of_AST_RPLC      --年累计置换资产金额
          ,TOT_YTD_WRTOF_AMT                 --年累计核销金额              					       
     --FROM SMY.LN_AR_INT_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = LAST_MONTH;--
     FROM SMY.LN_AR_INT_MTHLY_SMY WHERE ACG_DT >= LASTMTH_FIRST_DAY AND ACG_DT < MTH_FIRST_DAY;   --取上月数据
  ELSEIF ACCOUNTING_DATE=MTH_FIRST_DAY THEN
 	INSERT INTO SESSION.TMP 
	(
           CTR_AR_ID                         --合同号            
          ,CTR_ITM_ORDR_ID                   --合同序号          
          ,CDR_YR                            --日历年            
          ,CDR_MTH                           --日历月            
          ,ACG_DT                            --会计日期          
          ,LN_AR_ID                          --账户号            
          ,DNMN_CCY_ID                       --币种ID            
          ,YTD_ON_BST_INT_AMT_RCVD           --表内实收利息      
          ,YTD_OFF_BST_INT_AMT_RCVD          --表外实收利息      
          ,ON_BST_INT_RCVB                   --表内应收未收利息  
          ,OFF_BST_INT_RCVB                  --表外应收未收利息  
          ,TOT_YTD_AMT_OF_INT_INCM           --利息收入          
          ,LN_DRDWN_AMT                      --当天累放金额      
          ,AMT_LN_REPYMT_RCVD                --当天累收金额      
          ,TOT_MTD_LN_DRDWN_AMT              --月贷款累计发放金额
          ,TOT_QTD_LN_DRDWN_AMT              --季贷款累计发放金额
          ,TOT_YTD_LN_DRDWN_AMT              --年贷款累计发放金额
          ,TOT_MTD_AMT_LN_REPYMT_RCVD        --月累计收回贷款金额
          ,TOT_QTD_AMT_LN_RPYMT_RCVD         --季累计收回贷款金额
          ,TOT_YTD_AMT_LN_REPYMT_RCVD        --年累计收回贷款金额
          ,TOT_MTD_NBR_LN_RCVD_TXN           --月累计收回贷款笔数
          ,TOT_QTD_NBR_LN_RCVD_TXN           --季累计收回贷款笔数
          ,TOT_YTD_NBR_LN_RCVD_TXN           --年累计收回贷款笔数
          ,TOT_MTD_NBR_LN_DRDWNTXN           --月累计发放贷款笔数
          ,TOT_QTD_NBR_LN_DRDWN_TXN          --季累计发放贷款笔数
          ,TOT_YTD_NBR_LN_DRDWN_TXN          --年累计发放贷款笔数
          ,CUR_CR_AMT                        --贷方发生额        
          ,CUR_DB_AMT                        --借方发生额        
          ,TOT_MTD_CR_AMT                    --月累计贷方发生额  
          ,TOT_MTD_DB_AMT                    --月累计借方发生额  
          ,TOT_QTD_DB_AMT                    --季累计贷方发生额  
          ,TOT_QTD_CR_AMT                    --季累计借方发生额  
          ,TOT_YTD_CR_AMT                    --年累计贷方发生额  
          ,TOT_YTD_DB_AMT                    --年累计借方发生额
          ,OFF_BST_INT_RCVB_WRTOF            --表外应收利息核销金额
          ,OFF_BST_INT_RCVB_RPLC	           --表外应收利息置换金额
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST		 --抵债资产抵债利息收入
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN	   --核销贷款收回利息        
          ,BAL_AMT	            --
          ,MTD_ACML_BAL_AMT		--		月累计余额
          ,QTD_ACML_BAL_AMT		--		季累计余额
          ,YTD_ACML_BAL_AMT		--		年累计余额
          ,NOCLD_In_MTH				--		月日历天数
          ,NOD_In_MTH					--		月有效天数
          ,NOCLD_In_QTR				--		季日历天数
          ,NOD_In_QTR					--		季有效天数
          ,NOCLD_In_Year				--		年日历天数
          ,NOD_In_Year					--		年有效天数
          ,CUR_WRTOF_AMT_RCVD            --当天收回核销金额
          ,CUR_AMT_RCVD_Of_AST_RPLC      --当天收回置换资产金额
          ,CUR_WRTOF_AMT                 --当天核销金额
          ,TOT_MTD_WRTOF_AMT_RCVD        --月累计收回核销金额
          ,TOT_MTD_AMT_RCVD_Of_AST_RPLC  --月累计收回置换资产金额
          ,TOT_MTD_WRTOF_AMT             --月累计核销金额
          ,TOT_QTD_WRTOF_AMT_RCVD        --季累计收回核销金额
          ,TOT_QTD_AMT_RCVD_Of_AST_RPLC  --季累计收回置换资产金额
          ,TOT_QTD_WRTOF_AMT             --季累计核销金额
          ,TOT_YTD_WRTOF_AMT_RCVD        --年累计收回核销金额
          ,TOT_YTD_AMT_RCVD_Of_AST_RPLC  --年累计置换资产金额
          ,TOT_YTD_WRTOF_AMT             --年累计核销金额                  
          ) 
    SELECT 
           CTR_AR_ID                         --合同号            
          ,CTR_ITM_ORDR_ID                   --合同序号          
          ,CDR_YR                            --日历年            
          ,CDR_MTH                           --日历月            
          ,ACG_DT                            --会计日期          
          ,LN_AR_ID                          --账户号            
          ,DNMN_CCY_ID                       --币种ID            
          ,YTD_ON_BST_INT_AMT_RCVD           --表内实收利息      
          ,YTD_OFF_BST_INT_AMT_RCVD          --表外实收利息      
          ,ON_BST_INT_RCVB                   --表内应收未收利息  
          ,OFF_BST_INT_RCVB                  --表外应收未收利息  
          ,TOT_YTD_AMT_OF_INT_INCM           --利息收入          
          ,LN_DRDWN_AMT                      --当天累放金额      
          ,AMT_LN_REPYMT_RCVD                --当天累收金额      
          ,0              --月贷款累计发放金额
          ,TOT_QTD_LN_DRDWN_AMT              --季贷款累计发放金额
          ,TOT_YTD_LN_DRDWN_AMT              --年贷款累计发放金额
          ,0        --月累计收回贷款金额
          ,TOT_QTD_AMT_LN_RPYMT_RCVD         --季累计收回贷款金额
          ,TOT_YTD_AMT_LN_REPYMT_RCVD        --年累计收回贷款金额
          ,0           --月累计收回贷款笔数
          ,TOT_QTD_NBR_LN_RCVD_TXN           --季累计收回贷款笔数
          ,TOT_YTD_NBR_LN_RCVD_TXN           --年累计收回贷款笔数
          ,0           --月累计发放贷款笔数
          ,TOT_QTD_NBR_LN_DRDWN_TXN          --季累计发放贷款笔数
          ,TOT_YTD_NBR_LN_DRDWN_TXN          --年累计发放贷款笔数
          ,CUR_CR_AMT                        --贷方发生额        
          ,CUR_DB_AMT                        --借方发生额        
          ,0                    --月累计贷方发生额  
          ,0                    --月累计借方发生额  
          ,TOT_QTD_DB_AMT                    --季累计贷方发生额  
          ,TOT_QTD_CR_AMT                    --季累计借方发生额  
          ,TOT_YTD_CR_AMT                    --年累计贷方发生额  
          ,TOT_YTD_DB_AMT                    --年累计借方发生额
          ,OFF_BST_INT_RCVB_WRTOF            --表外应收利息核销金额
          ,OFF_BST_INT_RCVB_RPLC	           --表外应收利息置换金额
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST		 --抵债资产抵债利息收入
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN	   --核销贷款收回利息
          ,BAL_AMT	            --
          ,0		--		月累计余额
          ,QTD_ACML_BAL_AMT		--		季累计余额
          ,YTD_ACML_BAL_AMT		--		年累计余额
          ,0				--		月日历天数
          ,0					--		月有效天数
          ,NOCLD_In_QTR				--		季日历天数
          ,NOD_In_QTR					--		季有效天数
          ,NOCLD_In_Year				--		年日历天数
          ,NOD_In_Year					--		年有效天数
          ,CUR_WRTOF_AMT_RCVD            --当天收回核销金额
          ,CUR_AMT_RCVD_Of_AST_RPLC      --当天收回置换资产金额
          ,CUR_WRTOF_AMT                 --当天核销金额
          ,0        --月累计收回核销金额
          ,0  --月累计收回置换资产金额
          ,0             --月累计核销金额
          ,TOT_QTD_WRTOF_AMT_RCVD        --季累计收回核销金额
          ,TOT_QTD_AMT_RCVD_Of_AST_RPLC  --季累计收回置换资产金额
          ,TOT_QTD_WRTOF_AMT             --季累计核销金额
          ,TOT_YTD_WRTOF_AMT_RCVD        --年累计收回核销金额
          ,TOT_YTD_AMT_RCVD_Of_AST_RPLC  --年累计置换资产金额
          ,TOT_YTD_WRTOF_AMT             --年累计核销金额              					       
     --FROM SMY.LN_AR_INT_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = LAST_MONTH;
     FROM SMY.LN_AR_INT_MTHLY_SMY WHERE ACG_DT >= LASTMTH_FIRST_DAY AND ACG_DT < MTH_FIRST_DAY;   --取上月数据
  ELSE
  INSERT INTO SESSION.TMP 
	(
           CTR_AR_ID                         --合同号            
          ,CTR_ITM_ORDR_ID                   --合同序号          
          ,CDR_YR                            --日历年            
          ,CDR_MTH                           --日历月            
          ,ACG_DT                            --会计日期          
          ,LN_AR_ID                          --账户号            
          ,DNMN_CCY_ID                       --币种ID            
          ,YTD_ON_BST_INT_AMT_RCVD           --表内实收利息      
          ,YTD_OFF_BST_INT_AMT_RCVD          --表外实收利息      
          ,ON_BST_INT_RCVB                   --表内应收未收利息  
          ,OFF_BST_INT_RCVB                  --表外应收未收利息  
          ,TOT_YTD_AMT_OF_INT_INCM           --利息收入          
          ,LN_DRDWN_AMT                      --当天累放金额      
          ,AMT_LN_REPYMT_RCVD                --当天累收金额      
          ,TOT_MTD_LN_DRDWN_AMT              --月贷款累计发放金额
          ,TOT_QTD_LN_DRDWN_AMT              --季贷款累计发放金额
          ,TOT_YTD_LN_DRDWN_AMT              --年贷款累计发放金额
          ,TOT_MTD_AMT_LN_REPYMT_RCVD        --月累计收回贷款金额
          ,TOT_QTD_AMT_LN_RPYMT_RCVD         --季累计收回贷款金额
          ,TOT_YTD_AMT_LN_REPYMT_RCVD        --年累计收回贷款金额
          ,TOT_MTD_NBR_LN_RCVD_TXN           --月累计收回贷款笔数
          ,TOT_QTD_NBR_LN_RCVD_TXN           --季累计收回贷款笔数
          ,TOT_YTD_NBR_LN_RCVD_TXN           --年累计收回贷款笔数
          ,TOT_MTD_NBR_LN_DRDWNTXN           --月累计发放贷款笔数
          ,TOT_QTD_NBR_LN_DRDWN_TXN          --季累计发放贷款笔数
          ,TOT_YTD_NBR_LN_DRDWN_TXN          --年累计发放贷款笔数
          ,CUR_CR_AMT                        --贷方发生额        
          ,CUR_DB_AMT                        --借方发生额        
          ,TOT_MTD_CR_AMT                    --月累计贷方发生额  
          ,TOT_MTD_DB_AMT                    --月累计借方发生额  
          ,TOT_QTD_DB_AMT                    --季累计贷方发生额  
          ,TOT_QTD_CR_AMT                    --季累计借方发生额  
          ,TOT_YTD_CR_AMT                    --年累计贷方发生额  
          ,TOT_YTD_DB_AMT                    --年累计借方发生额
          ,OFF_BST_INT_RCVB_WRTOF            --表外应收利息核销金额
          ,OFF_BST_INT_RCVB_RPLC	           --表外应收利息置换金额
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST		 --抵债资产抵债利息收入
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN	   --核销贷款收回利息        
          ,BAL_AMT	            --
          ,MTD_ACML_BAL_AMT		--		月累计余额
          ,QTD_ACML_BAL_AMT		--		季累计余额
          ,YTD_ACML_BAL_AMT		--		年累计余额
          ,NOCLD_In_MTH				--		月日历天数
          ,NOD_In_MTH					--		月有效天数
          ,NOCLD_In_QTR				--		季日历天数
          ,NOD_In_QTR					--		季有效天数
          ,NOCLD_In_Year				--		年日历天数
          ,NOD_In_Year					--		年有效天数
          ,CUR_WRTOF_AMT_RCVD            --当天收回核销金额
          ,CUR_AMT_RCVD_Of_AST_RPLC      --当天收回置换资产金额
          ,CUR_WRTOF_AMT                 --当天核销金额
          ,TOT_MTD_WRTOF_AMT_RCVD        --月累计收回核销金额
          ,TOT_MTD_AMT_RCVD_Of_AST_RPLC  --月累计收回置换资产金额
          ,TOT_MTD_WRTOF_AMT             --月累计核销金额
          ,TOT_QTD_WRTOF_AMT_RCVD        --季累计收回核销金额
          ,TOT_QTD_AMT_RCVD_Of_AST_RPLC  --季累计收回置换资产金额
          ,TOT_QTD_WRTOF_AMT             --季累计核销金额
          ,TOT_YTD_WRTOF_AMT_RCVD        --年累计收回核销金额
          ,TOT_YTD_AMT_RCVD_Of_AST_RPLC  --年累计置换资产金额
          ,TOT_YTD_WRTOF_AMT             --年累计核销金额                  
          ) 
    SELECT 
           CTR_AR_ID                         --合同号            
          ,CTR_ITM_ORDR_ID                   --合同序号          
          ,CDR_YR                            --日历年            
          ,CDR_MTH                           --日历月            
          ,ACG_DT                            --会计日期          
          ,LN_AR_ID                          --账户号            
          ,DNMN_CCY_ID                       --币种ID            
          ,YTD_ON_BST_INT_AMT_RCVD           --表内实收利息      
          ,YTD_OFF_BST_INT_AMT_RCVD          --表外实收利息      
          ,ON_BST_INT_RCVB                   --表内应收未收利息  
          ,OFF_BST_INT_RCVB                  --表外应收未收利息  
          ,TOT_YTD_AMT_OF_INT_INCM           --利息收入          
          ,LN_DRDWN_AMT                      --当天累放金额      
          ,AMT_LN_REPYMT_RCVD                --当天累收金额      
          ,TOT_MTD_LN_DRDWN_AMT              --月贷款累计发放金额
          ,TOT_QTD_LN_DRDWN_AMT              --季贷款累计发放金额
          ,TOT_YTD_LN_DRDWN_AMT              --年贷款累计发放金额
          ,TOT_MTD_AMT_LN_REPYMT_RCVD        --月累计收回贷款金额
          ,TOT_QTD_AMT_LN_RPYMT_RCVD         --季累计收回贷款金额
          ,TOT_YTD_AMT_LN_REPYMT_RCVD        --年累计收回贷款金额
          ,TOT_MTD_NBR_LN_RCVD_TXN           --月累计收回贷款笔数
          ,TOT_QTD_NBR_LN_RCVD_TXN           --季累计收回贷款笔数
          ,TOT_YTD_NBR_LN_RCVD_TXN           --年累计收回贷款笔数
          ,TOT_MTD_NBR_LN_DRDWNTXN           --月累计发放贷款笔数
          ,TOT_QTD_NBR_LN_DRDWN_TXN          --季累计发放贷款笔数
          ,TOT_YTD_NBR_LN_DRDWN_TXN          --年累计发放贷款笔数
          ,CUR_CR_AMT                        --贷方发生额        
          ,CUR_DB_AMT                        --借方发生额        
          ,TOT_MTD_CR_AMT                    --月累计贷方发生额  
          ,TOT_MTD_DB_AMT                    --月累计借方发生额  
          ,TOT_QTD_DB_AMT                    --季累计贷方发生额  
          ,TOT_QTD_CR_AMT                    --季累计借方发生额  
          ,TOT_YTD_CR_AMT                    --年累计贷方发生额  
          ,TOT_YTD_DB_AMT                    --年累计借方发生额
          ,OFF_BST_INT_RCVB_WRTOF            --表外应收利息核销金额
          ,OFF_BST_INT_RCVB_RPLC	           --表外应收利息置换金额
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST		 --抵债资产抵债利息收入
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN	   --核销贷款收回利息
          ,BAL_AMT	            --
          ,MTD_ACML_BAL_AMT		--		月累计余额
          ,QTD_ACML_BAL_AMT		--		季累计余额
          ,YTD_ACML_BAL_AMT		--		年累计余额
          ,NOCLD_In_MTH				--		月日历天数
          ,NOD_In_MTH					--		月有效天数
          ,NOCLD_In_QTR				--		季日历天数
          ,NOD_In_QTR					--		季有效天数
          ,NOCLD_In_Year				--		年日历天数
          ,NOD_In_Year					--		年有效天数
          ,CUR_WRTOF_AMT_RCVD            --当天收回核销金额
          ,CUR_AMT_RCVD_Of_AST_RPLC      --当天收回置换资产金额
          ,CUR_WRTOF_AMT                 --当天核销金额
          ,TOT_MTD_WRTOF_AMT_RCVD        --月累计收回核销金额
          ,TOT_MTD_AMT_RCVD_Of_AST_RPLC  --月累计收回置换资产金额
          ,TOT_MTD_WRTOF_AMT             --月累计核销金额
          ,TOT_QTD_WRTOF_AMT_RCVD        --季累计收回核销金额
          ,TOT_QTD_AMT_RCVD_Of_AST_RPLC  --季累计收回置换资产金额
          ,TOT_QTD_WRTOF_AMT             --季累计核销金额
          ,TOT_YTD_WRTOF_AMT_RCVD        --年累计收回核销金额
          ,TOT_YTD_AMT_RCVD_Of_AST_RPLC  --年累计置换资产金额
          ,TOT_YTD_WRTOF_AMT             --年累计核销金额              					       
     --FROM SMY.LN_AR_INT_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = LAST_MONTH;
     FROM SMY.LN_AR_INT_MTHLY_SMY WHERE ACG_DT >= MTH_FIRST_DAY AND ACG_DT <= MTH_LAST_DAY;   --取当月数据
  END IF;
END IF;
----modify by wuzhanshan end	
       
      
 /** 收集操作信息 */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	SET SMY_STEPNUM = SMY_STEPNUM + 1;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	           --
		
		SET SMY_STEPDESC = '声明临时表SESSION.TMP_LN_TXN, 并往其中插入查询SOR.LN_TXN_DTL_INF中会计日期为当天的数据'; 		--
  /*声明LN_TXN_DTL_INF 临时表 */
   DECLARE GLOBAL TEMPORARY TABLE TMP_LN_TXN AS 
(
  	 	 SELECT  
  	 	 			 CTR_AR_ID   AS CTR_AR_ID        --合同号     
  	 	 			,CTR_SEQ_NBR AS CTR_SEQ_NBR  --合同序号
           ,SUM(CASE WHEN INT_BSH >0 AND INT_OFF_BSH + INT_BSH <>0  AND JRL_AC_F=17320001 THEN INT_BSH  WHEN JRL_AC_F=17320003 THEN INT_BSH*-1 ELSE 0 END )  AS ON_BST_INT_AMT_RCVD      --当日表内实收利息
           ,SUM(CASE WHEN INT_OFF_BSH >0 AND INT_OFF_BSH + INT_BSH <>0 AND JRL_AC_F=17320001 THEN INT_OFF_BSH ELSE 0 END )  AS OFF_BST_INT_AMT_RCVD     --当日表外实收利息
           , SUM(( CASE WHEN DSC_TP_ID NOT IN (16030161,16030165,16030286,16030287,16030288,16030282,16030283,16030284,16030285) AND JRL_AC_F = 17320001  THEN CMPD_INT_BSH + CMPD_INT_OFF_BSH + CRN_PR_INT_AMT ELSE 0 END )   -- 16030286:收核销本金 16030287:收核销利息 16030288:收核销本息 16030282:抵债本金 16030283:抵债利息 16030284:抵债本息 16030285:抵债处置 16030161,16030165摘要码已停用
           - ( CASE WHEN INT_BSH < 0  AND JRL_AC_F=17320001 THEN INT_BSH ELSE 0 END ) 
           + ( CASE WHEN INT_OFF_BSH > 0  AND DSC_TP_ID NOT IN (16030161,16030165,16030286,16030287,16030288,16030282,16030283,16030284,16030285)  AND JRL_AC_F=17320001 THEN INT_OFF_BSH ELSE 0 END )   -- 16030286:收核销本金 16030287:收核销利息 16030288:收核销本息 16030282:抵债本金 16030283:抵债利息 16030284:抵债本息 16030285:抵债处置 16030161,16030165摘要码已停用
           - ( CASE WHEN INT_BSH > 0 AND INT_BSH+INT_OFF_BSH=0  AND JRL_AC_F=17320001 THEN INT_BSH ELSE 0 END ) 
           - ( CASE WHEN JRL_AC_F=17320003  THEN INT_BSH ELSE 0 END) )
           	                         AS AMT_OF_INT_INCM        --利息收入 -- 当日的
           ,SUM(CASE WHEN DSC_TP_ID in (16030158,16030265) AND JRL_AC_F=17320001  THEN TXN_PNP_AMT ELSE 0 END)  AS LN_DRDWN_AMT           --当天累放金额 16030265: 柜面放款 16030158摘要码已停用
           ,SUM(CASE WHEN DSC_TP_ID IN (16030156,16030006,16030272,16030274,16030269,16030271)  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT ELSE 0 END)       AS AMT_LN_REPYMT_RCVD     --当天累收金额   16030272:自动收本金 16030274:自动收本息 16030269:柜面收本金 16030271:柜面收本息 16030156,16030006摘要码已停用
        	 ,SUM(CASE WHEN DSC_TP_ID in (16030158,16030265) THEN 1 ELSE 0 END)            AS NBR_LN_DRDWNTXN        --当日发放贷款笔数 16030265:柜面放款 16030158摘要码已停用
        	 ,SUM(CASE WHEN DSC_TP_ID IN (16030156,16030006,16030272,16030274,16030269,16030271) THEN 1 ELSE 0 END)                     AS NBR_LN_RCVD_TXN        --当日收回贷款笔数  16030156:还贷,16030006:销户 16030156,16030006摘要码已停用
           ,SUM(CASE WHEN CR_DB_IND=14280002  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT ELSE 0 END)         AS CUR_CR_AMT             --贷方发生额 14280002:贷方
           ,SUM(CASE WHEN CR_DB_IND=14280001  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT ELSE 0 END)         AS CUR_DB_AMT             --借方发生额 14280001,借
           ,SUM(CASE WHEN DSC_TP_ID IN (16030165,16030282,16030283,16030284,16030285)  AND JRL_AC_F=17320001 THEN INT_OFF_BSH+CMPD_INT_BSH + CMPD_INT_OFF_BSH + CRN_PR_INT_AMT ELSE 0 END )  AS INT_INCM_AMT_DEBT_AST		         --抵债资产抵债利息收入 16030282:抵债本金 16030283:抵债利息 16030284:抵债本息 16030285:抵债处置  当日 16030165摘要码已停用
		       ,SUM(CASE WHEN DSC_TP_ID IN (16030161,16030286,16030287,16030288)  AND JRL_AC_F=17320001 THEN INT_OFF_BSH+CMPD_INT_BSH + CMPD_INT_OFF_BSH + CRN_PR_INT_AMT ELSE 0 END )  AS INT_INCM_RTND_WRTOF_LN	         --核销贷款收回利息  16030286:收核销本金 16030287:收核销利息 16030288:收核销本息	当日		 16030161摘要码已停用
           ,SUM(CASE WHEN DSC_TP_ID IN (16030161,16030105,16030286,16030287,16030288) AND CR_DB_IND= 14280002  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT  ELSE 0 END) AS CUR_WRTOF_AMT_RCVD            --当天收回核销金额 -- 16030286:收核销本金 16030287:收核销利息 16030288:收核销本息  16030105:内转 16030161,16030105摘要码已停用
           ,SUM(CASE WHEN ((DSC_TP_ID =16030157 AND  DSC like ('专项票据资产%')) OR (DSC_TP_ID IN (16030273,16030274,16030270,16030271))) AND CR_DB_IND= 14280002  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT  ELSE 0 END) AS CUR_AMT_RCVD_Of_AST_RPLC      --当天收回置换资产金额  16030273:自动收利息 16030274:自动收本息 16030270:柜面收利息 16030271:柜面收本息 16030157摘要码已停用
           ,SUM(CASE WHEN DSC_TP_ID IN (16030063,16030279,16030280,16030281) AND CR_DB_IND= 14280002  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT  ELSE 0 END) AS CUR_WRTOF_AMT                 --当天核销金额 16030279:核销本金 16030280:核销利息 16030281:核销本息 16030063摘要码已停用
  	 	 FROM  SOR.LN_TXN_DTL_INF
  	 	 GROUP BY 
  	 	 				 CTR_AR_ID,
  	 	 				 CTR_SEQ_NBR
) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K 
     PARTITIONING KEY(CTR_AR_ID,CTR_SEQ_NBR)
   ;--
----add by wuzhansan begin
CREATE UNIQUE INDEX SESSION.IDX_TMP_LN_TXN ON SESSION.TMP_LN_TXN(CTR_AR_ID,CTR_SEQ_NBR);
----add by wuzhansan end

 /*插入临时表*/
 INSERT INTO  SESSION.TMP_LN_TXN
  	   SELECT  
  	 	 			 CTR_AR_ID   AS CTR_AR_ID        --合同号     
  	 	 			,CTR_SEQ_NBR AS CTR_SEQ_NBR  --合同序号
           ,SUM(CASE WHEN INT_BSH >0 AND INT_OFF_BSH + INT_BSH <>0  AND JRL_AC_F=17320001 THEN INT_BSH  WHEN JRL_AC_F=17320003 THEN INT_BSH*-1 ELSE 0 END )  AS ON_BST_INT_AMT_RCVD      --当日表内实收利息
           ,SUM(CASE WHEN INT_OFF_BSH >0 AND INT_OFF_BSH + INT_BSH <>0 AND JRL_AC_F=17320001 THEN INT_OFF_BSH ELSE 0 END )  AS OFF_BST_INT_AMT_RCVD     --当日表外实收利息
           -- start of modification on 2011-08-11
           /* , SUM(( CASE WHEN DSC_TP_ID NOT IN (16030161,16030165) AND JRL_AC_F IN (17320001,17320001)  THEN CMPD_INT_BSH + CMPD_INT_OFF_BSH + CRN_PR_INT_AMT ELSE 0 END )  --16030161:核销收回5106,16030165:抵债资产抵债5110 
           - ( CASE WHEN INT_BSH < 0  AND JRL_AC_F=17320001 THEN INT_BSH ELSE 0 END ) 
           + ( CASE WHEN INT_OFF_BSH > 0  AND DSC_TP_ID NOT IN (16030161,16030165)  AND JRL_AC_F=17320001 THEN INT_OFF_BSH ELSE 0 END )  --16030161:核销收回5106,16030165:抵债资产抵债5110 
           - ( CASE WHEN INT_BSH > 0 AND INT_BSH+INT_OFF_BSH=0  AND JRL_AC_F=17320001 THEN INT_BSH ELSE 0 END ) 
           - ( CASE WHEN JRL_AC_F=17320003  THEN INT_BSH ELSE 0 END) )
           	                         AS AMT_OF_INT_INCM        --利息收入 -- 当日的 */
           , SUM(( CASE WHEN DSC_TP_ID NOT IN (16030161,16030165,16030286,16030287,16030288,16030282,16030283,16030284,16030285) AND JRL_AC_F = 17320001  THEN CMPD_INT_BSH + CMPD_INT_OFF_BSH + CRN_PR_INT_AMT ELSE 0 END )   -- 16030286:收核销本金 16030287:收核销利息 16030288:收核销本息 16030282:抵债本金 16030283:抵债利息 16030284:抵债本息 16030285:抵债处置 16030161,16030165摘要码已停用
           - ( CASE WHEN INT_BSH < 0  AND JRL_AC_F=17320001 THEN INT_BSH ELSE 0 END ) 
           + ( CASE WHEN INT_OFF_BSH > 0  AND DSC_TP_ID NOT IN (16030161,16030165,16030286,16030287,16030288,16030282,16030283,16030284,16030285)  AND JRL_AC_F=17320001 THEN INT_OFF_BSH ELSE 0 END )   -- 16030286:收核销本金 16030287:收核销利息 16030288:收核销本息 16030282:抵债本金 16030283:抵债利息 16030284:抵债本息 16030285:抵债处置 16030161,16030165摘要码已停用
           - ( CASE WHEN INT_BSH > 0 AND INT_BSH+INT_OFF_BSH=0  AND JRL_AC_F=17320001 THEN INT_BSH ELSE 0 END ) 
           - ( CASE WHEN JRL_AC_F=17320003  THEN INT_BSH ELSE 0 END) )
           	                         AS AMT_OF_INT_INCM        --利息收入 -- 当日的
           --------------------------Start of modification on 2009-11-18-------------------------------------------------------------------------------------------------           	                         
           --,SUM(CASE WHEN DSC_TP_ID IN (16030006,16030158) THEN TXN_PNP_AMT ELSE 0 END)  AS LN_DRDWN_AMT           --当天累放金额  16030006:销户,16030158:借款           	
           /* ,SUM(CASE WHEN DSC_TP_ID = 16030158 AND JRL_AC_F=17320001  THEN TXN_PNP_AMT ELSE 0 END)  AS LN_DRDWN_AMT           --当天累放金额  16030158:借款 */
           ,SUM(CASE WHEN DSC_TP_ID in (16030158,16030265) AND JRL_AC_F=17320001  THEN TXN_PNP_AMT ELSE 0 END)  AS LN_DRDWN_AMT           --当天累放金额 16030265: 柜面放款 16030158摘要码已停用
           --,SUM(CASE WHEN DSC_TP_ID = 16030156 THEN TXN_PNP_AMT ELSE 0 END)       AS AMT_LN_REPYMT_RCVD     --当天累收金额     16030156:还贷           	
           /* ,SUM(CASE WHEN DSC_TP_ID IN (16030156,16030006)  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT ELSE 0 END)       AS AMT_LN_REPYMT_RCVD     --当天累收金额     16030156:还贷,16030006:销户 */
           ,SUM(CASE WHEN DSC_TP_ID IN (16030156,16030006,16030272,16030274,16030269,16030271)  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT ELSE 0 END)       AS AMT_LN_REPYMT_RCVD     --当天累收金额   16030272:自动收本金 16030274:自动收本息 16030269:柜面收本金 16030271:柜面收本息 16030156,16030006摘要码已停用
           --,SUM(CASE WHEN DSC_TP_ID IN (16030006,16030158) THEN 1 ELSE 0 END)            AS NBR_LN_RCVD_TXN        --当日收回贷款笔数 16030006:销户,16030158:借款
       ----------------------------------Start on 2009-12-16----------------------------------------------------------------------------------------------------------------------
       /*    ,SUM(CASE WHEN DSC_TP_ID = 16030158 THEN 1 ELSE 0 END)            AS NBR_LN_RCVD_TXN        --当日收回贷款笔数 16030158:借款
           --,SUM(CASE WHEN DSC_TP_ID IN (16030156) THEN 1 ELSE 0 END)                     AS NBR_LN_DRDWNTXN        --当日发放贷款笔数  16030156:还贷
           ,SUM(CASE WHEN DSC_TP_ID IN (16030156,16030006) THEN 1 ELSE 0 END)                     AS NBR_LN_DRDWNTXN        --当日发放贷款笔数  16030156:还贷,16030006:销户
           --------------------------End of modification on 2009-11-18-------------------------------------------------------------------------------------------------
       */
        	 /* ,SUM(CASE WHEN DSC_TP_ID = 16030158 THEN 1 ELSE 0 END)            AS NBR_LN_DRDWNTXN        --当日发放贷款笔数 16030158:借款 */
        	 ,SUM(CASE WHEN DSC_TP_ID in (16030158,16030265) THEN 1 ELSE 0 END)            AS NBR_LN_DRDWNTXN        --当日发放贷款笔数 16030265:柜面放款 16030158摘要码已停用
        	 /* ,SUM(CASE WHEN DSC_TP_ID IN (16030156,16030006) THEN 1 ELSE 0 END)                     AS NBR_LN_RCVD_TXN        --当日收回贷款笔数  16030156:还贷,16030006:销户 */
        	 ,SUM(CASE WHEN DSC_TP_ID IN (16030156,16030006,16030272,16030274,16030269,16030271) THEN 1 ELSE 0 END)                     AS NBR_LN_RCVD_TXN        --当日收回贷款笔数  16030156:还贷,16030006:销户 16030156,16030006摘要码已停用
			 ----------------------------------End on 2009-12-16----------------------------------------------------------------------------------------------------------------------           
           ,SUM(CASE WHEN CR_DB_IND=14280002  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT ELSE 0 END)         AS CUR_CR_AMT             --贷方发生额 14280002:贷方
           ,SUM(CASE WHEN CR_DB_IND=14280001  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT ELSE 0 END)         AS CUR_DB_AMT             --借方发生额 14280001,借
           /* ,SUM(CASE WHEN DSC_TP_ID  =16030165  AND JRL_AC_F=17320001 THEN INT_OFF_BSH+CMPD_INT_BSH + CMPD_INT_OFF_BSH + CRN_PR_INT_AMT ELSE 0 END )  AS INT_INCM_AMT_DEBT_AST		         --抵债资产抵债利息收入 16030165:抵债资产抵债5110  当日 */
           ,SUM(CASE WHEN DSC_TP_ID IN (16030165,16030282,16030283,16030284,16030285)  AND JRL_AC_F=17320001 THEN INT_OFF_BSH+CMPD_INT_BSH + CMPD_INT_OFF_BSH + CRN_PR_INT_AMT ELSE 0 END )  AS INT_INCM_AMT_DEBT_AST		         --抵债资产抵债利息收入 16030282:抵债本金 16030283:抵债利息 16030284:抵债本息 16030285:抵债处置  当日 16030165摘要码已停用
		       /* ,SUM(CASE WHEN DSC_TP_ID  =16030161  AND JRL_AC_F=17320001 THEN INT_OFF_BSH+CMPD_INT_BSH + CMPD_INT_OFF_BSH + CRN_PR_INT_AMT ELSE 0 END )  AS INT_INCM_RTND_WRTOF_LN	         --核销贷款收回利息  16030161:核销收回5106	当日 */
		       ,SUM(CASE WHEN DSC_TP_ID IN (16030161,16030286,16030287,16030288)  AND JRL_AC_F=17320001 THEN INT_OFF_BSH+CMPD_INT_BSH + CMPD_INT_OFF_BSH + CRN_PR_INT_AMT ELSE 0 END )  AS INT_INCM_RTND_WRTOF_LN	         --核销贷款收回利息  16030286:收核销本金 16030287:收核销利息 16030288:收核销本息	当日		 16030161摘要码已停用
           /* ,SUM(CASE WHEN DSC_TP_ID IN (16030161,16030105 ) AND CR_DB_IND= 14280002  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT  ELSE 0 END) AS CUR_WRTOF_AMT_RCVD            --当天收回核销金额 16030161:已核销贷款收回  16030105:内转 */
           ,SUM(CASE WHEN DSC_TP_ID IN (16030161,16030105,16030286,16030287,16030288) AND CR_DB_IND= 14280002  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT  ELSE 0 END) AS CUR_WRTOF_AMT_RCVD            --当天收回核销金额 -- 16030286:收核销本金 16030287:收核销利息 16030288:收核销本息  16030105:内转 16030161,16030105摘要码已停用
           /* ,SUM(CASE WHEN DSC_TP_ID  =16030157 AND CR_DB_IND= 14280002 AND  DSC like ('专项票据资产%')  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT  ELSE 0 END) AS CUR_AMT_RCVD_Of_AST_RPLC      --当天收回置换资产金额  16030157:还息 */
           ,SUM(CASE WHEN ((DSC_TP_ID =16030157 AND  DSC like ('专项票据资产%')) OR (DSC_TP_ID IN (16030273,16030274,16030270,16030271))) AND CR_DB_IND= 14280002  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT  ELSE 0 END) AS CUR_AMT_RCVD_Of_AST_RPLC      --当天收回置换资产金额  16030273:自动收利息 16030274:自动收本息 16030270:柜面收利息 16030271:柜面收本息 16030157摘要码已停用
           /* ,SUM(CASE WHEN DSC_TP_ID  =16030063 AND CR_DB_IND= 14280002  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT  ELSE 0 END) AS CUR_WRTOF_AMT                 --当天核销金额 16030063:核销 */
           ,SUM(CASE WHEN DSC_TP_ID IN (16030063,16030279,16030280,16030281) AND CR_DB_IND= 14280002  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT  ELSE 0 END) AS CUR_WRTOF_AMT                 --当天核销金额 16030279:核销本金 16030280:核销利息 16030281:核销本息 16030063摘要码已停用
  	 	 		-- end of modification on 2011-08-11
  	 	 FROM  SOR.LN_TXN_DTL_INF
  	 	 WHERE    TXN_DT = ACCOUNTING_DATE   -- 当日会计日期
            AND DEL_F=0
  	 	 GROUP BY 
  	 	 				 CTR_AR_ID,
  	 	 				 CTR_SEQ_NBR
  	 	;--
 /*  */ 	 	
 		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		SET SMY_STEPNUM = SMY_STEPNUM + 1;--
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	  --
	   	 	
		
		SET SMY_STEPDESC = '声明临时表SESSION.TMP_LN_INT_INF, 并往其中插入 以LN_AR_ID 分组的SOR.LN_INT_INF中的数据'; --
  	 	
DECLARE GLOBAL TEMPORARY TABLE TMP_LN_INT_INF AS 
  (  	 	 	 
  		SELECT 
            LN_AR_ID
           ,SUM(CASE WHEN ON_OFF_BSH_IND=1 AND STL_FLG = 15430007 THEN BAL_AMT ELSE 0 END )  AS ON_BST_INT_RCVB         --表内应收未收利息 1:表内; 15430007,未结清
           ,SUM(CASE WHEN ON_OFF_BSH_IND=0 AND STL_FLG = 15430007 THEN BAL_AMT ELSE 0 END )  AS OFF_BST_INT_RCVB        --表外应收未收利息 0:表外; 15430007:未结清
           ,SUM(CASE WHEN ON_OFF_BSH_IND=0 AND STL_FLG = 15430009 THEN BAL_AMT ELSE 0 END )  AS OFF_BST_INT_RCVB_WRTOF  --表外应收利息核销金额 0:表外;15430009:核销
           ,SUM(CASE WHEN ON_OFF_BSH_IND=0 AND STL_FLG = 15430010 THEN BAL_AMT ELSE 0 END )  AS OFF_BST_INT_RCVB_RPLC	  --表外应收利息置换金额 0:表外;15430010:置换  	 	 
  	 	   FROM SOR.LN_INT_INF  
  	 	   WHERE DEL_F=0	 	   
  	 	   GROUP BY 
  	 	   		  LN_AR_ID	 
  ) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K 
     PARTITIONING KEY(LN_AR_ID);--
----add by wuzhansan begin
CREATE UNIQUE INDEX SESSION.IDX_TMP_LN_INT_INF ON SESSION.TMP_LN_INT_INF(LN_AR_ID);
----add by wuzhansan end
/**/

 INSERT INTO SESSION.TMP_LN_INT_INF  	 	
  	 	 	 SELECT 
  	 	 	LN_AR_ID
           ,SUM(CASE WHEN ON_OFF_BSH_IND=1 AND STL_FLG = 15430007 THEN BAL_AMT ELSE 0 END )  AS ON_BST_INT_RCVB         --表内应收未收利息 1:表内; 15430007,未结清
           ,SUM(CASE WHEN ON_OFF_BSH_IND=0 AND STL_FLG = 15430007 THEN BAL_AMT ELSE 0 END )  AS OFF_BST_INT_RCVB        --表外应收未收利息 0:表外; 15430007:未结清
           ,SUM(CASE WHEN ON_OFF_BSH_IND=0 AND STL_FLG = 15430009 THEN BAL_AMT ELSE 0 END )  AS OFF_BST_INT_RCVB_WRTOF  --表外应收利息核销金额 0:表外;15430009:核销
           ,SUM(CASE WHEN ON_OFF_BSH_IND=0 AND STL_FLG = 15430010 THEN BAL_AMT ELSE 0 END )  AS OFF_BST_INT_RCVB_RPLC	  --表外应收利息置换金额 0:表外;15430010:置换  	 	 
  	 	   FROM SOR.LN_INT_INF
  	 	   WHERE DEL_F=0  	 	   
  	 	   GROUP BY 
  	 	   	    LN_AR_ID	 
 ;--
 /** Insert the log**/
 		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		SET SMY_STEPNUM = SMY_STEPNUM + 1;--
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	 --
		
		SET SMY_STEPDESC = '声明临时表SESSION.CUR, 存放SMY_LN_AR_INT_SMY 当日汇总后的数据'; 		--
		
/* 当前最新的数据统计*/
DECLARE GLOBAL TEMPORARY TABLE CUR AS (
  	 	 	 SELECT 
            LOAN_AR_SMY.CTR_AR_ID             AS CTR_AR_ID          --合同号
           ,LOAN_AR_SMY.CTR_ITM_ORDR_ID       AS CTR_ITM_ORDR_ID    --合同序号
           ,LOAN_AR_SMY.LN_AR_ID       AS LN_AR_ID                  --账户号
           ,LOAN_AR_SMY.DNMN_CCY_ID    AS DNMN_CCY_ID               --币种ID
           ,COALESCE(LN_TXN.ON_BST_INT_AMT_RCVD   ,0)                   AS ON_BST_INT_AMT_RCVD   --表内实收利息
           ,COALESCE(LN_TXN.OFF_BST_INT_AMT_RCVD  ,0)                   AS OFF_BST_INT_AMT_RCVD  --表外实收利息
           ,COALESCE(LN_INT_INF.ON_BST_INT_RCVB   ,0)                   AS ON_BST_INT_RCVB           --表内应收未收利息
           ,COALESCE(LN_INT_INF.OFF_BST_INT_RCVB  ,0)                   AS OFF_BST_INT_RCVB          --表外应收未收利息
           ,COALESCE(LN_TXN.AMT_OF_INT_INCM      ,0)                    AS AMT_OF_INT_INCM   --利息收入
           ,COALESCE(LN_TXN.LN_DRDWN_AMT         ,0)                    AS LN_DRDWN_AMT              --当天累放金额
           ,COALESCE(LN_TXN.AMT_LN_REPYMT_RCVD   ,0)                    AS AMT_LN_REPYMT_RCVD        --当天累收金额
           ,COALESCE(LN_TXN.NBR_LN_RCVD_TXN      ,0)                    AS NBR_LN_RCVD_TXN           --日累计收回贷款笔数
           ,COALESCE(LN_TXN.NBR_LN_DRDWNTXN      ,0)                    AS NBR_LN_DRDWN_TXN           --日累计发放贷款笔数
           ,COALESCE(LN_TXN.CUR_CR_AMT           ,0)                    AS CUR_CR_AMT                --贷方发生额
           ,COALESCE(LN_TXN.CUR_DB_AMT           ,0)                    AS CUR_DB_AMT                --借方发生额
           ,COALESCE(LN_INT_INF.OFF_BST_INT_RCVB_WRTOF  ,0)             AS OFF_BST_INT_RCVB_WRTOF    --表外应收利息核销金额
           ,COALESCE(LN_INT_INF.OFF_BST_INT_RCVB_RPLC	  ,0)            AS OFF_BST_INT_RCVB_RPLC	   --表外应收利息置换金额
           ,COALESCE(LN_TXN.INT_INCM_AMT_DEBT_AST	   ,0)               AS INT_INCM_AMT_DEBT_AST		 --抵债资产抵债利息收入
					 ,COALESCE(LN_TXN.INT_INCM_RTND_WRTOF_LN   ,0)                AS INT_INCM_RTND_WRTOF_LN	   --核销贷款收回利息
					 ,LOAN_AR_SMY.LN_BAL                             AS BAL_AMT
					 ,1   AS CUR_AR_FLAG --账户是否正常
           ,COALESCE(LN_TXN.CUR_WRTOF_AMT_RCVD        ,0)               AS CUR_WRTOF_AMT_RCVD            --当天收回核销金额
           ,COALESCE(LN_TXN.CUR_AMT_RCVD_Of_AST_RPLC  ,0)               AS CUR_AMT_RCVD_Of_AST_RPLC      --当天收回置换资产金额
           ,COALESCE(LN_TXN.CUR_WRTOF_AMT             ,0)               AS CUR_WRTOF_AMT                 --当天核销金额					 
   	   FROM		SMY.LOAN_AR_SMY    AS LOAN_AR_SMY        
        	LEFT OUTER JOIN	SESSION.TMP_LN_TXN AS LN_TXN   ON LOAN_AR_SMY.CTR_AR_ID = LN_TXN.CTR_AR_ID    AND LOAN_AR_SMY.CTR_ITM_ORDR_ID = LN_TXN.CTR_SEQ_NBR
        	LEFT OUTER JOIN SESSION.TMP_LN_INT_INF  AS LN_INT_INF ON LOAN_AR_SMY.LN_AR_ID  = LN_INT_INF.LN_AR_ID   -- LOAN_AR_SMY.LN_AR_ID 可能为空
--				 WHERE LOAN_AR_SMY.AR_LCS_TP_ID= 13360003 -- 正常	 	 
  ) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K 
     PARTITIONING KEY(CTR_AR_ID,CTR_ITM_ORDR_ID) ;--
     
----add by wuzhansan begin
CREATE  INDEX SESSION.IDX_CUR ON SESSION.CUR(CTR_AR_ID,CTR_ITM_ORDR_ID);
----add by wuzhansan end

/*声明LOAN_AR_SMY 临时表的数据*/
	DECLARE GLOBAL TEMPORARY TABLE TMP_LOAN_AR_SMY 
		LIKE SMY.LOAN_AR_SMY
	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(CTR_AR_ID,CTR_ITM_ORDR_ID);--
	
	CREATE INDEX SESSION.IDX_LN_AR_ID ON SESSION.TMP_LOAN_AR_SMY(LN_AR_ID);--
----add by wuzhansan begin
	CREATE INDEX SESSION.IDX_CTR_AR_ID ON SESSION.TMP_LOAN_AR_SMY(CTR_AR_ID,CTR_ITM_ORDR_ID);
----add by wuzhansan end
	
	INSERT INTO SESSION.TMP_LOAN_AR_SMY SELECT * FROM SMY.LOAN_AR_SMY;--
     
/**/
INSERT INTO SESSION.CUR
  	 	 	 SELECT 
            LOAN_AR_SMY.CTR_AR_ID             AS CTR_AR_ID          --合同号
           ,LOAN_AR_SMY.CTR_ITM_ORDR_ID       AS CTR_ITM_ORDR_ID    --合同序号
           ,LOAN_AR_SMY.LN_AR_ID       AS LN_AR_ID                  --账户号
           ,LOAN_AR_SMY.DNMN_CCY_ID    AS DNMN_CCY_ID               --币种ID
           ,COALESCE(LN_TXN.ON_BST_INT_AMT_RCVD        ,0)             AS ON_BST_INT_AMT_RCVD   --表内实收利息
           ,COALESCE(LN_TXN.OFF_BST_INT_AMT_RCVD       ,0)             AS OFF_BST_INT_AMT_RCVD  --表外实收利息
           ,COALESCE(LN_INT_INF.ON_BST_INT_RCVB        ,0)             AS ON_BST_INT_RCVB           --表内应收未收利息
           ,COALESCE(LN_INT_INF.OFF_BST_INT_RCVB       ,0)             AS OFF_BST_INT_RCVB          --表外应收未收利息
           ,COALESCE(LN_TXN.AMT_OF_INT_INCM            ,0)             AS AMT_OF_INT_INCM   --利息收入
           ,COALESCE(LN_TXN.LN_DRDWN_AMT               ,0)             AS LN_DRDWN_AMT              --当天累放金额
           ,COALESCE(LN_TXN.AMT_LN_REPYMT_RCVD         ,0)             AS AMT_LN_REPYMT_RCVD        --当天累收金额
           ,COALESCE(LN_TXN.NBR_LN_RCVD_TXN            ,0)             AS NBR_LN_RCVD_TXN           --日累计收回贷款笔数
           ,COALESCE(LN_TXN.NBR_LN_DRDWNTXN            ,0)             AS NBR_LN_DRDWN_TXN           --日累计发放贷款笔数
           ,COALESCE(LN_TXN.CUR_CR_AMT                 ,0)             AS CUR_CR_AMT                --贷方发生额
           ,COALESCE(LN_TXN.CUR_DB_AMT                 ,0)             AS CUR_DB_AMT                --借方发生额
           ,COALESCE(LN_INT_INF.OFF_BST_INT_RCVB_WRTOF ,0)             AS OFF_BST_INT_RCVB_WRTOF    --表外应收利息核销金额
           ,COALESCE(LN_INT_INF.OFF_BST_INT_RCVB_RPLC	 ,0)            AS OFF_BST_INT_RCVB_RPLC	   --表外应收利息置换金额
           ,COALESCE(LN_TXN.INT_INCM_AMT_DEBT_AST	     ,0)            AS INT_INCM_AMT_DEBT_AST		 --抵债资产抵债利息收入
					 ,COALESCE(LN_TXN.INT_INCM_RTND_WRTOF_LN     ,0)             AS INT_INCM_RTND_WRTOF_LN	   --核销贷款收回利息
					 ,COALESCE(LOAN_AR_SMY.LN_BAL   ,0)                          AS BAL_AMT
					 --------------------------------------Start on 20100106------------------------------------------
					 --,1                                                         AS CUR_AR_FLAG --账户是否正常	
					,case when LOAN_AR_SMY.END_DT=SMY_DATE then 0 else 1 end     AS CUR_AR_FLAG --账户是否正常	
					 --------------------------------------End on 20100106------------------------------------------
           ,COALESCE(LN_TXN.CUR_WRTOF_AMT_RCVD          ,0)                       AS CUR_WRTOF_AMT_RCVD            --当天收回核销金额
           ,COALESCE(LN_TXN.CUR_AMT_RCVD_Of_AST_RPLC    ,0)                       AS CUR_AMT_RCVD_Of_AST_RPLC      --当天收回置换资产金额
           ,COALESCE(LN_TXN.CUR_WRTOF_AMT               ,0)                       AS CUR_WRTOF_AMT                 --当天核销金额						 				 
   	   FROM		SESSION.TMP_LOAN_AR_SMY   AS LOAN_AR_SMY        
        	LEFT OUTER JOIN	SESSION.TMP_LN_TXN AS LN_TXN   ON LOAN_AR_SMY.CTR_AR_ID = LN_TXN.CTR_AR_ID    AND LOAN_AR_SMY.CTR_ITM_ORDR_ID = LN_TXN.CTR_SEQ_NBR
        	LEFT OUTER JOIN SESSION.TMP_LN_INT_INF  AS LN_INT_INF ON LOAN_AR_SMY.LN_AR_ID  = LN_INT_INF.LN_AR_ID   -- LOAN_AR_SMY.LN_AR_ID 可能为空
				 -------------------------Start on 20100119----------------------------------------------------------------
				 --Remove all the conditional statements on the AR_LCS_TP_ID
				 /*
				 WHERE LOAN_AR_SMY.AR_LCS_TP_ID= 13360003 -- 账户状态类型：正常	
				       --------------------------------------Start on 20100106------------------------------------------
				       --将当天销户的记录也包括进来
				       or
				       LOAN_AR_SMY.END_DT = SMY_DATE
				       --------------------------------------End on 20100106------------------------------------------
         */
         -------------------------End on 20100119----------------------------------------------------------------				       
    ;--
  
 /** Insert the log**/

 		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		SET SMY_STEPNUM = SMY_STEPNUM + 1;--
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	     --
	 
DECLARE GLOBAL TEMPORARY TABLE TMP_TMP AS 
(
    SELECT
          CTR_AR_ID
          ,CTR_ITM_ORDR_ID
          ,CDR_YR
          ,CDR_MTH
	        ,ACG_DT
          ,LN_AR_ID
          ,DNMN_CCY_ID
          ,YTD_ON_BST_INT_AMT_RCVD
          ,YTD_OFF_BST_INT_AMT_RCVD
          ,ON_BST_INT_RCVB
          ,OFF_BST_INT_RCVB
          ,TOT_YTD_AMT_OF_INT_INCM
          ,LN_DRDWN_AMT
          ,AMT_LN_REPYMT_RCVD
          ,TOT_MTD_LN_DRDWN_AMT
          ,TOT_QTD_LN_DRDWN_AMT
          ,TOT_YTD_LN_DRDWN_AMT
          ,TOT_MTD_AMT_LN_REPYMT_RCVD
          ,TOT_QTD_AMT_LN_RPYMT_RCVD
          ,TOT_YTD_AMT_LN_REPYMT_RCVD
          ,TOT_MTD_NBR_LN_RCVD_TXN
          ,TOT_QTD_NBR_LN_RCVD_TXN
          ,TOT_YTD_NBR_LN_RCVD_TXN
          ,TOT_MTD_NBR_LN_DRDWNTXN
          ,TOT_QTD_NBR_LN_DRDWN_TXN
          ,TOT_YTD_NBR_LN_DRDWN_TXN
          ,CUR_CR_AMT
          ,CUR_DB_AMT
          ,TOT_MTD_CR_AMT
          ,TOT_MTD_DB_AMT
          ,TOT_QTD_DB_AMT
          ,TOT_QTD_CR_AMT
          ,TOT_YTD_CR_AMT
          ,TOT_YTD_DB_AMT
          ,OFF_BST_INT_RCVB_WRTOF
          ,OFF_BST_INT_RCVB_RPLC
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN
					,BAL_AMT
          ,MTD_ACML_BAL_AMT
          ,QTD_ACML_BAL_AMT
          ,YTD_ACML_BAL_AMT
          ,NOCLD_In_MTH
          ,NOD_IN_MTH
          ,NOCLD_IN_QTR
          ,NOD_IN_QTR
          ,NOCLD_IN_YEAR
          ,NOD_IN_YEAR
          ,CUR_WRTOF_AMT_RCVD
          ,CUR_AMT_RCVD_Of_AST_RPLC
          ,CUR_WRTOF_AMT
          ,TOT_MTD_WRTOF_AMT_RCVD
          ,TOT_MTD_AMT_RCVD_Of_AST_RPLC
          ,TOT_MTD_WRTOF_AMT
          ,TOT_QTD_WRTOF_AMT_RCVD
          ,TOT_QTD_AMT_RCVD_Of_AST_RPLC
          ,TOT_QTD_WRTOF_AMT
          ,TOT_YTD_WRTOF_AMT_RCVD
          ,TOT_YTD_AMT_RCVD_Of_AST_RPLC
          ,TOT_YTD_WRTOF_AMT
    FROM SMY.LN_AR_INT_MTHLY_SMY
)DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(CTR_AR_ID,CTR_ITM_ORDR_ID);

CREATE INDEX SESSION.IDX_TMP_TMP ON SESSION.TMP_TMP(CTR_AR_ID,CTR_ITM_ORDR_ID,CDR_YR,CDR_MTH);

INSERT INTO SESSION.TMP_TMP
SELECT  
           CUR.CTR_AR_ID    AS CTR_AR_ID                     --合同号             
          ,CUR.CTR_ITM_ORDR_ID   AS CTR_ITM_ORDR_ID          --合同序号            
          ,CUR_YEAR   AS CDR_YR                              --日历年              
          ,CUR_MONTH  AS CDR_MTH                             --日历月              
          ,ACCOUNTING_DATE AS ACG_DT                         --会计日期            
          ,CUR.LN_AR_ID  AS   LN_AR_ID                       --账户号              
          ,CUR.DNMN_CCY_ID AS DNMN_CCY_ID                    --币种ID              
          ,COALESCE(PRE.YTD_ON_BST_INT_AMT_RCVD,0)  + CUR.ON_BST_INT_AMT_RCVD   AS YTD_ON_BST_INT_AMT_RCVD      --表内实收利息        
          ,COALESCE(PRE.YTD_OFF_BST_INT_AMT_RCVD ,0)  + CUR.OFF_BST_INT_AMT_RCVD  AS YTD_OFF_BST_INT_AMT_RCVD   --表外实收利息        
          ,CUR.ON_BST_INT_RCVB                   AS ON_BST_INT_RCVB  --表内应收未收利息    
          ,CUR.OFF_BST_INT_RCVB                  AS OFF_BST_INT_RCVB --表外应收未收利息    
          ,COALESCE(PRE.TOT_YTD_AMT_OF_INT_INCM,0) + CUR.AMT_OF_INT_INCM   AS TOT_YTD_AMT_OF_INT_INCM       --利息收入            
          ,CUR.LN_DRDWN_AMT                      AS LN_DRDWN_AMT        --当天累放金额        
          ,CUR.AMT_LN_REPYMT_RCVD                AS AMT_LN_REPYMT_RCVD  --当天累收金额        
          ,COALESCE(PRE.TOT_MTD_LN_DRDWN_AMT  ,0) + CUR.LN_DRDWN_AMT AS TOT_MTD_LN_DRDWN_AMT             --月贷款累计发放金额  
          ,COALESCE(PRE.TOT_QTD_LN_DRDWN_AMT  ,0) + CUR.LN_DRDWN_AMT AS TOT_QTD_LN_DRDWN_AMT           --季贷款累计发放金额  
          ,COALESCE(PRE.TOT_YTD_LN_DRDWN_AMT  ,0) + CUR.LN_DRDWN_AMT AS TOT_YTD_LN_DRDWN_AMT           --年贷款累计发放金额  
          ,COALESCE(PRE.TOT_MTD_AMT_LN_REPYMT_RCVD  ,0) + CUR.AMT_LN_REPYMT_RCVD  AS TOT_MTD_AMT_LN_REPYMT_RCVD    --月累计收回贷款金额  
          ,COALESCE(PRE.TOT_QTD_AMT_LN_RPYMT_RCVD   ,0) + CUR.AMT_LN_REPYMT_RCVD  AS TOT_QTD_AMT_LN_RPYMT_RCVD    --季累计收回贷款金额  
          ,COALESCE(PRE.TOT_YTD_AMT_LN_REPYMT_RCVD  ,0) + CUR.AMT_LN_REPYMT_RCVD  AS TOT_YTD_AMT_LN_REPYMT_RCVD   --年累计收回贷款金额  
          ,COALESCE(PRE.TOT_MTD_NBR_LN_RCVD_TXN  ,0) + CUR.NBR_LN_RCVD_TXN        AS TOT_MTD_NBR_LN_RCVD_TXN       --月累计收回贷款笔数  
          ,COALESCE(PRE.TOT_QTD_NBR_LN_RCVD_TXN  ,0) + CUR.NBR_LN_RCVD_TXN        AS TOT_QTD_NBR_LN_RCVD_TXN     --季累计收回贷款笔数  
          ,COALESCE(PRE.TOT_YTD_NBR_LN_RCVD_TXN  ,0) + CUR.NBR_LN_RCVD_TXN        AS TOT_YTD_NBR_LN_RCVD_TXN --年累计收回贷款笔数  
          ,COALESCE(PRE.TOT_MTD_NBR_LN_DRDWNTXN  ,0) + CUR.NBR_LN_DRDWN_TXN       AS TOT_MTD_NBR_LN_DRDWNTXN   --月累计发放贷款笔数  
          ,COALESCE(PRE.TOT_QTD_NBR_LN_DRDWN_TXN ,0) + CUR.NBR_LN_DRDWN_TXN       AS TOT_QTD_NBR_LN_DRDWN_TXN  --季累计发放贷款笔数  
          ,COALESCE(PRE.TOT_YTD_NBR_LN_DRDWN_TXN ,0) + CUR.NBR_LN_DRDWN_TXN       AS TOT_YTD_NBR_LN_DRDWN_TXN --年累计发放贷款笔数  
          ,CUR.CUR_CR_AMT  AS CUR_CR_AMT                      --贷方发生额          
          ,CUR.CUR_DB_AMT  AS CUR_DB_AMT                      --借方发生额          
          ,COALESCE(PRE.TOT_MTD_CR_AMT ,0) + CUR.CUR_CR_AMT  AS TOT_MTD_CR_AMT                 --月累计贷方发生额    
          ,COALESCE(PRE.TOT_MTD_DB_AMT ,0) + CUR.CUR_DB_AMT  AS TOT_MTD_DB_AMT                 --月累计借方发生额    
          ,COALESCE(PRE.TOT_QTD_DB_AMT ,0) + CUR.CUR_DB_AMT  AS TOT_QTD_DB_AMT                 --季累计贷方发生额    
          ,COALESCE(PRE.TOT_QTD_CR_AMT ,0) + CUR.CUR_CR_AMT  AS TOT_QTD_CR_AMT                 --季累计借方发生额    
          ,COALESCE(PRE.TOT_YTD_CR_AMT ,0) + CUR.CUR_CR_AMT  AS TOT_YTD_CR_AMT                 --年累计贷方发生额    
          ,COALESCE(PRE.TOT_YTD_DB_AMT ,0) + CUR.CUR_DB_AMT  AS TOT_YTD_DB_AMT                 --年累计借方发生额    
          ,CUR.OFF_BST_INT_RCVB_WRTOF         AS OFF_BST_INT_RCVB_WRTOF   --表外应收利息核销金额
          ,CUR.OFF_BST_INT_RCVB_RPLC	        AS OFF_BST_INT_RCVB_RPLC   --表外应收利息置换金额
          ,COALESCE(TOT_YTD_INT_INCM_AMT_DEBT_AST		,0) + CUR.INT_INCM_AMT_DEBT_AST	AS TOT_YTD_INT_INCM_AMT_DEBT_AST --抵债资产抵债利息收入
				  ,COALESCE(TOT_YTD_INT_INCM_RTND_WRTOF_LN	,0) + CUR.INT_INCM_RTND_WRTOF_LN  AS TOT_YTD_INT_INCM_RTND_WRTOF_LN --核销贷款收回利息
          ,COALESCE(CUR.BAL_AMT	,0) AS BAL_AMT           --
          ,COALESCE(PRE.MTD_ACML_BAL_AMT	,0) + CUR.BAL_AMT AS MTD_ACML_BAL_AMT	--月累计余额
          ,COALESCE(PRE.QTD_ACML_BAL_AMT	,0) + CUR.BAL_AMT	AS QTD_ACML_BAL_AMT--季累计余额
          ,COALESCE(PRE.YTD_ACML_BAL_AMT	,0) + CUR.BAL_AMT	AS YTD_ACML_BAL_AMT--年累计余额
          ,C_MON_DAY			AS NOCLD_In_MTH	--月日历天数
          ,COALESCE(PRE.NOD_IN_MTH ,0) + CUR.CUR_AR_FLAG AS NOD_In_MTH				--月有效天数
          ,C_QTR_DAY				AS NOCLD_IN_QTR--季日历天数
          ,COALESCE(NOD_IN_QTR ,0)	+ CUR.CUR_AR_FLAG	AS NOD_In_QTR			--季有效天数
          ,C_YR_DAY			AS NOCLD_In_Year--年日历天数
          ,COALESCE(NOD_IN_YEAR,0)	+ CUR.CUR_AR_FLAG	AS NOD_In_Year			--年有效天数
          ,CUR.CUR_WRTOF_AMT_RCVD      AS CUR_WRTOF_AMT_RCVD      --当天收回核销金额
          ,CUR.CUR_AMT_RCVD_Of_AST_RPLC    AS CUR_AMT_RCVD_Of_AST_RPLC  --当天收回置换资产金额
          ,CUR.CUR_WRTOF_AMT    AS CUR_WRTOF_AMT             --当天核销金额
          ,COALESCE(PRE.TOT_MTD_WRTOF_AMT_RCVD       ,0) + CUR.CUR_WRTOF_AMT_RCVD       AS TOT_MTD_WRTOF_AMT_RCVD--月累计收回核销金额
          ,COALESCE(PRE.TOT_MTD_AMT_RCVD_Of_AST_RPLC ,0) + CUR.CUR_AMT_RCVD_Of_AST_RPLC AS TOT_MTD_AMT_RCVD_Of_AST_RPLC--月累计收回置换资产金额
          ,COALESCE(PRE.TOT_MTD_WRTOF_AMT            ,0) + CUR.CUR_WRTOF_AMT            AS TOT_MTD_WRTOF_AMT--月累计核销金额
          ,COALESCE(PRE.TOT_QTD_WRTOF_AMT_RCVD       ,0) + CUR.CUR_WRTOF_AMT_RCVD       AS TOT_QTD_WRTOF_AMT_RCVD--季累计收回核销金额
          ,COALESCE(PRE.TOT_QTD_AMT_RCVD_Of_AST_RPLC ,0) + CUR.CUR_AMT_RCVD_Of_AST_RPLC AS TOT_QTD_AMT_RCVD_Of_AST_RPLC--季累计收回置换资产金额
          ,COALESCE(PRE.TOT_QTD_WRTOF_AMT            ,0) + CUR.CUR_WRTOF_AMT            AS TOT_QTD_WRTOF_AMT--季累计核销金额
          ,COALESCE(PRE.TOT_YTD_WRTOF_AMT_RCVD       ,0) + CUR.CUR_WRTOF_AMT_RCVD       AS TOT_YTD_WRTOF_AMT_RCVD--年累计收回核销金额
          ,COALESCE(PRE.TOT_YTD_AMT_RCVD_Of_AST_RPLC ,0) + CUR.CUR_AMT_RCVD_Of_AST_RPLC AS TOT_YTD_AMT_RCVD_Of_AST_RPLC--年累计置换资产金额
          ,COALESCE(PRE.TOT_YTD_WRTOF_AMT            ,0) + CUR.CUR_WRTOF_AMT            AS TOT_YTD_WRTOF_AMT--年累计核销金额              				   
    
	FROM  SESSION.CUR  AS CUR LEFT OUTER JOIN  SESSION.TMP AS PRE ON 
		          CUR.CTR_AR_ID       =  PRE.CTR_AR_ID      
          AND CUR.CTR_ITM_ORDR_ID =  PRE.CTR_ITM_ORDR_ID
;

IF (ACCOUNTING_DATE =MTH_FIRST_DAY AND MAX_ACG_DT <= ACCOUNTING_DATE) THEN

		SET SMY_STEPDESC = '月初直接插SMY 表'; 
		
		INSERT　INTO SMY.LN_AR_INT_MTHLY_SMY 
    (
           CTR_AR_ID                         --合同号            
          ,CTR_ITM_ORDR_ID                   --合同序号          
          ,CDR_YR                            --日历年            
          ,CDR_MTH                           --日历月        	 
	        ,ACG_DT                        
          ,LN_AR_ID                      
          ,DNMN_CCY_ID                   
          ,YTD_ON_BST_INT_AMT_RCVD       
          ,YTD_OFF_BST_INT_AMT_RCVD      
          ,ON_BST_INT_RCVB               
          ,OFF_BST_INT_RCVB              
          ,TOT_YTD_AMT_OF_INT_INCM       
          ,LN_DRDWN_AMT                  
          ,AMT_LN_REPYMT_RCVD            
          ,TOT_MTD_LN_DRDWN_AMT          
          ,TOT_QTD_LN_DRDWN_AMT          
          ,TOT_YTD_LN_DRDWN_AMT          
          ,TOT_MTD_AMT_LN_REPYMT_RCVD    
          ,TOT_QTD_AMT_LN_RPYMT_RCVD     
          ,TOT_YTD_AMT_LN_REPYMT_RCVD    
          ,TOT_MTD_NBR_LN_RCVD_TXN       
          ,TOT_QTD_NBR_LN_RCVD_TXN       
          ,TOT_YTD_NBR_LN_RCVD_TXN       
          ,TOT_MTD_NBR_LN_DRDWNTXN       
          ,TOT_QTD_NBR_LN_DRDWN_TXN      
          ,TOT_YTD_NBR_LN_DRDWN_TXN      
          ,CUR_CR_AMT                    
          ,CUR_DB_AMT                    
          ,TOT_MTD_CR_AMT                
          ,TOT_MTD_DB_AMT                
          ,TOT_QTD_DB_AMT                
          ,TOT_QTD_CR_AMT                
          ,TOT_YTD_CR_AMT                
          ,TOT_YTD_DB_AMT                
          ,OFF_BST_INT_RCVB_WRTOF        
          ,OFF_BST_INT_RCVB_RPLC	       
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST	
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN 
					,BAL_AMT	        
          ,MTD_ACML_BAL_AMT	
          ,QTD_ACML_BAL_AMT	
          ,YTD_ACML_BAL_AMT	
          ,NOCLD_In_MTH			
          ,NOD_IN_MTH				
          ,NOCLD_IN_QTR			
          ,NOD_IN_QTR				
          ,NOCLD_IN_YEAR		
          ,NOD_IN_YEAR			
          ,CUR_WRTOF_AMT_RCVD            --当天收回核销金额
          ,CUR_AMT_RCVD_Of_AST_RPLC      --当天收回置换资产金额
          ,CUR_WRTOF_AMT                 --当天核销金额
          ,TOT_MTD_WRTOF_AMT_RCVD        --月累计收回核销金额
          ,TOT_MTD_AMT_RCVD_Of_AST_RPLC  --月累计收回置换资产金额
          ,TOT_MTD_WRTOF_AMT             --月累计核销金额
          ,TOT_QTD_WRTOF_AMT_RCVD        --季累计收回核销金额
          ,TOT_QTD_AMT_RCVD_Of_AST_RPLC  --季累计收回置换资产金额
          ,TOT_QTD_WRTOF_AMT             --季累计核销金额
          ,TOT_YTD_WRTOF_AMT_RCVD        --年累计收回核销金额
          ,TOT_YTD_AMT_RCVD_Of_AST_RPLC  --年累计置换资产金额
          ,TOT_YTD_WRTOF_AMT             --年累计核销金额  					
    )
    SELECT * FROM SESSION.TMP_TMP
    ;

ELSE

	SET SMY_STEPDESC = '使用Merge语句,更新SMY 表'; 			         --
                
 MERGE INTO SMY.LN_AR_INT_MTHLY_SMY AS T
 		USING  SESSION.TMP_TMP AS S 
 	  ON  		  T.CTR_AR_ID       =  S.CTR_AR_ID      
          AND T.CTR_ITM_ORDR_ID =  S.CTR_ITM_ORDR_ID
          AND T.CDR_YR          =  S.CDR_YR         
          AND T.CDR_MTH         =  S.CDR_MTH    
WHEN MATCHED THEN UPDATE SET 
           T.ACG_DT                          = S.ACG_DT                         
          ,T.LN_AR_ID                        = S.LN_AR_ID                       
          ,T.DNMN_CCY_ID                     = S.DNMN_CCY_ID                    
          ,T.YTD_ON_BST_INT_AMT_RCVD         = S.YTD_ON_BST_INT_AMT_RCVD        
          ,T.YTD_OFF_BST_INT_AMT_RCVD        = S.YTD_OFF_BST_INT_AMT_RCVD       
          ,T.ON_BST_INT_RCVB                 = S.ON_BST_INT_RCVB                
          ,T.OFF_BST_INT_RCVB                = S.OFF_BST_INT_RCVB               
          ,T.TOT_YTD_AMT_OF_INT_INCM         = S.TOT_YTD_AMT_OF_INT_INCM        
          ,T.LN_DRDWN_AMT                    = S.LN_DRDWN_AMT                   
          ,T.AMT_LN_REPYMT_RCVD              = S.AMT_LN_REPYMT_RCVD             
          ,T.TOT_MTD_LN_DRDWN_AMT            = S.TOT_MTD_LN_DRDWN_AMT           
          ,T.TOT_QTD_LN_DRDWN_AMT            = S.TOT_QTD_LN_DRDWN_AMT           
          ,T.TOT_YTD_LN_DRDWN_AMT            = S.TOT_YTD_LN_DRDWN_AMT           
          ,T.TOT_MTD_AMT_LN_REPYMT_RCVD      = S.TOT_MTD_AMT_LN_REPYMT_RCVD     
          ,T.TOT_QTD_AMT_LN_RPYMT_RCVD       = S.TOT_QTD_AMT_LN_RPYMT_RCVD      
          ,T.TOT_YTD_AMT_LN_REPYMT_RCVD      = S.TOT_YTD_AMT_LN_REPYMT_RCVD     
          ,T.TOT_MTD_NBR_LN_RCVD_TXN         = S.TOT_MTD_NBR_LN_RCVD_TXN        
          ,T.TOT_QTD_NBR_LN_RCVD_TXN         = S.TOT_QTD_NBR_LN_RCVD_TXN        
          ,T.TOT_YTD_NBR_LN_RCVD_TXN         = S.TOT_YTD_NBR_LN_RCVD_TXN        
          ,T.TOT_MTD_NBR_LN_DRDWNTXN         = S.TOT_MTD_NBR_LN_DRDWNTXN        
          ,T.TOT_QTD_NBR_LN_DRDWN_TXN        = S.TOT_QTD_NBR_LN_DRDWN_TXN       
          ,T.TOT_YTD_NBR_LN_DRDWN_TXN        = S.TOT_YTD_NBR_LN_DRDWN_TXN       
          ,T.CUR_CR_AMT                      = S.CUR_CR_AMT                     
          ,T.CUR_DB_AMT                      = S.CUR_DB_AMT                     
          ,T.TOT_MTD_CR_AMT                  = S.TOT_MTD_CR_AMT                 
          ,T.TOT_MTD_DB_AMT                  = S.TOT_MTD_DB_AMT                 
          ,T.TOT_QTD_DB_AMT                  = S.TOT_QTD_DB_AMT                 
          ,T.TOT_QTD_CR_AMT                  = S.TOT_QTD_CR_AMT                 
          ,T.TOT_YTD_CR_AMT                  = S.TOT_YTD_CR_AMT                 
          ,T.TOT_YTD_DB_AMT                  = S.TOT_YTD_DB_AMT                 
          ,T.OFF_BST_INT_RCVB_WRTOF          = S.OFF_BST_INT_RCVB_WRTOF         
          ,T.OFF_BST_INT_RCVB_RPLC	         = S.OFF_BST_INT_RCVB_RPLC	        
          ,T.TOT_YTD_INT_INCM_AMT_DEBT_AST	 = S.TOT_YTD_INT_INCM_AMT_DEBT_AST
					,T.TOT_YTD_INT_INCM_RTND_WRTOF_LN	 = S.TOT_YTD_INT_INCM_RTND_WRTOF_LN 
					,T.BAL_AMT	                       = S.BAL_AMT	       
          ,T.MTD_ACML_BAL_AMT		             = S.MTD_ACML_BAL_AMT
          ,T.QTD_ACML_BAL_AMT		             = S.QTD_ACML_BAL_AMT
          ,T.YTD_ACML_BAL_AMT		             = S.YTD_ACML_BAL_AMT
          ,T.NOCLD_In_MTH				             = S.NOCLD_In_MTH			
          ,T.NOD_In_MTH					             = S.NOD_In_MTH				
          ,T.NOCLD_In_QTR				             = S.NOCLD_In_QTR			
          ,T.NOD_In_QTR					             = S.NOD_In_QTR				
          ,T.NOCLD_In_Year			             = S.NOCLD_In_Year		
          ,T.NOD_In_Year				             = S.NOD_In_Year			
          ,T.CUR_WRTOF_AMT_RCVD              =S.CUR_WRTOF_AMT_RCVD           --当天收回核销金额
          ,T.CUR_AMT_RCVD_Of_AST_RPLC        =S.CUR_AMT_RCVD_Of_AST_RPLC     --当天收回置换资产金额
          ,T.CUR_WRTOF_AMT                   =S.CUR_WRTOF_AMT                --当天核销金额
          ,T.TOT_MTD_WRTOF_AMT_RCVD          =S.TOT_MTD_WRTOF_AMT_RCVD       --月累计收回核销金额
          ,T.TOT_MTD_AMT_RCVD_Of_AST_RPLC    =S.TOT_MTD_AMT_RCVD_Of_AST_RPLC --月累计收回置换资产金额
          ,T.TOT_MTD_WRTOF_AMT               =S.TOT_MTD_WRTOF_AMT            --月累计核销金额
          ,T.TOT_QTD_WRTOF_AMT_RCVD          =S.TOT_QTD_WRTOF_AMT_RCVD       --季累计收回核销金额
          ,T.TOT_QTD_AMT_RCVD_Of_AST_RPLC    =S.TOT_QTD_AMT_RCVD_Of_AST_RPLC --季累计收回置换资产金额
          ,T.TOT_QTD_WRTOF_AMT               =S.TOT_QTD_WRTOF_AMT            --季累计核销金额
          ,T.TOT_YTD_WRTOF_AMT_RCVD          =S.TOT_YTD_WRTOF_AMT_RCVD       --年累计收回核销金额
          ,T.TOT_YTD_AMT_RCVD_Of_AST_RPLC    =S.TOT_YTD_AMT_RCVD_Of_AST_RPLC --年累计置换资产金额
          ,T.TOT_YTD_WRTOF_AMT               =S.TOT_YTD_WRTOF_AMT            --年累计核销金额  
 ;
  INSERT INTO SMY.LN_AR_INT_MTHLY_SMY
  (
          CTR_AR_ID                          --合同号            
          ,CTR_ITM_ORDR_ID                   --合同序号          
          ,CDR_YR                            --日历年            
          ,CDR_MTH                           --日历月        	 
	        ,ACG_DT                        
          ,LN_AR_ID                      
          ,DNMN_CCY_ID                   
          ,YTD_ON_BST_INT_AMT_RCVD       
          ,YTD_OFF_BST_INT_AMT_RCVD      
          ,ON_BST_INT_RCVB               
          ,OFF_BST_INT_RCVB              
          ,TOT_YTD_AMT_OF_INT_INCM       
          ,LN_DRDWN_AMT                  
          ,AMT_LN_REPYMT_RCVD            
          ,TOT_MTD_LN_DRDWN_AMT          
          ,TOT_QTD_LN_DRDWN_AMT          
          ,TOT_YTD_LN_DRDWN_AMT          
          ,TOT_MTD_AMT_LN_REPYMT_RCVD    
          ,TOT_QTD_AMT_LN_RPYMT_RCVD     
          ,TOT_YTD_AMT_LN_REPYMT_RCVD    
          ,TOT_MTD_NBR_LN_RCVD_TXN       
          ,TOT_QTD_NBR_LN_RCVD_TXN       
          ,TOT_YTD_NBR_LN_RCVD_TXN       
          ,TOT_MTD_NBR_LN_DRDWNTXN       
          ,TOT_QTD_NBR_LN_DRDWN_TXN      
          ,TOT_YTD_NBR_LN_DRDWN_TXN      
          ,CUR_CR_AMT                    
          ,CUR_DB_AMT                    
          ,TOT_MTD_CR_AMT                
          ,TOT_MTD_DB_AMT                
          ,TOT_QTD_DB_AMT                
          ,TOT_QTD_CR_AMT                
          ,TOT_YTD_CR_AMT                
          ,TOT_YTD_DB_AMT                
          ,OFF_BST_INT_RCVB_WRTOF        
          ,OFF_BST_INT_RCVB_RPLC	       
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST	
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN 
					,BAL_AMT	        
          ,MTD_ACML_BAL_AMT	
          ,QTD_ACML_BAL_AMT	
          ,YTD_ACML_BAL_AMT	
          ,NOCLD_In_MTH			
          ,NOD_IN_MTH				
          ,NOCLD_IN_QTR			
          ,NOD_IN_QTR				
          ,NOCLD_IN_YEAR		
          ,NOD_IN_YEAR			
          ,CUR_WRTOF_AMT_RCVD            --当天收回核销金额
          ,CUR_AMT_RCVD_Of_AST_RPLC      --当天收回置换资产金额
          ,CUR_WRTOF_AMT                 --当天核销金额
          ,TOT_MTD_WRTOF_AMT_RCVD        --月累计收回核销金额
          ,TOT_MTD_AMT_RCVD_Of_AST_RPLC  --月累计收回置换资产金额
          ,TOT_MTD_WRTOF_AMT             --月累计核销金额
          ,TOT_QTD_WRTOF_AMT_RCVD        --季累计收回核销金额
          ,TOT_QTD_AMT_RCVD_Of_AST_RPLC  --季累计收回置换资产金额
          ,TOT_QTD_WRTOF_AMT             --季累计核销金额
          ,TOT_YTD_WRTOF_AMT_RCVD        --年累计收回核销金额
          ,TOT_YTD_AMT_RCVD_Of_AST_RPLC  --年累计置换资产金额
          ,TOT_YTD_WRTOF_AMT             --年累计核销金额
  )
  SELECT
          S.CTR_AR_ID                          --合同号            
          ,S.CTR_ITM_ORDR_ID                   --合同序号          
          ,S.CDR_YR                            --日历年            
          ,S.CDR_MTH                           --日历月        
			    ,S.ACG_DT                        
          ,S.LN_AR_ID                      
          ,S.DNMN_CCY_ID                   
          ,S.YTD_ON_BST_INT_AMT_RCVD       
          ,S.YTD_OFF_BST_INT_AMT_RCVD      
          ,S.ON_BST_INT_RCVB               
          ,S.OFF_BST_INT_RCVB              
          ,S.TOT_YTD_AMT_OF_INT_INCM       
          ,S.LN_DRDWN_AMT                  
          ,S.AMT_LN_REPYMT_RCVD            
          ,S.TOT_MTD_LN_DRDWN_AMT          
          ,S.TOT_QTD_LN_DRDWN_AMT          
          ,S.TOT_YTD_LN_DRDWN_AMT          
          ,S.TOT_MTD_AMT_LN_REPYMT_RCVD    
          ,S.TOT_QTD_AMT_LN_RPYMT_RCVD     
          ,S.TOT_YTD_AMT_LN_REPYMT_RCVD    
          ,S.TOT_MTD_NBR_LN_RCVD_TXN       
          ,S.TOT_QTD_NBR_LN_RCVD_TXN       
          ,S.TOT_YTD_NBR_LN_RCVD_TXN       
          ,S.TOT_MTD_NBR_LN_DRDWNTXN       
          ,S.TOT_QTD_NBR_LN_DRDWN_TXN      
          ,S.TOT_YTD_NBR_LN_DRDWN_TXN      
          ,S.CUR_CR_AMT                    
          ,S.CUR_DB_AMT                    
          ,S.TOT_MTD_CR_AMT                
          ,S.TOT_MTD_DB_AMT                
          ,S.TOT_QTD_DB_AMT                
          ,S.TOT_QTD_CR_AMT                
          ,S.TOT_YTD_CR_AMT                
          ,S.TOT_YTD_DB_AMT                
          ,S.OFF_BST_INT_RCVB_WRTOF        
          ,S.OFF_BST_INT_RCVB_RPLC	       
          ,S.TOT_YTD_INT_INCM_AMT_DEBT_AST 
          ,S.TOT_YTD_INT_INCM_RTND_WRTOF_LN
					,S.BAL_AMT	        
          ,S.MTD_ACML_BAL_AMT	
          ,S.QTD_ACML_BAL_AMT	
          ,S.YTD_ACML_BAL_AMT	
          ,S.NOCLD_In_MTH			
          ,S.NOD_In_MTH				
          ,S.NOCLD_In_QTR			
          ,S.NOD_In_QTR				
          ,S.NOCLD_In_Year		
          ,S.NOD_In_Year			
          ,S.CUR_WRTOF_AMT_RCVD            --当天收回核销金额
          ,S.CUR_AMT_RCVD_Of_AST_RPLC      --当天收回置换资产金额
          ,S.CUR_WRTOF_AMT                 --当天核销金额
          ,S.TOT_MTD_WRTOF_AMT_RCVD        --月累计收回核销金额
          ,S.TOT_MTD_AMT_RCVD_Of_AST_RPLC  --月累计收回置换资产金额
          ,S.TOT_MTD_WRTOF_AMT             --月累计核销金额
          ,S.TOT_QTD_WRTOF_AMT_RCVD        --季累计收回核销金额
          ,S.TOT_QTD_AMT_RCVD_Of_AST_RPLC  --季累计收回置换资产金额
          ,S.TOT_QTD_WRTOF_AMT             --季累计核销金额
          ,S.TOT_YTD_WRTOF_AMT_RCVD        --年累计收回核销金额
          ,S.TOT_YTD_AMT_RCVD_Of_AST_RPLC  --年累计置换资产金额
          ,S.TOT_YTD_WRTOF_AMT
  FROM SESSION.TMP_TMP as S
    WHERE NOT EXISTS (
         SELECT 1 FROM SMY.LN_AR_INT_MTHLY_SMY T
         WHERE T.CTR_AR_ID           =  S.CTR_AR_ID      
               AND T.CTR_ITM_ORDR_ID =  S.CTR_ITM_ORDR_ID
               AND T.CDR_YR          =  S.CDR_YR         
               AND T.CDR_MTH         =  S.CDR_MTH
    )
	;
/*WHEN NOT MATCHED THEN INSERT  
	        
	 (      
           CTR_AR_ID                         --合同号            
          ,CTR_ITM_ORDR_ID                   --合同序号          
          ,CDR_YR                            --日历年            
          ,CDR_MTH                           --日历月        	 
	        ,ACG_DT                        
          ,LN_AR_ID                      
          ,DNMN_CCY_ID                   
          ,YTD_ON_BST_INT_AMT_RCVD       
          ,YTD_OFF_BST_INT_AMT_RCVD      
          ,ON_BST_INT_RCVB               
          ,OFF_BST_INT_RCVB              
          ,TOT_YTD_AMT_OF_INT_INCM       
          ,LN_DRDWN_AMT                  
          ,AMT_LN_REPYMT_RCVD            
          ,TOT_MTD_LN_DRDWN_AMT          
          ,TOT_QTD_LN_DRDWN_AMT          
          ,TOT_YTD_LN_DRDWN_AMT          
          ,TOT_MTD_AMT_LN_REPYMT_RCVD    
          ,TOT_QTD_AMT_LN_RPYMT_RCVD     
          ,TOT_YTD_AMT_LN_REPYMT_RCVD    
          ,TOT_MTD_NBR_LN_RCVD_TXN       
          ,TOT_QTD_NBR_LN_RCVD_TXN       
          ,TOT_YTD_NBR_LN_RCVD_TXN       
          ,TOT_MTD_NBR_LN_DRDWNTXN       
          ,TOT_QTD_NBR_LN_DRDWN_TXN      
          ,TOT_YTD_NBR_LN_DRDWN_TXN      
          ,CUR_CR_AMT                    
          ,CUR_DB_AMT                    
          ,TOT_MTD_CR_AMT                
          ,TOT_MTD_DB_AMT                
          ,TOT_QTD_DB_AMT                
          ,TOT_QTD_CR_AMT                
          ,TOT_YTD_CR_AMT                
          ,TOT_YTD_DB_AMT                
          ,OFF_BST_INT_RCVB_WRTOF        
          ,OFF_BST_INT_RCVB_RPLC	       
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST	
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN 
					,BAL_AMT	        
          ,MTD_ACML_BAL_AMT	
          ,QTD_ACML_BAL_AMT	
          ,YTD_ACML_BAL_AMT	
          ,NOCLD_In_MTH			
          ,NOD_IN_MTH				
          ,NOCLD_IN_QTR			
          ,NOD_IN_QTR				
          ,NOCLD_IN_YEAR		
          ,NOD_IN_YEAR			
          ,CUR_WRTOF_AMT_RCVD            --当天收回核销金额
          ,CUR_AMT_RCVD_Of_AST_RPLC      --当天收回置换资产金额
          ,CUR_WRTOF_AMT                 --当天核销金额
          ,TOT_MTD_WRTOF_AMT_RCVD        --月累计收回核销金额
          ,TOT_MTD_AMT_RCVD_Of_AST_RPLC  --月累计收回置换资产金额
          ,TOT_MTD_WRTOF_AMT             --月累计核销金额
          ,TOT_QTD_WRTOF_AMT_RCVD        --季累计收回核销金额
          ,TOT_QTD_AMT_RCVD_Of_AST_RPLC  --季累计收回置换资产金额
          ,TOT_QTD_WRTOF_AMT             --季累计核销金额
          ,TOT_YTD_WRTOF_AMT_RCVD        --年累计收回核销金额
          ,TOT_YTD_AMT_RCVD_Of_AST_RPLC  --年累计置换资产金额
          ,TOT_YTD_WRTOF_AMT             --年累计核销金额  					
					) 
			VALUES (  
            		S.CTR_AR_ID                         --合同号            
           		 ,S.CTR_ITM_ORDR_ID                   --合同序号          
          		 ,S.CDR_YR                            --日历年            
          		 ,S.CDR_MTH                           --日历月        
			         ,S.ACG_DT                        
               ,S.LN_AR_ID                      
               ,S.DNMN_CCY_ID                   
               ,S.YTD_ON_BST_INT_AMT_RCVD       
               ,S.YTD_OFF_BST_INT_AMT_RCVD      
               ,S.ON_BST_INT_RCVB               
               ,S.OFF_BST_INT_RCVB              
               ,S.TOT_YTD_AMT_OF_INT_INCM       
               ,S.LN_DRDWN_AMT                  
               ,S.AMT_LN_REPYMT_RCVD            
               ,S.TOT_MTD_LN_DRDWN_AMT          
               ,S.TOT_QTD_LN_DRDWN_AMT          
               ,S.TOT_YTD_LN_DRDWN_AMT          
               ,S.TOT_MTD_AMT_LN_REPYMT_RCVD    
               ,S.TOT_QTD_AMT_LN_RPYMT_RCVD     
               ,S.TOT_YTD_AMT_LN_REPYMT_RCVD    
               ,S.TOT_MTD_NBR_LN_RCVD_TXN       
               ,S.TOT_QTD_NBR_LN_RCVD_TXN       
               ,S.TOT_YTD_NBR_LN_RCVD_TXN       
               ,S.TOT_MTD_NBR_LN_DRDWNTXN       
               ,S.TOT_QTD_NBR_LN_DRDWN_TXN      
               ,S.TOT_YTD_NBR_LN_DRDWN_TXN      
               ,S.CUR_CR_AMT                    
               ,S.CUR_DB_AMT                    
               ,S.TOT_MTD_CR_AMT                
               ,S.TOT_MTD_DB_AMT                
               ,S.TOT_QTD_DB_AMT                
               ,S.TOT_QTD_CR_AMT                
               ,S.TOT_YTD_CR_AMT                
               ,S.TOT_YTD_DB_AMT                
               ,S.OFF_BST_INT_RCVB_WRTOF        
               ,S.OFF_BST_INT_RCVB_RPLC	       
               ,S.TOT_YTD_INT_INCM_AMT_DEBT_AST 
               ,S.TOT_YTD_INT_INCM_RTND_WRTOF_LN
					     ,S.BAL_AMT	        
               ,S.MTD_ACML_BAL_AMT	
               ,S.QTD_ACML_BAL_AMT	
               ,S.YTD_ACML_BAL_AMT	
               ,S.NOCLD_In_MTH			
               ,S.NOD_In_MTH				
               ,S.NOCLD_In_QTR			
               ,S.NOD_In_QTR				
               ,S.NOCLD_In_Year		
               ,S.NOD_In_Year			
               ,S.CUR_WRTOF_AMT_RCVD            --当天收回核销金额
               ,S.CUR_AMT_RCVD_Of_AST_RPLC      --当天收回置换资产金额
               ,S.CUR_WRTOF_AMT                 --当天核销金额
               ,S.TOT_MTD_WRTOF_AMT_RCVD        --月累计收回核销金额
               ,S.TOT_MTD_AMT_RCVD_Of_AST_RPLC  --月累计收回置换资产金额
               ,S.TOT_MTD_WRTOF_AMT             --月累计核销金额
               ,S.TOT_QTD_WRTOF_AMT_RCVD        --季累计收回核销金额
               ,S.TOT_QTD_AMT_RCVD_Of_AST_RPLC  --季累计收回置换资产金额
               ,S.TOT_QTD_WRTOF_AMT             --季累计核销金额
               ,S.TOT_YTD_WRTOF_AMT_RCVD        --年累计收回核销金额
               ,S.TOT_YTD_AMT_RCVD_Of_AST_RPLC  --年累计置换资产金额
               ,S.TOT_YTD_WRTOF_AMT             --年累计核销金额  
               )
   ;--*/

END IF;
   
   
/** Insert the log**/
 		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		SET SMY_STEPNUM = SMY_STEPNUM + 1;--
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
	 
	 COMMIT;   --
	 
END@