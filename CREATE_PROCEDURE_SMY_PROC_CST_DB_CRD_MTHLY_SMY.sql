CREATE PROCEDURE SMY.PROC_CST_DB_CRD_MTHLY_SMY(IN ACCOUNTING_DATE date)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_CST_DB_CRD_MTHLY_SMY.sql
-- Procedure name: 			SMY.PROC_CST_DB_CRD_MTHLY_SMY
-- Source Table:				SMY.CST_INF,SMY.DB_CRD_SMY
-- Target Table: 				SMY.CST_DB_CRD_MTHLY_SMY
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
-- 2009-12-04   Xu Yan					Rename the history table	
-- 2009-12-16   Xu Yan          Fixed a bug for reruning
-- 2010-08-11   Peng Yi tao     Modify the method of calendar days Calculating
-- 2011-08-03   wu zhan shan    Modify the merge method
-- 2012-02-28   Chen XiaoWen    CST_DB_CRD_MTHLY_SMY表的查询条件统一由年月查询改为ACG_DT分区键查询
-- 2012-04-09   Chen XiaoWen    增加表级过滤条件DB_CRD_SMY.CRD_LCS_TP_ID in (11920001,11920002,11920003)
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
		DECLARE CUR_QTR SMALLINT;--
		DECLARE MON_DAY SMALLINT;--
		DECLARE LAST_MONTH SMALLINT;--
		DECLARE EMP_SQL VARCHAR(200);  --
		DECLARE MTH_LAST_DAY DATE;

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
  
    SET SMY_PROCNM  ='PROC_CST_DB_CRD_MTHLY_SMY';--
    SET SMY_DATE    =ACCOUNTING_DATE;    --
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --取当前年份
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --取当前月份
    SET CUR_DAY     =DAY(ACCOUNTING_DATE);     --取月第几日
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- 取年初日
--------------------------------------------start on 20100811-------------------------------------------------------------    
    --SET C_YR_DAY      =DAYOFYEAR(DATE(TRIM(CHAR(YEAR(ACCOUNTING_DATE)))||'-12-31')); --取当年第几日
    SET C_YR_DAY      =DAYS(ACCOUNTING_DATE)-DAYS(YR_FIRST_DAY)+1;  --取当年第几日
--------------------------------------------end on 20100811-------------------------------------------------------------     
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --当前季度
    SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,ISO),1,7)||'-01'); --取当月初日
    SET MON_DAY     =DAY(ACCOUNTING_DATE);     --取月第几日
    VALUES(MTH_FIRST_DAY + 1 MONTH - 1 DAY) INTO MTH_LAST_DAY ;
    
    VALUES(ACCOUNTING_DATE - 1 DAY) INTO LAST_SMY_DATE;--
    SET LAST_MONTH = MONTH(LAST_SMY_DATE);      --
    --计算月日历天数
--------------------------------------------start on 20100811-------------------------------------------------------------    
    --SET C_MON_DAY = DAYS(MTH_FIRST_DAY + 1 MONTH ) - DAYS(MTH_FIRST_DAY);    --
      SET C_MON_DAY = DAY(ACCOUNTING_DATE);                 --
--------------------------------------------end on 20100811-------------------------------------------------------------      
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

  /*取当季日历天数*/ 
--------------------------------------------start on 20100811-------------------------------------------------------------
  	--SET C_QTR_DAY = DAYS(QTR_LAST_DAY) - DAYS(QTR_FIRST_DAY) + 1 ;--
  	SET C_QTR_DAY = DAYS(ACCOUNTING_DATE)-DAYS(QTR_FIRST_DAY)+1;--
--------------------------------------------end on 20100811------------------------------------------------------------- 
		
		SELECT COALESCE(MAX(ACG_DT),'1900-01-01') INTO MAX_ACG_DT FROM SMY.CST_DB_CRD_MTHLY_SMY;--

/*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE,并插入新的起始标志*/
		DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
			COMMIT;--
		
		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		
		SET SMY_STEPDESC = 	'存储过程开始运行' ;--
		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
				VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

/*数据恢复与备份*/
    IF MAX_ACG_DT = ACCOUNTING_DATE THEN
       --DELETE FROM SMY.CST_DB_CRD_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = CUR_MONTH;--
       DELETE FROM SMY.CST_DB_CRD_MTHLY_SMY WHERE ACG_DT >= MTH_FIRST_DAY AND ACG_DT <= MTH_LAST_DAY;
    /**每月第一日不需要从历史表中恢复**/       
       IF MON_DAY <> 1 THEN
      	 INSERT INTO SMY.CST_DB_CRD_MTHLY_SMY SELECT * FROM HIS.CST_DB_CRD_MTHLY_SMY ;--
       END IF;--
     ELSE
  /** 清空hist 备份表 **/

	    SET EMP_SQL= 'Alter TABLE HIS.CST_DB_CRD_MTHLY_SMY ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE' ;--
		
		  EXECUTE IMMEDIATE EMP_SQL;       --
      
      COMMIT;--

		  /**backup 昨日数据 **/
		  
		  --INSERT INTO HIS.CST_DB_CRD_MTHLY_SMY SELECT * FROM SMY.CST_DB_CRD_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = CUR_MONTH;--
		  INSERT INTO HIS.CST_DB_CRD_MTHLY_SMY SELECT * FROM SMY.CST_DB_CRD_MTHLY_SMY WHERE ACG_DT >= MTH_FIRST_DAY AND ACG_DT <= MTH_LAST_DAY;
    END IF;--

SET SMY_STEPNUM = 2 ;--
SET SMY_STEPDESC = '声明用户临时表,存放昨日SMY数据';--

	/*声明用户临时表*/
	
	DECLARE GLOBAL TEMPORARY TABLE TMP 
		LIKE SMY.CST_DB_CRD_MTHLY_SMY
	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(CST_ID);--

 /*如果是年第一日不需要插入*/

 IF YR_FIRST_DAY <>  ACCOUNTING_DATE THEN 
	INSERT INTO SESSION.TMP 
	(
         CST_ID                --客户内码         
        ,AC_OU_ID              --账户归属机构号   
        ,CRD_TP_ID             --卡类型           
        ,PSBK_RLTD_F           --卡折相关标识     
        ,IS_NONGXIN_CRD_F      --丰收卡/农信卡标识
        ,CCY                   --币种             
        ,CDR_YR                --年份YYYY         
        ,CDR_MTH               --月份MM           
        ,NOCLD_In_MTH          --当月日历天数     
        ,NOD_In_MTH            --当月有效天数     
        ,NOCLD_In_QTR          --当季日历天数     
        ,NOD_In_QTR            --当季有效天数     
        ,NOCLD_In_Year         --当年日历天数     
        ,NOD_In_Year           --当年有效天数     
        ,ACG_DT                --日期YYYY-MM-DD   
        ,CST_OU_ID             --客户机构号       
        ,CST_TP_ID             --客户类型         
        ,NBR_CRD               --账户个数         
        ,LST_DAY_BAL           --昨日余额         
        ,BAL                   --余额             
        ,MTD_ACML_BAL_AMT      --月累计余额       
        ,QTD_ACML_BAL_AMT      --季累计余额       
        ,YTD_ACML_BAL_AMT      --年累计余额 
          ) 
    SELECT
         CST_ID                --客户内码         
        ,AC_OU_ID              --账户归属机构号   
        ,CRD_TP_ID             --卡类型           
        ,PSBK_RLTD_F           --卡折相关标识     
        ,IS_NONGXIN_CRD_F      --丰收卡/农信卡标识
        ,CCY                   --币种             
        ,CDR_YR                --年份YYYY         
        ,CDR_MTH               --月份MM           
        ,NOCLD_In_MTH          --当月日历天数     
        ,NOD_In_MTH            --当月有效天数     
        ,NOCLD_In_QTR          --当季日历天数     
        ,NOD_In_QTR            --当季有效天数     
        ,NOCLD_In_Year         --当年日历天数     
        ,NOD_In_Year           --当年有效天数     
        ,ACG_DT                --日期YYYY-MM-DD   
        ,CST_OU_ID             --客户机构号       
        ,CST_TP_ID             --客户类型         
        ,NBR_CRD               --账户个数         
        ,LST_DAY_BAL           --昨日余额         
        ,BAL                   --余额             
        ,MTD_ACML_BAL_AMT      --月累计余额       
        ,QTD_ACML_BAL_AMT      --季累计余额       
        ,YTD_ACML_BAL_AMT      --年累计余额       
     FROM SMY.CST_DB_CRD_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = LAST_MONTH;   --季初、月初时LAST_MONTH的值为上月，平时的值为当月
 END IF ;   --
	
       
      
 /** 收集操作信息 */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	 --

----delete by wuzhanshan 20110803 begin
	 --IF  ACCOUNTING_DATE IN ( YR_FIRST_DAY )  --年 季 月 归零
	 --		THEN 
	 --			UPDATE SESSION.TMP 
	 --				SET  				
   --         MTD_ACML_BAL_AMT =0 --月累计余额
   --        ,QTD_ACML_BAL_AMT =0 --季累计余额
   --        ,YTD_ACML_BAL_AMT =0 --年累计余额  
   --        ,NOD_In_MTH       =0 --当月有效天数   
   --        ,NOD_In_QTR       =0 --当季有效天数  
   --        ,NOD_In_Year      =0 --当年有效天数 
   --                    
	 --		  ;--
	 --ELSE
----delete by wuzhanshan 20110803 end
	 IF ACCOUNTING_DATE IN (QTR_FIRST_DAY) --季 月 归零
	 	  THEN
	 			UPDATE SESSION.TMP 
	 				SET 
            MTD_ACML_BAL_AMT =0 --月累计余额
           ,QTD_ACML_BAL_AMT =0 --季累计余额 
           ,NOD_In_MTH       =0 --当月有效天数   
           ,NOD_In_QTR       =0 --当季有效天数         
	 			;	 	  --
	 	  	 		
	 ELSEIF ACCOUNTING_DATE IN ( MTH_FIRST_DAY ) --月归零
	 	  THEN 
	 			UPDATE SESSION.TMP 
	 				SET 			
            MTD_ACML_BAL_AMT =0 --月累计余额
           ,NOD_In_MTH       =0 --当月有效天数             
	 			;	 	--
	 END IF;--

 /*获得当日统计数据*/	
 
 
		SET SMY_STEPNUM = 3 ;--
		SET SMY_STEPDESC = '声明临时表SESSION.CUR, 存放借记卡当日汇总后的数据';--

  DECLARE GLOBAL TEMPORARY TABLE CUR AS (
		SELECT 
			   DB_CRD_SMY.CST_ID   AS CST_ID            --客户内码      
        ,DB_CRD_SMY.AC_OU_ID    AS AC_OU_ID          --账户归属机构号
        ,DB_CRD_SMY.DB_CRD_TP_ID               AS CRD_TP_ID                   --卡类型                          
        ,DB_CRD_SMY.PSBK_RLTD_F                AS PSBK_RLTD_F                 --卡折相关标识 
        ,DB_CRD_SMY.IS_NONGXIN_CRD_F           AS IS_NONGXIN_CRD_F            --丰收卡/农信卡标识
        ,DB_CRD_SMY.CCY      AS CCY               --币种          
        ,1              AS NOD_IN_MTH
        ,1              AS NOD_IN_QTR
        ,1              AS NOD_IN_YEAR
        ,COALESCE(CST_INF.RPRG_OU_IP_ID,'')                  AS CST_OU_ID             --客户机构号         
        ,COALESCE(CST_INF.ENT_IDV_IND,-1)                    AS CST_TP_ID         --客户类型    
        ,COALESCE(COUNT(DISTINCT DB_CRD_SMY.AC_AR_ID),0)     AS NBR_CRD           --账户个数    
        ,SUM(DB_CRD_SMY.AC_BAL_AMT)              AS BAL               --余额                                                                
		FROM            SMY.DB_CRD_SMY  AS DB_CRD_SMY
		LEFT OUTER JOIN SMY.CST_INF     AS CST_INF	ON DB_CRD_SMY.CST_ID=CST_INF.CST_ID
		GROUP BY 
			   DB_CRD_SMY.CST_ID		
        ,DB_CRD_SMY.AC_OU_ID
        ,DB_CRD_SMY.DB_CRD_TP_ID     
        ,DB_CRD_SMY.PSBK_RLTD_F      
        ,DB_CRD_SMY.IS_NONGXIN_CRD_F         
        ,DB_CRD_SMY.CCY
        ,COALESCE(CST_INF.RPRG_OU_IP_ID,'')
        ,COALESCE(CST_INF.ENT_IDV_IND,-1) 
   ) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K 
     PARTITIONING KEY(CST_ID)
   ;	 		--
		
  INSERT INTO SESSION.CUR 
		SELECT 
			   DB_CRD_SMY.CST_ID   AS CST_ID            --客户内码      
        ,DB_CRD_SMY.AC_OU_ID    AS AC_OU_ID          --账户归属机构号
        ,DB_CRD_SMY.DB_CRD_TP_ID               AS CRD_TP_ID                   --卡类型                          
        ,DB_CRD_SMY.PSBK_RLTD_F                AS PSBK_RLTD_F                 --卡折相关标识 
        ,DB_CRD_SMY.IS_NONGXIN_CRD_F           AS IS_NONGXIN_CRD_F            --丰收卡/农信卡标识
        ,DB_CRD_SMY.CCY      AS CCY               --币种          
        ,1              AS NOD_IN_MTH
        ,1              AS NOD_IN_QTR
        ,1              AS NOD_IN_YEAR
        ,COALESCE(CST_INF.RPRG_OU_IP_ID,'')                  AS CST_OU_ID             --客户机构号         
        ,COALESCE(CST_INF.ENT_IDV_IND,-1)                    AS CST_TP_ID         --客户类型    
        ,COALESCE(COUNT(DISTINCT DB_CRD_SMY.AC_AR_ID),0)     AS NBR_CRD           --账户个数    
        ,SUM(DB_CRD_SMY.AC_BAL_AMT)              AS BAL               --余额                                                                
		FROM            SMY.DB_CRD_SMY  AS DB_CRD_SMY
		LEFT OUTER JOIN SMY.CST_INF     AS CST_INF	ON DB_CRD_SMY.CST_ID=CST_INF.CST_ID
		WHERE DB_CRD_SMY.CRD_LCS_TP_ID in (11920001,11920002,11920003) --11920001:正常,11920002:新发卡未启用,11920003:新换卡未启用
		GROUP BY 
			   DB_CRD_SMY.CST_ID		
        ,DB_CRD_SMY.AC_OU_ID
        ,DB_CRD_SMY.DB_CRD_TP_ID     
        ,DB_CRD_SMY.PSBK_RLTD_F      
        ,DB_CRD_SMY.IS_NONGXIN_CRD_F         
        ,DB_CRD_SMY.CCY
        ,COALESCE(CST_INF.RPRG_OU_IP_ID,'')
        ,COALESCE(CST_INF.ENT_IDV_IND,-1)  
   ;	 --
 /** 收集操作信息 */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	 --

		SET SMY_STEPNUM = 4 ;--
		SET SMY_STEPDESC = '声明临时表SESSION.S, 用来存放借记卡汇总后要更新的数据'; 			 --
		
/**/
  DECLARE GLOBAL TEMPORARY TABLE S  LIKE SMY.CST_DB_CRD_MTHLY_SMY 
  	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K 
     PARTITIONING KEY(CST_ID);--
  
	INSERT INTO SESSION.S
          (
         CST_ID                --客户内码         
        ,AC_OU_ID              --账户归属机构号   
        ,CRD_TP_ID             --卡类型           
        ,PSBK_RLTD_F           --卡折相关标识     
        ,IS_NONGXIN_CRD_F      --丰收卡/农信卡标识
        ,CCY                   --币种             
        ,CDR_YR                --年份YYYY         
        ,CDR_MTH               --月份MM           
        ,NOCLD_In_MTH          --当月日历天数     
        ,NOD_In_MTH            --当月有效天数     
        ,NOCLD_In_QTR          --当季日历天数     
        ,NOD_In_QTR            --当季有效天数     
        ,NOCLD_In_Year         --当年日历天数     
        ,NOD_In_Year           --当年有效天数     
        ,ACG_DT                --日期YYYY-MM-DD   
        ,CST_OU_ID             --客户机构号       
        ,CST_TP_ID             --客户类型         
        ,NBR_CRD               --账户个数         
        ,LST_DAY_BAL           --昨日余额         
        ,BAL                   --余额             
        ,MTD_ACML_BAL_AMT      --月累计余额       
        ,QTD_ACML_BAL_AMT      --季累计余额       
        ,YTD_ACML_BAL_AMT      --年累计余额 
            )    	 	  
	SELECT  
        CUR.CST_ID            --客户内码      
       ,CUR.AC_OU_ID          --账户归属机构号
       ,CUR.CRD_TP_ID             --卡类型           
       ,CUR.PSBK_RLTD_F           --卡折相关标识     
       ,CUR.IS_NONGXIN_CRD_F      --丰收卡/农信卡标识   
       ,CUR.CCY               --币种          
       ,CUR_YEAR            --年份YYYY      
       ,CUR_MONTH           --月份MM        
       ,C_MON_DAY      --当月日历天数  
       ,COALESCE(PRE.NOD_In_MTH,0) + CUR.NOD_In_MTH        --当月有效天数  
       ,C_QTR_DAY      --当季日历天数  
       ,COALESCE(PRE.NOD_In_QTR,0) + CUR.NOD_In_QTR        --当季有效天数  
       ,C_YR_DAY          --当年日历天数  
       ,COALESCE(PRE.NOD_In_YEAR,0) + CUR.NOD_In_YEAR       --当年有效天数  
       ,ACCOUNTING_DATE            --日期YYYY-MM-DD
       ,CUR.CST_OU_ID           --客户机构号    
       ,CUR.CST_TP_ID        --客户类型      
       ,CUR.NBR_CRD        --账户个数      
       ,COALESCE(PRE.BAL,0)               --昨日余额      
       ,CUR.BAL             --余额          
       ,COALESCE(PRE.MTD_ACML_BAL_AMT,0) + CUR.BAL  --月累计余额    
       ,COALESCE(PRE.QTD_ACML_BAL_AMT,0) + CUR.BAL  --季累计余额    
       ,COALESCE(PRE.YTD_ACML_BAL_AMT,0) + CUR.BAL  --年累计余额  

	FROM  SESSION.CUR  AS CUR LEFT OUTER JOIN  SESSION.TMP AS PRE ON 
        CUR.CST_ID            =PRE.CST_ID          
    AND CUR.AC_OU_ID          =PRE.AC_OU_ID        
    AND CUR.CRD_TP_ID         =PRE.CRD_TP_ID       
    AND CUR.PSBK_RLTD_F       =PRE.PSBK_RLTD_F     
    AND CUR.IS_NONGXIN_CRD_F  =PRE.IS_NONGXIN_CRD_F
    AND CUR.CCY               =PRE.CCY          
      ;--
----add by wuzhanshan 20110803 begin
IF ACCOUNTING_DATE<>MTH_FIRST_DAY THEN
   CREATE INDEX SESSION.IDX_S ON SESSION.S(CST_ID,AC_OU_ID,CRD_TP_ID,PSBK_RLTD_F,IS_NONGXIN_CRD_F,CCY,CDR_YR,CDR_MTH);
END IF;
----add by wuzhanshan 20110803 end      
 		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;	          --

 /** Insert the log**/
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	--

		SET SMY_STEPNUM = 5 ;--
		SET SMY_STEPDESC = '使用Merge语句,更新SMY 表'; 			 --

----modify by wuzhanshan 20110803 begin
IF ACCOUNTING_DATE =MTH_FIRST_DAY AND MAX_ACG_DT <= ACCOUNTING_DATE THEN
   INSERT INTO SMY.CST_DB_CRD_MTHLY_SMY
   (     CST_ID                --客户内码         
        ,AC_OU_ID              --账户归属机构号   
        ,CRD_TP_ID             --卡类型           
        ,PSBK_RLTD_F           --卡折相关标识     
        ,IS_NONGXIN_CRD_F      --丰收卡/农信卡标识
        ,CCY                   --币种             
        ,CDR_YR                --年份YYYY         
        ,CDR_MTH               --月份MM           
        ,NOCLD_In_MTH          --当月日历天数     
        ,NOD_In_MTH            --当月有效天数     
        ,NOCLD_In_QTR          --当季日历天数     
        ,NOD_In_QTR            --当季有效天数     
        ,NOCLD_In_Year         --当年日历天数     
        ,NOD_In_Year           --当年有效天数     
        ,ACG_DT                --日期YYYY-MM-DD   
        ,CST_OU_ID             --客户机构号       
        ,CST_TP_ID             --客户类型         
        ,NBR_CRD               --账户个数         
        ,LST_DAY_BAL           --昨日余额         
        ,BAL                   --余额             
        ,MTD_ACML_BAL_AMT      --月累计余额       
        ,QTD_ACML_BAL_AMT      --季累计余额       
        ,YTD_ACML_BAL_AMT      --年累计余额 
    )
   SELECT S.CST_ID                --客户内码         
         ,S.AC_OU_ID              --账户归属机构号   
         ,S.CRD_TP_ID             --卡类型           
         ,S.PSBK_RLTD_F           --卡折相关标识     
         ,S.IS_NONGXIN_CRD_F      --丰收卡/农信卡标识
         ,S.CCY                   --币种             
         ,S.CDR_YR                --年份YYYY         
         ,S.CDR_MTH               --月份MM           
         ,S.NOCLD_In_MTH          --当月日历天数     
         ,S.NOD_In_MTH            --当月有效天数     
         ,S.NOCLD_In_QTR          --当季日历天数     
         ,S.NOD_In_QTR            --当季有效天数     
         ,S.NOCLD_In_Year         --当年日历天数     
         ,S.NOD_In_Year           --当年有效天数     
         ,S.ACG_DT                --日期YYYY-MM-DD   
         ,S.CST_OU_ID             --客户机构号       
         ,S.CST_TP_ID             --客户类型         
         ,S.NBR_CRD               --账户个数         
         ,S.LST_DAY_BAL           --昨日余额         
         ,S.BAL                   --余额             
         ,S.MTD_ACML_BAL_AMT      --月累计余额       
         ,S.QTD_ACML_BAL_AMT      --季累计余额       
         ,S.YTD_ACML_BAL_AMT      --年累计余额 
   FROM SESSION.S S;
ELSE
   MERGE INTO SMY.CST_DB_CRD_MTHLY_SMY AS T
    		USING  SESSION.S AS S 
    	  ON
           T.CST_ID            =S.CST_ID          
       AND T.AC_OU_ID          =S.AC_OU_ID        
       AND T.CRD_TP_ID         =S.CRD_TP_ID       
       AND T.PSBK_RLTD_F       =S.PSBK_RLTD_F     
       AND T.IS_NONGXIN_CRD_F  =S.IS_NONGXIN_CRD_F
       AND T.CCY               =S.CCY         
       AND T.CDR_YR            =S.CDR_YR     
       AND T.CDR_MTH           =S.CDR_MTH    	  	    
   WHEN MATCHED THEN UPDATE SET
           NOCLD_IN_MTH     = S.NOCLD_IN_MTH     --当月日历天数  
          ,NOD_IN_MTH       = S.NOD_IN_MTH       --当月有效天数  
          ,NOCLD_IN_QTR     = S.NOCLD_IN_QTR     --当季日历天数  
          ,NOD_IN_QTR       = S.NOD_IN_QTR       --当季有效天数  
          ,NOCLD_IN_YEAR    = S.NOCLD_IN_YEAR    --当年日历天数  
          ,NOD_IN_YEAR      = S.NOD_IN_YEAR      --当年有效天数  
          ,ACG_DT           = S.ACG_DT           --日期YYYY-MM-DD
          ,CST_OU_ID        = S.CST_OU_ID        --客户机构号    
          ,CST_TP_ID        = S.CST_TP_ID        --客户类型      
          ,NBR_CRD          = S.NBR_CRD          --账户个数      
          ,LST_DAY_BAL      = S.LST_DAY_BAL      --昨日余额      
          ,BAL              = S.BAL              --余额          
          ,MTD_ACML_BAL_AMT = S.MTD_ACML_BAL_AMT --月累计余额    
          ,QTD_ACML_BAL_AMT = S.QTD_ACML_BAL_AMT --季累计余额    
          ,YTD_ACML_BAL_AMT = S.YTD_ACML_BAL_AMT --年累计余额  
   WHEN NOT MATCHED THEN INSERT  	        
   	 (
            CST_ID                --客户内码         
           ,AC_OU_ID              --账户归属机构号   
           ,CRD_TP_ID             --卡类型           
           ,PSBK_RLTD_F           --卡折相关标识     
           ,IS_NONGXIN_CRD_F      --丰收卡/农信卡标识
           ,CCY                   --币种             
           ,CDR_YR                --年份YYYY         
           ,CDR_MTH               --月份MM           
           ,NOCLD_In_MTH          --当月日历天数     
           ,NOD_In_MTH            --当月有效天数     
           ,NOCLD_In_QTR          --当季日历天数     
           ,NOD_In_QTR            --当季有效天数     
           ,NOCLD_In_Year         --当年日历天数     
           ,NOD_In_Year           --当年有效天数     
           ,ACG_DT                --日期YYYY-MM-DD   
           ,CST_OU_ID             --客户机构号       
           ,CST_TP_ID             --客户类型         
           ,NBR_CRD               --账户个数         
           ,LST_DAY_BAL           --昨日余额         
           ,BAL                   --余额             
           ,MTD_ACML_BAL_AMT      --月累计余额       
           ,QTD_ACML_BAL_AMT      --季累计余额       
           ,YTD_ACML_BAL_AMT      --年累计余额 
           )
       VALUES 
       (
            S.CST_ID                --客户内码         
           ,S.AC_OU_ID              --账户归属机构号   
           ,S.CRD_TP_ID             --卡类型           
           ,S.PSBK_RLTD_F           --卡折相关标识     
           ,S.IS_NONGXIN_CRD_F      --丰收卡/农信卡标识
           ,S.CCY                   --币种             
           ,S.CDR_YR                --年份YYYY         
           ,S.CDR_MTH               --月份MM           
           ,S.NOCLD_In_MTH          --当月日历天数     
           ,S.NOD_In_MTH            --当月有效天数     
           ,S.NOCLD_In_QTR          --当季日历天数     
           ,S.NOD_In_QTR            --当季有效天数     
           ,S.NOCLD_In_Year         --当年日历天数     
           ,S.NOD_In_Year           --当年有效天数     
           ,S.ACG_DT                --日期YYYY-MM-DD   
           ,S.CST_OU_ID             --客户机构号       
           ,S.CST_TP_ID             --客户类型         
           ,S.NBR_CRD               --账户个数         
           ,S.LST_DAY_BAL           --昨日余额         
           ,S.BAL                   --余额             
           ,S.MTD_ACML_BAL_AMT      --月累计余额       
           ,S.QTD_ACML_BAL_AMT      --季累计余额       
           ,S.YTD_ACML_BAL_AMT      --年累计余额 
       )
          ;
END IF;--
----modify by wuzhanshan 20110803 end		
	 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
	 
	 COMMIT;--
END@