CREATE PROCEDURE SMY.PROC_CST_CR_CRD_MTHLY_SMY(IN ACCOUNTING_DATE date)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_CST_CR_CRD_MTHLY_SMY.sql
-- Procedure name: 			SMY.PROC_CST_CR_CRD_MTHLY_SMY
-- Source Table:				SMY.CR_CRD_SMY,SMY.CST_INF
-- Target Table: 				SMY.CST_CR_CRD_MTHLY_SMY
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
-- 2009-12-04   Xu Yan          Rename the history table 
-- 2009-12-16   Xu Yan          Fixed a bug for reruning
-- 2010-08-11   van fuqiao      Fixed a bug for column  'C_QTR_DAY'  'C_MON_DAY' 'C_YR_DAY'
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
  
    SET SMY_PROCNM  ='PROC_CST_CR_CRD_MTHLY_SMY';--
    SET SMY_DATE    =ACCOUNTING_DATE;    --
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --取当前年份
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --取当前月份
    SET CUR_DAY     =DAY(ACCOUNTING_DATE);     --取月第几日
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- 取年初日
    ------------------------Start on 20100811--------------------------
    --SET C_YR_DAY      =DAYOFYEAR(DATE(TRIM(CHAR(YEAR(ACCOUNTING_DATE)))||'-12-31')); --取当年第几日
    SET C_YR_DAY      =DAYOFYEAR(ACCOUNTING_DATE); --取当年第几日
    ------------------------End on 20100811--------------------------
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --当前季度
    SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,ISO),1,7)||'-01'); --取当月初日
    SET MON_DAY     =DAY(ACCOUNTING_DATE);     --取月第几日
    
    VALUES(ACCOUNTING_DATE - 1 DAY) INTO LAST_SMY_DATE;--
    SET LAST_MONTH = MONTH(LAST_SMY_DATE);      --
    --计算月日历天数
    ------------------------Start on 20100811--------------------------
    --SET C_MON_DAY = DAYS(MTH_FIRST_DAY + 1 MONTH ) - DAYS(MTH_FIRST_DAY);    --
    SET C_MON_DAY = DAYS(ACCOUNTING_DATE) - DAYS(MTH_FIRST_DAY)+1;    --计算月日历天数
    ------------------------End on 20100811--------------------------
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
    ------------------------Start on 20100811--------------------------
  	--SET C_QTR_DAY = DAYS(QTR_LAST_DAY) - DAYS(QTR_FIRST_DAY) + 1 ;--
  	SET C_QTR_DAY = DAYS(ACCOUNTING_DATE) - DAYS(QTR_FIRST_DAY) + 1 ;--取当季日历天数
  	------------------------End on 20100811--------------------------
		
		SELECT COALESCE(MAX(ACG_DT),'1900-01-01') INTO MAX_ACG_DT FROM SMY.CST_CR_CRD_MTHLY_SMY;--

/*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE,并插入新的起始标志*/
		DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
			COMMIT;--
		
		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		
		SET SMY_STEPDESC = 	'存储过程开始运行' ;--
		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
				VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

/*数据恢复与备份*/
    IF MAX_ACG_DT = ACCOUNTING_DATE THEN
       DELETE FROM SMY.CST_CR_CRD_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = CUR_MONTH;--
    /**每月第一日不需要从历史表中恢复**/       
       IF MON_DAY <> 1 THEN
      	 INSERT INTO SMY.CST_CR_CRD_MTHLY_SMY SELECT * FROM HIS.CST_CR_CRD_MTHLY_SMY ;--
       END IF;--
     ELSE
  /** 清空hist 备份表 **/

	    SET EMP_SQL= 'Alter TABLE HIS.CST_CR_CRD_MTHLY_SMY ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE' ;--
		
		  EXECUTE IMMEDIATE EMP_SQL;       		 --
      
      COMMIT;--
		  /**backup 昨日数据 **/
		  
		  INSERT INTO HIS.CST_CR_CRD_MTHLY_SMY SELECT * FROM SMY.CST_CR_CRD_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = CUR_MONTH;--
    END IF;--

SET SMY_STEPNUM = 2 ;--
SET SMY_STEPDESC = '声明用户临时表,存放昨日SMY数据';--

	/*声明用户临时表*/
	
	DECLARE GLOBAL TEMPORARY TABLE TMP 
		LIKE SMY.CST_CR_CRD_MTHLY_SMY
	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE   IN TS_USR_TMP32K PARTITIONING KEY(CST_ID);--

 /*如果是年第一日不需要插入*/

 IF YR_FIRST_DAY <>  ACCOUNTING_DATE THEN 
	INSERT INTO SESSION.TMP 
	(
         CST_ID                  --客户内码        
        ,CRD_OU_ID               --受理机构号      
        ,ENT_IDV_IND             --卡对象          
        ,MST_CRD_IND             --主/副卡标志     
        ,LN_FIVE_RTG_STS         --贷款五级形态类型
        ,AST_RSK_ASES_RTG_TP_CD  --资产风险分类    
        ,PD_GRP_CD               --产品类          
        ,PD_SUB_CD               --产品子代码      
        ,CCY                     --币种            
        ,NOCLD_In_MTH            --当月日历天数    
        ,NOD_In_MTH              --当月有效天数    
        ,NOCLD_In_QTR            --当季日历天数    
        ,NOD_In_QTR              --当季有效天数    
        ,NOCLD_In_Year           --当年日历天数    
        ,NOD_In_Year             --当年有效天数
        ,CDR_YR                  --日历年
        ,CDR_MTH                 --日历月         
        ,ACG_DT                  --日期YYYY-MM-DD  
        ,DEP_BAL_CRD             --银行卡存款余额  
        ,OD_BAL_AMT              --透支余额        
        ,CR_LMT                  --授信额度        
        ,INT_RCVB                --应收利息        
        ,LST_DAY_DEP_BAL         --昨日存款余额    
        ,LST_DAY_OD_BAL          --昨日透支余额    
        ,MTD_ACML_DEP_BAL_AMT    --月累计存款余额  
        ,QTD_ACML_DEP_BAL_AMT    --季累计存款余额  
        ,YTD_ACML_DEP_BAL_AMT    --年累计存款余额  
        ,MTD_ACML_OD_BAL_AMT     --月累计透支余额  
        ,QTD_ACML_OD_BAL_AMT     --季累计透支余额  
        ,YTD_ACML_OD_BAL_AMT     --年累计透支余额 
          ) 
    SELECT
         CST_ID                  --客户内码        
        ,CRD_OU_ID               --受理机构号      
        ,ENT_IDV_IND             --卡对象          
        ,MST_CRD_IND             --主/副卡标志     
        ,LN_FIVE_RTG_STS         --贷款五级形态类型
        ,AST_RSK_ASES_RTG_TP_CD  --资产风险分类    
        ,PD_GRP_CD               --产品类          
        ,PD_SUB_CD               --产品子代码      
        ,CCY                     --币种            
        ,NOCLD_In_MTH            --当月日历天数    
        ,NOD_In_MTH              --当月有效天数    
        ,NOCLD_In_QTR            --当季日历天数    
        ,NOD_In_QTR              --当季有效天数    
        ,NOCLD_In_Year           --当年日历天数    
        ,NOD_In_Year             --当年有效天数
        ,CDR_YR                  --日历年
        ,CDR_MTH                 --日历月              
        ,ACG_DT                  --日期YYYY-MM-DD  
        ,DEP_BAL_CRD             --银行卡存款余额  
        ,OD_BAL_AMT              --透支余额        
        ,CR_LMT                  --授信额度        
        ,INT_RCVB                --应收利息        
        ,LST_DAY_DEP_BAL         --昨日存款余额    
        ,LST_DAY_OD_BAL          --昨日透支余额    
        ,MTD_ACML_DEP_BAL_AMT    --月累计存款余额  
        ,QTD_ACML_DEP_BAL_AMT    --季累计存款余额  
        ,YTD_ACML_DEP_BAL_AMT    --年累计存款余额  
        ,MTD_ACML_OD_BAL_AMT     --月累计透支余额  
        ,QTD_ACML_OD_BAL_AMT     --季累计透支余额  
        ,YTD_ACML_OD_BAL_AMT     --年累计透支余额        
     FROM SMY.CST_CR_CRD_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = LAST_MONTH;--
 END IF ;   --
	
       
      
 /** 收集操作信息 */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	 --


	 IF  ACCOUNTING_DATE IN ( YR_FIRST_DAY )  --年 季 月 归零
	 		THEN 
	 			UPDATE SESSION.TMP 
	 				SET  				
             MTD_ACML_DEP_BAL_AMT =0 --月累计存款余额
            ,QTD_ACML_DEP_BAL_AMT =0 --季累计存款余额
            ,YTD_ACML_DEP_BAL_AMT =0 --年累计存款余额
            ,MTD_ACML_OD_BAL_AMT  =0 --月累计透支余额
            ,QTD_ACML_OD_BAL_AMT  =0 --季累计透支余额
            ,YTD_ACML_OD_BAL_AMT  =0 --年累计透支余额 
            ,NOD_In_MTH           =0 --当月有效天数  
            ,NOD_In_QTR           =0 --当季有效天数   
            ,NOD_In_Year          =0 --当年有效天数 
                       
	 		  ;--
	 ELSEIF ACCOUNTING_DATE IN (QTR_FIRST_DAY) --季 月 归零
	 	  THEN
	 			UPDATE SESSION.TMP 
	 				SET 
             MTD_ACML_DEP_BAL_AMT =0 --月累计存款余额
            ,QTD_ACML_DEP_BAL_AMT =0 --季累计存款余额
            ,MTD_ACML_OD_BAL_AMT  =0 --月累计透支余额
            ,QTD_ACML_OD_BAL_AMT  =0 --季累计透支余额
            ,NOD_In_MTH           =0 --当月有效天数  
            ,NOD_In_QTR           =0 --当季有效天数           
	 			;	 	  --
	 	  	 		
	 ELSEIF ACCOUNTING_DATE IN ( MTH_FIRST_DAY ) --月归零
	 	  THEN 
	 			UPDATE SESSION.TMP 
	 				SET 
             MTD_ACML_DEP_BAL_AMT =0 --月累计存款余额
            ,MTD_ACML_OD_BAL_AMT  =0 --月累计透支余额
            ,NOD_In_MTH           =0 --当月有效天数             
	 			;	 	--
	 END IF;--

 /*获得当日统计数据*/	
 
		SET SMY_STEPNUM = 3 ;--
		SET SMY_STEPDESC = '声明临时表SESSION.CUR, 存放信用卡当日汇总后的数据'; 		 --
 
 DECLARE GLOBAL TEMPORARY TABLE CUR AS (
		SELECT 
			   CR_CRD_SMY.CST_ID                  AS CST_ID                --客户内码      
        ,CR_CRD_SMY.OU_ID                   AS CRD_OU_ID             --账户归属机构号
        ,CR_CRD_SMY.ENT_IDV_IND             AS ENT_IDV_IND           --卡对象
        ,CR_CRD_SMY.MST_CRD_IND             AS MST_CRD_IND           --主/副卡标志
        ,CR_CRD_SMY.LN_FIVE_RTG_STS         AS LN_FIVE_RTG_STS       --贷款五级形态类型
        ,CR_CRD_SMY.AST_RSK_ASES_RTG_TP_CD  AS AST_RSK_ASES_RTG_TP_CD   --资产风险分类
        ,CR_CRD_SMY.PD_GRP_CD               AS PD_GRP_CD                --产品类      
        ,CR_CRD_SMY.PD_SUB_CD               AS PD_SUB_CD                --产品子代码     
        ,CR_CRD_SMY.CCY                     AS CCY                   --币种          
        ,1                                  AS NOD_IN_MTH  
        ,1                                  AS NOD_IN_QTR
        ,1                                  AS NOD_IN_YEAR    
        ,SUM(CR_CRD_SMY.DEP_BAL_CRD)        AS DEP_BAL_CRD        --银行卡存款余额
        ,SUM(CR_CRD_SMY.OD_BAL_AMT )        AS OD_BAL_AMT         --透支余额      
        ,SUM(CR_CRD_SMY.CR_LMT     )        AS CR_LMT             --授信额度      
        ,SUM(CR_CRD_SMY.INT_RCVB   )        AS INT_RCVB           --应收利息      
                                                                    
		FROM            SMY.CR_CRD_SMY  AS CR_CRD_SMY
		GROUP BY 
         CR_CRD_SMY.CST_ID                
        ,CR_CRD_SMY.OU_ID                 
        ,CR_CRD_SMY.ENT_IDV_IND           
        ,CR_CRD_SMY.MST_CRD_IND           
        ,CR_CRD_SMY.LN_FIVE_RTG_STS       
        ,CR_CRD_SMY.AST_RSK_ASES_RTG_TP_CD
        ,CR_CRD_SMY.PD_GRP_CD             
        ,CR_CRD_SMY.PD_SUB_CD             
        ,CR_CRD_SMY.CCY                   
       			    
  ) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K 
     PARTITIONING KEY(CST_ID) ;  			--
     
  INSERT INTO SESSION.CUR 
		SELECT 
			   CR_CRD_SMY.CST_ID                  AS CST_ID                --客户内码      
        ,CR_CRD_SMY.OU_ID                   AS CRD_OU_ID             --账户归属机构号
        ,CR_CRD_SMY.ENT_IDV_IND             AS ENT_IDV_IND           --卡对象
        ,CR_CRD_SMY.MST_CRD_IND             AS MST_CRD_IND           --主/副卡标志
        ,CR_CRD_SMY.LN_FIVE_RTG_STS         AS LN_FIVE_RTG_STS       --贷款五级形态类型
        ,CR_CRD_SMY.AST_RSK_ASES_RTG_TP_CD  AS AST_RSK_ASES_RTG_TP_CD   --资产风险分类
        ,CR_CRD_SMY.PD_GRP_CD               AS PD_GRP_CD                --产品类      
        ,CR_CRD_SMY.PD_SUB_CD               AS PD_SUB_CD                --产品子代码     
        ,CR_CRD_SMY.CCY                     AS CCY                   --币种          
        ,1                                  AS NOD_IN_MTH  
        ,1                                  AS NOD_IN_QTR
        ,1                                  AS NOD_IN_YEAR    
        ,SUM(CR_CRD_SMY.DEP_BAL_CRD)        AS DEP_BAL_CRD        --银行卡存款余额
        ,SUM(CR_CRD_SMY.OD_BAL_AMT )        AS OD_BAL_AMT         --透支余额      
        ,SUM(CR_CRD_SMY.CR_LMT     )        AS CR_LMT             --授信额度      
        ,SUM(CR_CRD_SMY.INT_RCVB   )        AS INT_RCVB           --应收利息      
                                                                    
		FROM  SMY.CR_CRD_SMY  AS CR_CRD_SMY  WHERE CR_CRD_SMY.CRD_LCS_TP_ID IN (11920001 ,11920002,11920003)  --11920001:正常,11920002:新发卡未启用,11920003:新换卡未启用
		GROUP BY 
         CR_CRD_SMY.CST_ID                
        ,CR_CRD_SMY.OU_ID                 
        ,CR_CRD_SMY.ENT_IDV_IND           
        ,CR_CRD_SMY.MST_CRD_IND           
        ,CR_CRD_SMY.LN_FIVE_RTG_STS       
        ,CR_CRD_SMY.AST_RSK_ASES_RTG_TP_CD
        ,CR_CRD_SMY.PD_GRP_CD             
        ,CR_CRD_SMY.PD_SUB_CD             
        ,CR_CRD_SMY.CCY   
   ;--
 /** 收集操作信息 */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
	 

		SET SMY_STEPNUM = 4 ;--
		SET SMY_STEPDESC = '声明临时表SESSION.S, 用来存放信用卡汇总后要更新的数据'; 			 --


/**/
  DECLARE GLOBAL TEMPORARY TABLE S  LIKE SMY.CST_CR_CRD_MTHLY_SMY 
  	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K 
     PARTITIONING KEY(CST_ID);--
  
	INSERT INTO SESSION.S
          (
         CST_ID                  --客户内码        
        ,CRD_OU_ID               --受理机构号      
        ,ENT_IDV_IND             --卡对象          
        ,MST_CRD_IND             --主/副卡标志     
        ,LN_FIVE_RTG_STS         --贷款五级形态类型
        ,AST_RSK_ASES_RTG_TP_CD  --资产风险分类    
        ,PD_GRP_CD               --产品类          
        ,PD_SUB_CD               --产品子代码      
        ,CCY                     --币种            
        ,NOCLD_In_MTH            --当月日历天数    
        ,NOD_In_MTH              --当月有效天数    
        ,NOCLD_In_QTR            --当季日历天数    
        ,NOD_In_QTR              --当季有效天数    
        ,NOCLD_In_Year           --当年日历天数    
        ,NOD_In_Year             --当年有效天数
        ,CDR_YR                  --日历年
        ,CDR_MTH                 --日历月            
        ,ACG_DT                  --日期YYYY-MM-DD  
        ,DEP_BAL_CRD             --银行卡存款余额  
        ,OD_BAL_AMT              --透支余额        
        ,CR_LMT                  --授信额度        
        ,INT_RCVB                --应收利息        
        ,LST_DAY_DEP_BAL         --昨日存款余额    
        ,LST_DAY_OD_BAL          --昨日透支余额    
        ,MTD_ACML_DEP_BAL_AMT    --月累计存款余额  
        ,QTD_ACML_DEP_BAL_AMT    --季累计存款余额  
        ,YTD_ACML_DEP_BAL_AMT    --年累计存款余额  
        ,MTD_ACML_OD_BAL_AMT     --月累计透支余额  
        ,QTD_ACML_OD_BAL_AMT     --季累计透支余额  
        ,YTD_ACML_OD_BAL_AMT     --年累计透支余额 
            )    	 	  
	SELECT  
			   CUR.CST_ID                  --客户内码      
        ,CUR.CRD_OU_ID                   --账户归属机构号
        ,CUR.ENT_IDV_IND             --卡对象
        ,CUR.MST_CRD_IND             --主/副卡标志
        ,CUR.LN_FIVE_RTG_STS         --贷款五级形态类型
        ,CUR.AST_RSK_ASES_RTG_TP_CD  --资产风险分类
        ,CUR.PD_GRP_CD               --产品类      
        ,CUR.PD_SUB_CD               --产品子代码     
        ,CUR.CCY                     --币种
        ,C_MON_DAY      --当月日历天数
        ,COALESCE(PRE.NOD_In_MTH,0) + CUR.NOD_In_MTH        --当月有效天数
        ,C_QTR_DAY      --当季日历天数  
        ,COALESCE(PRE.NOD_In_QTR,0) + CUR.NOD_In_QTR        --当季有效天数  
        ,C_YR_DAY          --当年日历天数  
        ,COALESCE(PRE.NOD_In_YEAR,0) + CUR.NOD_In_YEAR       --当年有效天数                                   
        ,CUR_YEAR            --年份YYYY      
        ,CUR_MONTH           --月份MM        
        ,ACCOUNTING_DATE            --日期YYYY-MM-DD    
        ,CUR.DEP_BAL_CRD             --银行卡存款余额  
        ,CUR.OD_BAL_AMT              --透支余额        
        ,CUR.CR_LMT                  --授信额度        
        ,CUR.INT_RCVB                --应收利息
        ,COALESCE(PRE.DEP_BAL_CRD,0) --昨日存款余额  
        ,COALESCE(PRE.OD_BAL_AMT ,0) --昨日透支余额
        ,COALESCE(PRE.MTD_ACML_DEP_BAL_AMT,0) + CUR.DEP_BAL_CRD    --月累计存款余额  
        ,COALESCE(PRE.QTD_ACML_DEP_BAL_AMT,0) + CUR.DEP_BAL_CRD    --季累计存款余额  
        ,COALESCE(PRE.YTD_ACML_DEP_BAL_AMT,0) + CUR.DEP_BAL_CRD    --年累计存款余额  
        ,COALESCE(PRE.MTD_ACML_OD_BAL_AMT ,0) + CUR.OD_BAL_AMT    --月累计透支余额  
        ,COALESCE(PRE.QTD_ACML_OD_BAL_AMT ,0) + CUR.OD_BAL_AMT    --季累计透支余额  
        ,COALESCE(PRE.YTD_ACML_OD_BAL_AMT ,0) + CUR.OD_BAL_AMT    --年累计透支余额                         

	FROM  SESSION.CUR  AS CUR LEFT OUTER JOIN  SESSION.TMP AS PRE ON 
       CUR.CST_ID                 =PRE.CST_ID                
   AND CUR.CRD_OU_ID              =PRE.CRD_OU_ID             
   AND CUR.CCY                    =PRE.CCY                   
   AND CUR.LN_FIVE_RTG_STS        =PRE.LN_FIVE_RTG_STS       
   AND CUR.MST_CRD_IND            =PRE.MST_CRD_IND           
   AND CUR.AST_RSK_ASES_RTG_TP_CD =PRE.AST_RSK_ASES_RTG_TP_CD
   AND CUR.PD_GRP_CD              =PRE.PD_GRP_CD             
   AND CUR.PD_SUB_CD              =PRE.PD_SUB_CD      
      ;--
 		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;	          --

 /** Insert the log**/
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	--

		SET SMY_STEPNUM = 5 ;--
		SET SMY_STEPDESC = '使用Merge语句,更新SMY 表'; 			 --
	 
MERGE INTO SMY.CST_CR_CRD_MTHLY_SMY AS T
 		USING  SESSION.S AS S 
 	  ON
         T.CST_ID                 =S.CST_ID                
     AND T.CRD_OU_ID              =S.CRD_OU_ID             
     AND T.CCY                    =S.CCY                   
     AND T.LN_FIVE_RTG_STS        =S.LN_FIVE_RTG_STS
     AND T.MST_CRD_IND            =S.MST_CRD_IND           
     AND T.AST_RSK_ASES_RTG_TP_CD =S.AST_RSK_ASES_RTG_TP_CD
     AND T.PD_GRP_CD              =S.PD_GRP_CD             
     AND T.PD_SUB_CD              =S.PD_SUB_CD
     AND T.CDR_YR                 =S.CDR_YR     
     AND T.CDR_MTH                =S.CDR_MTH    
WHEN MATCHED THEN UPDATE SET
    
         NOCLD_In_MTH             =S.NOCLD_In_MTH          --当月日历天数    
        ,NOD_In_MTH               =S.NOD_In_MTH            --当月有效天数    
        ,NOCLD_In_QTR             =S.NOCLD_In_QTR          --当季日历天数    
        ,NOD_In_QTR               =S.NOD_In_QTR            --当季有效天数    
        ,NOCLD_In_Year            =S.NOCLD_In_Year         --当年日历天数    
        ,NOD_In_Year              =S.NOD_In_Year           --当年有效天数         
        ,ACG_DT                   =S.ACG_DT                --日期YYYY-MM-DD  
        ,DEP_BAL_CRD              =S.DEP_BAL_CRD           --银行卡存款余额  
        ,OD_BAL_AMT               =S.OD_BAL_AMT            --透支余额        
        ,CR_LMT                   =S.CR_LMT                --授信额度        
        ,INT_RCVB                 =S.INT_RCVB              --应收利息        
        ,LST_DAY_DEP_BAL          =S.LST_DAY_DEP_BAL       --昨日存款余额    
        ,LST_DAY_OD_BAL           =S.LST_DAY_OD_BAL        --昨日透支余额    
        ,MTD_ACML_DEP_BAL_AMT     =S.MTD_ACML_DEP_BAL_AMT  --月累计存款余额  
        ,QTD_ACML_DEP_BAL_AMT     =S.QTD_ACML_DEP_BAL_AMT  --季累计存款余额  
        ,YTD_ACML_DEP_BAL_AMT     =S.YTD_ACML_DEP_BAL_AMT  --年累计存款余额  
        ,MTD_ACML_OD_BAL_AMT      =S.MTD_ACML_OD_BAL_AMT   --月累计透支余额  
        ,QTD_ACML_OD_BAL_AMT      =S.QTD_ACML_OD_BAL_AMT   --季累计透支余额  
        ,YTD_ACML_OD_BAL_AMT      =S.YTD_ACML_OD_BAL_AMT   --年累计透支余额  
          
WHEN NOT MATCHED THEN INSERT  	        
	 (
         CST_ID                  --客户内码        
        ,CRD_OU_ID               --受理机构号      
        ,ENT_IDV_IND             --卡对象          
        ,MST_CRD_IND             --主/副卡标志     
        ,LN_FIVE_RTG_STS         --贷款五级形态类型
        ,AST_RSK_ASES_RTG_TP_CD  --资产风险分类    
        ,PD_GRP_CD               --产品类          
        ,PD_SUB_CD               --产品子代码      
        ,CCY                     --币种            
        ,NOCLD_In_MTH            --当月日历天数    
        ,NOD_In_MTH              --当月有效天数    
        ,NOCLD_In_QTR            --当季日历天数    
        ,NOD_In_QTR              --当季有效天数    
        ,NOCLD_In_Year           --当年日历天数    
        ,NOD_In_Year             --当年有效天数
        ,CDR_YR                  --日历年
        ,CDR_MTH                 --日历月            
        ,ACG_DT                  --日期YYYY-MM-DD  
        ,DEP_BAL_CRD             --银行卡存款余额  
        ,OD_BAL_AMT              --透支余额        
        ,CR_LMT                  --授信额度        
        ,INT_RCVB                --应收利息        
        ,LST_DAY_DEP_BAL         --昨日存款余额    
        ,LST_DAY_OD_BAL          --昨日透支余额    
        ,MTD_ACML_DEP_BAL_AMT    --月累计存款余额  
        ,QTD_ACML_DEP_BAL_AMT    --季累计存款余额  
        ,YTD_ACML_DEP_BAL_AMT    --年累计存款余额  
        ,MTD_ACML_OD_BAL_AMT     --月累计透支余额  
        ,QTD_ACML_OD_BAL_AMT     --季累计透支余额  
        ,YTD_ACML_OD_BAL_AMT     --年累计透支余额  
        )
    VALUES 
    (
         S.CST_ID                  --客户内码        
        ,S.CRD_OU_ID               --受理机构号      
        ,S.ENT_IDV_IND             --卡对象          
        ,S.MST_CRD_IND             --主/副卡标志     
        ,S.LN_FIVE_RTG_STS         --贷款五级形态类型
        ,S.AST_RSK_ASES_RTG_TP_CD  --资产风险分类    
        ,S.PD_GRP_CD               --产品类          
        ,S.PD_SUB_CD               --产品子代码      
        ,S.CCY                     --币种            
        ,S.NOCLD_In_MTH            --当月日历天数    
        ,S.NOD_In_MTH              --当月有效天数    
        ,S.NOCLD_In_QTR            --当季日历天数    
        ,S.NOD_In_QTR              --当季有效天数    
        ,S.NOCLD_In_Year           --当年日历天数    
        ,S.NOD_In_Year             --当年有效天数
        ,S.CDR_YR                  --日历年
        ,S.CDR_MTH                 --日历月            
        ,S.ACG_DT                  --日期YYYY-MM-DD  
        ,S.DEP_BAL_CRD             --银行卡存款余额  
        ,S.OD_BAL_AMT              --透支余额        
        ,S.CR_LMT                  --授信额度        
        ,S.INT_RCVB                --应收利息        
        ,S.LST_DAY_DEP_BAL         --昨日存款余额    
        ,S.LST_DAY_OD_BAL          --昨日透支余额    
        ,S.MTD_ACML_DEP_BAL_AMT    --月累计存款余额  
        ,S.QTD_ACML_DEP_BAL_AMT    --季累计存款余额  
        ,S.YTD_ACML_DEP_BAL_AMT    --年累计存款余额  
        ,S.MTD_ACML_OD_BAL_AMT     --月累计透支余额  
        ,S.QTD_ACML_OD_BAL_AMT     --季累计透支余额  
        ,S.YTD_ACML_OD_BAL_AMT     --年累计透支余额 
    )	  	
	;--
	
	 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
	 
	 COMMIT;--
END@