DROP PROCEDURE SMY.PROC_MCHNT_CNSP_MTHLY_SMY@
CREATE PROCEDURE SMY.PROC_MCHNT_CNSP_MTHLY_SMY(IN ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_MCHNT_CNSP_MTHLY_SMY.sql
-- Procedure name: 			SMY.PROC_MCHNT_CNSP_MTHLY_SMY
-- Source Table:				SOR.STMT_DEP_AC_RGST ,SOR.MCHNT,SOR.CRD
-- Target Table: 				SMY.MCHNT_CNSP_MTHLY_SMY
-- Project     :        ZJ RCCB EDW
-- NOTES       :        
-- Purpose     :            
-- PROCESS METHOD      :  UPDATE EACH DAY ,INSERT IN THE PERIOD OF ONE MONTH
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
-- 2009-12-04   Xu Yan          Rename the history table	
-- 2009-12-16   Xu Yan          Fixed a bug for reruning
-- 2009-12-25   Xu Yan          Change the OU_ID to MCHNT.ISSU_OU_ID
-- 2010-01-14   Xu Yan          Redefined the script to orginate from STMT_DEP_AC_RGST.
-- 2010-01-18   Xu Yan          Added columns TXN_CNL_TP_CD, DB_CR_IND, CASH_TFR_IND, TXN_CNL_TP_ID, CNSPN_TXN_F
-- 2012-05-29   Chen XiaoWen    去除SOR.STMT_DEP_AC_RGST.TXN_TP_ID = 20460007(正常交易)的限制
-- 2012-07-16   Chen XiaoWen    修复数据未回插导致累计值断层后重新累计的BUG
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
    SET SMY_PROCNM  ='PROC_MCHNT_CNSP_MTHLY_SMY';--
    SET SMY_DATE    =ACCOUNTING_DATE;    --
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --取当前年份
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --取当前月份
    SET CUR_DAY     =DAY(ACCOUNTING_DATE);     --取月第几日
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- 取年初日
    SET C_YR_DAY      =DAYOFYEAR(DATE(TRIM(CHAR(YEAR(ACCOUNTING_DATE)))||'-12-31')); --取当年第几日
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --当前季度
    SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,ISO),1,7)||'-01'); --取当月初日
    SET MON_DAY     =DAY(ACCOUNTING_DATE);     --取月第几日
    
    VALUES(ACCOUNTING_DATE - 1 DAY) INTO LAST_SMY_DATE;--
    SET LAST_MONTH = MONTH(LAST_SMY_DATE);      --
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

  /*取当季日历天数*/ 
  	SET C_QTR_DAY = DAYS(QTR_LAST_DAY) - DAYS(QTR_FIRST_DAY) + 1 ;--
		
		SELECT COALESCE(MAX(ACG_DT),'1900-01-01') INTO MAX_ACG_DT FROM SMY.MCHNT_CNSP_MTHLY_SMY;--

/*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE,并插入新的起始标志*/
		DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
			COMMIT;--
		
		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		
		SET SMY_STEPDESC = 	'存储过程开始运行' ;--
		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
				VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

/*数据恢复与备份*/
    IF MAX_ACG_DT = ACCOUNTING_DATE THEN
       DELETE FROM SMY.MCHNT_CNSP_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = CUR_MONTH;--
    /**每月第一日不需要从历史表中恢复**/       
       IF MON_DAY <> 1 THEN
      	 INSERT INTO SMY.MCHNT_CNSP_MTHLY_SMY SELECT * FROM HIS.MCHNT_CNSP_MTHLY_SMY ;--
       END IF;--
     ELSE
  /** 清空hist 备份表 **/

	    SET EMP_SQL= 'Alter TABLE HIS.MCHNT_CNSP_MTHLY_SMY ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE' ;--
		
		  EXECUTE IMMEDIATE EMP_SQL;       --
      
      COMMIT;--
		  /**backup 昨日数据 **/
		  
		   INSERT INTO HIS.MCHNT_CNSP_MTHLY_SMY SELECT * FROM SMY.MCHNT_CNSP_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = CUR_MONTH;--
      
    END IF;--

SET SMY_STEPNUM = 2 ;--
SET SMY_STEPDESC = '声明用户临时表,存放昨日SMY数据';--

	/*声明用户临时表*/
	
	DECLARE GLOBAL TEMPORARY TABLE TMP 
		LIKE SMY.MCHNT_CNSP_MTHLY_SMY
	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(OU_ID);--

 /*如果是年第一日不需要插入*/

 IF YR_FIRST_DAY <>  ACCOUNTING_DATE THEN 
	INSERT INTO SESSION.TMP 
	(
      MCHNT_TP_CD      --商户类型
     ,OU_ID            --机构号
     ,CRD_TP_ID        --卡类型
     ,CCY              --币种
     ,CDR_YR           --年份YYYY
     ,CDR_MTH          --月份MM
     ,ACG_DT           --日期YYYY-MM-DD
     ,TOT_MTD_NBR_TXN  --月累计交易笔数
     ,TOT_MTD_AMT      --月累计金额
     ,TOT_QTD_AMT      --累计金额
     ,TOT_QTD_NBR_TXN  --累计交易笔数
     ,TOT_YTD_AMT      --累计金额
     ,TOT_YTD_NBR_TXN  --累计交易笔数 
     ,TXN_CNL_TP_CD    --交易渠道代码
     ,DB_CR_IND        --借贷标志
     ,CASH_TFR_IND     --现转标志
     ,TXN_CNL_TP_ID    --交易渠道类型
     ,CNSPN_TXN_F      --消费标志     
          ) 
    SELECT
         MCHNT_TP_CD      --商户类型
        ,OU_ID            --机构号
        ,CRD_TP_ID        --卡类型
        ,CCY              --币种
        ,CDR_YR           --年份YYYY
        ,CDR_MTH          --月份MM
        ,ACG_DT           --日期YYYY-MM-DD
        ,TOT_MTD_NBR_TXN  --月累计交易笔数
        ,TOT_MTD_AMT      --月累计金额
        ,TOT_QTD_AMT      --累计金额
        ,TOT_QTD_NBR_TXN  --累计交易笔数
        ,TOT_YTD_AMT      --累计金额
        ,TOT_YTD_NBR_TXN  --累计交易笔数  
	     ,TXN_CNL_TP_CD    --交易渠道代码
	     ,DB_CR_IND        --借贷标志
	     ,CASH_TFR_IND     --现转标志
	     ,TXN_CNL_TP_ID    --交易渠道类型 
	     ,CNSPN_TXN_F      --消费标志        			       
     FROM SMY.MCHNT_CNSP_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = LAST_MONTH;  --LAST_MONTH = MONTH(LAST_SMY_DATE)
 END IF ;   --
	

       
      
 /** 收集操作信息 */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	 --


	 /*IF  ACCOUNTING_DATE IN ( YR_FIRST_DAY)  --年 季 月 归零
	 		THEN 
	 			UPDATE SESSION.TMP 
	 				SET 
         TOT_MTD_NBR_TXN =0 --月累计交易笔数
        ,TOT_MTD_AMT     =0 --月累计金额
        ,TOT_QTD_AMT     =0 --累计金额
        ,TOT_QTD_NBR_TXN =0 --累计交易笔数
        ,TOT_YTD_AMT     =0 --累计金额
        ,TOT_YTD_NBR_TXN =0 --累计交易笔数
	 		  ;--
	 ELSE*/
	 IF ACCOUNTING_DATE IN (QTR_FIRST_DAY) --季 月 归零
	 	  THEN
	 			UPDATE SESSION.TMP 
	 				SET 
         TOT_MTD_NBR_TXN =0 --月累计交易笔数    
        ,TOT_MTD_AMT     =0 --月累计金额
        ,TOT_QTD_AMT     =0 --累计金额
        ,TOT_QTD_NBR_TXN =0 --累计交易笔数
	 			;	 	  --
	 	  	 		
	 ELSEIF ACCOUNTING_DATE IN ( MTH_FIRST_DAY ) --月归零
	 	  THEN 
	 			UPDATE SESSION.TMP 
	 				SET 
          TOT_MTD_NBR_TXN =0 --月累计交易笔数
        ,TOT_MTD_AMT     =0 --月累计金额
	 			;	 	--
	 END IF;--

 /*获得当日统计数据*/	
 
		SET SMY_STEPNUM = 3 ;--
		SET SMY_STEPDESC = '声明临时表SESSION.CUR, 存放当日汇总后的数据'; 		 --
 
 DECLARE GLOBAL TEMPORARY TABLE CUR AS (
 
  SELECT 
         MCHNT.MCHNT_TP_CD                AS MCHNT_TP_CD      --商户类型
        ,STMT_DEP_AC_RGST.TXN_OU_ID       AS OU_ID            --机构号  
        ,CRD.CRD_TP_ID               AS CRD_TP_ID        --卡类型  
        ,STMT_DEP_AC_RGST.DNMN_CCY_ID     AS CCY              --币种    
        ,COUNT(1)                         AS CUR_NBR_TXN
        ,SUM(STMT_DEP_AC_RGST.TXN_AMT)    AS CUR_AMT
	     ,STMT_DEP_AC_RGST.ALS_CNL AS TXN_CNL_TP_CD    --交易渠道代码
	     ,STMT_DEP_AC_RGST.DB_CR_IND        --借贷标志
	     ,STMT_DEP_AC_RGST.CASH_TFR_IND     --现转标志
	     ,STMT_DEP_AC_RGST.CNL_TP AS TXN_CNL_TP_ID    --交易渠道类型 
	     ,(CASE WHEN SUBSTR(STMT_DEP_AC_RGST.MCHNT_AC_AR_ID,2,1)='1' THEN 1 ELSE 0 END)  AS CNSPN_TXN_F      --消费标志         
  FROM SOR.STMT_DEP_AC_RGST  AS STMT_DEP_AC_RGST
  JOIN  SOR.MCHNT AS MCHNT    ON MCHNT.MCHNT_SEQ_NO = STMT_DEP_AC_RGST.MCHNT_SEQ_NBR
  LEFT OUTER JOIN  SOR.CRD   AS CRD      ON STMT_DEP_AC_RGST.CC_AC_AR_ID = CRD.CRD_NO
      GROUP BY 
      	   MCHNT.MCHNT_TP_CD           
          ,STMT_DEP_AC_RGST.TXN_OU_ID  
          ,CRD.CRD_TP_ID          
          ,STMT_DEP_AC_RGST.DNMN_CCY_ID 
           ,STMT_DEP_AC_RGST.ALS_CNL     --交易渠道代码
			     ,STMT_DEP_AC_RGST.DB_CR_IND        --借贷标志
			     ,STMT_DEP_AC_RGST.CASH_TFR_IND     --现转标志
			     ,STMT_DEP_AC_RGST.CNL_TP    --交易渠道类型 
			     ,(CASE WHEN SUBSTR(STMT_DEP_AC_RGST.MCHNT_AC_AR_ID,2,1)='1' THEN 1 ELSE 0 END)        --消费标志          
  ) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K 
     PARTITIONING KEY(OU_ID) ;  			--
     
  INSERT INTO SESSION.CUR 
  -------------------------Start on 20100114--------------------------------------------------------------------
  With CRD as (
     select  CRD_NO
            ,OU_ID
            ,DB_CRD_TP_ID as CRD_TP_ID
     from SMY.DB_CRD_SMY
     union all
     select  CRD_NO
            ,OU_ID
            ,CR_CRD_TP_ID as CRD_TP_ID
     from SMY.CR_CRD_SMY
  )
  SELECT 
         substr(STMT_DEP_AC_RGST.MCHNT_SEQ_NBR,8,4)               AS MCHNT_TP_CD      --商户类型
        --,STMT_DEP_AC_RGST.TXN_OU_ID       AS OU_ID            --机构号  
        ,CRD.OU_ID       AS OU_ID            --机构号  
        ,CRD.CRD_TP_ID               AS CRD_TP_ID        --卡类型  
        ,STMT_DEP_AC_RGST.DNMN_CCY_ID     AS CCY              --币种    
        ,COUNT(1)                         AS CUR_NBR_TXN
        ,SUM(STMT_DEP_AC_RGST.TXN_AMT)    AS CUR_AMT
	     ,STMT_DEP_AC_RGST.ALS_CNL AS TXN_CNL_TP_CD    --交易渠道代码
	     ,STMT_DEP_AC_RGST.DB_CR_IND        --借贷标志
	     ,STMT_DEP_AC_RGST.CASH_TFR_IND     --现转标志
	     ,STMT_DEP_AC_RGST.CNL_TP AS TXN_CNL_TP_ID    --交易渠道类型  
	     ,(CASE WHEN SUBSTR(STMT_DEP_AC_RGST.MCHNT_AC_AR_ID,2,1)='1' THEN 1 ELSE 0 END)  AS CNSPN_TXN_F      --消费标志        
  FROM SOR.STMT_DEP_AC_RGST  AS STMT_DEP_AC_RGST  
  JOIN  CRD  ON STMT_DEP_AC_RGST.CC_AC_AR_ID = CRD.CRD_NO
  where STMT_DEP_AC_RGST.TXN_DT = SMY_DATE
        and
        STMT_DEP_AC_RGST.DEL_F <> 1
      GROUP BY 
      	   substr(STMT_DEP_AC_RGST.MCHNT_SEQ_NBR,8,4)           
          ,CRD.OU_ID  
          ,CRD.CRD_TP_ID          
          ,STMT_DEP_AC_RGST.DNMN_CCY_ID 
       ,STMT_DEP_AC_RGST.ALS_CNL     --交易渠道代码
	     ,STMT_DEP_AC_RGST.DB_CR_IND        --借贷标志
	     ,STMT_DEP_AC_RGST.CASH_TFR_IND     --现转标志
	     ,STMT_DEP_AC_RGST.CNL_TP   --交易渠道类型 
	     ,(CASE WHEN SUBSTR(STMT_DEP_AC_RGST.MCHNT_AC_AR_ID,2,1)='1' THEN 1 ELSE 0 END) --消费标志
  -------------------------End on 20100114--------------------------------------------------------------------          
   ;--
 /** 收集操作信息 */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
	 
		SET SMY_STEPNUM = 4 ;--
		SET SMY_STEPDESC = '声明临时表SESSION.S, 用来存放要更新的数据'; 			 --


/**/
  DECLARE GLOBAL TEMPORARY TABLE S  LIKE SMY.MCHNT_CNSP_MTHLY_SMY 
  	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K 
     PARTITIONING KEY(OU_ID);--

	INSERT INTO SESSION.S
          (
         MCHNT_TP_CD      --商户类型
        ,OU_ID            --机构号
        ,CRD_TP_ID        --卡类型
        ,CCY              --币种
        ,CDR_YR           --年份YYYY
        ,CDR_MTH          --月份MM
        ,ACG_DT           --日期YYYY-MM-DD
        ,TOT_MTD_NBR_TXN  --月累计交易笔数
        ,TOT_MTD_AMT      --月累计金额
        ,TOT_QTD_AMT      --累计金额
        ,TOT_QTD_NBR_TXN  --累计交易笔数
        ,TOT_YTD_AMT      --累计金额
        ,TOT_YTD_NBR_TXN  --累计交易笔数   
	     ,TXN_CNL_TP_CD    --交易渠道代码
	     ,DB_CR_IND        --借贷标志
	     ,CASH_TFR_IND     --现转标志
	     ,TXN_CNL_TP_ID    --交易渠道类型  
	     ,CNSPN_TXN_F      --消费标志         	
            )      	 	  
	SELECT  
         CUR.MCHNT_TP_CD      --商户类型
        ,CUR.OU_ID            --机构号
        ,CUR.CRD_TP_ID        --卡类型
        ,CUR.CCY              --币种
        ,CUR_YEAR           --年份YYYY
        ,CUR_MONTH          --月份MM
        ,ACCOUNTING_DATE           --日期YYYY-MM-DD
        ,COALESCE(PRE.TOT_MTD_NBR_TXN ,0) + CUR.CUR_NBR_TXN --月累计交易笔数
        ,COALESCE(PRE.TOT_MTD_AMT     ,0) + CUR.CUR_AMT     --月累计金额
        ,COALESCE(PRE.TOT_QTD_AMT     ,0) + CUR.CUR_AMT     --累计金额
        ,COALESCE(PRE.TOT_QTD_NBR_TXN ,0) + CUR.CUR_NBR_TXN --累计交易笔数
        ,COALESCE(PRE.TOT_YTD_AMT     ,0) + CUR.CUR_AMT     --累计金额
        ,COALESCE(PRE.TOT_YTD_NBR_TXN ,0) + CUR.CUR_NBR_TXN --累计交易笔数 
	     ,CUR.TXN_CNL_TP_CD    --交易渠道代码
	     ,CUR.DB_CR_IND        --借贷标志
	     ,CUR.CASH_TFR_IND     --现转标志
	     ,CUR.TXN_CNL_TP_ID    --交易渠道类型
	     ,CUR.CNSPN_TXN_F      --消费标志  	        
	FROM  SESSION.CUR  AS CUR LEFT OUTER JOIN  SESSION.TMP AS PRE ON 
           CUR.MCHNT_TP_CD =PRE.MCHNT_TP_CD 
      AND CUR.CRD_TP_ID   =PRE.CRD_TP_ID   
      AND CUR.CCY         =PRE.CCY         
      AND CUR.OU_ID       =PRE.OU_ID  
      AND CUR.TXN_CNL_TP_CD=PRE.TXN_CNL_TP_CD    --交易渠道代码
      AND CUR.DB_CR_IND    =PRE.DB_CR_IND        --借贷标志    
      AND CUR.CASH_TFR_IND =PRE.CASH_TFR_IND     --现转标志    
      AND CUR.TXN_CNL_TP_ID=PRE.TXN_CNL_TP_ID    --交易渠道类型   
      AND CUR.CNSPN_TXN_F =PRE.CNSPN_TXN_F   --消费标志     
      ;--
 		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;	           --

 /** Insert the log**/
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	--

		SET SMY_STEPNUM = 5 ;--
		SET SMY_STEPDESC = '使用Merge语句,更新SMY 表'; 			 --
	 
MERGE INTO SMY.MCHNT_CNSP_MTHLY_SMY AS T
 		USING  SESSION.S AS S 
 	  ON
          T.MCHNT_TP_CD =S.MCHNT_TP_CD 
      AND T.CRD_TP_ID   =S.CRD_TP_ID   
      AND T.CCY         =S.CCY         
      AND T.OU_ID       =S.OU_ID
      AND T.CDR_YR      =S.CDR_YR
      AND T.CDR_MTH      =S.CDR_MTH
      AND T.TXN_CNL_TP_CD=S.TXN_CNL_TP_CD    --交易渠道代码
      AND T.DB_CR_IND    =S.DB_CR_IND        --借贷标志    
      AND T.CASH_TFR_IND =S.CASH_TFR_IND     --现转标志    
      AND T.TXN_CNL_TP_ID=S.TXN_CNL_TP_ID    --交易渠道类型 
      AND T.CNSPN_TXN_F = S.CNSPN_TXN_F     --消费标志       	                  
WHEN MATCHED THEN UPDATE SET
	       ACG_DT           =S.ACG_DT          --日期YYYY-MM-DD
        ,TOT_MTD_NBR_TXN  =S.TOT_MTD_NBR_TXN --月累计交易笔数
        ,TOT_MTD_AMT      =S.TOT_MTD_AMT     --月累计金额
        ,TOT_QTD_AMT      =S.TOT_QTD_AMT     --累计金额
        ,TOT_QTD_NBR_TXN  =S.TOT_QTD_NBR_TXN --累计交易笔数
        ,TOT_YTD_AMT      =S.TOT_YTD_AMT     --累计金额
        ,TOT_YTD_NBR_TXN  =S.TOT_YTD_NBR_TXN --累计交易笔数
          
WHEN NOT MATCHED THEN INSERT  	        
	 (
         MCHNT_TP_CD      --商户类型
        ,OU_ID            --机构号
        ,CRD_TP_ID        --卡类型
        ,CCY              --币种
        ,CDR_YR           --年份YYYY
        ,CDR_MTH          --月份MM
        ,ACG_DT           --日期YYYY-MM-DD
        ,TOT_MTD_NBR_TXN  --月累计交易笔数
        ,TOT_MTD_AMT      --月累计金额
        ,TOT_QTD_AMT      --累计金额
        ,TOT_QTD_NBR_TXN  --累计交易笔数
        ,TOT_YTD_AMT      --累计金额
        ,TOT_YTD_NBR_TXN  --累计交易笔数
	     ,TXN_CNL_TP_CD    --交易渠道代码
	     ,DB_CR_IND        --借贷标志
	     ,CASH_TFR_IND     --现转标志
	     ,TXN_CNL_TP_ID    --交易渠道类型   
	     ,CNSPN_TXN_F      --消费标志       	        
        )
    VALUES 
    (
         S.MCHNT_TP_CD    
        ,S.OU_ID          
        ,S.CRD_TP_ID      
        ,S.CCY            
        ,S.CDR_YR         
        ,S.CDR_MTH        
        ,S.ACG_DT         
        ,S.TOT_MTD_NBR_TXN
        ,S.TOT_MTD_AMT    
        ,S.TOT_QTD_AMT    
        ,S.TOT_QTD_NBR_TXN
        ,S.TOT_YTD_AMT    
        ,S.TOT_YTD_NBR_TXN   
	     ,S.TXN_CNL_TP_CD    --交易渠道代码
	     ,S.DB_CR_IND        --借贷标志
	     ,S.CASH_TFR_IND     --现转标志
	     ,S.TXN_CNL_TP_ID    --交易渠道类型  
	     ,S.CNSPN_TXN_F      --消费标志        	        
    )	  	
	;--

    --月初执行
    IF CUR_DAY = 1 THEN
        IF CUR_MONTH <> 1 THEN   --年初不需要回插
            GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
            INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS) 
                VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);
            SET SMY_STEPNUM = 6;
            SET SMY_STEPDESC = '将上月存在但本月没有的数据回插到原表';
            
            INSERT INTO SMY.MCHNT_CNSP_MTHLY_SMY
            (
                 MCHNT_TP_CD       --商户类型
                ,CRD_TP_ID         --卡类型
                ,TXN_CNL_TP_CD     --交易渠道代码
                ,DB_CR_IND         --借贷标志
                ,CASH_TFR_IND      --现转标志
                ,TXN_CNL_TP_ID     --交易渠道ID
                ,CNSPN_TXN_F       --消费交易标志
                ,CDR_YR
                ,CCY               --币种
                ,CDR_MTH
                ,OU_ID             --机构号
                ,ACG_DT
                ,TOT_MTD_NBR_TXN   --月累计交易笔数
                ,TOT_MTD_AMT       --月累计交易金额
                ,TOT_QTD_AMT       --季累计交易金额
                ,TOT_QTD_NBR_TXN   --季累计交易笔数
                ,TOT_YTD_AMT       --年累计交易金额
                ,TOT_YTD_NBR_TXN   --年累计交易笔数
            )
            SELECT
                 MCHNT_TP_CD
                ,CRD_TP_ID
                ,TXN_CNL_TP_CD
                ,DB_CR_IND
                ,CASH_TFR_IND
                ,TXN_CNL_TP_ID
                ,CNSPN_TXN_F
                ,CUR_YEAR
                ,CCY
                ,CUR_MONTH
                ,OU_ID
                ,SMY_DATE
                ,0
                ,0
                ,case when CUR_MONTH in (4,7,10) then 0 else TOT_QTD_AMT end
                ,case when CUR_MONTH in (4,7,10) then 0 else TOT_QTD_NBR_TXN end
                ,TOT_YTD_AMT
                ,TOT_YTD_NBR_TXN
            FROM SMY.MCHNT_CNSP_MTHLY_SMY PRE
            WHERE CDR_YR = CUR_YEAR and CDR_MTH = CUR_MONTH - 1
                AND NOT EXISTS(
                    SELECT 1 FROM SMY.MCHNT_CNSP_MTHLY_SMY CUR
                    WHERE CUR.MCHNT_TP_CD=PRE.MCHNT_TP_CD
                        AND CUR.CRD_TP_ID=PRE.CRD_TP_ID
                        AND CUR.CCY=PRE.CCY
                        AND CUR.OU_ID=PRE.OU_ID
                        AND CUR.TXN_CNL_TP_CD=PRE.TXN_CNL_TP_CD
                        AND CUR.DB_CR_IND=PRE.DB_CR_IND
                        AND CUR.CASH_TFR_IND=PRE.CASH_TFR_IND
                        AND CUR.TXN_CNL_TP_ID=PRE.TXN_CNL_TP_ID
                        AND CUR.CNSPN_TXN_F=PRE.CNSPN_TXN_F
                        AND CUR.CDR_YR=CDR_YR
                        AND CUR.CDR_MTH=CUR_MONTH)
            ;
        END IF;
    END IF;
	
	 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	 --
	 
	 COMMIT;--

END@