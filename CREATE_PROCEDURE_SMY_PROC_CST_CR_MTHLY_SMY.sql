CREATE PROCEDURE SMY.PROC_CST_CR_MTHLY_SMY(IN ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_CST_CR_MTHLY_SMY.sql
-- Procedure name: 			SMY.PROC_CST_CR_MTHLY_SMY
-- Source Table:				SOR.CST_LMT_DTL_INF,SOR.LMT_USED_INF
-- Target Table: 				SMY.CST_CR_MTHLY_SMY
-- Project     :        ZJ RCCB EDW
-- NOTES       :        
-- Purpose     :            
-- PROCESS METHOD      :  UPDATE EACH DAY ,INSERT IN THE PERIOD OF ONE MONTH
--=============================================================================
-- Creation Date:       2009.11.12
-- Origin Author:       JAMES SHANG IBM copyright 
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2009-11-12   JAMES SHANG     Create SP File	
-- 2009-12-04   Xu Yan          Rename the history table	
-- 2009-12-16   Xu Yan          Fixed a bug for rerunning
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
		DECLARE MON_DAY SMALLINT;
		DECLARE LAST_MONTH SMALLINT;
		DECLARE EMP_SQL VARCHAR(200);  

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
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);
      COMMIT;
      RESIGNAL;
    END;
    
    DECLARE CONTINUE HANDLER FOR SQLWARNING
    BEGIN
      SET SMY_SQLCODE = SQLCODE;
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);
      COMMIT;
    END;

   /*变量赋值*/
    SET SMY_PROCNM  ='PROC_CST_CR_MTHLY_SMY';
    SET SMY_DATE    =ACCOUNTING_DATE;    
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --取当前年份
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --取当前月份
    SET CUR_DAY     =DAY(ACCOUNTING_DATE);     --取月第几日
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- 取年初日
    SET C_YR_DAY      =DAYOFYEAR(DATE(TRIM(CHAR(YEAR(ACCOUNTING_DATE)))||'-12-31')); --取当年第几日
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --当前季度
    SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,iso),1,7)||'-01'); --取当月初日
    SET MON_DAY     =DAY(ACCOUNTING_DATE);     --取月第几日
    
    VALUES(ACCOUNTING_DATE - 1 DAY) INTO LAST_SMY_DATE;
    SET LAST_MONTH = MONTH(LAST_SMY_DATE);      
    --计算月日历天数
    SET C_MON_DAY = DAYS(MTH_FIRST_DAY + 1 MONTH ) - DAYS(MTH_FIRST_DAY);    
    
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
  	SET C_QTR_DAY = DAYS(QTR_LAST_DAY) - DAYS(QTR_FIRST_DAY) + 1 ;
		
		SELECT COALESCE(MAX(ACG_DT),'1900-01-01') INTO MAX_ACG_DT FROM SMY.CST_CR_MTHLY_SMY;

/*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE,并插入新的起始标志*/
		DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;
			COMMIT;
		
		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
		
		SET SMY_STEPDESC = 	'存储过程开始运行' ;
		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
				VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);

/*数据恢复与备份*/
    IF MAX_ACG_DT = ACCOUNTING_DATE THEN
       DELETE FROM SMY.CST_CR_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = CUR_MONTH;
    /**每月第一日不需要从历史表中恢复**/       
       IF MON_DAY <> 1 THEN
      	 INSERT INTO SMY.CST_CR_MTHLY_SMY SELECT * FROM HIS.CST_CR_MTHLY_SMY ;
       END IF;
     ELSE
  /** 清空hist 备份表 **/

	    SET EMP_SQL= 'Alter TABLE HIS.CST_CR_MTHLY_SMY ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE' ;
		
		  EXECUTE IMMEDIATE EMP_SQL;       
      
      COMMIT;
            	
		  /**backup 昨日数据 **/
		  
		  INSERT INTO HIS.CST_CR_MTHLY_SMY SELECT * FROM SMY.CST_CR_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = CUR_MONTH;
      
    END IF;

SET SMY_STEPNUM = 2 ;
SET SMY_STEPDESC = '声明用户临时表,存放昨日SMY数据';

	/*声明用户临时表*/
	
	DECLARE GLOBAL TEMPORARY TABLE TMP 
		LIKE SMY.CST_CR_MTHLY_SMY
	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K 
	PARTITIONING KEY(CST_ID) ;

 /*如果是年第一日不需要插入*/

 IF YR_FIRST_DAY <>  ACCOUNTING_DATE THEN 
	INSERT INTO SESSION.TMP 
	(
         CST_ID               --客户内码      
        ,OU_ID                --客户归属机构号
        ,CR_PD_LMT_CGY_TP_Id  --授信产品种类  
        ,CCY                  --币种          
        ,CDR_YR               --年份YYYY      
        ,CDR_MTH              --月份MM        
        ,ACG_DT               --日期YYYY-MM-DD
        ,CR_AMT               --授信金额      
        ,CR_LMT_AMT_USED      --已使用授信额度
        ,TMP_CRED_LMT         --临时授信金额  
          ) 
    SELECT
         CST_ID               --客户内码      
        ,OU_ID                --客户归属机构号
        ,CR_PD_LMT_CGY_TP_Id  --授信产品种类  
        ,CCY                  --币种          
        ,CDR_YR               --年份YYYY      
        ,CDR_MTH              --月份MM        
        ,ACG_DT               --日期YYYY-MM-DD
        ,CR_AMT               --授信金额      
        ,CR_LMT_AMT_USED      --已使用授信额度
        ,TMP_CRED_LMT         --临时授信金额 		       
     FROM SMY.CST_CR_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = LAST_MONTH;
 END IF ;   

 
		SET SMY_STEPNUM = 3 ;
		SET SMY_STEPDESC = '声明临时表SESSION.S, 用来存放DEP_AR_SMY 和SMY.CST_INF 汇总后要更新的数据'; 			 


/**/
  DECLARE GLOBAL TEMPORARY TABLE S  LIKE SMY.CST_CR_MTHLY_SMY 
  	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K 
     PARTITIONING KEY(CST_ID);
  
	INSERT INTO SESSION.S
          (
         CST_ID               --客户内码      
        ,OU_ID                --客户归属机构号
        ,CR_PD_LMT_CGY_TP_Id  --授信产品种类  
        ,CCY                  --币种          
        ,CDR_YR               --年份YYYY      
        ,CDR_MTH              --月份MM        
        ,ACG_DT               --日期YYYY-MM-DD
        ,CR_AMT               --授信金额      
        ,CR_LMT_AMT_USED      --已使用授信额度
        ,TMP_CRED_LMT         --临时授信金额  
            )
	WITH TMP_CST_LMT_DTL_INF  AS 
   (
     SELECT 
     	PRIM_CST_ID
     ,RPRG_OU_ID
     ,CRT_PD_LIM_CGY_TP_ID
     ,SUM(CASE WHEN TMP_LMT_F=0	 THEN CRT_LIM  ELSE 0 END) AS CR_AMT
     ,SUM(CASE WHEN TMP_LMT_F=1	 THEN CRT_LIM  ELSE 0 END) AS TMP_CRED_LMT   
     FROM SOR.CST_LMT_DTL_INF
     GROUP BY 
     	PRIM_CST_ID
     ,RPRG_OU_ID
     ,CRT_PD_LIM_CGY_TP_ID     		    
      )              	 	  
	SELECT  
        CST_LMT_DTL_INF.PRIM_CST_ID
       ,CST_LMT_DTL_INF.RPRG_OU_ID
       ,CST_LMT_DTL_INF.CRT_PD_LIM_CGY_TP_ID
       ,COALESCE(LMT_USED_INF.DNMN_CCY_ID,'CNY')	
       ,CUR_YEAR
       ,CUR_MONTH
       ,ACCOUNTING_DATE
       ,SUM(CST_LMT_DTL_INF.CR_AMT)
       ,SUM(LMT_USED_INF.USED_LMT_AMT + LMT_USED_INF.SUB_USED_LMT_AMT)
       ,SUM(CST_LMT_DTL_INF.TMP_CRED_LMT)

		FROM          TMP_CST_LMT_DTL_INF AS CST_LMT_DTL_INF
   LEFT OUTER JOIN SOR.LMT_USED_INF    AS LMT_USED_INF 
    ON  CST_LMT_DTL_INF.PRIM_CST_ID          = LMT_USED_INF.PRIM_CST_ID 
   AND CST_LMT_DTL_INF.CRT_PD_LIM_CGY_TP_ID = LMT_USED_INF.CRT_PD_LIM_CGY_TP_ID
   GROUP BY 
        CST_LMT_DTL_INF.PRIM_CST_ID
        ,CST_LMT_DTL_INF.RPRG_OU_ID
        ,CST_LMT_DTL_INF.CRT_PD_LIM_CGY_TP_ID
        ,LMT_USED_INF.DNMN_CCY_ID	   
      ;
 		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;	          

 /** Insert the log**/
    SET SMY_RCOUNT=0;
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	

		SET SMY_STEPNUM = 4 ;
		SET SMY_STEPDESC = '使用Merge语句,更新SMY 表'; 			 
	 
MERGE INTO SMY.CST_CR_MTHLY_SMY AS T
 		USING  SESSION.S AS S 
 	  ON
          S.CST_ID              =T.CST_ID             
      AND S.OU_ID            =T.OU_ID           
      AND S.CR_PD_LMT_CGY_TP_ID =T.CR_PD_LMT_CGY_TP_ID
      AND S.CCY                 =T.CCY                
      AND S.CDR_YR              =T.CDR_YR             
      AND S.CDR_MTH             =T.CDR_MTH            
WHEN MATCHED THEN UPDATE SET

        T.ACG_DT                 =S.ACG_DT          
       ,T.CR_AMT                 =S.CR_AMT          
       ,T.CR_LMT_AMT_USED        =S.CR_LMT_AMT_USED 
       ,T.TMP_CRED_LMT           =S.TMP_CRED_LMT                
WHEN NOT MATCHED THEN INSERT  	        
	 (
         CST_ID               --客户内码      
        ,OU_ID                --客户归属机构号
        ,CR_PD_LMT_CGY_TP_Id  --授信产品种类  
        ,CCY                  --币种          
        ,CDR_YR               --年份YYYY      
        ,CDR_MTH              --月份MM        
        ,ACG_DT               --日期YYYY-MM-DD
        ,CR_AMT               --授信金额      
        ,CR_LMT_AMT_USED      --已使用授信额度
        ,TMP_CRED_LMT         --临时授信金额  
        )
    VALUES 
    (
         S.CST_ID               --客户内码      
        ,S.OU_ID                --客户归属机构号
        ,S.CR_PD_LMT_CGY_TP_Id  --授信产品种类  
        ,S.CCY                  --币种          
        ,S.CDR_YR               --年份YYYY      
        ,S.CDR_MTH              --月份MM        
        ,S.ACG_DT               --日期YYYY-MM-DD
        ,S.CR_AMT               --授信金额      
        ,S.CR_LMT_AMT_USED      --已使用授信额度
        ,S.TMP_CRED_LMT         --临时授信金额  
    )	  	
	;
	
	 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	
	 
	 COMMIT;
END@