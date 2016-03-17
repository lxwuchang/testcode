CREATE PROCEDURE SMY.PROC_CST_DEP_MTHLY_SMY(IN ACCOUNTING_DATE date)
-------------------------------------------------------------------------------                                              
-- (C) Copyright ZJRCU and IBM <date>                                                                                        
--                                                                                                                           
-- File name:           SMY.PROC_CST_DEP_MTHLY_SMY.sql                                                                       
-- Procedure name: 			SMY.PROC_CST_DEP_MTHLY_SMY                                                                           
-- Source Table:				SMY.DEP_AR_SMY,SMY.CST_INF                                                                           
-- Target Table: 				SMY.CST_DEP_MTHLY_SMY                                                                                
-- Project     :        ZJ RCCB EDW                                                                                          
-- NOTES       :                                                                                                             
-- Purpose     :                                                                                                             
-- PROCESS METHOD      :  UPDATE EACH DAY ,INSERT IN THE PERIOD OF ONE MONTH                                                 
-- OPTIMIZE :                                                                                                                
--            CREATE INDEX SMY.IDX_CST_DEP_MTHLY_SMY_ACG_DT ON SMY.CST_DEP_MTHLY_SMY(ACG_DT);                                --
--            CREATE INDEX SMY.IDX_DEP_AR_SMY_CST_ID ON SMY.DEP_AR_SMY(CST_ID);                                              --
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
-- 2009-12-25   Xu Yan          Rewrite the merge statement to solve the problem which system temp tablespace is full
-- 2010-08-11   Peng Yi tao     Modify the method of calendar days Calculating 
-- 2011-05-31   Chen XiaoWen    1、合并插入临时表TMP的逻辑,后续不需要再update
--                              2、调整索引SESSION.IDX_S,旧版本建在临时表TMP上
-- 2011-08-05   Li Shen Yu      Add if-else clause for step 5 to deal the data of month first day and other days separately
-- 2012-03-16   Chen XiaoWen    1、SMY.CST_DEP_MTHLY_SMY修改原查询条件，使用ACG_DT分区键查询
--                              2、增加临时表TMP_CUR，缓存中间结果后再进行group by
--                              3、最后一步INSERT增加ACG_DT筛选条件
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
		DECLARE CUR_YEAR SMALLINT;                                                                                               --
		DECLARE CUR_MONTH SMALLINT;                                                                                              --
		DECLARE CUR_DAY INTEGER;                                                                                                 --
		DECLARE YR_FIRST_DAY DATE;                                                                                               --
		DECLARE QTR_FIRST_DAY DATE;                                                                                              --
		DECLARE YR_DAY SMALLINT;                                                                                                 --
		DECLARE QTR_DAY SMALLINT;                                                                                                --
		DECLARE MAX_ACG_DT DATE;                                                                                                 --
		DECLARE LAST_SMY_DATE DATE;                                                                                              --
		DECLARE MTH_FIRST_DAY DATE;                                                                                              --
		DECLARE V_T SMALLINT;                                                                                                    --
    DECLARE C_YR_DAY SMALLINT;                                                                                               --
		DECLARE C_QTR_DAY SMALLINT;                                                                                              --
		DECLARE QTR_LAST_DAY DATE;                                                                                               --
		DECLARE C_MON_DAY SMALLINT;                                                                                              --
		DECLARE CUR_QTR SMALLINT;                                                                                                --
		DECLARE MON_DAY SMALLINT;                                                                                                --
		DECLARE LAST_MONTH SMALLINT;                                                                                             --
		DECLARE EMP_SQL VARCHAR(200);                                                                                            --
		DECLARE MTH_LAST_DAY DATE;
                                                                                                                             
/*                                                                                                                           
	1.定义针对SQL异常情况的句柄(EXIT方式).                                                                                     
  2.将出现SQL异常时在存储过程中的位置(SMY_STEPNUM),位置描述(SMY_STEPDESC),SQLCODE(SMY_SQLCODE)记入表SMY_LOG中作调试用.       
  3.调用RESIGNAL重新引发异常,跳出存储过程执行体,对引发SQL异常之前存储过程体中所完成的操作进行回滚.                           
*/                                                                                                                           
                                                                                                                             
		DECLARE CONTINUE HANDLER FOR NOT FOUND                                                                                   
		  SET V_T=0 ;                                                                                                            --
		                                                                                                                         
    DECLARE EXIT HANDLER FOR SQLEXCEPTION                                                                                    
    BEGIN                                                                                                                    
    	SET SMY_SQLCODE = SQLCODE;                                                                                             --
      ROLLBACK;                                                                                                              --
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP); --
      COMMIT;                                                                                                                --
      RESIGNAL;                                                                                                              --
    END;                                                                                                                     --
                                                                                                                             
    DECLARE CONTINUE HANDLER FOR SQLWARNING                                                                                  
    BEGIN                                                                                                                    
      SET SMY_SQLCODE = SQLCODE;                                                                                             --
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP); --
      COMMIT;                                                                                                                --
    END;                                                                                                                     --
                                                                                                                             
   /*变量赋值*/                                                                                                              
    SET SMY_PROCNM  ='PROC_CST_DEP_MTHLY_SMY';                                                                               --
    SET SMY_DATE    =ACCOUNTING_DATE;                                                                                        --
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --取当前年份                                                                    
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --取当前月份                                                                    
    SET CUR_DAY     =DAY(ACCOUNTING_DATE);     --取月第几日                                                                  
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- 取年初日 
--------------------------------------------start on 20100811-------------------------------------------------------------                                                     
    --SET C_YR_DAY      =DAYOFYEAR(DATE(TRIM(CHAR(YEAR(ACCOUNTING_DATE)))||'-12-31')); --取当年第几日 
    SET C_YR_DAY      =DAYS(ACCOUNTING_DATE)-DAYS(YR_FIRST_DAY)+1;--取当年第几日         
--------------------------------------------end on 20100811-------------------------------------------------------------                          
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --当前季度                                                                  
    SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,iso),1,7)||'-01'); --取当月初日                                       
    SET MON_DAY     =DAY(ACCOUNTING_DATE);     --取月第几日                                                                  
                                                                                                                             
    VALUES(ACCOUNTING_DATE - 1 DAY) INTO LAST_SMY_DATE;                                                                      --
    VALUES(MTH_FIRST_DAY + 1 MONTH - 1 DAY) INTO MTH_LAST_DAY ;
    SET LAST_MONTH = MONTH(LAST_SMY_DATE);                                                                                   --
    --计算月日历天数   
--------------------------------------------start on 20100811-------------------------------------------------------------                                                                                                          
    --SET C_MON_DAY = DAYS(MTH_FIRST_DAY + 1 MONTH ) - DAYS(MTH_FIRST_DAY);                                                    --
    SET C_MON_DAY = DAY(ACCOUNTING_DATE);                                                                                      --
--------------------------------------------end on 20100811-------------------------------------------------------------                                                                                                                              
    --计算季度日历天数                                                                                                       
    IF CUR_QTR = 1                                                                                                           
       THEN                                                                                                                  
       	SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');                                                              --
       	SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-04-01') - 1 DAY ;                                                     --
    ELSEIF CUR_QTR = 2                                                                                                       
       THEN                                                                                                                  
       	SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-04-01');                                                              --
       	SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-07-01') - 1 DAY ;       	                                             --
    ELSEIF CUR_QTR = 3                                                                                                       
       THEN                                                                                                                  
       	SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-07-01');                                                              --
       	SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-10-01') - 1 DAY ;       	                                             --
    ELSE                                                                                                                     
       SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-10-01');                                                               --
       SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-12-31');                                                               --
    END IF;                                                                                                                  --
                                                                                                                             
  /*取当季日历天数*/       
--------------------------------------------start on 20100811-------------------------------------------------------------                                                                                                      
  	--SET C_QTR_DAY = DAYS(QTR_LAST_DAY) - DAYS(QTR_FIRST_DAY) + 1 ;                                                        --
  	SET C_QTR_DAY = DAYS(ACCOUNTING_DATE)-DAYS(QTR_FIRST_DAY)+1;                                                            --
--------------------------------------------end on 20100811-------------------------------------------------------------                                                     --
		                                                                                                                         
		SELECT COALESCE(MAX(ACG_DT),'1900-01-01') INTO MAX_ACG_DT FROM SMY.CST_DEP_MTHLY_SMY;                                    --
                                                                                                                             
/*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE,并插入新的起始标志*/                                 
		DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;                                        --
			COMMIT;                                                                                                                --
		                                                                                                                         
		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;                                                                                  --
		                                                                                                                         
		SET SMY_STEPDESC = 	'存储过程开始运行' ;                                                                                 --
		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)                  
				VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);                 --
                                                                                                                             
/*数据恢复与备份*/                                                                                                           
    IF MAX_ACG_DT = ACCOUNTING_DATE THEN                                                                                     
       --DELETE FROM SMY.CST_DEP_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = CUR_MONTH;                                    --
       DELETE FROM SMY.CST_DEP_MTHLY_SMY WHERE ACG_DT>=MTH_FIRST_DAY and ACG_DT<=MTH_LAST_DAY;
    /**每月第一日不需要从历史表中恢复**/                                                                                     
       IF MON_DAY <> 1 THEN                                                                                                  
      	 INSERT INTO SMY.CST_DEP_MTHLY_SMY SELECT * FROM HIS.CST_DEP_MTHLY_SMY ;                                             --
       END IF;                                                                                                               --
     ELSE                                                                                                                    
  /** 清空hist 备份表 **/                                                                                                    
                                                                                                                             
	    SET EMP_SQL= 'Alter TABLE HIS.CST_DEP_MTHLY_SMY ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE' ;                      --
		                                                                                                                         
		  EXECUTE IMMEDIATE EMP_SQL;                                                                                             --
                                                                                                                             
      COMMIT;                                                                                                                --
		  /**backup 昨日数据 **/                                                                                                 
		                                                                                                                         
		   --INSERT INTO HIS.CST_DEP_MTHLY_SMY SELECT * FROM SMY.CST_DEP_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = CUR_MONTH;--
		   INSERT INTO HIS.CST_DEP_MTHLY_SMY SELECT * FROM SMY.CST_DEP_MTHLY_SMY WHERE ACG_DT>=MTH_FIRST_DAY and ACG_DT<=MTH_LAST_DAY;
    END IF;                                                                                                                  --
                                                                                                                             
SET SMY_STEPNUM = 2 ;                                                                                                        --
SET SMY_STEPDESC = '声明用户临时表,存放昨日SMY数据';                                                                         --
                                                                                                                             
	/*声明用户临时表*/                                                                                                         
	                                                                                                                           
	DECLARE GLOBAL TEMPORARY TABLE TMP                                                                                         
		LIKE SMY.CST_DEP_MTHLY_SMY                                                                                               
	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(CST_ID);                                --
	                                                                                                                           
	--优化by James shang                                                                                                       
	CREATE INDEX SESSION.IDX_TMP ON SESSION.TMP(CST_ID,AC_OU_ID,DEP_TP_ID,PD_GRP_CD,PD_SUB_CD,CCY);                            --
                                                                                                                             
 /*如果是年第一日不需要插入*/
	 IF  ACCOUNTING_DATE IN ( YR_FIRST_DAY )  --年 季 月 归零
	    THEN
	      COMMIT;
	 ELSEIF ACCOUNTING_DATE IN (QTR_FIRST_DAY ) --季 月 归零                                                                   
	 	  THEN                                                                                                                   
	 	    INSERT INTO SESSION.TMP
	 	    (                                                                                                                          
            CST_ID                --客户内码                                                                                 
           ,AC_OU_ID              --账户归属机构号                                                                           
           ,DEP_TP_ID             --存款类型                                                                                 
           ,PD_GRP_CD             --产品组代码                                                                               
           ,PD_SUB_CD             --产品子代码                                                                               
           ,CCY                   --币种                                                                                     
           ,CDR_YR                --年份YYYY                                                                                 
           ,CDR_MTH               --月份MM                                                                                   
           ,ACG_DT                --日期YYYY-MM-DD                                                                           
           ,NOCLD_IN_MTH          --当月日历天数                                                                             
           ,NOCLD_IN_QTR          --当季日历天数                                                                             
           ,NOCLD_IN_YEAR         --当年日历天数                                                                             
           ,NOD_IN_MTH            --当月有效天数                                                                             
           ,NOD_IN_QTR            --当季有效天数                                                                             
           ,NOD_IN_YEAR           --当年有效天数                                                                             
           ,OU_ID                 --机构号                                                                                   
           ,CST_TP_ID             --客户类型                                                                                 
           ,NBR_AC                --账户个数                                                                                 
           ,LST_DAY_BAL           --昨日余额                                                                                 
           ,DEP_BAL               --存款余额                                                                                 
           ,MTD_ACML_DEP_BAL_AMT  --月累计余额                                                                               
           ,QTD_ACML_DEP_BAL_AMT  --季累计余额                                                                               
           ,YTD_ACML_DEP_BAL_AMT  --年累计余额                                                                               
        )
        SELECT                                                                                                                   
            CST_ID                --客户内码                                                                                 
           ,AC_OU_ID              --账户归属机构号                                                                           
           ,DEP_TP_ID             --存款类型                                                                                 
           ,PD_GRP_CD             --产品组代码                                                                               
           ,PD_SUB_CD             --产品子代码                                                                               
           ,CCY                   --币种                                                                                     
           ,CDR_YR                --年份YYYY                                                                                 
           ,CDR_MTH               --月份MM                                                                                   
           ,ACG_DT                --日期YYYY-MM-DD                                                                           
           ,0                     --当月日历天数                                                                             
           ,0                     --当季日历天数                                                                             
           ,NOCLD_IN_YEAR         --当年日历天数                                                                             
           ,0                     --当月有效天数                                                                             
           ,0                     --当季有效天数                                                                             
           ,NOD_IN_YEAR           --当年有效天数                                                                             
           ,OU_ID                 --机构号                                                                                   
           ,CST_TP_ID             --客户类型                                                                                 
           ,NBR_AC                --账户个数                                                                                 
           ,LST_DAY_BAL           --昨日余额                                                                                 
           ,DEP_BAL               --存款余额                                                                                 
           ,0                     --月累计余额                                                                               
           ,0                     --季累计余额                                                                               
           ,YTD_ACML_DEP_BAL_AMT  --年累计余额
        FROM SMY.CST_DEP_MTHLY_SMY WHERE ACG_DT= LAST_SMY_DATE;
	 ELSEIF ACCOUNTING_DATE IN ( MTH_FIRST_DAY ) --月归零                                                                      
	 	  THEN
	 	    INSERT INTO SESSION.TMP
	 	    (                                                                                                                          
            CST_ID                --客户内码                                                                                 
           ,AC_OU_ID              --账户归属机构号                                                                           
           ,DEP_TP_ID             --存款类型                                                                                 
           ,PD_GRP_CD             --产品组代码                                                                               
           ,PD_SUB_CD             --产品子代码                                                                               
           ,CCY                   --币种                                                                                     
           ,CDR_YR                --年份YYYY                                                                                 
           ,CDR_MTH               --月份MM                                                                                   
           ,ACG_DT                --日期YYYY-MM-DD                                                                           
           ,NOCLD_IN_MTH          --当月日历天数                                                                             
           ,NOCLD_IN_QTR          --当季日历天数                                                                             
           ,NOCLD_IN_YEAR         --当年日历天数                                                                             
           ,NOD_IN_MTH            --当月有效天数                                                                             
           ,NOD_IN_QTR            --当季有效天数                                                                             
           ,NOD_IN_YEAR           --当年有效天数                                                                             
           ,OU_ID                 --机构号                                                                                   
           ,CST_TP_ID             --客户类型                                                                                 
           ,NBR_AC                --账户个数                                                                                 
           ,LST_DAY_BAL           --昨日余额                                                                                 
           ,DEP_BAL               --存款余额                                                                                 
           ,MTD_ACML_DEP_BAL_AMT  --月累计余额                                                                               
           ,QTD_ACML_DEP_BAL_AMT  --季累计余额                                                                               
           ,YTD_ACML_DEP_BAL_AMT  --年累计余额                                                                               
        )
        SELECT                                                                                                                   
            CST_ID                --客户内码                                                                                 
           ,AC_OU_ID              --账户归属机构号                                                                           
           ,DEP_TP_ID             --存款类型                                                                                 
           ,PD_GRP_CD             --产品组代码                                                                               
           ,PD_SUB_CD             --产品子代码                                                                               
           ,CCY                   --币种                                                                                     
           ,CDR_YR                --年份YYYY                                                                                 
           ,CDR_MTH               --月份MM                                                                                   
           ,ACG_DT                --日期YYYY-MM-DD                                                                           
           ,0                     --当月日历天数                                                                             
           ,NOCLD_IN_QTR          --当季日历天数                                                                             
           ,NOCLD_IN_YEAR         --当年日历天数                                                                             
           ,0                     --当月有效天数                                                                             
           ,NOD_IN_QTR            --当季有效天数                                                                             
           ,NOD_IN_YEAR           --当年有效天数                                                                             
           ,OU_ID                 --机构号                                                                                   
           ,CST_TP_ID             --客户类型                                                                                 
           ,NBR_AC                --账户个数                                                                                 
           ,LST_DAY_BAL           --昨日余额                                                                                 
           ,DEP_BAL               --存款余额                                                                                 
           ,0                     --月累计余额                                                                               
           ,QTD_ACML_DEP_BAL_AMT  --季累计余额                                                                               
           ,YTD_ACML_DEP_BAL_AMT  --年累计余额			                                                                         
        FROM SMY.CST_DEP_MTHLY_SMY WHERE ACG_DT= LAST_SMY_DATE;
   ELSE
        INSERT INTO SESSION.TMP
        (                                                                                                                          
            CST_ID                --客户内码                                                                                 
           ,AC_OU_ID              --账户归属机构号                                                                           
           ,DEP_TP_ID             --存款类型                                                                                 
           ,PD_GRP_CD             --产品组代码                                                                               
           ,PD_SUB_CD             --产品子代码                                                                               
           ,CCY                   --币种                                                                                     
           ,CDR_YR                --年份YYYY                                                                                 
           ,CDR_MTH               --月份MM                                                                                   
           ,ACG_DT                --日期YYYY-MM-DD                                                                           
           ,NOCLD_IN_MTH          --当月日历天数                                                                             
           ,NOCLD_IN_QTR          --当季日历天数                                                                             
           ,NOCLD_IN_YEAR         --当年日历天数                                                                             
           ,NOD_IN_MTH            --当月有效天数                                                                             
           ,NOD_IN_QTR            --当季有效天数                                                                             
           ,NOD_IN_YEAR           --当年有效天数                                                                             
           ,OU_ID                 --机构号                                                                                   
           ,CST_TP_ID             --客户类型                                                                                 
           ,NBR_AC                --账户个数                                                                                 
           ,LST_DAY_BAL           --昨日余额                                                                                 
           ,DEP_BAL               --存款余额                                                                                 
           ,MTD_ACML_DEP_BAL_AMT  --月累计余额                                                                               
           ,QTD_ACML_DEP_BAL_AMT  --季累计余额                                                                               
           ,YTD_ACML_DEP_BAL_AMT  --年累计余额                                                                               
        )
        SELECT                                                                                                                   
            CST_ID                --客户内码                                                                                 
           ,AC_OU_ID              --账户归属机构号                                                                           
           ,DEP_TP_ID             --存款类型                                                                                 
           ,PD_GRP_CD             --产品组代码                                                                               
           ,PD_SUB_CD             --产品子代码                                                                               
           ,CCY                   --币种                                                                                     
           ,CDR_YR                --年份YYYY                                                                                 
           ,CDR_MTH               --月份MM                                                                                   
           ,ACG_DT                --日期YYYY-MM-DD                                                                           
           ,NOCLD_IN_MTH          --当月日历天数                                                                             
           ,NOCLD_IN_QTR          --当季日历天数                                                                             
           ,NOCLD_IN_YEAR         --当年日历天数                                                                             
           ,NOD_IN_MTH            --当月有效天数                                                                             
           ,NOD_IN_QTR            --当季有效天数                                                                             
           ,NOD_IN_YEAR           --当年有效天数                                                                             
           ,OU_ID                 --机构号                                                                                   
           ,CST_TP_ID             --客户类型                                                                                 
           ,NBR_AC                --账户个数                                                                                 
           ,LST_DAY_BAL           --昨日余额                                                                                 
           ,DEP_BAL               --存款余额                                                                                 
           ,MTD_ACML_DEP_BAL_AMT  --月累计余额                                                                               
           ,QTD_ACML_DEP_BAL_AMT  --季累计余额                                                                               
           ,YTD_ACML_DEP_BAL_AMT  --年累计余额			                                                                         
        FROM SMY.CST_DEP_MTHLY_SMY WHERE ACG_DT= LAST_SMY_DATE;
	 END IF;

 /** 收集操作信息 */		                                                                                                     
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;                                                                                    --
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)                    
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	                     --
                                                                                                                             
		SET SMY_STEPNUM = 3 ;                                                                                                    --
		SET SMY_STEPDESC = '声明临时表SESSION.CUR, 存放当日汇总后的数据'; 		                                                   --
                                                                                                                             
 DECLARE GLOBAL TEMPORARY TABLE CUR AS (                                                                                     
		SELECT                                                                                                                   
          COALESCE(DEP_AR_SMY.CST_ID,'')         AS CST_ID                --客户内码                                                      
         ,DEP_AR_SMY.RPRG_OU_IP_ID  AS AC_OU_ID              --账户归属机构号                                                
         ,DEP_AR_SMY.DEP_TP_ID      AS DEP_TP_ID             --存款类型                                                      
         ,COALESCE(DEP_AR_SMY.PD_GRP_CODE,'')    AS PD_GRP_CD             --产品组代码                                                    
         ,COALESCE(DEP_AR_SMY.PD_SUB_CODE,'')    AS PD_SUB_CD             --产品子代码                                                    
         ,DEP_AR_SMY.DNMN_CCY_ID    AS CCY                   --币种                                                          
         ,1                         AS NOD_IN_MTH            --当月有效天数                                                  
         ,1                         AS NOD_IN_QTR            --当季有效天数                                                  
         ,1                         AS NOD_IN_YEAR           --当年有效天数		                                               
         ,COALESCE(CST_INF.RPRG_OU_IP_ID,'')     AS OU_ID                 --机构号                                           
         ,COALESCE(CST_INF.ENT_IDV_IND ,-1)      AS CST_TP_ID             --客户类型                                         
         ,COUNT(DISTINCT DEP_AR_SMY.DEP_AR_ID) AS NBR_AC                --账户个数                                           
         ,SUM(DEP_AR_SMY.BAL_AMT)   AS DEP_BAL               --存款余额		                                                   
		FROM            SMY.DEP_AR_SMY  AS DEP_AR_SMY                                                                            
		LEFT OUTER JOIN SMY.CST_INF     AS CST_INF	ON DEP_AR_SMY.CST_ID=CST_INF.CST_ID                                          
		GROUP BY                                                                                                                 
          DEP_AR_SMY.CST_ID                                                                                                  
         ,DEP_AR_SMY.RPRG_OU_IP_ID                                                                                           
         ,DEP_AR_SMY.DEP_TP_ID                                                                                               
         ,DEP_AR_SMY.PD_GRP_CODE                                                                                             
         ,DEP_AR_SMY.PD_SUB_CODE                                                                                             
         ,DEP_AR_SMY.DNMN_CCY_ID                                                                                             
			   ,COALESCE(CST_INF.RPRG_OU_IP_ID,'')                                                                                 
				 ,COALESCE(CST_INF.ENT_IDV_IND ,-1)                                                                                  
                                                                                                                             
  ) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K                                         
     PARTITIONING KEY(CST_ID) ;                                                                                              --

	--优化by James shang                                                                                                       
	CREATE INDEX SESSION.IDX_CUR ON SESSION.CUR(CST_ID,AC_OU_ID,DEP_TP_ID,PD_GRP_CD,PD_SUB_CD,CCY);       			               --
                                                                                                                             
  DECLARE GLOBAL TEMPORARY TABLE TMP_CUR AS (
    SELECT
          COALESCE(DEP_AR_SMY.CST_ID,'')         AS CST_ID
         ,DEP_AR_SMY.RPRG_OU_IP_ID  AS AC_OU_ID
         ,DEP_AR_SMY.DEP_TP_ID      AS DEP_TP_ID
         ,COALESCE(DEP_AR_SMY.PD_GRP_CODE,'')    AS PD_GRP_CD
         ,COALESCE(DEP_AR_SMY.PD_SUB_CODE,'')    AS PD_SUB_CD
         ,DEP_AR_SMY.DNMN_CCY_ID    AS CCY
         ,COALESCE(CST_INF.RPRG_OU_IP_ID,'')    AS OU_ID
         ,COALESCE(CST_INF.ENT_IDV_IND ,-1)       AS CST_TP_ID
         ,DEP_AR_SMY.DEP_AR_ID AS NBR_AC
         ,DEP_AR_SMY.BAL_AMT   AS DEP_BAL
		FROM            SMY.DEP_AR_SMY  AS DEP_AR_SMY
		LEFT OUTER JOIN SMY.CST_INF     AS CST_INF	ON DEP_AR_SMY.CST_ID=CST_INF.CST_ID
) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(CST_ID);

  CREATE INDEX SESSION.IDX_TMP_CUR ON SESSION.TMP_CUR(CST_ID,AC_OU_ID,DEP_TP_ID,PD_GRP_CD,PD_SUB_CD,CCY,OU_ID,CST_TP_ID);

  INSERT INTO SESSION.TMP_CUR
    SELECT
          COALESCE(DEP_AR_SMY.CST_ID,'')         AS CST_ID
         ,DEP_AR_SMY.RPRG_OU_IP_ID  AS AC_OU_ID
         ,DEP_AR_SMY.DEP_TP_ID      AS DEP_TP_ID
         ,COALESCE(DEP_AR_SMY.PD_GRP_CODE,'')    AS PD_GRP_CD
         ,COALESCE(DEP_AR_SMY.PD_SUB_CODE,'')    AS PD_SUB_CD
         ,DEP_AR_SMY.DNMN_CCY_ID    AS CCY
         ,COALESCE(CST_INF.RPRG_OU_IP_ID,'')    AS OU_ID
         ,COALESCE(CST_INF.ENT_IDV_IND ,-1)       AS CST_TP_ID
         ,DEP_AR_SMY.DEP_AR_ID AS NBR_AC
         ,DEP_AR_SMY.BAL_AMT   AS DEP_BAL
		FROM            SMY.DEP_AR_SMY  AS DEP_AR_SMY
		LEFT OUTER JOIN SMY.CST_INF     AS CST_INF	ON DEP_AR_SMY.CST_ID=CST_INF.CST_ID
;

  INSERT INTO SESSION.CUR
		SELECT                                                                                                                   
          CST_ID
         ,AC_OU_ID
         ,DEP_TP_ID
         ,PD_GRP_CD
         ,PD_SUB_CD
         ,CCY
         ,1
         ,1
         ,1
         ,OU_ID
         ,CST_TP_ID
         ,COUNT(DISTINCT NBR_AC) AS NBR_AC
         ,SUM(DEP_BAL)   AS DEP_BAL
		FROM SESSION.TMP_CUR
		GROUP BY                                                                                                                 
          CST_ID
         ,AC_OU_ID
         ,DEP_TP_ID
         ,PD_GRP_CD
         ,PD_SUB_CD
         ,CCY
			   ,OU_ID
				 ,CST_TP_ID
;

 /** 收集操作信息 */		                                                                                                     
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;                                                                                    --
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)                    
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);                      --
	                                                                                                                           
		SET SMY_STEPNUM = 4 ;                                                                                                    --
		SET SMY_STEPDESC = '声明临时表SESSION.S, 用来存放DEP_AR_SMY 和SMY.CST_INF 汇总后要更新的数据'; 			                     --
                                                                                                                             
                                                                                                                             
/**/                                                                                                                         
  DECLARE GLOBAL TEMPORARY TABLE S  LIKE SMY.CST_DEP_MTHLY_SMY                                                               
  	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K                                                         
     PARTITIONING KEY(CST_ID);                                                                                               --
                                                                                                                             
	--优化by James shang                                                                                                       
	CREATE INDEX SESSION.IDX_S ON SESSION.S(CST_ID,AC_OU_ID,DEP_TP_ID,PD_GRP_CD,PD_SUB_CD,CCY,CDR_YR,CDR_MTH);               --
                                                                                                                             
	INSERT INTO SESSION.S                                                                                                      
          (                                                                                                                  
            CST_ID                --客户内码                                                                                 
           ,AC_OU_ID              --账户归属机构号                                                                           
           ,DEP_TP_ID             --存款类型                                                                                 
           ,PD_GRP_CD             --产品组代码                                                                               
           ,PD_SUB_CD             --产品子代码                                                                               
           ,CCY                   --币种                                                                                     
           ,CDR_YR                --年份YYYY                                                                                 
           ,CDR_MTH               --月份MM                                                                                   
           ,ACG_DT                --日期YYYY-MM-DD                                                                           
           ,NOCLD_IN_MTH          --当月日历天数                                                                             
           ,NOCLD_IN_QTR          --当季日历天数                                                                             
           ,NOCLD_IN_YEAR         --当年日历天数                                                                             
           ,NOD_IN_MTH            --当月有效天数                                                                             
           ,NOD_IN_QTR            --当季有效天数                                                                             
           ,NOD_IN_YEAR           --当年有效天数                                                                             
           ,OU_ID                 --机构号                                                                                   
           ,CST_TP_ID             --客户类型                                                                                 
           ,NBR_AC                --账户个数                                                                                 
           ,LST_DAY_BAL           --昨日余额                                                                                 
           ,DEP_BAL               --存款余额                                                                                 
           ,MTD_ACML_DEP_BAL_AMT  --月累计余额                                                                               
           ,QTD_ACML_DEP_BAL_AMT  --季累计余额                                                                               
           ,YTD_ACML_DEP_BAL_AMT  --年累计余额                                                                               
            )
	SELECT                                                                                                                     
            COALESCE(CUR.CST_ID,'')                --客户内码                                                                             
           ,CUR.AC_OU_ID              --账户归属机构号                                                                       
           ,CUR.DEP_TP_ID             --存款类型                                                                             
           ,COALESCE(CUR.PD_GRP_CD,'')             --产品组代码                                                                           
           ,COALESCE(CUR.PD_SUB_CD,'')             --产品子代码                                                                           
           ,CUR.CCY                   --币种                                                                                 
           ,CUR_YEAR                --年份YYYY                                                                               
           ,CUR_MONTH               --月份MM                                                                                 
           ,ACCOUNTING_DATE                --日期YYYY-MM-DD                                                                  
           ,C_MON_DAY        --当月日历天数                                                                                  
           ,C_QTR_DAY        --当季日历天数                                                                                  
           ,C_YR_DAY        --当年日历天数                                                                                   
           ,COALESCE(PRE.NOD_IN_MTH  ,0) + CUR.NOD_IN_MTH            --当月有效天数                                          
           ,COALESCE(PRE.NOD_IN_QTR  ,0) + CUR.NOD_IN_QTR            --当季有效天数                                          
           ,COALESCE(PRE.NOD_IN_YEAR ,0) + CUR.NOD_IN_YEAR           --当年有效天数                                          
           ,CUR.OU_ID                 --机构号                                                                               
           ,CUR.CST_TP_ID             --客户类型                                                                             
           ,CUR.NBR_AC                --账户个数                                                                             
           ,COALESCE(PRE.DEP_BAL,0)           --昨日余额                                                                     
           ,CUR.DEP_BAL               --存款余额                                                                             
           ,COALESCE(MTD_ACML_DEP_BAL_AMT,0) + CUR.DEP_BAL  --月累计余额                                                     
           ,COALESCE(QTD_ACML_DEP_BAL_AMT,0) + CUR.DEP_BAL  --季累计余额                                                     
           ,COALESCE(YTD_ACML_DEP_BAL_AMT,0) + CUR.DEP_BAL  --年累计余额                                                     
                                                                                                                             
	FROM  SESSION.CUR  AS CUR LEFT OUTER JOIN  SESSION.TMP AS PRE ON                                                           
         CUR.CST_ID     =PRE.CST_ID                                                                                          
     AND CUR.AC_OU_ID   =PRE.AC_OU_ID                                                                                        
     AND CUR.DEP_TP_ID  =PRE.DEP_TP_ID                                                                                       
     AND CUR.PD_GRP_CD  =PRE.PD_GRP_CD                                                                                       
     AND CUR.PD_SUB_CD  =PRE.PD_SUB_CD                                                                                       
     AND CUR.CCY        =PRE.CCY                                                                                             
      ;                                                                                                                      --
 		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;	                                                                                 --
                                                                                                                             
 /** Insert the log**/                                                                                                       
                                                                                                                             
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)                  
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	                     --
                                                                                                                             
		SET SMY_STEPNUM = 5 ;                                                                                                    --
		SET SMY_STEPDESC = '使用Merge语句,更新SMY 表'; 		--
	                                                                     
/*                                                                                                                            
MERGE INTO SMY.CST_DEP_MTHLY_SMY AS T                                                                                        
 		USING  SESSION.S AS S                                                                                                    
 	  ON                                                                                                                       
         S.CST_ID     =T.CST_ID                                                                                              
     AND S.AC_OU_ID   =T.AC_OU_ID                                                                                            
     AND S.DEP_TP_ID  =T.DEP_TP_ID                                                                                           
     AND S.PD_GRP_CD  =T.PD_GRP_CD                                                                                           
     AND S.PD_SUB_CD  =T.PD_SUB_CD                                                                                           
     AND S.CCY        =T.CCY                                                                                                 
     AND S.CDR_YR     =T.CDR_YR                                                                                              
		 AND S.CDR_MTH    =T.CDR_MTH                                                                                             
WHEN MATCHED THEN UPDATE SET                                                                                                 
                                                                                                                             
        ACG_DT               =S.ACG_DT                --日期YYYY-MM-DD                                                       
       ,NOCLD_IN_MTH         =S.NOCLD_IN_MTH          --当月日历天数                                                         
       ,NOCLD_IN_QTR         =S.NOCLD_IN_QTR          --当季日历天数                                                         
       ,NOCLD_IN_YEAR        =S.NOCLD_IN_YEAR         --当年日历天数                                                         
       ,NOD_IN_MTH           =S.NOD_IN_MTH            --当月有效天数                                                         
       ,NOD_IN_QTR           =S.NOD_IN_QTR            --当季有效天数                                                         
       ,NOD_IN_YEAR          =S.NOD_IN_YEAR           --当年有效天数                                                         
       ,OU_ID                =S.OU_ID                 --机构号                                                               
       ,CST_TP_ID            =S.CST_TP_ID             --客户类型                                                             
       ,NBR_AC               =S.NBR_AC                --账户个数                                                             
       ,LST_DAY_BAL          =S.LST_DAY_BAL           --昨日余额                                                             
       ,DEP_BAL              =S.DEP_BAL               --存款余额                                                             
       ,MTD_ACML_DEP_BAL_AMT =S.MTD_ACML_DEP_BAL_AMT  --月累计余额                                                           
       ,QTD_ACML_DEP_BAL_AMT =S.QTD_ACML_DEP_BAL_AMT  --季累计余额                                                           
       ,YTD_ACML_DEP_BAL_AMT =S.YTD_ACML_DEP_BAL_AMT  --年累计余额                                                           
                                                                                                                             
WHEN NOT MATCHED THEN INSERT  	                                                                                             
	 (                                                                                                                         
            CST_ID                --客户内码                                                                                 
           ,AC_OU_ID              --账户归属机构号                                                                           
           ,DEP_TP_ID             --存款类型                                                                                 
           ,PD_GRP_CD             --产品组代码                                                                               
           ,PD_SUB_CD             --产品子代码                                                                               
           ,CCY                   --币种                                                                                     
           ,CDR_YR                --年份YYYY                                                                                 
           ,CDR_MTH               --月份MM                                                                                   
           ,ACG_DT                --日期YYYY-MM-DD                                                                           
           ,NOCLD_IN_MTH          --当月日历天数                                                                             
           ,NOCLD_IN_QTR          --当季日历天数                                                                             
           ,NOCLD_IN_YEAR         --当年日历天数                                                                             
           ,NOD_IN_MTH            --当月有效天数                                                                             
           ,NOD_IN_QTR            --当季有效天数                                                                             
           ,NOD_IN_YEAR           --当年有效天数                                                                             
           ,OU_ID                 --机构号                                                                                   
           ,CST_TP_ID             --客户类型                                                                                 
           ,NBR_AC                --账户个数                                                                                 
           ,LST_DAY_BAL           --昨日余额                                                                                 
           ,DEP_BAL               --存款余额                                                                                 
           ,MTD_ACML_DEP_BAL_AMT  --月累计余额                                                                               
           ,QTD_ACML_DEP_BAL_AMT  --季累计余额                                                                               
           ,YTD_ACML_DEP_BAL_AMT  --年累计余额                                                                               
        )                                                                                                                    
    VALUES                                                                                                                   
    (                                                                                                                        
            COALESCE(S.CST_ID,'')                --客户内码                                                                               
           ,S.AC_OU_ID              --账户归属机构号                                                                         
           ,S.DEP_TP_ID             --存款类型                                                                               
           ,COALESCE(S.PD_GRP_CD,'')             --产品组代码                                                                             
           ,COALESCE(S.PD_SUB_CD,'')             --产品子代码                                                                             
           ,S.CCY                   --币种                                                                                   
           ,S.CDR_YR                --年份YYYY                                                                               
           ,S.CDR_MTH               --月份MM                                                                                 
           ,S.ACG_DT                --日期YYYY-MM-DD                                                                         
           ,S.NOCLD_IN_MTH          --当月日历天数                                                                           
           ,S.NOCLD_IN_QTR          --当季日历天数                                                                           
           ,S.NOCLD_IN_YEAR         --当年日历天数                                                                           
           ,S.NOD_IN_MTH            --当月有效天数                                                                           
           ,S.NOD_IN_QTR            --当季有效天数                                                                           
           ,S.NOD_IN_YEAR           --当年有效天数                                                                           
           ,S.OU_ID                 --机构号                                                                                 
           ,S.CST_TP_ID             --客户类型                                                                               
           ,S.NBR_AC                --账户个数                                                                               
           ,S.LST_DAY_BAL           --昨日余额                                                                               
           ,S.DEP_BAL               --存款余额                                                                               
           ,S.MTD_ACML_DEP_BAL_AMT  --月累计余额                                                                             
           ,S.QTD_ACML_DEP_BAL_AMT  --季累计余额                                                                             
           ,S.YTD_ACML_DEP_BAL_AMT  --年累计余额                                                                             
    )	  	                                                                                                                   
	;  --
*/       
---------------------------Start of Modification on 20091225-------------------------------------------------------------------
IF ACCOUNTING_DATE = MTH_FIRST_DAY AND MAX_ACG_DT <= ACCOUNTING_DATE THEN
  INSERT INTO SMY.CST_DEP_MTHLY_SMY 
  (                                                                                                                         
            CST_ID                --客户内码                                                                                 
           ,AC_OU_ID              --账户归属机构号                                                                           
           ,DEP_TP_ID             --存款类型                                                                                 
           ,PD_GRP_CD             --产品组代码                                                                               
           ,PD_SUB_CD             --产品子代码                                                                               
           ,CCY                   --币种                                                                                     
           ,CDR_YR                --年份YYYY                                                                                 
           ,CDR_MTH               --月份MM                                                                                   
           ,ACG_DT                --日期YYYY-MM-DD                                                                           
           ,NOCLD_IN_MTH          --当月日历天数                                                                             
           ,NOCLD_IN_QTR          --当季日历天数                                                                             
           ,NOCLD_IN_YEAR         --当年日历天数                                                                             
           ,NOD_IN_MTH            --当月有效天数                                                                             
           ,NOD_IN_QTR            --当季有效天数                                                                             
           ,NOD_IN_YEAR           --当年有效天数                                                                             
           ,OU_ID                 --机构号                                                                                   
           ,CST_TP_ID             --客户类型                                                                                 
           ,NBR_AC                --账户个数                                                                                 
           ,LST_DAY_BAL           --昨日余额                                                                                 
           ,DEP_BAL               --存款余额                                                                                 
           ,MTD_ACML_DEP_BAL_AMT  --月累计余额                                                                               
           ,QTD_ACML_DEP_BAL_AMT  --季累计余额                                                                               
           ,YTD_ACML_DEP_BAL_AMT  --年累计余额                                                                               
        )                                                                                                                    
    SELECT                                                                                                                                                                                                                                                
            COALESCE(S.CST_ID,'')                --客户内码                                                                               
           ,S.AC_OU_ID              --账户归属机构号                                                                         
           ,S.DEP_TP_ID             --存款类型                                                                               
           ,COALESCE(S.PD_GRP_CD,'')             --产品组代码                                                                             
           ,COALESCE(S.PD_SUB_CD,'')             --产品子代码                                                                             
           ,S.CCY                   --币种                                                                                   
           ,S.CDR_YR                --年份YYYY                                                                               
           ,S.CDR_MTH               --月份MM                                                                                 
           ,S.ACG_DT                --日期YYYY-MM-DD                                                                         
           ,S.NOCLD_IN_MTH          --当月日历天数                                                                           
           ,S.NOCLD_IN_QTR          --当季日历天数                                                                           
           ,S.NOCLD_IN_YEAR         --当年日历天数                                                                           
           ,S.NOD_IN_MTH            --当月有效天数                                                                           
           ,S.NOD_IN_QTR            --当季有效天数                                                                           
           ,S.NOD_IN_YEAR           --当年有效天数                                                                           
           ,S.OU_ID                 --机构号                                                                                 
           ,S.CST_TP_ID             --客户类型                                                                               
           ,S.NBR_AC                --账户个数                                                                               
           ,S.LST_DAY_BAL           --昨日余额                                                                               
           ,S.DEP_BAL               --存款余额                                                                               
           ,S.MTD_ACML_DEP_BAL_AMT  --月累计余额                                                                             
           ,S.QTD_ACML_DEP_BAL_AMT  --季累计余额                                                                             
           ,S.YTD_ACML_DEP_BAL_AMT  --年累计余额                                                                             
    FROM SESSION.S as S                                                                                                             
	;
ELSE
	MERGE INTO (select * from SMY.CST_DEP_MTHLY_SMY where ACG_DT>=MTH_FIRST_DAY and ACG_DT<=MTH_LAST_DAY) AS T 
	 		USING  SESSION.S AS S                                               
	 	  ON                                                                  
	         S.CST_ID     =T.CST_ID                                         
	     AND S.AC_OU_ID   =T.AC_OU_ID                                       
	     AND S.DEP_TP_ID  =T.DEP_TP_ID                                      
	     AND S.PD_GRP_CD  =T.PD_GRP_CD                                      
	     AND S.PD_SUB_CD  =T.PD_SUB_CD                                      
	     AND S.CCY        =T.CCY                                            
	     AND S.CDR_YR     =T.CDR_YR                                         
			 AND S.CDR_MTH    =T.CDR_MTH                                        
	WHEN MATCHED THEN UPDATE SET                                            
	                                                                        
	        ACG_DT               =S.ACG_DT                --日期YYYY-MM-DD  
	       ,NOCLD_IN_MTH         =S.NOCLD_IN_MTH          --当月日历天数    
	       ,NOCLD_IN_QTR         =S.NOCLD_IN_QTR          --当季日历天数    
	       ,NOCLD_IN_YEAR        =S.NOCLD_IN_YEAR         --当年日历天数    
	       ,NOD_IN_MTH           =S.NOD_IN_MTH            --当月有效天数    
	       ,NOD_IN_QTR           =S.NOD_IN_QTR            --当季有效天数    
	       ,NOD_IN_YEAR          =S.NOD_IN_YEAR           --当年有效天数    
	       ,OU_ID                =S.OU_ID                 --机构号          
	       ,CST_TP_ID            =S.CST_TP_ID             --客户类型        
	       ,NBR_AC               =S.NBR_AC                --账户个数        
	       ,LST_DAY_BAL          =S.LST_DAY_BAL           --昨日余额        
	       ,DEP_BAL              =S.DEP_BAL               --存款余额        
	       ,MTD_ACML_DEP_BAL_AMT =S.MTD_ACML_DEP_BAL_AMT  --月累计余额      
	       ,QTD_ACML_DEP_BAL_AMT =S.QTD_ACML_DEP_BAL_AMT  --季累计余额      
	       ,YTD_ACML_DEP_BAL_AMT =S.YTD_ACML_DEP_BAL_AMT  --年累计余额      
  ;

  INSERT INTO SMY.CST_DEP_MTHLY_SMY 
  (                                                                                                                         
            CST_ID                --客户内码                                                                                 
           ,AC_OU_ID              --账户归属机构号                                                                           
           ,DEP_TP_ID             --存款类型                                                                                 
           ,PD_GRP_CD             --产品组代码                                                                               
           ,PD_SUB_CD             --产品子代码                                                                               
           ,CCY                   --币种                                                                                     
           ,CDR_YR                --年份YYYY                                                                                 
           ,CDR_MTH               --月份MM                                                                                   
           ,ACG_DT                --日期YYYY-MM-DD                                                                           
           ,NOCLD_IN_MTH          --当月日历天数                                                                             
           ,NOCLD_IN_QTR          --当季日历天数                                                                             
           ,NOCLD_IN_YEAR         --当年日历天数                                                                             
           ,NOD_IN_MTH            --当月有效天数                                                                             
           ,NOD_IN_QTR            --当季有效天数                                                                             
           ,NOD_IN_YEAR           --当年有效天数                                                                             
           ,OU_ID                 --机构号                                                                                   
           ,CST_TP_ID             --客户类型                                                                                 
           ,NBR_AC                --账户个数                                                                                 
           ,LST_DAY_BAL           --昨日余额                                                                                 
           ,DEP_BAL               --存款余额                                                                                 
           ,MTD_ACML_DEP_BAL_AMT  --月累计余额                                                                               
           ,QTD_ACML_DEP_BAL_AMT  --季累计余额                                                                               
           ,YTD_ACML_DEP_BAL_AMT  --年累计余额                                                                               
        )                                                                                                                    
    SELECT                                                                                                                                                                                                                                                
            COALESCE(S.CST_ID,'')                --客户内码                                                                               
           ,S.AC_OU_ID              --账户归属机构号                                                                         
           ,S.DEP_TP_ID             --存款类型                                                                               
           ,COALESCE(S.PD_GRP_CD,'')             --产品组代码                                                                             
           ,COALESCE(S.PD_SUB_CD,'')             --产品子代码                                                                             
           ,S.CCY                   --币种                                                                                   
           ,S.CDR_YR                --年份YYYY                                                                               
           ,S.CDR_MTH               --月份MM                                                                                 
           ,S.ACG_DT                --日期YYYY-MM-DD                                                                         
           ,S.NOCLD_IN_MTH          --当月日历天数                                                                           
           ,S.NOCLD_IN_QTR          --当季日历天数                                                                           
           ,S.NOCLD_IN_YEAR         --当年日历天数                                                                           
           ,S.NOD_IN_MTH            --当月有效天数                                                                           
           ,S.NOD_IN_QTR            --当季有效天数                                                                           
           ,S.NOD_IN_YEAR           --当年有效天数                                                                           
           ,S.OU_ID                 --机构号                                                                                 
           ,S.CST_TP_ID             --客户类型                                                                               
           ,S.NBR_AC                --账户个数                                                                               
           ,S.LST_DAY_BAL           --昨日余额                                                                               
           ,S.DEP_BAL               --存款余额                                                                               
           ,S.MTD_ACML_DEP_BAL_AMT  --月累计余额                                                                             
           ,S.QTD_ACML_DEP_BAL_AMT  --季累计余额                                                                             
           ,S.YTD_ACML_DEP_BAL_AMT  --年累计余额                                                                             
    FROM SESSION.S as S
    WHERE NOT EXISTS (
         SELECT 1 FROM SMY.CST_DEP_MTHLY_SMY T
         WHERE S.CST_ID     =T.CST_ID                                         
					     AND S.AC_OU_ID   =T.AC_OU_ID                                       
					     AND S.DEP_TP_ID  =T.DEP_TP_ID                                      
					     AND S.PD_GRP_CD  =T.PD_GRP_CD                                      
					     AND S.PD_SUB_CD  =T.PD_SUB_CD                                      
					     AND S.CCY        =T.CCY                                            
					     AND S.CDR_YR     =T.CDR_YR                                         
							 AND S.CDR_MTH    =T.CDR_MTH 
							 AND T.ACG_DT>=MTH_FIRST_DAY AND T.ACG_DT<=MTH_LAST_DAY
    )                                                                                                                  
	;
END IF;
---------------------------End of Modification on 20091225-------------------------------------------------------------------
	 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;                                                                                  --
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)                  
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);                      --
                                                                                                                             
		SET SMY_STEPNUM = 6 ;                                                                                                    --
		SET SMY_STEPDESC = '存储过程结束！'; 		                                                                                 --
                                                                                                                             
	 	SET SMY_RCOUNT = 0;                                                                                                      --
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)                  
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);                      --
	 	                                                                                                                         
	 COMMIT;                                                                                                                   --
END@