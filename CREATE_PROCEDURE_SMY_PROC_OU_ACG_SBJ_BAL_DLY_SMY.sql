CREATE PROCEDURE SMY.PROC_OU_ACG_SBJ_BAL_DLY_SMY(IN ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_OU_ACG_SBJ_BAL_DLY_SMY.sql
-- Procedure name: 			SMY.PROC_OU_ACG_SBJ_BAL_DLY_SMY
-- Source Table:				sor.FT_DEP_AR union SOR.DMD_DEP_SUB_AR union SOR.INTRBNK_DEP_SUB_AR union SOR.LOAN_AR union SOR.DCN_CTR_AR union SOR.EQTY_AC_SUB_AR union SOR.ON_BST_AC_AR union SOR.OFF_BST_AC_AR
-- Target Table: 				SMY.OU_ACG_SBJ_BAL_DLY_SMY
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
-- 2009-11-17   Xu Yan          Modified some condition statements
-- 2009-11-21   Xu Yan          Added a new column BAL_ACG_EFF_TP_Id
-- 2009-11-23   Xu Yan          Added a new column NBR_AC
-- 2009-11-24   SHANG           增加月表处理
-- 2009-11-27   Xu Yan          Updated the column 'NEW_ACG_SBJ_ID'
-- 2009-11-30   Xu Yan          Updated 'NEW_ACG_SBJ_ID' joint table 
-- 2009-12-01   Xu Yan          Added a new column 'NBR_AC_WITH_BAL'
-- 2009-12-02   Xu Yan          Updated 'NEW_ACG_SBJ_ID' due to the SOR change
-- 2010-01-28   Xu Yan          Inserted the records of the last day which do not exist on the current day
-- 2010-08-10   Fang Yihua      Added three new columns 'NOCLD_IN_MTH','NOCLD_IN_QTR','NOCLD_IN_YEAR'
-- 2010-10-27   Xu Yan          Set the variable MTH_LAST_DAY
-------------------------------------------------------------------------------
LANGUAGE SQL
BEGIN

/*声明异常处理使用变量*/
		DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
		DECLARE SMY_STEPNUM INT DEFAULT 0;                     --过程内部位置标记
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
		-- 账务日期月的最后一天
		DECLARE MTH_LAST_DAY DATE; 		    --

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
      SET SMY_STEPNUM =  SMY_STEPNUM + 1 ;--
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
      COMMIT;--
      RESIGNAL;--
    END;--
    
    DECLARE CONTINUE HANDLER FOR SQLWARNING
    BEGIN
      SET SMY_SQLCODE = SQLCODE;--
      SET SMY_STEPNUM =  SMY_STEPNUM + 1 ;--
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
      COMMIT;--
    END;--

   /*变量赋值*/
    SET SMY_PROCNM  ='PROC_OU_ACG_SBJ_BAL_DLY_SMY';--
    SET SMY_DATE    =ACCOUNTING_DATE;    --
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --取当前年份
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --取当前月份
    SET CUR_DAY     =DAY(ACCOUNTING_DATE);     --取月第几日
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- 取年初日
    SET C_YR_DAY      =DAYOFYEAR(DATE(TRIM(CHAR(YEAR(ACCOUNTING_DATE)))||'-12-31')); --取当年第几日
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --当前季度
    SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,ISO),1,7)||'-01'); --取当月初日
    
    VALUES(ACCOUNTING_DATE - 1 DAY) INTO LAST_SMY_DATE;--
    ------------------------------------Start on 20101027------------------------------
    VALUES(MTH_FIRST_DAY + 1 MONTH - 1 DAY) INTO MTH_LAST_DAY ;  --
    ------------------------------------End on 20101027--------------------------------
    
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
		
		SELECT COALESCE(MAX(ACG_DT),'1900-01-01') INTO MAX_ACG_DT FROM SMY.OU_ACG_SBJ_BAL_DLY_SMY;--

/*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE,并插入新的起始标志*/
		DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
			COMMIT;--
		
		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		
		SET SMY_STEPDESC = 	'存储过程开始运行' ;--
		SET SMY_STEPNUM =  SMY_STEPNUM + 1 ;--
		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
				VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);		 --

/*数据恢复与备份*/
    IF MAX_ACG_DT = ACCOUNTING_DATE THEN
       DELETE FROM SMY.OU_ACG_SBJ_BAL_DLY_SMY WHERE ACG_DT = ACCOUNTING_DATE ;--
       COMMIT;--
    END IF;--

/*月表的恢复*/
   IF ACCOUNTING_DATE = MTH_LAST_DAY THEN 
   		DELETE FROM SMY.OU_ACG_SBJ_BAL_MTHLY_SMY WHERE ACG_DT = ACCOUNTING_DATE ;--
   		COMMIT;--
   	END IF;   --

--SET SMY_STEPNUM = 2 ;--
SET SMY_STEPDESC = '声明用户临时表,存放昨日SMY数据';--

	/*声明用户临时表*/
	
	DECLARE GLOBAL TEMPORARY TABLE TMP 
		LIKE SMY.OU_ACG_SBJ_BAL_DLY_SMY
	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE PARTITIONING KEY(ACG_OU_IP_ID,ACG_SBJ_ID);--

 /*如果是年第一日不需要插入*/

 IF YR_FIRST_DAY <>  ACCOUNTING_DATE THEN 
	INSERT INTO SESSION.TMP 
	(
     ACG_OU_IP_ID       --核算机构
    ,ACG_SBJ_ID         --科目（核算码）
    ------------------------Start of modification on 2009-11-21--------------------------------------
    ,BAL_ACG_EFF_TP_Id  --余额方向
    ------------------------End of modification on 2009-11-21--------------------------------------
    ,CCY                --币种
    ,ACG_DT             --日期
    ,CDR_YR             --年份
    ,CDR_MTH            --月份MM
    ,NOD_In_MTH         --当月有效天数  
    ,NOD_In_QTR         --当季有效天数
    ,NOD_In_Year        --当年有效天数
    ,NEW_ACG_SBJ_ID     --新科目
    ,BAL_AMT            --余额
    ------------------------Start of modification on 2009-11-23--------------------------------------
    ,NBR_AC             --账户数
    ------------------------End of modification on 2009-11-23--------------------------------------
    ,MTD_ACML_BAL_AMT   --月累计余额
    ,QTD_ACML_BAL_AMT   --季累计余额
    ,YTD_ACML_BAL_AMT   --年累计余额
    ------------------------Start of 2009-12-01-----------------------------------------------
    ,NBR_AC_WITH_BAL    --有余额账户数
    ------------------------End of 2009-12-01-----------------------------------------------    
         ) 
    SELECT 
     ACG_OU_IP_ID       --核算机构
    ,ACG_SBJ_ID         --科目（核算码）
    ------------------------Start of modification on 2009-11-21--------------------------------------
    ,BAL_ACG_EFF_TP_Id  --余额方向
    ------------------------End of modification on 2009-11-21--------------------------------------
    ,CCY                --币种
    ,ACG_DT             --日期
    ,CDR_YR             --年份
    ,CDR_MTH            --月份MM
    ,NOD_In_MTH         --当月有效天数  
    ,NOD_In_QTR         --当季有效天数
    ,NOD_In_Year        --当年有效天数
    ,NEW_ACG_SBJ_ID     --新科目
    ,BAL_AMT            --余额
    ------------------------Start of modification on 2009-11-23--------------------------------------
    ,NBR_AC             --账户数
    ------------------------End of modification on 2009-11-23--------------------------------------    
    ,MTD_ACML_BAL_AMT   --月累计余额
    ,QTD_ACML_BAL_AMT   --季累计余额
    ,YTD_ACML_BAL_AMT   --年累计余额      
    ------------------------Start of 2009-12-01-----------------------------------------------
    ,NBR_AC_WITH_BAL    --有余额账户数
    ------------------------End of 2009-12-01-----------------------------------------------
  FROM SMY.OU_ACG_SBJ_BAL_DLY_SMY WHERE ACG_DT = LAST_SMY_DATE ;--
 
END IF;  --
      
 /** 收集操作信息 */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	SET SMY_STEPNUM =  SMY_STEPNUM + 1 ;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	           --

	 IF  ACCOUNTING_DATE IN (YR_FIRST_DAY )  --年 季 月 归零
	 		THEN 
	 			UPDATE SESSION.TMP 
	 				SET 
            NOD_In_MTH       =0 --当月有效天数  
           ,NOD_In_QTR       =0 --当季有效天数
           ,NOD_In_Year      =0 --当年有效天数
           ,MTD_ACML_BAL_AMT =0 --月累计余额
           ,QTD_ACML_BAL_AMT =0 --季累计余额
           ,YTD_ACML_BAL_AMT =0 --年累计余额   
	 			;--
	 ELSEIF ACCOUNTING_DATE IN (QTR_FIRST_DAY) --季 月 归零
	 	  THEN
	 			UPDATE SESSION.TMP 
	 				SET 
            NOD_In_MTH       =0 --当月有效天数  
           ,NOD_In_QTR       =0 --当季有效天数
           ,MTD_ACML_BAL_AMT =0 --月累计余额
           ,QTD_ACML_BAL_AMT =0 --季累计余额
	 	  	;--
	 ELSEIF ACCOUNTING_DATE IN (MTH_FIRST_DAY) --月归零
	 	  THEN 
	 			UPDATE SESSION.TMP 
	 				SET 
            NOD_In_MTH       =0 --当月有效天数  
           ,MTD_ACML_BAL_AMT =0 --月累计余额
	 			;	 	--
	 END IF;--

		--SET SMY_STEPNUM = 3 ;--
		SET SMY_STEPDESC = '声明用户临时表,存放8张从SOR表中余额汇总统计数据';	 --

	DECLARE GLOBAL TEMPORARY TABLE TMP_BAL_AMT AS 
  	(  	
  	  ------------------------Start of modification on 2009-11-23--------------------------------------  
  	  /*      
  		  SELECT RPRG_OU_IP_ID AS ACG_OU_IP_ID,ACG_SBJ_ID AS ACG_SBJ_ID,DNMN_CCY_ID AS CCY, SUM(BAL_AMT) AS BAL_AMT FROM  SOR.FT_DEP_AR            GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID UNION ALL
        SELECT RPRG_OU_IP_ID AS ACG_OU_IP_ID,ACG_SBJ_ID AS ACG_SBJ_ID,DNMN_CCY_ID AS CCY, SUM(BAL_AMT) AS BAL_AMT FROM  SOR.DMD_DEP_SUB_AR       GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID UNION ALL
        SELECT RPRG_OU_IP_ID AS ACG_OU_IP_ID,ACG_SBJ_ID AS ACG_SBJ_ID,DNMN_CCY_ID AS CCY, SUM(BAL_AMT) AS BAL_AMT FROM  SOR.INTRBNK_DEP_SUB_AR   GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID UNION ALL
        SELECT RPRG_OU_IP_ID AS ACG_OU_IP_ID,ACG_SBJ_ID AS ACG_SBJ_ID,DNMN_CCY_ID AS CCY, SUM(BAL_AMT) AS BAL_AMT FROM  SOR.LOAN_AR              GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID UNION ALL
        SELECT RPRG_OU_IP_ID AS ACG_OU_IP_ID,ACG_SBJ_ID AS ACG_SBJ_ID,DNMN_CCY_ID AS CCY, SUM(BAL_AMT) AS BAL_AMT FROM  SOR.DCN_CTR_AR           GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID UNION ALL
        SELECT RPRG_OU_IP_ID AS ACG_OU_IP_ID,ACG_SBJ_ID AS ACG_SBJ_ID,DNMN_CCY_ID AS CCY, SUM(BAL_AMT) AS BAL_AMT FROM  SOR.EQTY_AC_SUB_AR       GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID UNION ALL
        SELECT RPRG_OU_IP_ID AS ACG_OU_IP_ID,ACG_SBJ_ID AS ACG_SBJ_ID,DNMN_CCY_ID AS CCY, SUM(BAL_AMT) AS BAL_AMT FROM  SOR.ON_BST_AC_AR         GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID UNION ALL
        SELECT RPRG_OU_IP_ID AS ACG_OU_IP_ID,ACG_SBJ_ID AS ACG_SBJ_ID,DNMN_CCY_ID AS CCY, SUM(BAL_AMT) AS BAL_AMT FROM  SOR.OFF_BST_AC_AR        GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID    
      */
        SELECT RPRG_OU_IP_ID AS ACG_OU_IP_ID,ACG_SBJ_ID AS ACG_SBJ_ID,DNMN_CCY_ID AS CCY, SUM(BAL_AMT) AS BAL_AMT, COUNT(DISTINCT INR_AC_AR_ID) AS NBR_AC, COUNT(1) AS NBR_AC_WITH_BAL FROM  SOR.OFF_BST_AC_AR        GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID    
  	) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE
     PARTITIONING KEY(ACG_OU_IP_ID);--

 INSERT INTO SESSION.TMP_BAL_AMT (
 				ACG_OU_IP_ID             --核算机构      
 			 ,ACG_SBJ_ID                --科目（核算码）
 			 ,CCY               --币种
 			 ,BAL_AMT										--余额
 			 ,NBR_AC										--账户数
 			 ,NBR_AC_WITH_BAL						--有余额的账户数
 )
 WITH TMP_AC as (
 					--活期存款 存款科目
			    select 
			         DMD_DEP_AR_ID as AC_AR_ID
			        ,RPRG_OU_IP_ID
			        ,ACG_SBJ_ID
			        ,DNMN_CCY_ID
			        ,SUM(BAL_AMT) AS BAL_AMT        
			    from SOR.DMD_DEP_SUB_AR DMD
			    where DMD.DEL_F = 0
								and
								AR_LCS_TP_ID <> 20370008 -- 销户					
								and
								BAL_AMT>=0									
				  GROUP BY DMD_DEP_AR_ID,RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID
				  
				  union all
	        --活期存款 透支科目
          SELECT DMD_DEP_AR_ID AS AC_AR_ID 
                ,RPRG_OU_IP_ID
          			,OD_ACG_SBJ_ID AS ACG_SBJ_ID
          			,DNMN_CCY_ID
          			, SUM(BAL_AMT) AS BAL_AMT           			
        	FROM  SOR.DMD_DEP_SUB_AR  DMD     
          where DMD.DEL_F = 0
								and
								AR_LCS_TP_ID <> 20370008 -- 销户					
								and
								BAL_AMT < 0									
          GROUP BY DMD_DEP_AR_ID,RPRG_OU_IP_ID,OD_ACG_SBJ_ID,DNMN_CCY_ID 
	  
	  			UNION ALL
	  			--同业存款
          SELECT INTRBNK_DEP_AR_ID as AC_AR_ID
          			,RPRG_OU_IP_ID
          			,ACG_SBJ_ID
          			,DNMN_CCY_ID
          			, SUM(BAL_AMT) AS BAL_AMT           			
          	FROM  SOR.INTRBNK_DEP_SUB_AR   SUB
          	where SUB.DEL_F = 0
						      and
						      SUB.AR_LCS_TP_ID <> 20370008 -- 销户			
          	GROUP BY INTRBNK_DEP_AR_ID,RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID 

          UNION ALL
          --股金
          SELECT EQTY_AC_AR_ID as AC_AR_ID
          			,RPRG_OU_IP_ID
          			,ACG_SBJ_ID
          			,DNMN_CCY_ID
          			, SUM(BAL_AMT) AS BAL_AMT           			
          	FROM  SOR.EQTY_AC_SUB_AR    SUB
          	where SUB.DEL_F = 0
						  		and
						  		SUB.AR_LCS_TP_ID <> 20370008 --销户   
          	GROUP BY EQTY_AC_AR_ID,RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID 
   
 ),
  T AS (  					
          SELECT RPRG_OU_IP_ID
          			,ACG_SBJ_ID
          			,DNMN_CCY_ID
          			,SUM(BAL_AMT) AS BAL_AMT
          			,COUNT(1) AS NBR_AC
          		  ,SUM(case when BAL_AMT<>0 then 1 else 0 end) AS NBR_AC_WITH_BAL
          from TMP_AC
          GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID 
          
          UNION ALL
					----------------------------Start of Modification on 2009-11-17----------------------------------------        
          --定期存款
          SELECT RPRG_OU_IP_ID
          			,ACG_SBJ_ID
          			,DNMN_CCY_ID
          			, SUM(BAL_AMT) AS BAL_AMT 
          			------------------------Start of modification on 2009-11-23--------------------------------------  
          			, count(1) as NBR_AC
          			------------------------End of modification on 2009-11-23--------------------------------------            			
          			,SUM(case when BAL_AMT<>0 then 1 else 0 end) AS NBR_AC_WITH_BAL
          	FROM  SOR.FT_DEP_AR  FT         
          	WHERE FT.DEL_F = 0
						      AND
						      AR_LCS_TP_ID<> 20370008  -- 销户
          	GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID 
          					
          union all
				    --信用卡分户	存款科目
						select
							 RPRG_OU_IP_ID
							,DEP_ACG_SBJ_ID as ACG_SBJ_ID --Accounting Subject Item							
							,DNMN_CCY_ID 
							,SUM(BAL_AMT) as BAL_AMT	
							------------------------Start of modification on 2009-11-23--------------------------------------  
          		, count(1) as NBR_AC          		
          		------------------------End of modification on 2009-11-23--------------------------------------  				
          		,SUM(case when BAL_AMT>0 then 1 else 0 end) AS NBR_AC_WITH_BAL
						from SOR.CC_AC_AR CC								 
						where CC.DEL_F = 0
						  		and
						  		AR_LCS_TP_ID = 20370007 --正常
						  		and
						  		BAL_AMT >= 0 		  		
						GROUP BY RPRG_OU_IP_ID,DEP_ACG_SBJ_ID,DNMN_CCY_ID 
          
          union all
				    --信用卡分户	透支科目
						select
							 RPRG_OU_IP_ID
							,OD_ACG_SBJ_ID as ACG_SBJ_ID --Accounting Subject Item							
							,DNMN_CCY_ID 
							,-SUM(BAL_AMT) as BAL_AMT	
							------------------------Start of modification on 2009-11-23--------------------------------------  
          		,count(1) as NBR_AC
          		------------------------End of modification on 2009-11-23--------------------------------------  								
          		,count(1) AS NBR_AC_WITH_BAL
						from SOR.CC_AC_AR CC								 
						where CC.DEL_F = 0
						  		and
						  		AR_LCS_TP_ID = 20370007 --正常
						  		and
						  		BAL_AMT < 0 		  		
						GROUP BY RPRG_OU_IP_ID,OD_ACG_SBJ_ID,DNMN_CCY_ID
						 
          UNION ALL
          --按揭贷款分户 BLFMAMTZ, 普通贷款
          SELECT RPRG_OU_IP_ID
          			,ACG_SBJ_ID
          			,DNMN_CCY_ID
          			, SUM(BAL_AMT) AS BAL_AMT 
								------------------------Start of modification on 2009-11-23--------------------------------------  
	          		, count(1) as NBR_AC
	          		------------------------End of modification on 2009-11-23--------------------------------------  								          			
	          		,SUM(case when BAL_AMT<>0 then 1 else 0 end) AS NBR_AC_WITH_BAL
          	FROM  SOR.LOAN_AR LN
          	WHERE LN.DEL_F = 0
						  		and
						  		AR_LCS_TP_ID = 13360003 --正常             
          	GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID 
          
          UNION ALL
          --贴现分户
          SELECT RPRG_OU_IP_ID
          			,ACG_SBJ_ID
          			,DNMN_CCY_ID
          			, SUM(BAL_AMT) AS BAL_AMT 
          			------------------------Start of modification on 2009-11-23--------------------------------------  
	          		, count(1) as NBR_AC
	          		------------------------End of modification on 2009-11-23--------------------------------------  								          			
	          		,SUM(case when BAL_AMT<>0 then 1 else 0 end) AS NBR_AC_WITH_BAL
          	FROM  SOR.DCN_CTR_AR   DCN
          	where DCN.DEL_F = 0
		  						and
		  						AR_LCS_TP_ID = 13360003 --正常
          	GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID 
          
          UNION ALL
          --表内内部账户
          
          SELECT OBST.RPRG_OU_IP_ID as RPRG_OU_IP_ID
          			,OBST.ACG_SBJ_ID as ACG_SBJ_ID
          			,OBST.DNMN_CCY_ID as DNMN_CCY_ID
          	--------------------Start of modification on 2009-11-21---------------------------------------
          			,sum(case when OBST.BAL_ACG_EFF_TP_ID <> GACC.BAL_ACG_EFF_TP_ID AND OBST.BAL_ACG_EFF_TP_ID = 15070002 then -BAL_AMT else BAL_AMT end ) as BAL_AMT											
          			------------------------Start of modification on 2009-11-23--------------------------------------  
	          		, count(1) as NBR_AC
	          		------------------------End of modification on 2009-11-23--------------------------------------  								          			
	          		,SUM(case when BAL_AMT<>0 then 1 else 0 end) AS NBR_AC_WITH_BAL
          	FROM  SOR.ON_BST_AC_AR OBST
          				left join SOR.ACG_SBJ_ITM GACC on OBST.ACG_SBJ_ID = GACC.ACG_SBJ_ID
           --------------------End of modification on 2009-11-21---------------------------------------
          	where OBST.DEL_F = 0
						  		and
						  		OBST.AR_LCS_TP_ID = 20370007 --正常		  				  					  	       
          	GROUP BY RPRG_OU_IP_ID,OBST.ACG_SBJ_ID,DNMN_CCY_ID 
          
          UNION ALL
          --表外内部账户
          SELECT RPRG_OU_IP_ID
          			,ACG_SBJ_ID
          			,DNMN_CCY_ID
          			, SUM(BAL_AMT) AS BAL_AMT
          			------------------------Start of modification on 2009-11-23--------------------------------------  
	          		, count(1) as NBR_AC
	          		------------------------End of modification on 2009-11-23--------------------------------------  								          			 
	          		,SUM(case when BAL_AMT<>0 then 1 else 0 end) AS NBR_AC_WITH_BAL
          	FROM  SOR.OFF_BST_AC_AR 
          	WHERE DEL_F = 0
						  		and
						  		AR_LCS_TP_ID = 20370007 --正常	       
          	GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID
          ----------------------------End of Modification on 2009-11-17----------------------------------------                                           
  )
   SELECT    
        RPRG_OU_IP_ID           --核算机构             
       ,ACG_SBJ_ID               --科目（核算码）      
       , DNMN_CCY_ID             --币种                
       , SUM(BAL_AMT)          	--余额                 
       , SUM(NBR_AC)           	--账户数               
       , SUM(NBR_AC_WITH_BAL)  	--有余额的账户数       
   FROM  T
   GROUP BY RPRG_OU_IP_ID,ACG_SBJ_ID,DNMN_CCY_ID   
;    --
 ---------------------------Start on 2010-01-28-----------------------------------------------
 create index SESSION.IDX_TMP_BAL on SESSION.TMP_BAL_AMT(ACG_OU_IP_ID, ACG_SBJ_ID, CCY);--
 create index SESSION.IDX_TMP on SESSION.TMP(ACG_OU_IP_ID, ACG_SBJ_ID, CCY);--
 
 Insert into SESSION.TMP_BAL_AMT
	 select  ACG_OU_IP_ID       --核算机构
			    ,ACG_SBJ_ID         --科目（核算码）		    
			    ,CCY                --币种
					,0                  --余额    
	    		,0                  --账户数
	    		,0              		--有余额的账户数  		    
	 from SESSION.TMP as PRE
	 where not exists (
	 		select
						1		
	 		from SESSION.TMP_BAL_AMT CUR
	 		where  CUR.ACG_OU_IP_ID = PRE.ACG_OU_IP_ID
       AND  CUR.ACG_SBJ_ID   = PRE.ACG_SBJ_ID  
       AND  CUR.CCY   			 = PRE.CCY 
	 );  --
	---------------------------End on 2010-01-28-----------------------------------------------

 /** 收集操作信息 */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	SET SMY_STEPNUM =  SMY_STEPNUM + 1 ;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	  --
	 

		--SET SMY_STEPNUM = 4 ;--
		SET SMY_STEPDESC = '往表SMY.OU_ACG_SBJ_BAL_DLY_SMY 中插入会计日期为当天的数据';--
		

		INSERT INTO SMY.OU_ACG_SBJ_BAL_DLY_SMY
 	(
     ACG_OU_IP_ID       --核算机构
    ,ACG_SBJ_ID         --科目（核算码）
    ------------------------Start of modification on 2009-11-21--------------------------------------
    ,BAL_ACG_EFF_TP_Id  --余额方向
    ------------------------End of modification on 2009-11-21--------------------------------------    
    ,CCY                --币种
    ,ACG_DT             --日期
    ,CDR_YR             --年份
    ,CDR_MTH            --月份MM
    ,NOD_IN_MTH         --当月有效天数  
    ,NOD_IN_QTR         --当季有效天数
    ,NOD_IN_YEAR        --当年有效天数
    ,NEW_ACG_SBJ_ID     --新科目
    ,BAL_AMT            --余额
    ------------------------Start of modification on 2009-11-23--------------------------------------
    ,NBR_AC             --账户数
    ------------------------End of modification on 2009-11-23--------------------------------------
    ,MTD_ACML_BAL_AMT   --月累计余额
    ,QTD_ACML_BAL_AMT   --季累计余额
    ,YTD_ACML_BAL_AMT   --年累计余额
    ,NBR_AC_WITH_BAL						--有余额的账户数  
------------------------------------Start on 2010-08-10 -------------------------------------------------
         ,NOCLD_IN_MTH        --当月日历天数
         ,NOCLD_IN_QTR        --当季日历天数
         ,NOCLD_IN_YEAR       --当年日历天数
------------------------------------End on 2010-08-10 ---------------------------------------------------

      )              
		SELECT  
          CUR.ACG_OU_IP_ID       --核算机构
         ,CUR.ACG_SBJ_ID         --科目（核算码）
			   ------------------------Start of modification on 2009-11-21--------------------------------------
			   --对于借贷双方并列的默认取借方，通过余额正负来标识真正的余额方向
			   ,Value(case when GACC.BAL_ACG_EFF_TP_Id = 15070003 then 15070001 else GACC.BAL_ACG_EFF_TP_Id end,15070001) as BAL_ACG_EFF_TP_Id  --余额方向
			   ------------------------End of modification on 2009-11-21--------------------------------------                  
         ,CUR.CCY                --币种
         ,ACCOUNTING_DATE
         ,CUR_YEAR
         ,CUR_MONTH
         ,COALESCE(PRE.NOD_IN_MTH  ,0) + 1		   
         ,COALESCE(PRE.NOD_IN_QTR  ,0) + 1
         ,COALESCE(PRE.NOD_IN_YEAR ,0) + 1 
         ,VALUE(ACG_MAP.NEW_ACG_SBJ_ID,'')   --新科目
         ,CUR.BAL_AMT
         ------------------------Start of modification on 2009-11-23--------------------------------------
			   ,CUR.NBR_AC             --账户数
			   ------------------------End of modification on 2009-11-23--------------------------------------
         ,COALESCE(MTD_ACML_BAL_AMT ,0) + CUR.BAL_AMT
         ,COALESCE(QTD_ACML_BAL_AMT ,0) + CUR.BAL_AMT
         ,COALESCE(YTD_ACML_BAL_AMT ,0) + CUR.BAL_AMT
         ,CUR.NBR_AC_WITH_BAL						--有余额的账户数
------------------------------------Start on 2010-08-10 -------------------------------------------------
         ,CUR_DAY                 --当月日历天数
         ,QTR_DAY                 --当季日历天数
         ,YR_DAY                  --当年日历天数
------------------------------------End on 2010-08-10 ---------------------------------------------------
   	FROM SESSION.TMP_BAL_AMT    AS CUR   
   	LEFT OUTER JOIN SESSION.TMP AS PRE
   		ON 
            CUR.ACG_OU_IP_ID = PRE.ACG_OU_IP_ID
       AND  CUR.ACG_SBJ_ID   = PRE.ACG_SBJ_ID  
       AND  CUR.CCY   			 = PRE.CCY 
    ------------------------Start of modification on 2009-11-21--------------------------------------
    left join SOR.ACG_SBJ_ITM GACC on CUR.ACG_SBJ_ID = GACC.ACG_SBJ_ID    
    ------------------------END of modification on 2009-11-21--------------------------------------
    left join SOR.ACG_SBJ_CODE_MAPPING ACG_MAP on ACG_MAP.ACG_SBJ_ID = CUR.ACG_SBJ_ID and ACG_MAP.END_DT = '9999-12-31'
	;	--
		

 /** 收集操作信息 */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	SET SMY_STEPNUM =  SMY_STEPNUM + 1 ;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	 --

/*月表的插入*/
   IF ACCOUNTING_DATE = MTH_LAST_DAY THEN   		
		  SET SMY_STEPDESC = '本账务日期为该月最后一天,往月表SMY.OU_ACG_SBJ_BAL_MTHLY_SMY 中插入数据';  --
		INSERT INTO SMY.OU_ACG_SBJ_BAL_MTHLY_SMY
 	(
     ACG_OU_IP_ID       --核算机构
    ,ACG_SBJ_ID         --科目（核算码）
    ------------------------Start of modification on 2009-11-21--------------------------------------
    ,BAL_ACG_EFF_TP_Id  --余额方向
    ------------------------End of modification on 2009-11-21--------------------------------------    
    ,CCY                --币种
    ,ACG_DT             --日期
    ,CDR_YR             --年份
    ,CDR_MTH            --月份MM
    ,NOD_IN_MTH         --当月有效天数  
    ,NOD_IN_QTR         --当季有效天数
    ,NOD_IN_YEAR        --当年有效天数
    ,NEW_ACG_SBJ_ID     --新科目
    ,BAL_AMT            --余额
    ------------------------Start of modification on 2009-11-23--------------------------------------
    ,NBR_AC             --账户数
    ------------------------End of modification on 2009-11-23--------------------------------------
    ,MTD_ACML_BAL_AMT   --月累计余额
    ,QTD_ACML_BAL_AMT   --季累计余额
    ,YTD_ACML_BAL_AMT   --年累计余额  
    ,NBR_AC_WITH_BAL						--有余额的账户数
------------------------------------Start on 2010-08-10 -------------------------------------------------
         ,NOCLD_IN_MTH        --当月日历天数
         ,NOCLD_IN_QTR        --当季日历天数
         ,NOCLD_IN_YEAR       --当年日历天数
------------------------------------End on 2010-08-10 ---------------------------------------------------
      )              
		SELECT 		
     ACG_OU_IP_ID       --核算机构
    ,ACG_SBJ_ID         --科目（核算码）
    ------------------------Start of modification on 2009-11-21--------------------------------------
    ,BAL_ACG_EFF_TP_Id  --余额方向
    ------------------------End of modification on 2009-11-21--------------------------------------    
    ,CCY                --币种
    ,ACG_DT             --日期
    ,CDR_YR             --年份
    ,CDR_MTH            --月份MM
    ,NOD_IN_MTH         --当月有效天数  
    ,NOD_IN_QTR         --当季有效天数
    ,NOD_IN_YEAR        --当年有效天数
    ,NEW_ACG_SBJ_ID     --新科目
    ,BAL_AMT            --余额
    ------------------------Start of modification on 2009-11-23--------------------------------------
    ,NBR_AC             --账户数
    ------------------------End of modification on 2009-11-23--------------------------------------
    ,MTD_ACML_BAL_AMT   --月累计余额
    ,QTD_ACML_BAL_AMT   --季累计余额
    ,YTD_ACML_BAL_AMT   --年累计余额  
    ,NBR_AC_WITH_BAL						--有余额的账户数
------------------------------------Start on 2010-08-10 -------------------------------------------------
         ,NOCLD_IN_MTH        --当月日历天数
         ,NOCLD_IN_QTR        --当季日历天数
         ,NOCLD_IN_YEAR       --当年日历天数
------------------------------------End on 2010-08-10 ---------------------------------------------------
  FROM SMY.OU_ACG_SBJ_BAL_DLY_SMY WHERE ACG_DT = ACCOUNTING_DATE ;--
  
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	SET SMY_STEPNUM =  SMY_STEPNUM + 1 ;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
	 
 END IF;  --


COMMIT;--
END@