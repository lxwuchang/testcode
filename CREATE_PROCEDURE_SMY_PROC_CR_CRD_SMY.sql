CREATE PROCEDURE SMY.PROC_CR_CRD_SMY(IN ACCOUNTING_DATE date)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_CR_CRD_SMY.sql
-- Procedure name: 			SMY.PROC_CR_CRD_SMY
-- Source Table:				SOR.CR_CRD,SOR.CRD,SOR.CC_AC_AR,SOR.CC_REPYMT_TXN_DTL,SOR.CC_INT_RCVB_RGST
-- Target Table: 				SMY.CR_CRD_SMY
-- Project     :        ZJ RCCB EDW
-- NOTES       :        
-- Purpose     :            
-- PROCESS METHOD      :  empty and INSERT
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
-- 2009-11-19   Xu Yan          Added two new columns
-- 2009-11-24   Xu Yan          Updated some conditional statements
-- 2010-01-19   Xu Yan          Updated AMT_RCVD_For_LST_TM_OD getting rules
-- 2010-05-21   Xu Yan          Added a column 'DRMT_CRD_F', please refer to SMY.PROC_CRD_PRFL_CRD_DLY_SMY.
-- 2011-04-21   Wang You Bing   Added a column 'INT_RCVB_EXCPT_OFF_BST';Added DEL_F on each SOR table
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
		DECLARE EMP_SQL VARCHAR(200);--

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
    SET SMY_PROCNM  ='PROC_CR_CRD_SMY';--
    SET SMY_DATE    =ACCOUNTING_DATE;    --
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --取当前年份
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --取当前月份
    SET CUR_DAY     =DAY(ACCOUNTING_DATE);     --取月第几日
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- 取年初日
    SET C_YR_DAY      =DAYOFYEAR(DATE(TRIM(CHAR(YEAR(ACCOUNTING_DATE)))||'-12-31')); --取当年第几日
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --当前季度
    SET MTH_FIRST_DAY=DATE(TRIM(CHAR(TRIM(CHAR(CUR_YEAR))||'-'||RIGHT('0'||TRIM(CHAR(CUR_MONTH)),2)||'-01'))); --取当月初日
    SET MON_DAY     =DAY(ACCOUNTING_DATE);     --取月第几日
   
    SET LAST_SMY_DATE=DATE(SMY_DATE) - 1 DAYS ;--

    SET LAST_MONTH = MONTH(LAST_SMY_DATE);--

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
	

/*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE,并插入新的起始标志*/
		DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
			COMMIT;--
		
		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		
		SET SMY_STEPDESC = 	'存储过程开始运行' ;--
		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
				VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--


	   SET EMP_SQL= 'Alter TABLE SMY.CR_CRD_SMY ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE' ;--
		
		  EXECUTE IMMEDIATE EMP_SQL;       --
      
      COMMIT;--

SET SMY_STEPNUM = 2 ;--
SET SMY_STEPDESC = '声明用户临时表,存放SOR.CC_REPYMT_TXN_DTL 临时数据';--

	/*声明用户临时表*/
	
	DECLARE GLOBAL TEMPORARY TABLE T_REPYMT_AMT 
   AS 
   (
   		SELECT 
   				CC_AC_AR_ID
   			 ,DNMN_CCY_ID
   			 ,SUM( REPYMT_AMT ) AS AMT_RCVD_For_LST_TM_OD
   			 ,SUM(REPYMT_AMT ) AS AMT_RCVD
   		FROM SOR.CC_REPYMT_TXN_DTL 
   		GROUP BY CC_AC_AR_ID,DNMN_CCY_ID
      ) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE
     PARTITIONING KEY(CC_AC_AR_ID) IN TS_USR_TMP32K;  	--
     
  INSERT INTO SESSION.T_REPYMT_AMT
   		SELECT 
   				CC_AC_AR_ID
   				,DNMN_CCY_ID
    ------------------------------------------Start of 2009-11-24----------------------------------------------------------------------
   			  --,SUM(CASE WHEN REPYMT_DT=ACCOUNTING_DATE AND MONTH(TXN_DT)=LAST_MONTH  THEN REPYMT_AMT ELSE 0 END) AS AMT_RCVD_For_LST_TM_OD
   			  -----------------------------------------Start on 20100119-----------------------------------------------------------------------
   			  --,SUM(CASE WHEN REPYMT_DT=ACCOUNTING_DATE AND MONTH(TXN_DT) < LAST_MONTH  THEN REPYMT_AMT ELSE 0 END) AS AMT_RCVD_For_LST_TM_OD
   			  ,SUM(CASE WHEN TXN_DT < MTH_FIRST_DAY  THEN REPYMT_AMT ELSE 0 END) AS AMT_RCVD_For_LST_TM_OD
   			  -----------------------------------------End on 20100119-----------------------------------------------------------------------
   	------------------------------------------End of 2009-11-24----------------------------------------------------------------------
   			  -------------------------Start on 20100119----------------------------------------
   			 --,SUM(CASE WHEN REPYMT_DT=ACCOUNTING_DATE THEN REPYMT_AMT ELSE 0 END) AS AMT_RCVD
   			 	,SUM(REPYMT_AMT) AS AMT_RCVD
   			 	-------------------------End on 20100119----------------------------------------
   		FROM SOR.CC_REPYMT_TXN_DTL 
   		-------------------------Start on 20100119----------------------------------------
   		WHERE REPYMT_DT=ACCOUNTING_DATE 
   		-------------------------Start on 20110421--------------------------------------
   		AND DEL_F=0
   		-------------------------End on 20110421----------------------------------------
   		-------------------------End on 20100119----------------------------------------
   		GROUP BY CC_AC_AR_ID , DNMN_CCY_ID	
   ;      --
 /* 收集操作信息 */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	 --

 
		SET SMY_STEPNUM = 3 ;--
		SET SMY_STEPDESC = '声明临时表, 存放从SOR.CC_AC_AR 汇总后的数据'; 		 --
 
	DECLARE GLOBAL TEMPORARY TABLE T_CC_AC_AR 
   AS 
   (
   		SELECT
   				 CC_AC_AR_ID                AS CC_AC_AR_ID
          ,AST_RSK_ASES_RTG_TP_CD     AS AST_RSK_ASES_RTG_TP_CD  --
          ,LN_FR_RSLT_TP_ID           AS LN_FIVE_RTG_STS         --     
          ,BAL_AMT                    AS AC_BAL_AMT              --账户余额
          ,(CASE WHEN BAL_AMT <=0 THEN 0 ELSE BAL_AMT END)  AS DEP_BAL_CRD  --银行卡存款余额 
          ,(CASE WHEN BAL_AMT > 0 THEN 0 ELSE ABS(BAL_AMT-ODUE_INT_AMT-FEE_AMT_DUE) END)  AS OD_BAL_AMT --透支余额
          ,(CASE WHEN BAL_AMT >=0 then 0 ELSE ABS(BAL_AMT) END )  AS AMT_PNP_ARS   --透支本金
          ,(CASE WHEN BAL_AMT-ODUE_INT_AMT-FEE_AMT_DUE < 0 THEN BAL_AMT-ODUE_INT_AMT-FEE_AMT_DUE ELSE 0 END) AS OTSND_AMT_RCVB --应收账款余额
          ,(FEE_AMT_DUE)                           AS FEE_RCVB  --应收费用
          ,(ODUE_INT_AMT)                          AS INT_RCVB  --应收利息
          ,TMP_CRED_LMT_AMT                        AS TMP_CRED_LMT        --临时授信金额
          ,CR_LMT                                      --授信额度    
          ,DEP_ACG_SBJ_ID                 --存款科目
          ,OD_ACG_SBJ_ID              --透支科目
                   	 
   		FROM SOR.CC_AC_AR 
      )  DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE
     PARTITIONING KEY(CC_AC_AR_ID) IN TS_USR_TMP32K;  	--
     
 INSERT INTO SESSION.T_CC_AC_AR 
       SELECT  
   				 CC_AC_AR_ID                AS CC_AC_AR_ID
          ,AST_RSK_ASES_RTG_TP_CD     AS AST_RSK_ASES_RTG_TP_CD  --
          ,LN_FR_RSLT_TP_ID           AS LN_FIVE_RTG_STS         --     
          ,BAL_AMT                    AS AC_BAL_AMT              --账户余额
          ,(CASE WHEN BAL_AMT <=0 THEN 0 ELSE BAL_AMT END)  AS DEP_BAL_CRD  --银行卡存款余额 
          ,(CASE WHEN BAL_AMT > 0 THEN 0 ELSE ABS(BAL_AMT-ODUE_INT_AMT-FEE_AMT_DUE) END)  AS OD_BAL_AMT --透支余额
          ,(CASE WHEN BAL_AMT >=0 then 0 ELSE ABS(BAL_AMT) END )  AS AMT_PNP_ARS   --透支本金
          ,(CASE WHEN BAL_AMT-ODUE_INT_AMT-FEE_AMT_DUE < 0 THEN BAL_AMT-ODUE_INT_AMT-FEE_AMT_DUE ELSE 0 END) AS OTSND_AMT_RCVB --应收账款余额
          ,(FEE_AMT_DUE)                           AS FEE_RCVB  --应收费用
          ,(ODUE_INT_AMT)                          AS INT_RCVB  --应收利息
          ,TMP_CRED_LMT_AMT                        AS TMP_CRED_LMT                 --临时授信金额
          ,CR_LMT                                               --授信额度    
          ,DEP_ACG_SBJ_ID             --存款科目
          ,OD_ACG_SBJ_ID              --透支科目
          	 
   		FROM SOR.CC_AC_AR 
   		-------------------------Start on 20110421--------------------------------------
   		WHERE DEL_F=0
   		-------------------------End on 20110421----------------------------------------
 		
 		;     --
 /* 收集操作信息 */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

-----Start On 20110421---
		SET SMY_STEPNUM = 4 ;--
		SET SMY_STEPDESC = '声明临时表, 存放从SOR.CC_INT_RCVB_RGST汇总后的数据'; 		 --	
		
	 DECLARE GLOBAL TEMPORARY TABLE T_CC_INT_RCVB_RGST AS 
   (SELECT CC_AC_AR_ID,
           DNMN_CCY_ID,
           SUM(COALESCE(INT_AMT_RCVB_OTSND_PART,0)) INT_RCVB_EXCPT_OFF_BST 
    FROM  SOR.CC_INT_RCVB_RGST
    GROUP BY  CC_AC_AR_ID,DNMN_CCY_ID
   ) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE PARTITIONING KEY(CC_AC_AR_ID) IN TS_USR_TMP32K;  
	 
	 INSERT INTO SESSION.T_CC_INT_RCVB_RGST
	 SELECT CC_AC_AR_ID,
          DNMN_CCY_ID,
          SUM(COALESCE(INT_AMT_RCVB_OTSND_PART,0)) INT_RCVB_EXCPT_OFF_BST 
   FROM  SOR.CC_INT_RCVB_RGST
   WHERE DEL_F=0 AND ON_OFF_BSH_IND_TP_ID=16240001
   GROUP BY  CC_AC_AR_ID,DNMN_CCY_ID;
	
	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
  VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);-- 

-----Start On 20110421---
	 
		SET SMY_STEPNUM = 5 ;--
		SET SMY_STEPDESC = '往汇总表CR_CRD_SMY,插入当日的数据'; 	 --

  INSERT INTO SMY.CR_CRD_SMY
  
  (
          CRD_NO                  --卡号                    
         ,CR_CRD_TP_ID            --卡类型                  
         ,CRD_Brand_TP_Id         --卡品牌类型              
         ,CRD_PRVL_TP_ID          --卡级别                  
         ,ENT_IDV_IND             --卡对象                  
         ,MST_CRD_IND             --主/副卡标志             
         ,NGO_CRD_IND             --协议卡类型              
         ,MULT_CCY_F              --双币卡标志              
         ,AST_RSK_ASES_RTG_TP_CD  --资产风险分类            
         ,LN_FIVE_RTG_STS         --贷款五级形态类型        
         ,PD_GRP_CD               --产品类                  
         ,PD_SUB_CD               --产品子代码              
         ,CRD_LCS_TP_ID           --卡状态                  
         ,OU_ID                   --受理机构号              
         ,CCY                     --币种                    
         ,ISSU_CRD_OU_Id          --发卡机构号              
         ,TMP_CRED_LMT            --临时授信金额            
         ,CR_LMT                  --授信额度                
         ,AC_AR_Id                --相关账号                
         ,AC_BAL_AMT              --账户余额                
         ,DEP_BAL_CRD             --银行卡存款余额          
         ,OD_BAL_AMT              --透支余额                
         ,AMT_PNP_ARS             --透支本金                
         ,OTSND_AMT_RCVB          --应收账款余额            
         ,FEE_RCVB                --应收费用                
         ,INT_RCVB                --应收利息                
         ,AMT_RCVD_For_LST_TM_OD  --本期还的上个月之前的金额
         ,AMT_RCVD                --已还款金额              
         ,EFF_DT                  --卡启用日期              
         ,END_DT                  --销户日期                
         ,CST_ID                  --客户内码                
         ,LST_CST_AVY_DT          --客户最后活动日期        
         ,EXP_MTH_YEAR            --到期年月                
         ,CRD_CHG_DT              --换卡日期                
         ,CRD_DLVD_DT             --开卡日期                
         ,BIZ_CGY_TP_ID           --业务类别
         ,CST_NO	                --客户号
         ,CST_NM	                --客户名称
         ,CST_CR_RSK_RTG_ID	      --客户资信等级               
------------------------------Start of modification on 2009-11-19----------------------------------------------
	       ,AC_RVL_LMT_AC_F         --循环账户标志
	       ,AC_BYND_LMT_F           --超限账户标志
         ,DEP_ACG_SBJ_ID          --存款科目
         ,OD_ACG_SBJ_ID           --透支科目
------------------------------End of modification on 2009-11-19----------------------------------------------
----------------------------Start on 2010-05-21--------------------------------------------------
         ,DRMT_CRD_F              --睡眠卡标志
----------------------------End on 2010-05-21--------------------------------------------------        
-------------------------Start On 20110421-----------------
         ,INT_RCVB_EXCPT_OFF_BST                          
-------------------------End On 20110421-----------------  
  )
  	SELECT 
          CR_CRD.CC_NO                              --卡号                        
         ,CR_CRD.CC_TP_ID                           --卡类型                      
         ,CRD.CRD_BRND_TP_ID                        --卡品牌类型                  
         ,CR_CRD.CRD_PRVL_TP_ID                     --卡级别                      
         ,CRD.ENT_IDV_CST_IND                       --卡对象                      
         ,CR_CRD.MST_CRD_IND                        --主/副卡标志                 
         ,CRD.NGO_CRD_IND                           --协议卡类型                  
         ,CRD.MULTI_CCY_F                           --双币卡标志                  
         ,COALESCE(T_CC_AC_AR.AST_RSK_ASES_RTG_TP_CD,'')         --资产风险分类                
         ,COALESCE(T_CC_AC_AR.LN_FIVE_RTG_STS,-1)               --贷款五级形态类型            
         ,CRD.PD_GRP_CD                             --产品类                      
         ,CRD.PD_SUB_CD                             --产品子代码                  
         ,CRD.CRD_LCS_TP_ID                         --卡状态                      
         ,CR_CRD.APL_ACPT_OU_IP_ID                  --受理机构号                  
         ,CR_CRD.PRIM_CCY_ID                        --币种                        
         ,CR_CRD.ISSU_CRD_OU_IP_ID                  --发卡机构号                  
         ,COALESCE(T_CC_AC_AR.TMP_CRED_LMT,0)                   --临时授信金额                
         ,COALESCE(T_CC_AC_AR.CR_LMT ,0)                        --授信额度                    
         ,CRD.AC_AR_ID                              --相关账号                    
         ,COALESCE(T_CC_AC_AR.AC_BAL_AMT               ,0)      --账户余额                    
         ,COALESCE(T_CC_AC_AR.DEP_BAL_CRD              ,0)      --银行卡存款余额              
         ,COALESCE(T_CC_AC_AR.OD_BAL_AMT               ,0)      --透支余额                    
         ,COALESCE(T_CC_AC_AR.AMT_PNP_ARS              ,0)      --透支本金                    
         ,COALESCE(T_CC_AC_AR.OTSND_AMT_RCVB           ,0)      --应收账款余额                
         ,COALESCE(T_CC_AC_AR.FEE_RCVB                 ,0)      --应收费用                    
         ,COALESCE(T_CC_AC_AR.INT_RCVB                 ,0)      --应收利息                    
         ,COALESCE(T_REPYMT_AMT.AMT_RCVD_FOR_LST_TM_OD ,0)      --本期还的上个月之前的金额    
         ,COALESCE(T_REPYMT_AMT.AMT_RCVD               ,0)      --已还款金额                  
         ,CRD.EFF_DT                                --卡启用日期                  
         ,CRD.END_DT                                --销户日期                    
         ,CRD.PRIM_CST_ID                           --客户内码                    
         ,CRD.LAST_CST_AVY_DT                       --客户最后活动日期            
         ,CR_CRD.EXP_MTH_YEAR                       --到期年月                    
         ,CR_CRD.CRD_CHG_DT                         --换卡日期                    
         ,CR_CRD.CRD_DLVD_DT                        --开卡日期                    
         ,CRD.BIZ_TP_ID                             --业务类别                    
         ,COALESCE(CST_INF.CST_NO ,'')                           --客户号                      
         ,COALESCE(CST_INF.CST_NM ,'')                           --客户名称                    
         ,COALESCE(CST_INF.CST_CR_RSK_RTG_ID,-1)               --客户资信等级                
------------------------------Start of modification on 2009-11-19----------------------------------------------
	       ,COALESCE(CC_AC_SMY.RVL_LMT_AC_F     , -1)       --循环账户标志
         ,COALESCE(CC_AC_SMY.BYND_LMT_F       , -1)      --超限账户标志
         ,COALESCE(T_CC_AC_AR.DEP_ACG_SBJ_ID  , '')       --存款科目
         ,COALESCE(T_CC_AC_AR.OD_ACG_SBJ_ID   , '')      --透支科目
------------------------------End of modification on 2009-11-19----------------------------------------------
----------------------------Start on 2010-05-21--------------------------------------------------
         ,case when DAYS(SMY_DATE) - DAYS(LAST_CST_AVY_DT) >= 180   				
  									and DAYS(SMY_DATE) - DAYS(CRD.EFF_DT) >= 180
  									and DAYS(SMY_DATE) - DAYS(CRD_DLVD_DT) >= 180   
  									and CRD_LCS_TP_ID in ( 11920001 --正常
  																				,11920002 --新发卡未启用
  																				,11920003 --新换卡未启用
  																			 )
  			  then 1 else 0 end as DRMT_CRD_F              --睡眠卡标志
----------------------------End on 2010-05-21-------------------------------------------------- 
----------------------------Start On 20110421-------------------------------                                 
        ,COALESCE(INT_RCVB_EXCPT_OFF_BST,0)
----------------------------End On 20110421-------------------------------
          	
  	FROM      SOR.CR_CRD AS CR_CRD INNER  JOIN       SOR.CRD              AS  CRD           ON CR_CRD.CC_NO=CRD.CRD_NO AND CRD.DEL_F=0
  	                                LEFT OUTER JOIN  SESSION.T_CC_AC_AR   AS  T_CC_AC_AR    ON CRD.AC_AR_ID    =T_CC_AC_AR.CC_AC_AR_ID
  	                                LEFT OUTER JOIN  SESSION.T_REPYMT_AMT AS  T_REPYMT_AMT  ON CRD.AC_AR_ID    =T_REPYMT_AMT.CC_AC_AR_ID AND CR_CRD.PRIM_CCY_ID = T_REPYMT_AMT.DNMN_CCY_ID
  	                                LEFT OUTER JOIN  SMY.CST_INF          AS CST_INF        ON CRD.PRIM_CST_ID =CST_INF.CST_ID
------------------------------Start of modification on 2009-11-19----------------------------------------------
					LEFT OUTER JOIN  SMY.MTHLY_CR_CRD_AC_ACML_BAL_AMT AS CC_AC_SMY ON CC_AC_SMY.AC_AR_ID = CR_CRD.AC_AR_ID AND CC_AC_SMY.CCY = CR_CRD.PRIM_CCY_ID AND CC_AC_SMY.ACG_DT=SMY_DATE
   		-------------------------Start on 20110421--------------------------------------
   		LEFT JOIN SESSION.T_CC_INT_RCVB_RGST AS T_CC_INT_RCVB_RGST ON CRD.AC_AR_ID=T_CC_INT_RCVB_RGST.CC_AC_AR_ID and CR_CRD.PRIM_CCY_ID=T_CC_INT_RCVB_RGST.DNMN_CCY_ID
   		WHERE CR_CRD.DEL_F=0
   		-------------------------End on 20110421----------------------------------------		
------------------------------End of modification on 2009-11-19----------------------------------------------
   ;	 --
 /* 收集操作信息 */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
	 
	 COMMIT;--
END@