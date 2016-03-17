CREATE PROCEDURE SMY.PROC_OU_LN_DLY_SMY(IN ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_OU_LN_DLY_SMY.sql
-- Procedure name: 			SMY.PROC_OU_LN_DLY_SMY
-- Source Table:				SMY.LOAN_AR_SMY, SMY.CST_INF ,SMY.LN_AR_INT_MTHLY_SMY
-- Target Table: 				SMY.OU_LN_DLY_SMY
--                      SMY.OU_LN_MTHLY_SMY
-- Project     :        ZJ RCCB EDW
-- NOTES       :        DEPENDENCY  SMY.LOAN_AR_SMY, SMY.CST_INF , SMY.LN_AR_INT_MTHLY_SMY
-- Purpose     :            
-- PROCESS METHOD      :  INSERT ONLY EACH DAY
--=============================================================================
-- Creation Date:       2009.11.03
-- Origin Author:       JAMES SHANG IBM copyright 
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2009-11-03   JAMES SHANG     Create SP File
-- 2009-11-24   JAMES SHANG     增加月表处理		
-- 2009-11-27   Xu Yan          Updated NEW_ACG_SBJ_ID 
-- 2009-11-30   Xu Yan          Updated LN_CTR_TP_ID -> LN_CGY_TP_ID for the LOAN_AR_SMY
-- 2009-11-30   Xu Yan          Added 3 new columns: CST_Area_LVL1_TP_Id, CST_Area_LVL2_TP_Id, CST_Area_LVL3_TP_Id
-- 2009-12-14   Xu Yan          Updated LN_CST_TP_ID and Added a new column ENT_IDV_IND
-- 2009-12-15   Xu Yan          Added a new column: LN_INVST_DIRC_TP_ID
-- 2010-01-06   Xu Yan          Included the related transactions on the account closing day
-- 2010-01-17   Xu Yan          Updated the acmulated balance computing rules
-- 2010-01-18   Xu Yan          Got ACML_BAL_AMT for SMY.LN_AR_INT_MTHLY_SMY directly and also change the NOD getting logic
-- 2010-01-19   Xu Yan          Updated NOD getting logic
-- 2010-01-26   Xu Yan          Using temporary table to improve the performance
-- 2010-08-10   Zhou Chunrong   Added three new columns 'NOCLD_IN_MTH','NOCLD_IN_QTR','NOCLD_IN_YEAR'
-- 2011-05-31   Chen XiaoWen    1、调整临时表TMP的分区键
--                              2、添加临时表T_CUR_TMP先缓存关联数据,再针对临时表group by
--                              3、修改月表插入逻辑
-- 2011-09-06   Li ShenYu       Add PD_UN_CODE for test environment only
-- 2012-02-28   Chen XiaoWen    1、去除临时表TMP_LOAN_AR_SMY,直接从原表读取
--                              2、修改LN_AR_INT_MTHLY_SMY查询条件,改为使用ACG_DT分区键
--                              3、调整临时表T_CUR_TMP索引
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
		DECLARE CUR_QTR SMALLINT;    --
		-- 账务日期月的最后一天
		DECLARE MTH_LAST_DAY DATE; --
	
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
      SET SMY_STEPNUM = SMY_STEPNUM + 1;--
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
      COMMIT;--
      RESIGNAL;--
    END;--
    

   /*变量赋值*/
    SET SMY_PROCNM  ='PROC_OU_LN_DLY_SMY';--
    SET SMY_DATE    =ACCOUNTING_DATE;    --
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --取当前年份
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --取当前月份
    SET CUR_DAY     =DAY(ACCOUNTING_DATE);     --取月第几日
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- 取年初日
    SET C_YR_DAY      =DAYOFYEAR(ACCOUNTING_DATE); --取当年第几日
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --当前季度
    SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,ISO),1,7)||'-01'); --取当月初日
    
    VALUES(ACCOUNTING_DATE - 1 DAY) INTO LAST_SMY_DATE;--
    VALUES(MTH_FIRST_DAY + 1 MONTH - 1 DAY) INTO MTH_LAST_DAY ;      --
    --计算月日历天数
    SET C_MON_DAY = DAY(SMY_DATE);    --
    
    
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
  	SET C_QTR_DAY = DAYS(SMY_DATE) - DAYS(QTR_FIRST_DAY) + 1 ;--
		
--------------------------start on 2010-08-09------------------------------------------------------------------------	
		SET YR_DAY=DAYS(ACCOUNTING_DATE)-DAYS(YR_FIRST_DAY)+1;    --日历天数
		SET QTR_DAY=DAYS(ACCOUNTING_DATE)-DAYS(QTR_FIRST_DAY)+1;    --日历天数
--------------------------end   on 2010-08-09------------------------------------------------------------------------			
		SELECT COALESCE(MAX(ACG_DT),'1900-01-01') INTO MAX_ACG_DT FROM SMY.OU_LN_DLY_SMY;--

/*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE,并插入新的起始标志*/
		DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
			COMMIT;--
		
		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		
		SET SMY_STEPDESC = 	'存储过程开始运行' ;--
		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
				VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);		 --

/*数据恢复与备份*/
    IF MAX_ACG_DT = ACCOUNTING_DATE THEN
       DELETE FROM SMY.OU_LN_DLY_SMY WHERE ACG_DT = ACCOUNTING_DATE ;       --
       COMMIT;--
    END IF;--
/*月表的恢复*/
   IF ACCOUNTING_DATE = MTH_LAST_DAY THEN 
   		DELETE FROM SMY.OU_LN_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = CUR_MONTH ;--
   		COMMIT;--
   	END IF;    --
   

--SET SMY_STEPNUM = 2 ;--
SET SMY_STEPDESC = '声明用户临时表,存放昨日SMY数据';--

	/*声明用户临时表*/
	
	DECLARE GLOBAL TEMPORARY TABLE TMP  as (
	    SELECT 
    			 ACG_OU_IP_ID
          ,ACG_SBJ_ID
          ,LN_CTR_TP_ID
          ,LN_GNT_TP_ID
          ,LN_PPS_TP_ID
          ,FND_SRC_TP_ID
          ,LN_TERM_TP_ID
          ,TM_MAT_SEG_ID
          ,LN_CST_TP_ID
          ,Farmer_TP_Id
          ,LN_LCS_STS_TP_ID
          ,IDY_CL_ID
          ,LN_FIVE_RTG_STS
          ,LN_FNC_STS_TP_ID
          ,CST_CR_RSK_RTG_ID
          ,PD_GRP_CD
          ,PD_SUB_CD
          ,CST_Scale_TP_Id
          ,CCY
          ,ACG_DT
          ,PD_UN_CODE
          ,CDR_YR
          ,CDR_MTH
          ,NOD_In_MTH
          ,NOD_IN_QTR
          ,NOD_IN_YEAR
          ,LN_BAL                     					
					,CST_Area_LVL1_TP_Id               --客户区域类型1
					,CST_Area_LVL2_TP_Id               --客户区域类型2
					,CST_Area_LVL3_TP_Id               --客户区域类型3          
          ,ENT_IDV_IND  										 --个人企业标志                  
          ,LN_INVST_DIRC_TP_ID               --行业投向
     FROM SMY.OU_LN_DLY_SMY
		)
	definition only ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(ACG_OU_IP_ID,ACG_SBJ_ID);--

 /*如果是年第一日不需要插入*/

 IF YR_FIRST_DAY <>  ACCOUNTING_DATE THEN 
	INSERT INTO SESSION.TMP 
	(
		        ACG_OU_IP_ID
          , ACG_SBJ_ID
          , LN_CTR_TP_ID
          , LN_GNT_TP_ID
          , LN_PPS_TP_ID
          , FND_SRC_TP_ID
          , LN_TERM_TP_ID
          , TM_MAT_SEG_ID
          , LN_CST_TP_ID
          , Farmer_TP_Id
          , LN_LCS_STS_TP_ID
          , IDY_CL_ID
          , LN_FIVE_RTG_STS
          , LN_FNC_STS_TP_ID
          , CST_CR_RSK_RTG_ID
          , PD_GRP_CD
          , PD_SUB_CD
          , CST_Scale_TP_Id
          , CCY
          , ACG_DT
          , PD_UN_CODE
          , CDR_YR
          , CDR_MTH
          , NOD_In_MTH
          , NOD_IN_QTR
          , NOD_IN_YEAR
          , LN_BAL        					         
          ----------------------Start on 2009-1-30-----------------------------------------------
					,CST_Area_LVL1_TP_Id               --客户区域类型1
					,CST_Area_LVL2_TP_Id               --客户区域类型2
					,CST_Area_LVL3_TP_Id               --客户区域类型3
          ----------------------End on 2009-11-30-----------------------------------------------
          ----------------------Start on 2009-1-30-----------------------------------------------
          ,ENT_IDV_IND  										 --个人企业标志
          ----------------------End on 2009-11-30-----------------------------------------------
          ,LN_INVST_DIRC_TP_ID               --行业投向
          ) 
    SELECT 
    			 ACG_OU_IP_ID
          ,ACG_SBJ_ID
          ,LN_CTR_TP_ID
          ,LN_GNT_TP_ID
          ,LN_PPS_TP_ID
          ,FND_SRC_TP_ID
          ,LN_TERM_TP_ID
          ,TM_MAT_SEG_ID
          ,LN_CST_TP_ID
          ,Farmer_TP_Id
          ,LN_LCS_STS_TP_ID
          ,IDY_CL_ID
          ,LN_FIVE_RTG_STS
          ,LN_FNC_STS_TP_ID
          ,CST_CR_RSK_RTG_ID
          ,PD_GRP_CD
          ,PD_SUB_CD
          ,CST_Scale_TP_Id
          ,CCY
          ,ACG_DT
          ,PD_UN_CODE
          ,CDR_YR
          ,CDR_MTH
          ,NOD_In_MTH
          ,NOD_IN_QTR
          ,NOD_IN_YEAR
          ,LN_BAL                    
 					----------------------Start on 2009-1-30-----------------------------------------------
					,CST_Area_LVL1_TP_Id               --客户区域类型1
					,CST_Area_LVL2_TP_Id               --客户区域类型2
					,CST_Area_LVL3_TP_Id               --客户区域类型3
          ----------------------End on 2009-11-30-----------------------------------------------          
          ----------------------Start on 2009-1-30-----------------------------------------------
          ,ENT_IDV_IND  										 --个人企业标志
          ----------------------End on 2009-11-30-----------------------------------------------          
          ,LN_INVST_DIRC_TP_ID               --行业投向
     FROM SMY.OU_LN_DLY_SMY WHERE ACG_DT = LAST_SMY_DATE ;  --
     
   END IF;     --
      
      
 /** 收集操作信息 */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	SET SMY_STEPNUM = SMY_STEPNUM + 1;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	           --

CREATE INDEX SESSION.IDX_TMP ON SESSION.TMP(ACG_OU_IP_ID,ACG_SBJ_ID,LN_CTR_TP_ID,LN_GNT_TP_ID,LN_PPS_TP_ID);
	
		SET SMY_STEPDESC = '声明用户临时表存放会计日期为当天的数据';--
		
		
DECLARE GLOBAL TEMPORARY TABLE T_CUR AS 
 (		
					SELECT                                                                                                         																					
               SMY_LOAN_AR.RPRG_OU_IP_ID         AS ACG_OU_IP_ID     --核算机构
              ,SMY_LOAN_AR.ACG_SBJ_ID            AS ACG_SBJ_ID       --存款科目(核算码)
              ---------------------------Start of 20091127--------------------------------------------
              ,SMY_LOAN_AR.NEW_ACG_SBJ_ID        AS NEW_ACG_SBJ_ID   --新科目
              ---------------------------End of 20091127--------------------------------------------
              ,SMY_LOAN_AR.LN_CGY_TP_ID          AS LN_CTR_TP_ID     --业务品种
              ,SMY_LOAN_AR.CLT_TP_ID             AS LN_GNT_TP_ID     --贷款担保方式
              ,SMY_LOAN_AR.LN_PPS_TP_ID          AS LN_PPS_TP_ID     --贷款用途类型
              ,SMY_LOAN_AR.FND_SRC_DST_TP_ID     AS FND_SRC_TP_ID    --资金来源
              ,SMY_LOAN_AR.LN_TERM_TP_ID         AS LN_TERM_TP_ID     --贷款期限类型
              ,SMY_LOAN_AR.TM_MAT_SEG_ID         AS TM_MAT_SEG_ID   --到期期限类型
              --,SMY_CST_INF.ENT_IDV_IND           AS LN_CST_TP_ID           --贷款客户类型
              ,SMY_LOAN_AR.ALT_TP_ID           AS LN_CST_TP_ID           --贷款客户类型
              ,SMY_CST_INF.FARMER_TP_ID          AS Farmer_TP_Id           --农户类别
              ,SMY_LOAN_AR.AR_LCS_TP_ID          AS LN_LCS_STS_TP_ID       --贷款生命周期
              ,SMY_LOAN_AR.CNRL_BNK_IDY_CL_ID    AS IDY_CL_ID              --行业代码
              ,SMY_LOAN_AR.LN_FR_RSLT_TP_ID      AS LN_FIVE_RTG_STS        --四/五级形态分类
              ,SMY_LOAN_AR.AR_FNC_ST_TP_ID       AS LN_FNC_STS_TP_ID       --贷款四级形态类型
              ,SMY_CST_INF.CST_CR_RSK_RTG_ID     AS CST_CR_RSK_RTG_ID      --客户资信等级
              ,SMY_LOAN_AR.PD_GRP_CODE           AS PD_GRP_CD              --产品组代码
              ,SMY_LOAN_AR.PD_SUB_CODE           AS PD_SUB_CD              --产品字代码
              ,SMY_CST_INF.ORG_SCALE_TP_ID       AS CST_Scale_TP_Id        --企业规模类型
              ,SMY_LOAN_AR.DNMN_CCY_ID           AS CCY                    --币种
              ,SMY_LOAN_AR.PD_UN_CODE            AS PD_UN_CODE             --产品代码
              ,(SMY_LOAN_AR.LN_BAL)              AS LN_BAL                 --贷款余额
              ,1         AS NBR_CST           --客户数量
              ,1           AS NBR_AC             --账户数
              ,1           AS NBR_NEW_AC    --新增账户数
              ,1             AS NBR_NEW_CST   --新增客户数
              ,1            AS NBR_AC_CLS    --当天销户账户数  13360005:销户
              ,SMY_LN_AR_INT_MTHLY.YTD_ON_BST_INT_AMT_RCVD   AS YTD_ON_BST_INT_AMT_RCVD         --表内实收利息            
              ,SMY_LN_AR_INT_MTHLY.YTD_OFF_BST_INT_AMT_RCVD   AS YTD_OFF_BST_INT_AMT_RCVD        --表外实收利息                       
              ,SMY_LN_AR_INT_MTHLY.TOT_YTD_AMT_OF_INT_INCM   AS TOT_YTD_AMT_OF_INT_INCM        --利息收入            
              ,SMY_LN_AR_INT_MTHLY.ON_BST_INT_RCVB        AS ON_BST_INT_RCVB             --表内应收未收利息        
              ,SMY_LN_AR_INT_MTHLY.OFF_BST_INT_RCVB       AS OFF_BST_INT_RCVB            --表外应收未收利息
              ,(CASE WHEN SMY_LN_AR_INT_MTHLY.LN_DRDWN_AMT > 0 THEN 1 ELSE 0 END )  AS NBR_LN_DRDWN_AC        --当日累放账户数
              ,SMY_LN_AR_INT_MTHLY.TOT_MTD_LN_DRDWN_AMT             AS TOT_MTD_LN_DRDWN_AMT           --月贷款累计发放金额
              ,SMY_LN_AR_INT_MTHLY.TOT_MTD_AMT_LN_REPYMT_RCVD       AS TOT_MTD_AMT_LN_REPYMT_RCVD     --月累计收回贷款金额
              ,(CASE WHEN SMY_LN_AR_INT_MTHLY.AMT_LN_REPYMT_RCVD > 0 THEN 1 ELSE 0 END )  AS NBR_LN_REPYMT_RCVD_AC  --当日累收账户数                     
              ,SMY_LN_AR_INT_MTHLY.TOT_QTD_LN_DRDWN_AMT             AS TOT_QTD_LN_DRDWN_AMT           --季度贷款累计发放金额
              ,SMY_LN_AR_INT_MTHLY.TOT_QTD_AMT_LN_RPYMT_RCVD        AS TOT_QTD_AMT_LN_RPYMT_RCVD      --季度累计收回贷款金额
              ,SMY_LN_AR_INT_MTHLY.TOT_YTD_LN_DRDWN_AMT             AS TOT_YTD_LN_DRDWN_AMT           --年度贷款累计发放金额
              ,SMY_LN_AR_INT_MTHLY.TOT_YTD_AMT_LN_REPYMT_RCVD       AS TOT_YTD_AMT_LN_REPYMT_RCVD     --年度累计收回贷款金额                  
              ,SMY_LN_AR_INT_MTHLY.CUR_CR_AMT             AS CUR_CR_AMT    			--贷方发生额      
              ,SMY_LN_AR_INT_MTHLY.CUR_DB_AMT             AS CUR_DB_AMT         --借方发生额      
              ,SMY_LN_AR_INT_MTHLY.TOT_MTD_CR_AMT         AS TOT_MTD_CR_AMT     --月累计贷方发生额
              ,SMY_LN_AR_INT_MTHLY.TOT_MTD_DB_AMT         AS TOT_MTD_DB_AMT     --月累计借方发生额
              ,SMY_LN_AR_INT_MTHLY.TOT_QTD_DB_AMT         AS TOT_QTD_DB_AMT     --季累计贷方发生额
              ,SMY_LN_AR_INT_MTHLY.TOT_QTD_CR_AMT         AS TOT_QTD_CR_AMT     --季累计借方发生额
              ,SMY_LN_AR_INT_MTHLY.TOT_YTD_CR_AMT         AS TOT_YTD_CR_AMT     --年累计贷方发生额
              ,SMY_LN_AR_INT_MTHLY.TOT_YTD_DB_AMT         AS TOT_YTD_DB_AMT     --年累计借方发生额
              ,SMY_LN_AR_INT_MTHLY.OFF_BST_INT_RCVB_WRTOF          AS  OFF_BST_INT_RCVB_WRTOF           --表外应收利息核销金额
              ,SMY_LN_AR_INT_MTHLY.OFF_BST_INT_RCVB_RPLC	         AS  OFF_BST_INT_RCVB_RPLC	           --表外应收利息置换金额
              ,SMY_LN_AR_INT_MTHLY.TOT_YTD_INT_INCM_AMT_DEBT_AST	 AS  TOT_YTD_INT_INCM_AMT_DEBT_AST		 --抵债资产抵债利息收入
					    ,SMY_LN_AR_INT_MTHLY.TOT_YTD_INT_INCM_RTND_WRTOF_LN	 AS  TOT_YTD_INT_INCM_RTND_WRTOF_LN   --核销贷款收回利息
              ,SMY_LN_AR_INT_MTHLY.TOT_MTD_NBR_LN_DRDWNTXN         AS  TOT_MTD_NBR_LN_DRDWNTXN     --月累计发放贷款笔数
              ,SMY_LN_AR_INT_MTHLY.TOT_QTD_NBR_LN_DRDWN_TXN        AS  TOT_QTD_NBR_LN_DRDWN_TXN    --季累计发放贷款笔数
              ,SMY_LN_AR_INT_MTHLY.TOT_YTD_NBR_LN_DRDWN_TXN        AS  TOT_YTD_NBR_LN_DRDWN_TXN    --年累计发放贷款笔数
              ,SMY_LN_AR_INT_MTHLY.AMT_LN_REPYMT_RCVD  					   AS  AMT_LN_REPYMT_RCVD  			               
              ----------------------Start on 2009-1-30-----------------------------------------------
							,VALUE(SMY_CST_INF.Area_LVL1_TP_Id, -1)             AS CST_Area_LVL1_TP_Id   --客户区域类型1
							,VALUE(SMY_CST_INF.Area_LVL2_TP_Id, -1)             AS CST_Area_LVL2_TP_Id   --客户区域类型2
							,VALUE(SMY_CST_INF.Area_LVL3_TP_Id, -1)             AS CST_Area_LVL3_TP_Id   --客户区域类型3
		          ----------------------End on 2009-11-30-----------------------------------------------
		          ----------------------Start on 2009-1-30-----------------------------------------------
		          ,SMY_LOAN_AR.ENT_IDV_IND  									AS ENT_IDV_IND	 --个人企业标志
		          ----------------------End on 2009-11-30-----------------------------------------------
		          ,SMY_LOAN_AR.LN_INVST_DIRC_TP_ID               --行业投向
		          ,1                                          AS CUR_AR_FLAG --账户是否正常
		          ,SMY_LN_AR_INT_MTHLY.LN_DRDWN_AMT AS LN_DRDWN_AMT
							,MTD_ACML_BAL_AMT
							,QTD_ACML_BAL_AMT
							,YTD_ACML_BAL_AMT
		          ,1 TOT_MTD_NBR_NEW_CST                  --当月累计新增客户数 
		          ,1 TOT_MTD_NBR_AC_CLS                   --当月累计销户账户数                   
		          ,1 TOT_MTD_NBR_LN_DRDWN_AC              --月度累放账户数     
		          ,1 TOT_MTD_NBR_LN_REPYMT_RCVD_AC        --月度累收账户数 
		          ,1 TOT_MTD_NBR_NEW_AC                   --月度新增账户数   
		          ,1 TOT_QTD_NBR_NEW_CST                  --当季累计新增客户数               
		          ,1 TOT_QTD_NBR_AC_CLS                   --当季累计销户账户数               
		          ,1 TOT_QTD_NBR_LN_DRDWN_AC              --季度累放账户数                                                       
		          ,1 TOT_QTD_NBR_LN_REPYMT_RCVD_AC        --季度累收账户数  
		          ,1 TOT_QTD_NBR_NEW_AC                   --季度新增账户数                                       
		          ,1 TOT_YTD_NBR_NEW_CST                  --当月累计新增客户数   
		          ,1 TOT_YTD_NBR_AC_CLS                   --当月累计销户账户数   
		          ,1 TOT_YTD_NBR_LN_DRDWN_AC              --月度累放账户数             
		          ,1 TOT_YTD_NBR_LN_REPYMT_RCVD_AC        --月度累收账户数       
		          ,1 TOT_YTD_NBR_NEW_AC                   --年度新增账户数      
        FROM   
														SMY.LOAN_AR_SMY              AS SMY_LOAN_AR
            INNER JOIN SMY.LN_AR_INT_MTHLY_SMY      AS SMY_LN_AR_INT_MTHLY ON SMY_LOAN_AR.LN_AR_ID     = SMY_LN_AR_INT_MTHLY.LN_AR_ID
        		LEFT OUTER JOIN SMY.CST_INF                  AS SMY_CST_INF   	    ON SMY_LOAN_AR.PRIM_CST_ID 	= SMY_CST_INF.CST_ID 														                  
) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE
     IN TS_USR_TMP32K 
     PARTITIONING KEY(ACG_OU_IP_ID,ACG_SBJ_ID) ;--

-----------------------------------------Start on 2010-01-26---------------------------------------------
/*声明LOAN_AR_SMY 临时表的数据*/
/*DECLARE GLOBAL TEMPORARY TABLE TMP_LOAN_AR_SMY 
		LIKE SMY.LOAN_AR_SMY
	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(CTR_AR_ID,CTR_ITM_ORDR_ID);--
	
	CREATE INDEX SESSION.IDX_LN_AR_ID ON SESSION.TMP_LOAN_AR_SMY(CTR_AR_ID,CTR_ITM_ORDR_ID);--
	
	CREATE INDEX SESSION.IDX_LN_CST_ID ON SESSION.TMP_LOAN_AR_SMY(PRIM_CST_ID);--
	
	INSERT INTO SESSION.TMP_LOAN_AR_SMY SELECT * FROM SMY.LOAN_AR_SMY;--*/
	
	DECLARE GLOBAL TEMPORARY TABLE TMP_LN_AR_INT_MTHLY_SMY
		LIKE SMY.LN_AR_INT_MTHLY_SMY
	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(CTR_AR_ID,CTR_ITM_ORDR_ID);	--
	
	INSERT INTO SESSION.TMP_LN_AR_INT_MTHLY_SMY SELECT * FROM SMY.LN_AR_INT_MTHLY_SMY where ACG_DT>=MTH_FIRST_DAY and ACG_DT<=MTH_LAST_DAY; --where CDR_YR = CUR_YEAR and CDR_MTH = CUR_MONTH;--
	
	CREATE INDEX SESSION.IDX_LN_INT_AR_ID ON SESSION.TMP_LN_AR_INT_MTHLY_SMY(CTR_AR_ID,CTR_ITM_ORDR_ID);--
-----------------------------------------End on 2010-01-26---------------------------------------------	
	     


		
		DECLARE GLOBAL TEMPORARY TABLE T_CUR_TMP  AS (
		SELECT                                                                                                         																					
               SMY_LOAN_AR.RPRG_OU_IP_ID                       AS ACG_OU_IP_ID     --核算机构
              ,SMY_LOAN_AR.ACG_SBJ_ID                          AS ACG_SBJ_ID       --存款科目(核算码)
              ,VALUE(SMY_LOAN_AR.NEW_ACG_SBJ_ID,'')        							 AS NEW_ACG_SBJ_ID   --新科目
              ,SMY_LOAN_AR.LN_CGY_TP_ID                        AS LN_CTR_TP_ID     --业务品种
              ,SMY_LOAN_AR.CLT_TP_ID                           AS LN_GNT_TP_ID     --贷款担保方式
              ,SMY_LOAN_AR.LN_PPS_TP_ID                        AS LN_PPS_TP_ID     --贷款用途类型
              ,SMY_LOAN_AR.FND_SRC_DST_TP_ID                   AS FND_SRC_TP_ID    --资金来源
              ,SMY_LOAN_AR.LN_TERM_TP_ID                       AS LN_TERM_TP_ID     --贷款期限类型
              ,COALESCE(SMY_LOAN_AR.TM_MAT_SEG_ID,-1)                      AS TM_MAT_SEG_ID   --到期期限类型
              ,COALESCE(SMY_LOAN_AR.ALT_TP_ID  ,-1)         AS LN_CST_TP_ID           --贷款客户类型
              ,COALESCE(SMY_CST_INF.FARMER_TP_ID ,-1)         AS Farmer_TP_Id           --农户类别
              ,COALESCE(SMY_LOAN_AR.AR_LCS_TP_ID          ,-1)             AS LN_LCS_STS_TP_ID       --贷款生命周期
              ,COALESCE(SMY_LOAN_AR.CNRL_BNK_IDY_CL_ID    ,-1)             AS IDY_CL_ID              --行业代码
              ,COALESCE(SMY_LOAN_AR.LN_FR_RSLT_TP_ID      ,-1)             AS LN_FIVE_RTG_STS        --四/五级形态分类
              ,COALESCE(SMY_LOAN_AR.AR_FNC_ST_TP_ID       ,-1)             AS LN_FNC_STS_TP_ID       --贷款四级形态类型
              ,COALESCE(SMY_CST_INF.CST_CR_RSK_RTG_ID ,-1)     AS CST_CR_RSK_RTG_ID      --客户资信等级
              ,COALESCE(SMY_LOAN_AR.PD_GRP_CODE  ,' ')                       AS PD_GRP_CD              --产品组代码
              ,COALESCE(SMY_LOAN_AR.PD_SUB_CODE  ,' ')                       AS PD_SUB_CD              --产品字代码
              ,COALESCE(SMY_CST_INF.ORG_SCALE_TP_ID,-1)       AS CST_SCALE_TP_ID        --企业规模类型
              ,SMY_LOAN_AR.DNMN_CCY_ID                      AS CCY                    --币种
              ,COALESCE(SMY_LOAN_AR.PD_UN_CODE ,' ')       AS PD_UN_CODE             --产品代码
              ,SMY_LOAN_AR.LN_BAL                        AS LN_BAL                 --贷款余额
              ,SMY_LOAN_AR.PRIM_CST_ID               AS NBR_CST           --客户数量
              ,case when SMY_LOAN_AR.AR_LCS_TP_ID = 13360003 then 1 else 0 end AS NBR_AC            --账户数
              ,CASE WHEN SMY_LOAN_AR.LN_DRDWN_DT='1900-01-01' THEN SMY_LOAN_AR.LN_AR_ID ELSE '0' END AS NBR_NEW_AC --新增账户数
              ,CASE WHEN SMY_CST_INF.EFF_CST_DT='1900-01-01' THEN  COALESCE(SMY_CST_INF.CST_ID,'0') ELSE '0' END AS NBR_NEW_CST   --新增客户数
              ,CASE WHEN SMY_LOAN_AR.END_DT='1900-01-01' AND SMY_LOAN_AR.AR_LCS_TP_ID=13360005 THEN 1 ELSE 0 END AS NBR_AC_CLS --当天销户账户数13360005:销户
              ,COALESCE(SMY_LN_AR_INT_MTHLY.YTD_ON_BST_INT_AMT_RCVD  ,0)  AS YTD_ON_BST_INT_AMT_RCVD --表内实收利息            
              ,COALESCE(SMY_LN_AR_INT_MTHLY.YTD_OFF_BST_INT_AMT_RCVD ,0)  AS YTD_OFF_BST_INT_AMT_RCVD        --表外实收利息                       
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_AMT_OF_INT_INCM  ,0)  AS TOT_YTD_AMT_OF_INT_INCM        --利息收入            
              ,COALESCE(SMY_LN_AR_INT_MTHLY.ON_BST_INT_RCVB          ,0)  AS ON_BST_INT_RCVB             --表内应收未收利息        
              ,COALESCE(SMY_LN_AR_INT_MTHLY.OFF_BST_INT_RCVB         ,0)  AS OFF_BST_INT_RCVB            --表外应收未收利息
              ,CASE WHEN COALESCE(SMY_LN_AR_INT_MTHLY.LN_DRDWN_AMT,0) > 0 THEN 1 ELSE 0 END AS NBR_LN_DRDWN_AC        --当日累放账户数
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_MTD_LN_DRDWN_AMT ,0)            AS TOT_MTD_LN_DRDWN_AMT           --月贷款累计发放金额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_MTD_AMT_LN_REPYMT_RCVD ,0)      AS TOT_MTD_AMT_LN_REPYMT_RCVD     --月累计收回贷款金额
              ,CASE WHEN COALESCE(SMY_LN_AR_INT_MTHLY.AMT_LN_REPYMT_RCVD,0) > 0 THEN 1 ELSE 0 END   AS NBR_LN_REPYMT_RCVD_AC  --当日累收账户数                     
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_QTD_LN_DRDWN_AMT          ,0)  AS TOT_QTD_LN_DRDWN_AMT           --季度贷款累计发放金额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_QTD_AMT_LN_RPYMT_RCVD     ,0)  AS TOT_QTD_AMT_LN_RPYMT_RCVD      --季度累计收回贷款金额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_LN_DRDWN_AMT          ,0)  AS TOT_YTD_LN_DRDWN_AMT           --年度贷款累计发放金额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_AMT_LN_REPYMT_RCVD    ,0)  AS TOT_YTD_AMT_LN_REPYMT_RCVD     --年度累计收回贷款金额                  
              ,COALESCE(SMY_LN_AR_INT_MTHLY.CUR_CR_AMT                    ,0)  AS CUR_CR_AMT    			--贷方发生额      
              ,COALESCE(SMY_LN_AR_INT_MTHLY.CUR_DB_AMT                    ,0)  AS CUR_DB_AMT         --借方发生额      
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_MTD_CR_AMT                ,0)  AS TOT_MTD_CR_AMT     --月累计贷方发生额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_MTD_DB_AMT                ,0)  AS TOT_MTD_DB_AMT     --月累计借方发生额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_QTD_DB_AMT                ,0)  AS TOT_QTD_DB_AMT     --季累计贷方发生额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_QTD_CR_AMT                ,0)  AS TOT_QTD_CR_AMT     --季累计借方发生额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_CR_AMT                ,0)  AS TOT_YTD_CR_AMT     --年累计贷方发生额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_DB_AMT                ,0)  AS TOT_YTD_DB_AMT     --年累计借方发生额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.OFF_BST_INT_RCVB_WRTOF        ,0) AS  OFF_BST_INT_RCVB_WRTOF           --表外应收利息核销金额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.OFF_BST_INT_RCVB_RPLC	        ,0) AS  OFF_BST_INT_RCVB_RPLC	           --表外应收利息置换金额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_INT_INCM_AMT_DEBT_AST	,0) AS  TOT_YTD_INT_INCM_AMT_DEBT_AST		 --抵债资产抵债利息收入
					    ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_INT_INCM_RTND_WRTOF_LN,0) AS  TOT_YTD_INT_INCM_RTND_WRTOF_LN   --核销贷款收回利息
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_MTD_NBR_LN_DRDWNTXN       ,0) AS  TOT_MTD_NBR_LN_DRDWNTXN     --月累计发放贷款笔数
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_QTD_NBR_LN_DRDWN_TXN      ,0) AS  TOT_QTD_NBR_LN_DRDWN_TXN    --季累计发放贷款笔数
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_NBR_LN_DRDWN_TXN      ,0) AS  TOT_YTD_NBR_LN_DRDWN_TXN    --年累计发放贷款笔数
              ,COALESCE(SMY_LN_AR_INT_MTHLY.AMT_LN_REPYMT_RCVD  					,0) AS  AMT_LN_REPYMT_RCVD  			               
							,VALUE(SMY_CST_INF.Area_LVL1_TP_Id, -1)             AS CST_Area_LVL1_TP_Id   --客户区域类型1
							,VALUE(SMY_CST_INF.Area_LVL2_TP_Id, -1)             AS CST_Area_LVL2_TP_Id   --客户区域类型2
							,VALUE(SMY_CST_INF.Area_LVL3_TP_Id, -1)             AS CST_Area_LVL3_TP_Id   --客户区域类型3
		          ,SMY_LOAN_AR.ENT_IDV_IND  									AS ENT_IDV_IND	 --个人企业标志
		          ,SMY_LOAN_AR.LN_INVST_DIRC_TP_ID AS LN_INVST_DIRC_TP_ID               --行业投向
		          ,case when SMY_LOAN_AR.AR_LCS_TP_ID = 13360003 then 1 else 0 end AS CUR_AR_FLAG --账户是否正常
		          ,COALESCE(SMY_LN_AR_INT_MTHLY.LN_DRDWN_AMT  					,0)   AS LN_DRDWN_AMT
							,COALESCE(SMY_LN_AR_INT_MTHLY.MTD_ACML_BAL_AMT  ,0) AS MTD_ACML_BAL_AMT
							,COALESCE(SMY_LN_AR_INT_MTHLY.QTD_ACML_BAL_AMT  ,0) AS QTD_ACML_BAL_AMT
							,COALESCE(SMY_LN_AR_INT_MTHLY.YTD_ACML_BAL_AMT  ,0) AS YTD_ACML_BAL_AMT
		          ,CASE WHEN year(SMY_CST_INF.EFF_CST_DT)=2011 and month(SMY_CST_INF.EFF_CST_DT)= 5
		                    THEN  COALESCE(SMY_CST_INF.CST_ID,'0') 
		                    ELSE '0' END AS TOT_MTD_NBR_NEW_CST                  --当月累计新增客户数 
		          ,CASE WHEN year(SMY_LOAN_AR.END_DT)=2011 and month(SMY_LOAN_AR.END_DT) = 5  
		                          AND 
		                          SMY_LOAN_AR.AR_LCS_TP_ID= 13360005 
		                          THEN 1 ELSE 0 END AS TOT_MTD_NBR_AC_CLS                   --当月累计销户账户数 
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_MTD_LN_DRDWN_AMT > 0 
		                         THEN 1 ELSE 0 END AS TOT_MTD_NBR_LN_DRDWN_AC              --月度累放账户数   
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_MTD_AMT_LN_REPYMT_RCVD > 0 
		                         THEN 1 ELSE 0 END AS TOT_MTD_NBR_LN_REPYMT_RCVD_AC        --月度累收账户数 
		          ,CASE WHEN year(SMY_LOAN_AR.LN_DRDWN_DT)=2011 and month(SMY_LOAN_AR.LN_DRDWN_DT) = 5 
		                         THEN 1 ELSE 0 END AS TOT_MTD_NBR_NEW_AC                   --月度新增账户数 
		          ,CASE WHEN year(SMY_CST_INF.EFF_CST_DT)=2011 and quarter(SMY_CST_INF.EFF_CST_DT)= 2
		                    THEN  COALESCE(SMY_CST_INF.CST_ID,'0') 
		                    ELSE '0' END AS TOT_QTD_NBR_NEW_CST                  --当季累计新增客户数 
		          ,CASE WHEN year(SMY_LOAN_AR.END_DT)=5 and quarter(SMY_LOAN_AR.END_DT) = 2  
		                          AND 
		                          SMY_LOAN_AR.AR_LCS_TP_ID= 13360005 
		                          THEN 1 ELSE 0 END AS TOT_QTD_NBR_AC_CLS                   --当季累计销户账户数
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_QTD_LN_DRDWN_AMT > 0 
		                         THEN 1 ELSE 0 END AS TOT_QTD_NBR_LN_DRDWN_AC              --季度累放账户数  
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_QTD_AMT_LN_RPYMT_RCVD > 0 
		                         THEN 1 ELSE 0 END AS TOT_QTD_NBR_LN_REPYMT_RCVD_AC        --季度累收账户数  
		          ,CASE WHEN year(SMY_LOAN_AR.LN_DRDWN_DT)=2011 and quarter(SMY_LOAN_AR.LN_DRDWN_DT) = 2 
		                         THEN 1 ELSE 0 END AS TOT_QTD_NBR_NEW_AC                   --季度新增账户数   
		          ,CASE WHEN year(SMY_CST_INF.EFF_CST_DT)= 2011
		                    THEN  COALESCE(SMY_CST_INF.CST_ID,'0') 
		                    ELSE '0' END AS TOT_YTD_NBR_NEW_CST                  --当年累计新增客户数   
		          ,CASE WHEN year(SMY_LOAN_AR.END_DT) = 2011  
		                          AND 
		                          SMY_LOAN_AR.AR_LCS_TP_ID= 13360005 
		                          THEN 1 ELSE 0 END AS TOT_YTD_NBR_AC_CLS                   --当年累计销户账户数  
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_YTD_LN_DRDWN_AMT > 0 
		                         THEN 1 ELSE 0 END AS TOT_YTD_NBR_LN_DRDWN_AC              --年度累放账户数
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_YTD_AMT_LN_REPYMT_RCVD > 0 
		                         THEN 1 ELSE 0 END AS TOT_YTD_NBR_LN_REPYMT_RCVD_AC        --年度累收账户数       
		          ,CASE WHEN year(SMY_LOAN_AR.LN_DRDWN_DT) = 2011 
		                         THEN 1 ELSE 0 END AS TOT_YTD_NBR_NEW_AC                   --年度新增账户数 							
        FROM SMY.LOAN_AR_SMY AS SMY_LOAN_AR
            INNER JOIN SESSION.TMP_LN_AR_INT_MTHLY_SMY AS SMY_LN_AR_INT_MTHLY ON SMY_LOAN_AR.CTR_AR_ID = SMY_LN_AR_INT_MTHLY.CTR_AR_ID AND SMY_LOAN_AR.CTR_ITM_ORDR_ID = SMY_LN_AR_INT_MTHLY.CTR_ITM_ORDR_ID
        		LEFT OUTER JOIN SMY.CST_INF AS SMY_CST_INF ON SMY_LOAN_AR.PRIM_CST_ID = SMY_CST_INF.CST_ID
    )DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(ACG_OU_IP_ID,ACG_SBJ_ID);
		
		INSERT INTO SESSION.T_CUR_TMP
		SELECT                                                                                                         																					
               SMY_LOAN_AR.RPRG_OU_IP_ID                       AS ACG_OU_IP_ID     --核算机构
              ,SMY_LOAN_AR.ACG_SBJ_ID                          AS ACG_SBJ_ID       --存款科目(核算码)
              ,VALUE(SMY_LOAN_AR.NEW_ACG_SBJ_ID,'')        							 AS NEW_ACG_SBJ_ID   --新科目
              ,SMY_LOAN_AR.LN_CGY_TP_ID                        AS LN_CTR_TP_ID     --业务品种
              ,SMY_LOAN_AR.CLT_TP_ID                           AS LN_GNT_TP_ID     --贷款担保方式
              ,SMY_LOAN_AR.LN_PPS_TP_ID                        AS LN_PPS_TP_ID     --贷款用途类型
              ,SMY_LOAN_AR.FND_SRC_DST_TP_ID                   AS FND_SRC_TP_ID    --资金来源
              ,SMY_LOAN_AR.LN_TERM_TP_ID                       AS LN_TERM_TP_ID     --贷款期限类型
              ,COALESCE(SMY_LOAN_AR.TM_MAT_SEG_ID,-1)                      AS TM_MAT_SEG_ID   --到期期限类型
              ,COALESCE(SMY_LOAN_AR.ALT_TP_ID  ,-1)         AS LN_CST_TP_ID           --贷款客户类型
              ,COALESCE(SMY_CST_INF.FARMER_TP_ID ,-1)         AS Farmer_TP_Id           --农户类别
              ,COALESCE(SMY_LOAN_AR.AR_LCS_TP_ID          ,-1)             AS LN_LCS_STS_TP_ID       --贷款生命周期
              ,COALESCE(SMY_LOAN_AR.CNRL_BNK_IDY_CL_ID    ,-1)             AS IDY_CL_ID              --行业代码
              ,COALESCE(SMY_LOAN_AR.LN_FR_RSLT_TP_ID      ,-1)             AS LN_FIVE_RTG_STS        --四/五级形态分类
              ,COALESCE(SMY_LOAN_AR.AR_FNC_ST_TP_ID       ,-1)             AS LN_FNC_STS_TP_ID       --贷款四级形态类型
              ,COALESCE(SMY_CST_INF.CST_CR_RSK_RTG_ID ,-1)     AS CST_CR_RSK_RTG_ID      --客户资信等级
              ,COALESCE(SMY_LOAN_AR.PD_GRP_CODE  ,' ')                       AS PD_GRP_CD              --产品组代码
              ,COALESCE(SMY_LOAN_AR.PD_SUB_CODE  ,' ')                       AS PD_SUB_CD              --产品字代码
              ,COALESCE(SMY_CST_INF.ORG_SCALE_TP_ID,-1)       AS CST_SCALE_TP_ID        --企业规模类型
              ,SMY_LOAN_AR.DNMN_CCY_ID                      AS CCY                    --币种
              ,COALESCE(SMY_LOAN_AR.PD_UN_CODE  ,' ')        AS PD_UN_CODE             --产品代码
              ,SMY_LOAN_AR.LN_BAL                        AS LN_BAL                 --贷款余额
              ,SMY_LOAN_AR.PRIM_CST_ID               AS NBR_CST           --客户数量
              ,case when SMY_LOAN_AR.AR_LCS_TP_ID = 13360003 then 1 else 0 end AS NBR_AC            --账户数
              ,CASE WHEN SMY_LOAN_AR.LN_DRDWN_DT=ACCOUNTING_DATE THEN SMY_LOAN_AR.LN_AR_ID ELSE '0' END --新增账户数
              ,CASE WHEN SMY_CST_INF.EFF_CST_DT=ACCOUNTING_DATE THEN COALESCE(SMY_CST_INF.CST_ID,'0') ELSE '0' END --新增客户数
              ,CASE WHEN SMY_LOAN_AR.END_DT=ACCOUNTING_DATE AND SMY_LOAN_AR.AR_LCS_TP_ID=13360005 THEN 1 ELSE 0 END --当天销户账户数13360005:销户
              ,COALESCE(SMY_LN_AR_INT_MTHLY.YTD_ON_BST_INT_AMT_RCVD  ,0)  AS YTD_ON_BST_INT_AMT_RCVD --表内实收利息            
              ,COALESCE(SMY_LN_AR_INT_MTHLY.YTD_OFF_BST_INT_AMT_RCVD ,0)  AS YTD_OFF_BST_INT_AMT_RCVD        --表外实收利息                       
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_AMT_OF_INT_INCM  ,0)  AS TOT_YTD_AMT_OF_INT_INCM        --利息收入            
              ,COALESCE(SMY_LN_AR_INT_MTHLY.ON_BST_INT_RCVB          ,0)  AS ON_BST_INT_RCVB             --表内应收未收利息        
              ,COALESCE(SMY_LN_AR_INT_MTHLY.OFF_BST_INT_RCVB         ,0)  AS OFF_BST_INT_RCVB            --表外应收未收利息
              ,CASE WHEN COALESCE(SMY_LN_AR_INT_MTHLY.LN_DRDWN_AMT,0) > 0 THEN 1 ELSE 0 END   AS NBR_LN_DRDWN_AC        --当日累放账户数
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_MTD_LN_DRDWN_AMT ,0)            AS TOT_MTD_LN_DRDWN_AMT           --月贷款累计发放金额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_MTD_AMT_LN_REPYMT_RCVD ,0)      AS TOT_MTD_AMT_LN_REPYMT_RCVD     --月累计收回贷款金额
              ,CASE WHEN COALESCE(SMY_LN_AR_INT_MTHLY.AMT_LN_REPYMT_RCVD,0) > 0 THEN 1 ELSE 0 END   AS NBR_LN_REPYMT_RCVD_AC  --当日累收账户数                     
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_QTD_LN_DRDWN_AMT          ,0)  AS TOT_QTD_LN_DRDWN_AMT           --季度贷款累计发放金额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_QTD_AMT_LN_RPYMT_RCVD     ,0)  AS TOT_QTD_AMT_LN_RPYMT_RCVD      --季度累计收回贷款金额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_LN_DRDWN_AMT          ,0)  AS TOT_YTD_LN_DRDWN_AMT           --年度贷款累计发放金额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_AMT_LN_REPYMT_RCVD    ,0)  AS TOT_YTD_AMT_LN_REPYMT_RCVD     --年度累计收回贷款金额                  
              ,COALESCE(SMY_LN_AR_INT_MTHLY.CUR_CR_AMT                    ,0)  AS CUR_CR_AMT    			--贷方发生额      
              ,COALESCE(SMY_LN_AR_INT_MTHLY.CUR_DB_AMT                    ,0)  AS CUR_DB_AMT         --借方发生额      
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_MTD_CR_AMT                ,0)  AS TOT_MTD_CR_AMT     --月累计贷方发生额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_MTD_DB_AMT                ,0)  AS TOT_MTD_DB_AMT     --月累计借方发生额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_QTD_DB_AMT                ,0)  AS TOT_QTD_DB_AMT     --季累计贷方发生额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_QTD_CR_AMT                ,0)  AS TOT_QTD_CR_AMT     --季累计借方发生额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_CR_AMT                ,0)  AS TOT_YTD_CR_AMT     --年累计贷方发生额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_DB_AMT                ,0)  AS TOT_YTD_DB_AMT     --年累计借方发生额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.OFF_BST_INT_RCVB_WRTOF        ,0) AS  OFF_BST_INT_RCVB_WRTOF           --表外应收利息核销金额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.OFF_BST_INT_RCVB_RPLC	        ,0) AS  OFF_BST_INT_RCVB_RPLC	           --表外应收利息置换金额
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_INT_INCM_AMT_DEBT_AST	,0) AS  TOT_YTD_INT_INCM_AMT_DEBT_AST		 --抵债资产抵债利息收入
					    ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_INT_INCM_RTND_WRTOF_LN,0)	 AS  TOT_YTD_INT_INCM_RTND_WRTOF_LN   --核销贷款收回利息
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_MTD_NBR_LN_DRDWNTXN       ,0)  AS  TOT_MTD_NBR_LN_DRDWNTXN     --月累计发放贷款笔数
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_QTD_NBR_LN_DRDWN_TXN      ,0)  AS  TOT_QTD_NBR_LN_DRDWN_TXN    --季累计发放贷款笔数
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_NBR_LN_DRDWN_TXN      ,0)  AS  TOT_YTD_NBR_LN_DRDWN_TXN    --年累计发放贷款笔数
              ,COALESCE(SMY_LN_AR_INT_MTHLY.AMT_LN_REPYMT_RCVD  					,0)  AS  AMT_LN_REPYMT_RCVD  			               
							,VALUE(SMY_CST_INF.Area_LVL1_TP_Id, -1)             AS CST_Area_LVL1_TP_Id   --客户区域类型1
							,VALUE(SMY_CST_INF.Area_LVL2_TP_Id, -1)             AS CST_Area_LVL2_TP_Id   --客户区域类型2
							,VALUE(SMY_CST_INF.Area_LVL3_TP_Id, -1)             AS CST_Area_LVL3_TP_Id   --客户区域类型3
		          ,SMY_LOAN_AR.ENT_IDV_IND  									AS ENT_IDV_IND	 --个人企业标志
		          ,SMY_LOAN_AR.LN_INVST_DIRC_TP_ID               --行业投向
		          ,case when SMY_LOAN_AR.AR_LCS_TP_ID = 13360003 then 1 else 0 end --账户是否正常
		          ,COALESCE(SMY_LN_AR_INT_MTHLY.LN_DRDWN_AMT,0)   AS LN_DRDWN_AMT
							,COALESCE(SMY_LN_AR_INT_MTHLY.MTD_ACML_BAL_AMT  ,0) AS MTD_ACML_BAL_AMT
							,COALESCE(SMY_LN_AR_INT_MTHLY.QTD_ACML_BAL_AMT  ,0) AS QTD_ACML_BAL_AMT
							,COALESCE(SMY_LN_AR_INT_MTHLY.YTD_ACML_BAL_AMT  ,0) AS YTD_ACML_BAL_AMT
		          ,CASE WHEN year(SMY_CST_INF.EFF_CST_DT)=cur_year and month(SMY_CST_INF.EFF_CST_DT)= cur_month
		                    THEN  COALESCE(SMY_CST_INF.CST_ID,'0') ELSE '0' END --当月累计新增客户数 
		          ,CASE WHEN year(SMY_LOAN_AR.END_DT)=cur_year and month(SMY_LOAN_AR.END_DT) = cur_month AND SMY_LOAN_AR.AR_LCS_TP_ID= 13360005 
		                    THEN 1 ELSE 0 END --当月累计销户账户数 
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_MTD_LN_DRDWN_AMT > 0 
		                    THEN 1 ELSE 0 END --月度累放账户数   
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_MTD_AMT_LN_REPYMT_RCVD > 0 
		                    THEN 1 ELSE 0 END --月度累收账户数 
		          ,CASE WHEN year(SMY_LOAN_AR.LN_DRDWN_DT)=cur_year and month(SMY_LOAN_AR.LN_DRDWN_DT) = cur_month 
		                    THEN 1 ELSE 0 END --月度新增账户数 
		          ,CASE WHEN year(SMY_CST_INF.EFF_CST_DT)=cur_year and quarter(SMY_CST_INF.EFF_CST_DT)= cur_qtr
		                    THEN  COALESCE(SMY_CST_INF.CST_ID,'0') ELSE '0' END --当季累计新增客户数 
		          ,CASE WHEN year(SMY_LOAN_AR.END_DT)=cur_year and quarter(SMY_LOAN_AR.END_DT) = cur_qtr AND SMY_LOAN_AR.AR_LCS_TP_ID= 13360005 
		                    THEN 1 ELSE 0 END --当季累计销户账户数
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_QTD_LN_DRDWN_AMT > 0 
		                    THEN 1 ELSE 0 END --季度累放账户数
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_QTD_AMT_LN_RPYMT_RCVD > 0 
		                    THEN 1 ELSE 0 END --季度累收账户数
		          ,CASE WHEN year(SMY_LOAN_AR.LN_DRDWN_DT)=cur_year and quarter(SMY_LOAN_AR.LN_DRDWN_DT) = cur_qtr 
		                    THEN 1 ELSE 0 END --季度新增账户数
		          ,CASE WHEN year(SMY_CST_INF.EFF_CST_DT)= cur_year
		                    THEN  COALESCE(SMY_CST_INF.CST_ID,'0') ELSE '0' END --当年累计新增客户数
		          ,CASE WHEN year(SMY_LOAN_AR.END_DT) = cur_year AND SMY_LOAN_AR.AR_LCS_TP_ID= 13360005 
		                    THEN 1 ELSE 0 END --当年累计销户账户数	 
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_YTD_LN_DRDWN_AMT > 0 
		                    THEN 1 ELSE 0 END --年度累放账户数    	             
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_YTD_AMT_LN_REPYMT_RCVD > 0 THEN 1 ELSE 0 END --年度累收账户数
		          ,CASE WHEN year(SMY_LOAN_AR.LN_DRDWN_DT) = cur_year THEN 1 ELSE 0 END --年度新增账户数
        FROM SMY.LOAN_AR_SMY AS SMY_LOAN_AR 
        INNER JOIN SESSION.TMP_LN_AR_INT_MTHLY_SMY AS SMY_LN_AR_INT_MTHLY ON SMY_LOAN_AR.CTR_AR_ID = SMY_LN_AR_INT_MTHLY.CTR_AR_ID AND SMY_LOAN_AR.CTR_ITM_ORDR_ID = SMY_LN_AR_INT_MTHLY.CTR_ITM_ORDR_ID 
        LEFT OUTER JOIN SMY.CST_INF AS SMY_CST_INF ON SMY_LOAN_AR.PRIM_CST_ID = SMY_CST_INF.CST_ID
    ;
    
    CREATE INDEX SESSION.T_CUR_TMP_GB ON SESSION.T_CUR_TMP(ACG_OU_IP_ID,ACG_SBJ_ID,NEW_ACG_SBJ_ID,LN_CTR_TP_ID,LN_GNT_TP_ID,LN_PPS_TP_ID,FND_SRC_TP_ID,LN_TERM_TP_ID,TM_MAT_SEG_ID,LN_CST_TP_ID,Farmer_TP_Id,LN_LCS_STS_TP_ID,IDY_CL_ID,LN_FIVE_RTG_STS,LN_FNC_STS_TP_ID,CST_CR_RSK_RTG_ID,PD_GRP_CD,PD_SUB_CD,CST_SCALE_TP_ID,CCY,CST_Area_LVL1_TP_Id,CST_Area_LVL2_TP_Id,CST_Area_LVL3_TP_Id,ENT_IDV_IND,LN_INVST_DIRC_TP_ID,PD_UN_CODE);
		
		INSERT INTO SESSION.T_CUR 
					SELECT                                                                                                         																					
               ACG_OU_IP_ID                              --核算机构
              ,ACG_SBJ_ID                                --存款科目(核算码)
              ,NEW_ACG_SBJ_ID                            --新科目
              ,LN_CTR_TP_ID                              --业务品种
              ,LN_GNT_TP_ID                              --贷款担保方式
              ,LN_PPS_TP_ID                              --贷款用途类型
              ,FND_SRC_TP_ID                             --资金来源
              ,LN_TERM_TP_ID                             --贷款期限类型
              ,TM_MAT_SEG_ID                             --到期期限类型
              ,LN_CST_TP_ID                              --贷款客户类型
              ,Farmer_TP_Id                              --农户类别
              ,LN_LCS_STS_TP_ID                          --贷款生命周期
              ,IDY_CL_ID                                 --行业代码
              ,LN_FIVE_RTG_STS                           --四/五级形态分类
              ,LN_FNC_STS_TP_ID                          --贷款四级形态类型
              ,CST_CR_RSK_RTG_ID                         --客户资信等级
              ,PD_GRP_CD                                 --产品组代码
              ,PD_SUB_CD                                 --产品字代码
              ,CST_SCALE_TP_ID                           --企业规模类型
              ,CCY                                       --币种
              ,PD_UN_CODE                                --产品代码
              ,SUM(LN_BAL)                               --贷款余额
              ,COUNT(distinct NBR_CST)                   --客户数量
              ,SUM(NBR_AC)                               --账户数
              ,COUNT(distinct NBR_NEW_AC) -1             --新增账户数
              ,COUNT(distinct NBR_NEW_CST) -1            --新增客户数
              ,SUM(NBR_AC_CLS)                           --当天销户账户数  13360005:销户
              ,SUM(YTD_ON_BST_INT_AMT_RCVD)              --表内实收利息            
              ,SUM(YTD_OFF_BST_INT_AMT_RCVD)             --表外实收利息                       
              ,SUM(TOT_YTD_AMT_OF_INT_INCM)              --利息收入            
              ,SUM(ON_BST_INT_RCVB)                      --表内应收未收利息        
              ,SUM(OFF_BST_INT_RCVB)                     --表外应收未收利息
              ,SUM(NBR_LN_DRDWN_AC)                      --当日累放账户数
              ,SUM(TOT_MTD_LN_DRDWN_AMT)                 --月贷款累计发放金额
              ,SUM(TOT_MTD_AMT_LN_REPYMT_RCVD)           --月累计收回贷款金额
              ,SUM(NBR_LN_REPYMT_RCVD_AC)                --当日累收账户数                     
              ,SUM(TOT_QTD_LN_DRDWN_AMT)                 --季度贷款累计发放金额
              ,SUM(TOT_QTD_AMT_LN_RPYMT_RCVD)            --季度累计收回贷款金额
              ,SUM(TOT_YTD_LN_DRDWN_AMT)                 --年度贷款累计发放金额
              ,SUM(TOT_YTD_AMT_LN_REPYMT_RCVD)           --年度累计收回贷款金额                  
              ,SUM(CUR_CR_AMT)     			                 --贷方发生额      
              ,SUM(CUR_DB_AMT)                           --借方发生额      
              ,SUM(TOT_MTD_CR_AMT)                       --月累计贷方发生额
              ,SUM(TOT_MTD_DB_AMT)                       --月累计借方发生额
              ,SUM(TOT_QTD_DB_AMT)                       --季累计贷方发生额
              ,SUM(TOT_QTD_CR_AMT)                       --季累计借方发生额
              ,SUM(TOT_YTD_CR_AMT)                       --年累计贷方发生额
              ,SUM(TOT_YTD_DB_AMT)                       --年累计借方发生额
              ,SUM(OFF_BST_INT_RCVB_WRTOF)               --表外应收利息核销金额
              ,SUM(OFF_BST_INT_RCVB_RPLC) 	             --表外应收利息置换金额
              ,SUM(TOT_YTD_INT_INCM_AMT_DEBT_AST) 		   --抵债资产抵债利息收入
					    ,SUM(TOT_YTD_INT_INCM_RTND_WRTOF_LN)       --核销贷款收回利息
              ,SUM(TOT_MTD_NBR_LN_DRDWNTXN)              --月累计发放贷款笔数
              ,SUM(TOT_QTD_NBR_LN_DRDWN_TXN)             --季累计发放贷款笔数
              ,SUM(TOT_YTD_NBR_LN_DRDWN_TXN)             --年累计发放贷款笔数
              ,SUM(AMT_LN_REPYMT_RCVD) 
							,CST_Area_LVL1_TP_Id                       --客户区域类型1
							,CST_Area_LVL2_TP_Id                       --客户区域类型2
							,CST_Area_LVL3_TP_Id                       --客户区域类型3
		          ,ENT_IDV_IND	                             --个人企业标志
		          ,LN_INVST_DIRC_TP_ID                       --行业投向
		          ,SUM(CUR_AR_FLAG)                          --账户是否正常
		          ,SUM(LN_DRDWN_AMT)
							,SUM(MTD_ACML_BAL_AMT)
							,SUM(QTD_ACML_BAL_AMT)
							,SUM(YTD_ACML_BAL_AMT)
		          ,COUNT(distinct TOT_MTD_NBR_NEW_CST) -1    --当月累计新增客户数 
		          ,SUM(TOT_MTD_NBR_AC_CLS)                   --当月累计销户账户数 
		          ,SUM(TOT_MTD_NBR_LN_DRDWN_AC)              --月度累放账户数   
		          ,SUM(TOT_MTD_NBR_LN_REPYMT_RCVD_AC)        --月度累收账户数 
		          ,SUM(TOT_MTD_NBR_NEW_AC)                   --月度新增账户数 
		          ,COUNT(distinct TOT_QTD_NBR_NEW_CST) -1    --当季累计新增客户数 
		          ,SUM(TOT_QTD_NBR_AC_CLS)                   --当季累计销户账户数
		          ,SUM(TOT_QTD_NBR_LN_DRDWN_AC)              --季度累放账户数  
		          ,SUM(TOT_QTD_NBR_LN_REPYMT_RCVD_AC)        --季度累收账户数  
		          ,SUM(TOT_QTD_NBR_NEW_AC)                   --季度新增账户数   
		          ,COUNT(distinct TOT_YTD_NBR_NEW_CST) -1    --当年累计新增客户数   
		          ,SUM(TOT_YTD_NBR_AC_CLS)                   --当年累计销户账户数
		          ,SUM(TOT_YTD_NBR_LN_DRDWN_AC)              --年度累放账户数
		          ,SUM(TOT_YTD_NBR_LN_REPYMT_RCVD_AC)        --年度累收账户数
		          ,SUM(TOT_YTD_NBR_NEW_AC)                   --年度新增账户数
        FROM SESSION.T_CUR_TMP
        GROUP BY 
               ACG_OU_IP_ID
              ,ACG_SBJ_ID
              ,NEW_ACG_SBJ_ID
              ,LN_CTR_TP_ID
              ,LN_GNT_TP_ID
              ,LN_PPS_TP_ID
              ,FND_SRC_TP_ID
              ,LN_TERM_TP_ID
              ,TM_MAT_SEG_ID
              ,LN_CST_TP_ID
              ,Farmer_TP_Id
              ,LN_LCS_STS_TP_ID
              ,IDY_CL_ID
              ,LN_FIVE_RTG_STS
              ,LN_FNC_STS_TP_ID
              ,CST_CR_RSK_RTG_ID
              ,PD_GRP_CD
              ,PD_SUB_CD
              ,CST_SCALE_TP_ID
              ,CCY
              ,CST_Area_LVL1_TP_Id
              ,CST_Area_LVL2_TP_Id
              ,CST_Area_LVL3_TP_Id
              ,ENT_IDV_IND
              ,LN_INVST_DIRC_TP_ID
              ,PD_UN_CODE
;

 /** 收集操作信息 */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --3
	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	    --

CREATE INDEX SESSION.TMP_T_CUR ON SESSION.T_CUR(ACG_OU_IP_ID,ACG_SBJ_ID,LN_CTR_TP_ID,LN_GNT_TP_ID,LN_PPS_TP_ID);

		SET SMY_STEPDESC = '往表SMY.OU_LN_DLY_SMY 中插入会计日期为当天的数据';     --
		
	INSERT INTO SMY.OU_LN_DLY_SMY
          (
           ACG_OU_IP_ID
          ,ACG_SBJ_ID
          ,LN_CTR_TP_ID
          ,LN_GNT_TP_ID
          ,LN_PPS_TP_ID
          ,FND_SRC_TP_ID
          ,LN_TERM_TP_ID
          ,TM_MAT_SEG_ID
          ,LN_CST_TP_ID
          ,FARMER_TP_ID
          ,LN_LCS_STS_TP_ID
          ,IDY_CL_ID
          ,LN_FIVE_RTG_STS
          ,LN_FNC_STS_TP_ID
          ,CST_CR_RSK_RTG_ID
          ,PD_GRP_CD
          ,PD_SUB_CD
          ,CST_SCALE_TP_ID
          ,CCY
          ,ACG_DT
          ,PD_UN_CODE
          ,CDR_YR
          ,CDR_MTH
          ,NOD_In_MTH
          ,NOD_IN_QTR
          ,NOD_IN_YEAR
          ,LN_BAL
          ,LST_DAY_LN_BAL
          ,NBR_CST
          ,NBR_AC
          ,NBR_NEW_AC
          ,NBR_NEW_CST
          ,NBR_AC_CLS
          ,YTD_ON_BST_INT_AMT_RCVD
          ,YTD_OFF_BST_INT_AMT_RCVD
          ,TOT_YTD_AMT_OF_INT_INCM
          ,ON_BST_INT_RCVB
          ,OFF_BST_INT_RCVB
          ,TOT_MTD_NBR_NEW_CST
          ,TOT_MTD_NBR_AC_CLS
          ,MTD_ACML_BAL_AMT
          ,TOT_MTD_NBR_LN_DRDWN_AC
          ,TOT_MTD_LN_DRDWN_AMT
          ,TOT_MTD_AMT_LN_REPYMT_RCVD
          ,TOT_MTD_NBR_LN_REPYMT_RCVD_AC
          ,TOT_QTD_NBR_NEW_CST
          ,TOT_QTD_NBR_AC_CLS
          ,QTD_ACML_BAL_AMT
          ,TOT_QTD_NBR_LN_DRDWN_AC
          ,TOT_QTD_LN_DRDWN_AMT
          ,TOT_QTD_AMT_LN_RPYMT_RCVD
          ,TOT_QTD_NBR_LN_REPYMT_RCVD_AC
          ,TOT_QTD_NBR_NEW_AC
          ,TOT_YTD_NBR_NEW_CST
          ,TOT_YTD_NBR_AC_CLS
          ,YTD_ACML_BAL_AMT
          ,TOT_YTD_NBR_LN_DRDWN_AC
          ,TOT_YTD_LN_DRDWN_AMT
          ,TOT_YTD_AMT_LN_REPYMT_RCVD
          ,TOT_YTD_NBR_LN_REPYMT_RCVD_AC
          ,TOT_YTD_NBR_NEW_AC
          ,TOT_MTD_NBR_NEW_AC
          ,CUR_CR_AMT     
          ,CUR_DB_AMT     
          ,TOT_MTD_CR_AMT 
          ,TOT_MTD_DB_AMT 
          ,TOT_QTD_DB_AMT 
          ,TOT_QTD_CR_AMT 
          ,TOT_YTD_CR_AMT 
          ,TOT_YTD_DB_AMT
          ,OFF_BST_INT_RCVB_WRTOF            --表外应收利息核销金额
          ,OFF_BST_INT_RCVB_RPLC	           --表外应收利息置换金额
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST		 --抵债资产抵债利息收入
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN	   --核销贷款收回利息
          ,TOT_MTD_NBR_LN_DRDWNTXN           --月累计发放贷款笔数
          ,TOT_QTD_NBR_LN_DRDWN_TXN          --季累计发放贷款笔数
          ,TOT_YTD_NBR_LN_DRDWN_TXN          --年累计发放贷款笔数
          ,AMT_LN_REPYMT_RCVD
          ,NEW_ACG_SBJ_ID
 					----------------------Start on 2009-1-30-----------------------------------------------
					,CST_Area_LVL1_TP_Id               --客户区域类型1
					,CST_Area_LVL2_TP_Id               --客户区域类型2
					,CST_Area_LVL3_TP_Id               --客户区域类型3
          ----------------------End on 2009-11-30-----------------------------------------------    
					----------------------Start on 2009-1-30-----------------------------------------------
          ,ENT_IDV_IND  										 --个人企业标志
          ----------------------End on 2009-11-30-----------------------------------------------                          
          ,LN_INVST_DIRC_TP_ID
          ,LN_DRDWN_AMT
--------------------------start on 2010-08-09------------------------------------------------------------------------	
          ,NOCLD_IN_MTH
          ,NOCLD_IN_QTR
          ,NOCLD_IN_YEAR
--------------------------end   on 2010-08-09------------------------------------------------------------------------	
          )            
     
SELECT
           CUR.ACG_OU_IP_ID
          ,CUR.ACG_SBJ_ID
          ,CUR.LN_CTR_TP_ID
          ,CUR.LN_GNT_TP_ID
          ,CUR.LN_PPS_TP_ID
          ,CUR.FND_SRC_TP_ID
          ,CUR.LN_TERM_TP_ID
          ,CUR.TM_MAT_SEG_ID
          ,CUR.LN_CST_TP_ID
          ,CUR.FARMER_TP_ID
          ,CUR.LN_LCS_STS_TP_ID
          ,CUR.IDY_CL_ID
          ,CUR.LN_FIVE_RTG_STS
          ,CUR.LN_FNC_STS_TP_ID
          ,CUR.CST_CR_RSK_RTG_ID
          ,CUR.PD_GRP_CD
          ,CUR.PD_SUB_CD
          ,CUR.CST_SCALE_TP_ID
          ,CUR.CCY
          ,ACCOUNTING_DATE
          ,CUR.PD_UN_CODE
          ,CUR_YEAR
          ,CUR_MONTH
          ,C_MON_DAY AS NOD_IN_MTH
        	,C_QTR_DAY AS NOD_IN_QTR
        	,C_YR_DAY  AS NOD_IN_YEAR
          ,CUR.LN_BAL 
          ,case when PRE.ACG_DT = SMY_DATE then PRE.LN_BAL else 0 end
          ,case when CUR.CUR_AR_FLAG = 0 then 0 else CUR.NBR_CST end
          ,CUR.NBR_AC
          ,CUR.NBR_NEW_AC
          ,CUR.NBR_NEW_CST
          ,CUR.NBR_AC_CLS
          ,CUR.YTD_ON_BST_INT_AMT_RCVD
          ,CUR.YTD_OFF_BST_INT_AMT_RCVD
          ,CUR.TOT_YTD_AMT_OF_INT_INCM
          ,CUR.ON_BST_INT_RCVB
          ,CUR.OFF_BST_INT_RCVB
          ,CUR.TOT_MTD_NBR_NEW_CST             --当月累计新增客户数        
          ,CUR.TOT_MTD_NBR_AC_CLS              --当月累计销户账户数
          ,CUR.MTD_ACML_BAL_AMT		             --月累计余额
          ,CUR.TOT_MTD_NBR_LN_DRDWN_AC         --月度累放账户数                            
          ,CUR.TOT_MTD_LN_DRDWN_AMT            --月贷款累计发放金额
          ,CUR.TOT_MTD_AMT_LN_REPYMT_RCVD      --月累计收回贷款金额          
          ,CUR.TOT_MTD_NBR_LN_REPYMT_RCVD_AC       --月度累收账户数
          ,CUR.TOT_QTD_NBR_NEW_CST          --当季累计新增客户数
          ,CUR.TOT_QTD_NBR_AC_CLS           --当季累计销户账户数
          ,CUR.QTD_ACML_BAL_AMT             --当季累计余额
          ,CUR.TOT_QTD_NBR_LN_DRDWN_AC      --季度累放账户数                            
          ,CUR.TOT_QTD_LN_DRDWN_AMT                                    --季度贷款累计发放金额
          ,CUR.TOT_QTD_AMT_LN_RPYMT_RCVD                               --季度累计收回贷款金额          
          ,CUR.TOT_QTD_NBR_LN_REPYMT_RCVD_AC   --季度累收账户数
          ,CUR.TOT_QTD_NBR_NEW_AC              --季度新增账户数                               
          ,CUR.TOT_YTD_NBR_NEW_CST                  --年度累计新增客户数
          ,CUR.TOT_YTD_NBR_AC_CLS                  --年度累计销户账户数
          ---------------------Start on 20100118-----------------------------
          --,COALESCE(PRE.YTD_ACML_BAL_AMT              ,0) + CUR.LN_BAL              --年度累计余额
          ,CUR.YTD_ACML_BAL_AMT                   --年度累计余额
          ---------------------End on 20100118-----------------------------
          ,CUR.TOT_YTD_NBR_LN_DRDWN_AC        --年度累放账户数
          ,CUR.TOT_YTD_LN_DRDWN_AMT                                   --年度贷款累计发放金额
          ,CUR.TOT_YTD_AMT_LN_REPYMT_RCVD                             --年度累计收回贷款金额
          ,CUR.TOT_YTD_NBR_LN_REPYMT_RCVD_AC    --年度累收账户数
          ,CUR.TOT_YTD_NBR_NEW_AC               --年度新增账户数
          ,CUR.TOT_MTD_NBR_NEW_AC               --月度新增账户数                  
          ,CUR.CUR_CR_AMT            --贷方发生额      
          ,CUR.CUR_DB_AMT            --借方发生额      
          ,CUR.TOT_MTD_CR_AMT        --月累计贷方发生额
          ,CUR.TOT_MTD_DB_AMT        --月累计借方发生额
          ,CUR.TOT_QTD_DB_AMT        --季累计贷方发生额
          ,CUR.TOT_QTD_CR_AMT        --季累计借方发生额
          ,CUR.TOT_YTD_CR_AMT        --年累计贷方发生额
          ,CUR.TOT_YTD_DB_AMT        --年累计借方发生额
          ,CUR.OFF_BST_INT_RCVB_WRTOF           --表外应收利息核销金额   
          ,CUR.OFF_BST_INT_RCVB_RPLC	           --表外应收利息置换金额   
          ,CUR.TOT_YTD_INT_INCM_AMT_DEBT_AST		 --抵债资产抵债利息收入   
          ,CUR.TOT_YTD_INT_INCM_RTND_WRTOF_LN   --核销贷款收回利息
          ,CUR.TOT_MTD_NBR_LN_DRDWNTXN           --月累计发放贷款笔数 
          ,CUR.TOT_QTD_NBR_LN_DRDWN_TXN          --季累计发放贷款笔数 
          ,CUR.TOT_YTD_NBR_LN_DRDWN_TXN          --年累计发放贷款笔数        
          ,CUR.AMT_LN_REPYMT_RCVD 
          ,CUR.NEW_ACG_SBJ_ID         --新科目         
 					----------------------Start on 2009-1-30-----------------------------------------------
					,CUR.CST_Area_LVL1_TP_Id               --客户区域类型1
					,CUR.CST_Area_LVL2_TP_Id               --客户区域类型2
					,CUR.CST_Area_LVL3_TP_Id               --客户区域类型3
          ----------------------End on 2009-11-30-----------------------------------------------          
          ----------------------Start on 2009-1-30-----------------------------------------------
          ,CUR.ENT_IDV_IND  									AS ENT_IDV_IND	 --个人企业标志
          ----------------------End on 2009-11-30-----------------------------------------------
          ,CUR.LN_INVST_DIRC_TP_ID
          ,CUR.LN_DRDWN_AMT
--------------------------start on 2010-08-09------------------------------------------------------------------------	
          ,CUR_DAY as  NOCLD_IN_MTH
          ,QTR_DAY as NOCLD_IN_QTR
        	,YR_DAY as NOCLD_IN_YEAR
--------------------------end   on 2010-08-09------------------------------------------------------------------------	            
FROM             SESSION.T_CUR AS CUR  
 LEFT OUTER JOIN SESSION.TMP AS PRE
		 ON CUR.ACG_OU_IP_ID      =PRE.ACG_OU_IP_ID
    AND CUR.ACG_SBJ_ID        =PRE.ACG_SBJ_ID
    AND CUR.LN_CTR_TP_ID      =PRE.LN_CTR_TP_ID
    AND CUR.LN_GNT_TP_ID      =PRE.LN_GNT_TP_ID
    AND CUR.LN_PPS_TP_ID      =PRE.LN_PPS_TP_ID
    AND CUR.FND_SRC_TP_ID     =PRE.FND_SRC_TP_ID
    AND CUR.LN_TERM_TP_ID     =PRE.LN_TERM_TP_ID
    AND CUR.TM_MAT_SEG_ID     =PRE.TM_MAT_SEG_ID
    AND CUR.LN_CST_TP_ID      =PRE.LN_CST_TP_ID
    AND CUR.FARMER_TP_ID      =PRE.FARMER_TP_ID
    AND CUR.LN_LCS_STS_TP_ID  =PRE.LN_LCS_STS_TP_ID 
    AND CUR.IDY_CL_ID         =PRE.IDY_CL_ID
    AND CUR.LN_FIVE_RTG_STS   =PRE.LN_FIVE_RTG_STS
    AND CUR.LN_FNC_STS_TP_ID  =PRE.LN_FNC_STS_TP_ID
    AND CUR.CST_CR_RSK_RTG_ID =PRE.CST_CR_RSK_RTG_ID
    AND CUR.PD_GRP_CD         =PRE.PD_GRP_CD
    AND CUR.PD_SUB_CD         =PRE.PD_SUB_CD
    AND CUR.PD_UN_CODE          =PRE.PD_UN_CODE
    AND CUR.CST_SCALE_TP_ID   =PRE.CST_SCALE_TP_ID
    AND CUR.CCY               =PRE.CCY    
    AND CUR.CST_Area_LVL1_TP_Id = PRE.CST_Area_LVL1_TP_Id
    AND CUR.CST_Area_LVL2_TP_Id = PRE.CST_Area_LVL2_TP_Id
    AND CUR.CST_Area_LVL3_TP_Id = PRE.CST_Area_LVL3_TP_Id
    AND CUR.ENT_IDV_IND         = PRE.ENT_IDV_IND 
    AND CUR.LN_INVST_DIRC_TP_ID = PRE.LN_INVST_DIRC_TP_ID   
 ;--
 /** 收集操作信息 */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	SET SMY_STEPNUM =  SMY_STEPNUM + 1 ;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	    --

--月表的插入
   IF ACCOUNTING_DATE = MTH_LAST_DAY THEN  		
		  SET SMY_STEPDESC = '本账务日期为该月最后一天,往月表SMY.OU_LN_MTHLY_SMY 中插入数据';   --  
    	INSERT INTO SMY.OU_LN_MTHLY_SMY
     (
 	         ACG_OU_IP_ID
          ,ACG_SBJ_ID
          ,LN_CTR_TP_ID
          ,LN_GNT_TP_ID
          ,LN_PPS_TP_ID
          ,FND_SRC_TP_ID
          ,LN_TERM_TP_ID
          ,TM_MAT_SEG_ID
          ,LN_CST_TP_ID
          ,FARMER_TP_ID
          ,LN_LCS_STS_TP_ID
          ,IDY_CL_ID
          ,LN_FIVE_RTG_STS
          ,LN_FNC_STS_TP_ID
          ,CST_CR_RSK_RTG_ID
          ,PD_GRP_CD
          ,PD_SUB_CD
          ,CST_SCALE_TP_ID
          ,CCY
          ,PD_UN_CODE
          ,ACG_DT
          ,CDR_YR
          ,CDR_MTH
          ,NOD_In_MTH
          ,NOD_IN_QTR
          ,NOD_IN_YEAR
          ,LN_BAL
          ,LST_DAY_LN_BAL
          ,NBR_CST
          ,NBR_AC
          ,NBR_NEW_AC
          ,NBR_NEW_CST
          ,NBR_AC_CLS
          ,YTD_ON_BST_INT_AMT_RCVD
          ,YTD_OFF_BST_INT_AMT_RCVD
          ,TOT_YTD_AMT_OF_INT_INCM
          ,ON_BST_INT_RCVB
          ,OFF_BST_INT_RCVB
          ,TOT_MTD_NBR_NEW_CST
          ,TOT_MTD_NBR_AC_CLS
          ,MTD_ACML_BAL_AMT
          ,TOT_MTD_NBR_LN_DRDWN_AC
          ,TOT_MTD_LN_DRDWN_AMT
          ,TOT_MTD_AMT_LN_REPYMT_RCVD
          ,TOT_MTD_NBR_LN_REPYMT_RCVD_AC
          ,TOT_QTD_NBR_NEW_CST
          ,TOT_QTD_NBR_AC_CLS
          ,QTD_ACML_BAL_AMT
          ,TOT_QTD_NBR_LN_DRDWN_AC
          ,TOT_QTD_LN_DRDWN_AMT
          ,TOT_QTD_AMT_LN_RPYMT_RCVD
          ,TOT_QTD_NBR_LN_REPYMT_RCVD_AC
          ,TOT_QTD_NBR_NEW_AC
          ,TOT_YTD_NBR_NEW_CST
          ,TOT_YTD_NBR_AC_CLS
          ,YTD_ACML_BAL_AMT
          ,TOT_YTD_NBR_LN_DRDWN_AC
          ,TOT_YTD_LN_DRDWN_AMT
          ,TOT_YTD_AMT_LN_REPYMT_RCVD
          ,TOT_YTD_NBR_LN_REPYMT_RCVD_AC
          ,TOT_YTD_NBR_NEW_AC
          ,TOT_MTD_NBR_NEW_AC
          ,CUR_CR_AMT     
          ,CUR_DB_AMT     
          ,TOT_MTD_CR_AMT 
          ,TOT_MTD_DB_AMT 
          ,TOT_QTD_DB_AMT 
          ,TOT_QTD_CR_AMT 
          ,TOT_YTD_CR_AMT 
          ,TOT_YTD_DB_AMT
          ,OFF_BST_INT_RCVB_WRTOF            --表外应收利息核销金额
          ,OFF_BST_INT_RCVB_RPLC	           --表外应收利息置换金额
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST		 --抵债资产抵债利息收入
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN	   --核销贷款收回利息
          ,TOT_MTD_NBR_LN_DRDWNTXN           --月累计发放贷款笔数
          ,TOT_QTD_NBR_LN_DRDWN_TXN          --季累计发放贷款笔数
          ,TOT_YTD_NBR_LN_DRDWN_TXN          --年累计发放贷款笔数
          ,AMT_LN_REPYMT_RCVD
          ,NEW_ACG_SBJ_ID
          ----------------------Start on 2009-1-30-----------------------------------------------
					,CST_Area_LVL1_TP_Id               --客户区域类型1
					,CST_Area_LVL2_TP_Id               --客户区域类型2
					,CST_Area_LVL3_TP_Id               --客户区域类型3
          ----------------------End on 2009-11-30-----------------------------------------------          
					----------------------Start on 2009-1-30-----------------------------------------------
          ,ENT_IDV_IND  									   --个人企业标志
          ----------------------End on 2009-11-30-----------------------------------------------          
          ,LN_INVST_DIRC_TP_ID
          ,LN_DRDWN_AMT
--------------------------start on 2010-08-09------------------------------------------------------------------------	
          ,NOCLD_IN_MTH
          ,NOCLD_IN_QTR
          ,NOCLD_IN_YEAR
--------------------------end   on 2010-08-09------------------------------------------------------------------------	
       )
       SELECT 
           S.ACG_OU_IP_ID
          ,S.ACG_SBJ_ID
          ,S.LN_CTR_TP_ID
          ,S.LN_GNT_TP_ID
          ,S.LN_PPS_TP_ID
          ,S.FND_SRC_TP_ID
          ,S.LN_TERM_TP_ID
          ,S.TM_MAT_SEG_ID
          ,S.LN_CST_TP_ID
          ,S.FARMER_TP_ID
          ,S.LN_LCS_STS_TP_ID
          ,S.IDY_CL_ID
          ,S.LN_FIVE_RTG_STS
          ,S.LN_FNC_STS_TP_ID
          ,S.CST_CR_RSK_RTG_ID
          ,S.PD_GRP_CD
          ,S.PD_SUB_CD
          ,S.CST_SCALE_TP_ID
          ,S.CCY
          ,S.PD_UN_CODE
          ,S.ACG_DT
          ,CDR_YR
          ,CDR_MTH
          ,NOD_In_MTH
          ,NOD_IN_QTR
          ,NOD_IN_YEAR
          ,LN_BAL
          ,LST_DAY_LN_BAL
          ,NBR_CST
          ,NBR_AC
          ,NBR_NEW_AC
          ,NBR_NEW_CST
          ,NBR_AC_CLS
          ,YTD_ON_BST_INT_AMT_RCVD
          ,YTD_OFF_BST_INT_AMT_RCVD
          ,TOT_YTD_AMT_OF_INT_INCM
          ,ON_BST_INT_RCVB
          ,OFF_BST_INT_RCVB
          ,TOT_MTD_NBR_NEW_CST
          ,TOT_MTD_NBR_AC_CLS
          ,MTD_ACML_BAL_AMT
          ,TOT_MTD_NBR_LN_DRDWN_AC
          ,TOT_MTD_LN_DRDWN_AMT
          ,TOT_MTD_AMT_LN_REPYMT_RCVD
          ,TOT_MTD_NBR_LN_REPYMT_RCVD_AC
          ,TOT_QTD_NBR_NEW_CST
          ,TOT_QTD_NBR_AC_CLS
          ,QTD_ACML_BAL_AMT
          ,TOT_QTD_NBR_LN_DRDWN_AC
          ,TOT_QTD_LN_DRDWN_AMT
          ,TOT_QTD_AMT_LN_RPYMT_RCVD
          ,TOT_QTD_NBR_LN_REPYMT_RCVD_AC
          ,TOT_QTD_NBR_NEW_AC
          ,TOT_YTD_NBR_NEW_CST
          ,TOT_YTD_NBR_AC_CLS
          ,YTD_ACML_BAL_AMT
          ,TOT_YTD_NBR_LN_DRDWN_AC
          ,TOT_YTD_LN_DRDWN_AMT
          ,TOT_YTD_AMT_LN_REPYMT_RCVD
          ,TOT_YTD_NBR_LN_REPYMT_RCVD_AC
          ,TOT_YTD_NBR_NEW_AC
          ,TOT_MTD_NBR_NEW_AC
          ,CUR_CR_AMT     
          ,CUR_DB_AMT     
          ,TOT_MTD_CR_AMT 
          ,TOT_MTD_DB_AMT 
          ,TOT_QTD_DB_AMT 
          ,TOT_QTD_CR_AMT 
          ,TOT_YTD_CR_AMT 
          ,TOT_YTD_DB_AMT
          ,OFF_BST_INT_RCVB_WRTOF            --表外应收利息核销金额
          ,OFF_BST_INT_RCVB_RPLC	           --表外应收利息置换金额
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST		 --抵债资产抵债利息收入
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN	   --核销贷款收回利息
          ,TOT_MTD_NBR_LN_DRDWNTXN           --月累计发放贷款笔数
          ,TOT_QTD_NBR_LN_DRDWN_TXN          --季累计发放贷款笔数
          ,TOT_YTD_NBR_LN_DRDWN_TXN          --年累计发放贷款笔数
          ,AMT_LN_REPYMT_RCVD
          ,NEW_ACG_SBJ_ID 
          ----------------------Start on 2009-1-30-----------------------------------------------
					,S.CST_Area_LVL1_TP_Id               --客户区域类型1
					,S.CST_Area_LVL2_TP_Id               --客户区域类型2
					,S.CST_Area_LVL3_TP_Id               --客户区域类型3
          ----------------------End on 2009-11-30-----------------------------------------------           
 					----------------------Start on 2009-1-30-----------------------------------------------
          ,S.ENT_IDV_IND  			AS ENT_IDV_IND	 --个人企业标志
          ----------------------End on 2009-11-30----------------------------------------------- 
          ,S.LN_INVST_DIRC_TP_ID
          ,LN_DRDWN_AMT                          
--------------------------start on 2010-08-09------------------------------------------------------------------------	
          ,S.NOCLD_IN_MTH
          ,S.NOCLD_IN_QTR
          ,S.NOCLD_IN_YEAR
--------------------------end   on 2010-08-09------------------------------------------------------------------------	
      FROM SMY.OU_LN_DLY_SMY S 
      WHERE S.ACG_DT=ACCOUNTING_DATE	       
      ; --
       
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	SET SMY_STEPNUM = SMY_STEPNUM+ 1 ; --
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	       --

  END IF ;--
  
	 COMMIT;--
END@