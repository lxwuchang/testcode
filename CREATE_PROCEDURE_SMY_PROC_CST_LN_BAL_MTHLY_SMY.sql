CREATE PROCEDURE SMY.PROC_CST_LN_BAL_MTHLY_SMY(ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_CST_LN_BAL_MTHLY_SMY.sql
-- Procedure name: 			SMY.PROC_CST_LN_BAL_MTHLY_SMY
-- Source Table:				SMY.LN_AR_SMY,SMY.MAT_SEG,SMY.LN_AR_INT_MTHLY_SMY
-- Target Table: 				SMY.CST_LN_BAL_MTHLY_SMY
-- Project:             ZJ RCCB EDW
-- Note                 Delete and Insert and Update
-- Purpose:             
--
--=============================================================================
-- Creation Date:       2009.11.12
-- Origin Author:       Peng Jie
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2009-11-12   Peng Jie     Create SP File		
-- 2009-12-04   Xu Yan       Rename the history table
-- 2009-12-16   Xu Yan       Added a new column 'LN_AR_TP_ID' and redefined the rerun rules using the ACG_DT instead.
-- 2009-12-22   Xu Yan       Fixed a bug
-- 2010-01-06   Xu Yan       Included the related transactions on the account closing day
-- 2010-01-07   Xu Yan       Fixed a bug for rerunning
-- 2010-08-11   van fuqiao   Fixed a bug for column  'QTR_DAY'  'MONTH_DAY' 'YR_DAY'
-- 2011-05-04   Chen XiaoWen 1、SMY.LOAN_AR_SMY,SMY.LN_AR_INT_MTHLY_SMY关联时,增加PARTITIONING KEY(ACG_DT)作为优先筛选条件,可达到分区定界筛选效果.
--                           2、增加临时表TMP_CST_LN_BAL_MTHLY_SMY_TMP缓存SMY.LOAN_AR_SMY,SMY.LN_AR_INT_MTHLY_SMY先关联结果,随后再针对临时表group by.
-- 2012-02-28   Chen XiaoWen 将merge当中的match、not match逻辑拆分为match update和单独的insert语句。
-------------------------------------------------------------------------------
LANGUAGE SQL
BEGIN
/*声明异常处理使用变量*/
DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
DECLARE SMY_STEPNUM INT DEFAULT 1;                     --过程内部位置标记
DECLARE SMY_STEPDESC VARCHAR(100) DEFAULT '';          --过程内部位置描述
DECLARE SMY_DATE DATE;                                 --临时日期变量
DECLARE SMY_RCOUNT INT;                                --DML语句作用记录数
DECLARE SMY_PROCNM VARCHAR(100);    --
DECLARE CUR_YEAR SMALLINT;--
DECLARE CUR_MONTH SMALLINT;--
DECLARE CUR_DAY INTEGER;--
DECLARE YR_FIRST_DAY DATE;--
DECLARE QTR_FIRST_DAY DATE;--
DECLARE MONTH_DAY SMALLINT;--
DECLARE YR_DAY SMALLINT;--
DECLARE QTR_DAY SMALLINT; --
DECLARE MAX_ACG_DT DATE;--
DECLARE LAST_SMY_DATE DATE;--
------------------Start on 2011-05-04---------------
DECLARE MONTH_FIRST_DAY DATE;--当月第一天
DECLARE MONTH_LAST_DAY DATE;--当月最后一天
------------------End on 2011-05-04-----------------

	
/*1.定义针对SQL异常情况的句柄(EXIT方式).
  2.将出现SQL异常时在存储过程中的位置(SMY_STEPNUM),位置描述(SMY_STEPDESC)，SQLCODE(SMY_SQLCODE)记入表SMY_LOG中作调试用.
  3.调用RESIGNAL重新引发异常,跳出存储过程执行体,对引发SQL异常之前存储过程体中所完成的操作进行回滚.*/
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
SET SMY_PROCNM = 'PROC_CST_LN_BAL_MTHLY_SMY';--
SET SMY_DATE=ACCOUNTING_DATE;--
SET CUR_YEAR=YEAR(ACCOUNTING_DATE);  --取当前年份
SET CUR_MONTH=MONTH(ACCOUNTING_DATE); --取当前月份
SET CUR_DAY=DAY(ACCOUNTING_DATE);     --取当日
SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');--

----------------------------------------Start on 2011-05-04-------------------------------
SET MONTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,ISO),1,7)||'-01');
VALUES(MONTH_FIRST_DAY + 1 months -1 days) INTO MONTH_LAST_DAY;
----------------------------------------End on 2011-05-04---------------------------------

VALUES(ACCOUNTING_DATE - 1 DAY) INTO LAST_SMY_DATE;--
IF CUR_MONTH IN (1,2,3) THEN 
   SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');--
------------------------Start on 20100810--------------------------
   --SET QTR_DAY = DAYS(TRIM(CHAR(CUR_YEAR))||'-3-31') - DAYS(QTR_FIRST_DAY) + 1;--
------------------------end on 20100810-------------------------- 
ELSEIF CUR_MONTH IN (4,5,6) THEN 
       SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-04-01');--
       ------------------------Start on 20100810--------------------------
       --SET QTR_DAY = DAYS(TRIM(CHAR(CUR_YEAR))||'-6-30') - DAYS(QTR_FIRST_DAY) + 1;--
       ------------------------end on 20100810-------------------------- 
    ELSEIF CUR_MONTH IN (7,8,9) THEN 
           SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-07-01');--
           ------------------------Start on 20100810--------------------------
           --SET QTR_DAY = DAYS(TRIM(CHAR(CUR_YEAR))||'-9-30') - DAYS(QTR_FIRST_DAY) + 1;--
           ------------------------end on 20100810-------------------------- 
        ELSE
            SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-10-01');--
            ------------------------Start on 20100810--------------------------
           -- SET QTR_DAY = DAYS(TRIM(CHAR(CUR_YEAR))||'-12-31') - DAYS(QTR_FIRST_DAY) + 1;--
           ------------------------end on 20100810-------------------------- 
END IF;--
------------------------Start on 20100810------------------------
   SET QTR_DAY = DAYS(ACCOUNTING_DATE) - DAYS(QTR_FIRST_DAY) + 1;--季度日历天数
------------------------end on 20100810-------------------------- 


------------------------Start on 20100810------------------------
/*
IF CUR_MONTH <> 12 THEN
  SET MONTH_DAY = DAYS(DATE(TRIM(CHAR(CUR_YEAR))||'-'||TRIM(CHAR(CUR_MONTH + 1))||'-01')) - DAYS(DATE(TRIM(CHAR(CUR_YEAR))||'-'||TRIM(CHAR(CUR_MONTH))||'-01'));--
ELSE 
  SET MONTH_DAY = 31;--
END IF;--
*/

SET MONTH_DAY = DAYS(ACCOUNTING_DATE) - DAYS(DATE(TRIM(CHAR(CUR_YEAR))||'-'||TRIM(CHAR(CUR_MONTH))||'-01'))+1;--月日历天数
------------------------end on 20100810-------------------------- 


------------------------Start on 20100810------------------------
--SET YR_DAY = DAYS(DATE(TRIM(CHAR(CUR_YEAR))||'-12-31')) - DAYS(YR_FIRST_DAY) + 1;--
SET YR_DAY = DAYS(ACCOUNTING_DATE) - DAYS(YR_FIRST_DAY) + 1;--年日历天数
------------------------end on 20100810-------------------------- 


SELECT MAX(ACG_DT) INTO MAX_ACG_DT FROM SMY.CST_LN_BAL_MTHLY_SMY;--

/*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE,并插入新的起始标志*/
DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
COMMIT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, 'PROCEDURE START.', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;   --
   
/*数据恢复与备份*/
IF MAX_ACG_DT=ACCOUNTING_DATE THEN
   DELETE FROM SMY.CST_LN_BAL_MTHLY_SMY WHERE ACG_DT = MAX_ACG_DT;--
   COMMIT;--
   IF CUR_DAY<>1 THEN
      -----------------------------------Start on 20100107-------------------------------
      --INSERT INTO SMY.CST_LN_BAL_MTHLY_SMY SELECT * FROM HIS.CST_LN_BAL_MTHLY_SMY;--
      INSERT INTO SMY.CST_LN_BAL_MTHLY_SMY 
				SELECT * FROM HIS.CST_LN_BAL_MTHLY_SMY b
				where not exists (
					select 1 from SMY.CST_LN_BAL_MTHLY_SMY a 
					where 
							  a.CST_ID              = b.CST_ID               
							and a.LN_GNT_TP_ID        = b.LN_GNT_TP_ID         
							and a.TM_MAT_SEG_ID       = b.TM_MAT_SEG_ID        
							and a.LN_TERM_TP_ID       = b.LN_TERM_TP_ID        
							and a.LN_LCS_STS_TP_ID    = b.LN_LCS_STS_TP_ID     
							and a.LN_FIVE_RTG_STS     = b.LN_FIVE_RTG_STS      
							and a.LN_PPS_TP_ID        = b.LN_PPS_TP_ID         
							and a.IDY_CL_ID           = b.IDY_CL_ID            
							and a.LN_FNC_STS_TP_ID    = b.LN_FNC_STS_TP_ID     
							and a.LN_CGY_TP_ID        = b.LN_CGY_TP_ID         
							and a.FND_SRC_TP_ID       = b.FND_SRC_TP_ID        
							and a.PD_GRP_CD           = b.PD_GRP_CD            
							and a.PD_SUB_CD           = b.PD_SUB_CD            
							and a.AC_OU_ID            = b.AC_OU_ID             
							and a.CCY                 = b.CCY                  
							and a.ALT_TP_ID           = b.ALT_TP_ID            
							and a.LN_AR_TP_ID         = b.LN_AR_TP_ID          
							and a.CDR_YR              = b.CDR_YR               
							and a.CDR_MTH             = b.CDR_MTH              
							and a.LN_INVST_DIRC_TP_ID = b.LN_INVST_DIRC_TP_ID   
				);--
      -----------------------------------End on 20100107-------------------------------
      COMMIT;--
   END IF;--
ELSE
   DELETE FROM HIS.CST_LN_BAL_MTHLY_SMY;--
   COMMIT;--
   INSERT INTO HIS.CST_LN_BAL_MTHLY_SMY SELECT * FROM SMY.CST_LN_BAL_MTHLY_SMY WHERE ACG_DT = LAST_SMY_DATE;--
   COMMIT;--
END IF;--

SET SMY_STEPNUM = SMY_STEPNUM+1;--
SET SMY_STEPDESC = '创建临时表,并把当日数据插入';--

DECLARE GLOBAL TEMPORARY TABLE TMP_CST_LN_BAL_MTHLY_SMY(
			CST_ID                            CHARACTER(18)
     ,LN_GNT_TP_ID                     INTEGER                                                                
     ,TM_MAT_SEG_ID                    INTEGER                                                                
     ,LN_TERM_TP_ID                    INTEGER                                                                
     ,LN_LCS_STS_TP_ID                 INTEGER                                                                
     ,LN_FIVE_RTG_STS                  INTEGER                                                                
     ,LN_PPS_TP_ID                     INTEGER                                                                
     ,IDY_CL_ID                        INTEGER                                                                
     ,LN_FNC_STS_TP_ID                 INTEGER                                                                
     ,LN_CGY_TP_Id                     INTEGER                                                                
     ,FND_SRC_TP_ID                    INTEGER                                                                
     ,PD_GRP_CD                        CHARACTER(2)                                                           
     ,PD_SUB_CD                        CHARACTER(3)                                                           
     ,AC_OU_ID                         CHARACTER(18)                                                          
     ,LN_INVST_DIRC_TP_ID              INTEGER                                                                
     ,ALT_TP_ID                        INTEGER                                                                
     ,CCY                              CHARACTER(3)                                                           
     ,ENT_IDV_IND                      INTEGER                                                                
     ,LN_BAL                           DECIMAL(17,2)                                                          
     ,NBR_LN                           INTEGER                                                                
     ,ACR_INT_RCVD                     DECIMAL(17,2)                                                          
     ,ACR_INT_RCVB                     DECIMAL(17,2)                                                          
     ,MTD_ACML_LN_BAL_AMT              DECIMAL(17,2)                                                          
     ,TOT_MTD_NBR_LN_DRDWNS            INTEGER                                                                
     ,TOT_MTD_NBR_LN_REPYMT_RCVD       INTEGER                                                                
     ,TOT_MTD_WRTOF_AMT_RCVD           DECIMAL(17,2)                                                          
     ,TOT_MTD_WRTOF_AMT                DECIMAL(17,2)                                                          
     ,TOT_MTD_AMT_RCVD_OF_AST_RPLC     DECIMAL(17,2)                                                          
     ,TOT_MTD_LN_DRDWN_AMT             DECIMAL(17,2)                                                          
     ,TOT_MTD_AMT_LN_REPYMT_RCVD       DECIMAL(17,2)                                                          
     ,QTD_ACML_LN_BAL_AMT              DECIMAL(17,2)                                                          
     ,TOT_QTD_NBR_LN_DRDWN             INTEGER                                                                
     ,TOT_QTD_NBR_LN_RPYMT_RCVD        INTEGER                                                                
     ,TOT_QTD_WRTOF_AMT_RCVD           DECIMAL(17,2)                                                          
     ,TOT_QTD_AMT_RCVD_OF_AST_RPLC     DECIMAL(17,2)                                                          
     ,TOT_QTD_LN_DRDWN_AMT             DECIMAL(17,2)                                                          
     ,TOT_QTD_WRTOF_AMT                DECIMAL(17,2)                                                          
     ,TOT_QTD_AMT_LN_RPYMT_RCVD        DECIMAL(17,2)                                                          
     ,YTD_ACML_LN_BAL_AMT              DECIMAL(17,2)                                                          
     ,TOT_YTD_NBR_LN_DRDWN             INTEGER                                                                
     ,TOT_YTD_NBR_LN_REPYMT_RCVD       INTEGER                                                                
     ,TOT_YTD_WRTOF_AMT_RCVD           DECIMAL(17,2)                                                          
     ,TOT_YTD_AMT_RCVD_OF_AST_RPLC     DECIMAL(17,2)                                                          
     ,TOT_YTD_LN_DRDWN_AMT             DECIMAL(17,2)                                                          
     ,TOT_YTD_WRTOF_AMT                DECIMAL(17,2)                                                          
     ,TOT_YTD_AMT_LN_REPYMT_RCVD       DECIMAL(17,2)                                                          
     ,ON_BST_INT_RCVB                  DECIMAL(17,2)                                                          
     ,OFF_BST_INT_RCVB                 DECIMAL(17,2)                                                          
     ,OFF_BST_INT_RCVB_WRTOF           DECIMAL(17,2)                                                          
     ,OFF_BST_INT_RCVB_RPLC            DECIMAL(17,2)                                                          
     ,YTD_On_BST_INT_AMT_RCVD          DECIMAL(17,2)
     ------------------------Start on 20091216--------------------------
     ,LN_AR_TP_ID                  		 INTEGER
     ------------------------End on 20091216--------------------------     
     )                                                         
ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(CST_ID);

DECLARE GLOBAL TEMPORARY TABLE TMP_CST_LN_BAL_MTHLY_SMY_TMP
LIKE SESSION.TMP_CST_LN_BAL_MTHLY_SMY 
ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(CST_ID);

INSERT INTO SESSION.TMP_CST_LN_BAL_MTHLY_SMY_TMP
SELECT 
         a.PRIM_CST_ID
				 ,a.CLT_TP_ID
				 ,COALESCE(a.TM_MAT_SEG_ID,-1) as TM_MAT_SEG_ID
				 ,a.LN_TERM_TP_ID
				 ,a.AR_LCS_TP_ID
				 ,a.LN_FR_RSLT_TP_ID
				 ,a.LN_PPS_TP_ID
				 ,a.CNRL_BNK_IDY_CL_ID
				 ,a.AR_FNC_ST_TP_ID
				 ,a.LN_CGY_TP_Id
				 ,a.FND_SRC_DST_TP_ID
				 ,a.PD_GRP_CODE
				 ,a.PD_SUB_CODE
				 ,a.RPRG_OU_IP_ID
				 ,a.LN_INVST_DIRC_TP_ID
         ,a.ALT_TP_ID
         ,a.DNMN_CCY_ID
         ,a.ENT_IDV_IND
         ,a.LN_BAL
	       ,case when a.AR_LCS_TP_ID = 13360003 then 1 else 0 end
	       ,COALESCE(YTD_On_BST_INT_AMT_RCVD,0) + COALESCE(YTD_Off_BST_INT_AMT_RCVD,0)
	       ,COALESCE(ON_BST_INT_RCVB,0) + COALESCE(OFF_BST_INT_RCVB,0)
	       ,COALESCE(MTD_ACML_BAL_AMT,0)
	       ,COALESCE(TOT_MTD_NBR_LN_DRDWNTXN,0)
	       ,COALESCE(TOT_MTD_NBR_LN_RCVD_TXN,0)
	       ,COALESCE(TOT_MTD_WRTOF_AMT_RCVD,0)
	       ,COALESCE(TOT_MTD_WRTOF_AMT,0)
	       ,COALESCE(TOT_MTD_AMT_RCVD_OF_AST_RPLC,0)
	       ,COALESCE(TOT_MTD_LN_DRDWN_AMT,0)
	       ,COALESCE(TOT_MTD_AMT_LN_REPYMT_RCVD,0)
	       ,COALESCE(QTD_ACML_BAL_AMT,0)
	       ,COALESCE(TOT_QTD_NBR_LN_DRDWN_TXN,0)
	       ,COALESCE(TOT_QTD_NBR_LN_RCVD_TXN,0)
	       ,COALESCE(TOT_QTD_WRTOF_AMT_RCVD,0)
	       ,COALESCE(TOT_QTD_AMT_RCVD_OF_AST_RPLC,0)
	       ,COALESCE(TOT_QTD_LN_DRDWN_AMT,0)
	       ,COALESCE(TOT_QTD_WRTOF_AMT,0)
	       ,COALESCE(TOT_QTD_AMT_LN_RPYMT_RCVD,0)
	       ,COALESCE(YTD_ACML_BAL_AMT,0)
	       ,COALESCE(TOT_YTD_NBR_LN_DRDWN_TXN,0)
	       ,COALESCE(TOT_YTD_NBR_LN_RCVD_TXN,0)
	       ,COALESCE(TOT_YTD_WRTOF_AMT_RCVD,0)
	       ,COALESCE(TOT_YTD_AMT_RCVD_OF_AST_RPLC,0)
	       ,COALESCE(TOT_YTD_LN_DRDWN_AMT,0)
	       ,COALESCE(TOT_YTD_WRTOF_AMT,0)
	       ,COALESCE(TOT_YTD_AMT_LN_REPYMT_RCVD,0)
	       ,COALESCE(ON_BST_INT_RCVB,0)
	       ,COALESCE(OFF_BST_INT_RCVB,0)
	       ,COALESCE(OFF_BST_INT_RCVB_WRTOF,0)
	       ,COALESCE(OFF_BST_INT_RCVB_RPLC,0)
	       ,COALESCE(YTD_On_BST_INT_AMT_RCVD,0)
	       ,a.LN_AR_TP_ID
    FROM SMY.LOAN_AR_SMY a INNER JOIN SMY.LN_AR_INT_MTHLY_SMY b
    ON a.CTR_AR_ID = b.CTR_AR_ID AND 
    a.CTR_ITM_ORDR_ID = b.CTR_ITM_ORDR_ID AND 
    b.ACG_DT >= MONTH_FIRST_DAY AND b.ACG_DT <= MONTH_LAST_DAY AND 
    b.CDR_YR = CUR_YEAR AND 
    b.CDR_MTH = CUR_MONTH
;

CREATE INDEX SESSION.TMP_IDX ON SESSION.TMP_CST_LN_BAL_MTHLY_SMY_TMP(CST_ID,LN_GNT_TP_ID,TM_MAT_SEG_ID,LN_TERM_TP_ID,LN_LCS_STS_TP_ID,LN_FIVE_RTG_STS,LN_PPS_TP_ID,IDY_CL_ID,LN_FNC_STS_TP_ID,LN_CGY_TP_Id,FND_SRC_TP_ID,PD_GRP_CD,PD_SUB_CD,AC_OU_ID,LN_INVST_DIRC_TP_ID,ALT_TP_ID,CCY,ENT_IDV_IND,LN_AR_TP_ID);

INSERT INTO SESSION.TMP_CST_LN_BAL_MTHLY_SMY
SELECT 
     CST_ID                                                                 
    ,LN_GNT_TP_ID                                                           
    ,TM_MAT_SEG_ID                                                          
    ,LN_TERM_TP_ID                                                          
    ,LN_LCS_STS_TP_ID                                                       
    ,LN_FIVE_RTG_STS                                                        
    ,LN_PPS_TP_ID                                                           
    ,IDY_CL_ID                                                              
    ,LN_FNC_STS_TP_ID                                                       
    ,LN_CGY_TP_Id                                                           
    ,FND_SRC_TP_ID                                                          
    ,PD_GRP_CD                                                              
    ,PD_SUB_CD                                                              
    ,AC_OU_ID                                                               
    ,LN_INVST_DIRC_TP_ID                                                    
    ,ALT_TP_ID                                                              
    ,CCY                                                                    
    ,ENT_IDV_IND                                                            
    ,SUM(LN_BAL)                                                            
    ,sum(NBR_LN)                                                  
    ,SUM(ACR_INT_RCVD)                                                      
    ,SUM(ACR_INT_RCVB)                                                      
    ,SUM(MTD_ACML_LN_BAL_AMT)                                                  
    ,SUM(TOT_MTD_NBR_LN_DRDWNS)                                           
    ,SUM(TOT_MTD_NBR_LN_REPYMT_RCVD)                                           
    ,SUM(TOT_MTD_WRTOF_AMT_RCVD)                                            
    ,SUM(TOT_MTD_WRTOF_AMT)                                                 
    ,SUM(TOT_MTD_AMT_RCVD_OF_AST_RPLC)                                      
    ,SUM(TOT_MTD_LN_DRDWN_AMT)                                              
    ,SUM(TOT_MTD_AMT_LN_REPYMT_RCVD)                                        
    ,SUM(QTD_ACML_LN_BAL_AMT)                                                  
    ,SUM(TOT_QTD_NBR_LN_DRDWN)                                          
    ,SUM(TOT_QTD_NBR_LN_RPYMT_RCVD)                                           
    ,SUM(TOT_QTD_WRTOF_AMT_RCVD)                                            
    ,SUM(TOT_QTD_AMT_RCVD_OF_AST_RPLC)                                      
    ,SUM(TOT_QTD_LN_DRDWN_AMT)                                              
    ,SUM(TOT_QTD_WRTOF_AMT)                                                 
    ,SUM(TOT_QTD_AMT_LN_RPYMT_RCVD)                                         
    ,SUM(YTD_ACML_LN_BAL_AMT)                                                  
    ,SUM(TOT_YTD_NBR_LN_DRDWN)                                          
    ,SUM(TOT_YTD_NBR_LN_REPYMT_RCVD)                                           
    ,SUM(TOT_YTD_WRTOF_AMT_RCVD)                                            
    ,SUM(TOT_YTD_AMT_RCVD_OF_AST_RPLC)                                      
    ,SUM(TOT_YTD_LN_DRDWN_AMT)                                              
    ,SUM(TOT_YTD_WRTOF_AMT)                                                 
    ,SUM(TOT_YTD_AMT_LN_REPYMT_RCVD)                                        
    ,SUM(ON_BST_INT_RCVB)                                                   
    ,SUM(OFF_BST_INT_RCVB)                                                  
    ,SUM(OFF_BST_INT_RCVB_WRTOF)                                            
    ,SUM(OFF_BST_INT_RCVB_RPLC)                                             
    ,SUM(YTD_On_BST_INT_AMT_RCVD)                                           
    ,LN_AR_TP_ID                                                            
FROM SESSION.TMP_CST_LN_BAL_MTHLY_SMY_TMP                                                                
GROUP BY 
    CST_ID
	  ,LN_GNT_TP_ID
	  ,TM_MAT_SEG_ID
	  ,LN_TERM_TP_ID
	  ,LN_LCS_STS_TP_ID
	  ,LN_FIVE_RTG_STS
	  ,LN_PPS_TP_ID
	  ,IDY_CL_ID
	  ,LN_FNC_STS_TP_ID
	  ,LN_CGY_TP_Id
	  ,FND_SRC_TP_ID
	  ,PD_GRP_CD
	  ,PD_SUB_CD
	  ,AC_OU_ID
	  ,LN_INVST_DIRC_TP_ID
    ,ALT_TP_ID  							 
    ,CCY
    ,ENT_IDV_IND
    ,LN_AR_TP_ID
;

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

SET SMY_STEPNUM = SMY_STEPNUM+1;--

IF CUR_DAY = 1 THEN
   IF CUR_MONTH = 1 THEN
      SET SMY_STEPDESC = '插入年初数据';--
      INSERT INTO SMY.CST_LN_BAL_MTHLY_SMY
				(CST_ID
				,LN_GNT_TP_ID
				,TM_MAT_SEG_ID
				,LN_TERM_TP_ID
				,LN_LCS_STS_TP_ID
				,LN_FIVE_RTG_STS
				,LN_PPS_TP_ID
				,IDY_CL_ID
				,LN_FNC_STS_TP_ID
				,LN_CGY_TP_ID
				,FND_SRC_TP_ID
				,PD_GRP_CD
				,PD_SUB_CD
				,AC_OU_ID
				,LN_INVST_DIRC_TP_ID
				,ALT_TP_ID
				,CCY
				,CDR_YR
				,CDR_MTH
				,NOCLD_IN_MTH
				,NOD_IN_MTH
				,NOCLD_IN_QTR
				,NOD_IN_QTR
				,NOCLD_IN_YEAR
				,NOD_IN_YEAR
				,ACG_DT
				,ENT_IDV_IND
				,LN_BAL
				,LST_DAY_BAL
				,NBR_LN
				,ACR_INT_RCVD
				,ACR_INT_RCVB
				,MTD_ACML_LN_BAL_AMT
				,TOT_MTD_NBR_LN_DRDWNS
				,TOT_MTD_NBR_LN_REPYMT_RCVD
				,TOT_MTD_WRTOF_AMT_RCVD
				,TOT_MTD_WRTOF_AMT
				,TOT_MTD_AMT_RCVD_OF_AST_RPLC
				,TOT_MTD_LN_DRDWN_AMT
				,TOT_MTD_AMT_LN_REPYMT_RCVD
				,QTD_ACML_LN_BAL_AMT
				,TOT_QTD_NBR_LN_DRDWN
				,TOT_QTD_NBR_LN_RPYMT_RCVD
				,TOT_QTD_WRTOF_AMT_RCVD
				,TOT_QTD_AMT_RCVD_OF_AST_RPLC
				,TOT_QTD_LN_DRDWN_AMT
				,TOT_QTD_WRTOF_AMT
				,TOT_QTD_AMT_LN_RPYMT_RCVD
				,YTD_ACML_LN_BAL_AMT
				,TOT_YTD_NBR_LN_DRDWN
				,TOT_YTD_NBR_LN_REPYMT_RCVD
				,TOT_YTD_WRTOF_AMT_RCVD
				,TOT_YTD_AMT_RCVD_OF_AST_RPLC
				,TOT_YTD_LN_DRDWN_AMT
				,TOT_YTD_WRTOF_AMT
				,TOT_YTD_AMT_LN_REPYMT_RCVD
				,ON_BST_INT_RCVB
				,OFF_BST_INT_RCVB
				,OFF_BST_INT_RCVB_WRTOF
				,OFF_BST_INT_RCVB_RPLC
				,YTD_On_BST_INT_AMT_RCVD
				------------------------Start on 20091216--------------------------
	      ,LN_AR_TP_ID                  	
	     	------------------------End on 20091216--------------------------			                                                       
				)
			SELECT 
         a.CST_ID                                 --CST_ID                                                       
        ,a.LN_GNT_TP_ID                           --LN_GNT_TP_ID                                     
        ,a.TM_MAT_SEG_ID                          --TM_MAT_SEG_ID                                    
        ,a.LN_TERM_TP_ID                          --LN_TERM_TP_ID                                    
        ,a.LN_LCS_STS_TP_ID                       --LN_LCS_STS_TP_ID                                 
        ,a.LN_FIVE_RTG_STS                        --LN_FIVE_RTG_STS                                  
        ,a.LN_PPS_TP_ID                           --LN_PPS_TP_ID                                     
        ,a.IDY_CL_ID                              --IDY_CL_ID                                        
        ,a.LN_FNC_STS_TP_ID                       --LN_FNC_STS_TP_ID                                 
        ,a.LN_CGY_TP_ID                           --LN_CGY_TP_ID                                     
        ,a.FND_SRC_TP_ID                          --FND_SRC_TP_ID                                    
        ,a.PD_GRP_CD                              --PD_GRP_CD                                        
        ,a.PD_SUB_CD                              --PD_SUB_CD                                        
        ,a.AC_OU_ID                               --AC_OU_ID                                         
        ,a.LN_INVST_DIRC_TP_ID                    --LN_INVST_DIRC_TP_ID                              
        ,a.ALT_TP_ID                              --ALT_TP_ID                                        
        ,a.CCY                                    --CCY                                              
        ,CUR_YEAR                                 --CDR_YR                                           
        ,CUR_MONTH                                --CDR_MTH                                          
        ,MONTH_DAY                                --NOCLD_IN_MTH                                     
        ,case when a.NBR_LN =0 then 0 else 1  end                                  --NOD_IN_MTH                                       
        ,QTR_DAY                                  --NOCLD_IN_QTR                                     
        ,case when a.NBR_LN =0 then 0 else 1  end                                        --NOD_IN_QTR                                       
        ,YR_DAY                                   --NOCLD_IN_YEAR                                    
        ,case when a.NBR_LN =0 then 0 else 1  end                                        --NOD_IN_YEAR                                      
        ,SMY_DATE                                 --ACG_DT                                           
        ,a.ENT_IDV_IND                            --ENT_IDV_IND                                      
        ,a.LN_BAL                                 --LN_BAL                                           
        ,COALESCE(b.LN_BAL,0.00)                  --LST_DAY_BAL                                      
        ,a.NBR_LN                                 --NBR_LN                                           
        ,a.ACR_INT_RCVD                           --ACR_INT_RCVD                                     
        ,a.ACR_INT_RCVB                           --ACR_INT_RCVB                                     
        ,a.MTD_ACML_LN_BAL_AMT                    --MTD_ACML_LN_BAL_AMT                              
        ,a.TOT_MTD_NBR_LN_DRDWNS                  --TOT_MTD_NBR_LN_DRDWNS                            
        ,a.TOT_MTD_NBR_LN_REPYMT_RCVD             --TOT_MTD_NBR_LN_REPYMT_RCVD                       
        ,a.TOT_MTD_WRTOF_AMT_RCVD                 --TOT_MTD_WRTOF_AMT_RCVD                           
        ,a.TOT_MTD_WRTOF_AMT                      --TOT_MTD_WRTOF_AMT                                
        ,a.TOT_MTD_AMT_RCVD_OF_AST_RPLC           --TOT_MTD_AMT_RCVD_OF_AST_RPLC                     
        ,a.TOT_MTD_LN_DRDWN_AMT                   --TOT_MTD_LN_DRDWN_AMT                             
        ,a.TOT_MTD_AMT_LN_REPYMT_RCVD             --TOT_MTD_AMT_LN_REPYMT_RCVD                       
        ,a.QTD_ACML_LN_BAL_AMT                    --QTD_ACML_LN_BAL_AMT                              
        ,a.TOT_QTD_NBR_LN_DRDWN                   --TOT_QTD_NBR_LN_DRDWN                             
        ,a.TOT_QTD_NBR_LN_RPYMT_RCVD              --TOT_QTD_NBR_LN_RPYMT_RCVD                        
        ,a.TOT_QTD_WRTOF_AMT_RCVD                 --TOT_QTD_WRTOF_AMT_RCVD                           
        ,a.TOT_QTD_AMT_RCVD_OF_AST_RPLC           --TOT_QTD_AMT_RCVD_OF_AST_RPLC                     
        ,a.TOT_QTD_LN_DRDWN_AMT                   --TOT_QTD_LN_DRDWN_AMT                             
        ,a.TOT_QTD_WRTOF_AMT                      --TOT_QTD_WRTOF_AMT                                
        ,a.TOT_QTD_AMT_LN_RPYMT_RCVD              --TOT_QTD_AMT_LN_RPYMT_RCVD                        
        ,a.YTD_ACML_LN_BAL_AMT                    --YTD_ACML_LN_BAL_AMT                              
        ,a.TOT_YTD_NBR_LN_DRDWN                   --TOT_YTD_NBR_LN_DRDWN                             
        ,a.TOT_YTD_NBR_LN_REPYMT_RCVD             --TOT_YTD_NBR_LN_REPYMT_RCVD                       
        ,a.TOT_YTD_WRTOF_AMT_RCVD                 --TOT_YTD_WRTOF_AMT_RCVD                           
        ,a.TOT_YTD_AMT_RCVD_OF_AST_RPLC           --TOT_YTD_AMT_RCVD_OF_AST_RPLC                     
        ,a.TOT_YTD_LN_DRDWN_AMT                   --TOT_YTD_LN_DRDWN_AMT                             
        ,a.TOT_YTD_WRTOF_AMT                      --TOT_YTD_WRTOF_AMT                                
        ,a.TOT_YTD_AMT_LN_REPYMT_RCVD             --TOT_YTD_AMT_LN_REPYMT_RCVD                       
        ,a.ON_BST_INT_RCVB                        --ON_BST_INT_RCVB                                  
        ,a.OFF_BST_INT_RCVB                       --OFF_BST_INT_RCVB                                 
        ,a.OFF_BST_INT_RCVB_WRTOF                 --OFF_BST_INT_RCVB_WRTOF                           
        ,a.OFF_BST_INT_RCVB_RPLC                  --OFF_BST_INT_RCVB_RPLC                            
        ,a.YTD_On_BST_INT_AMT_RCVD                --YTD_On_BST_INT_AMT_RCVD)  
 				------------------------Start on 20091216--------------------------
	      ,a.LN_AR_TP_ID                  	
	     	------------------------End on 20091216--------------------------			                                                                              
      FROM SESSION.TMP_CST_LN_BAL_MTHLY_SMY a LEFT JOIN SMY.CST_LN_BAL_MTHLY_SMY b
      ON a.CST_ID              = b.CST_ID AND
         a.LN_GNT_TP_ID        = b.LN_GNT_TP_ID AND
         a.TM_MAT_SEG_ID       = b.TM_MAT_SEG_ID AND
         a.LN_TERM_TP_ID       = b.LN_TERM_TP_ID AND
         a.LN_LCS_STS_TP_ID    = b.LN_LCS_STS_TP_ID AND
         a.LN_FIVE_RTG_STS     = b.LN_FIVE_RTG_STS AND
         a.LN_PPS_TP_ID        = b.LN_PPS_TP_ID AND
         a.IDY_CL_ID           = b.IDY_CL_ID AND
         a.LN_FNC_STS_TP_ID    = b.LN_FNC_STS_TP_ID AND
         a.LN_CGY_TP_ID        = b.LN_CGY_TP_ID AND
         a.FND_SRC_TP_ID       = b.FND_SRC_TP_ID AND
         a.PD_GRP_CD           = b.PD_GRP_CD AND
         a.PD_SUB_CD           = b.PD_SUB_CD AND
         a.AC_OU_ID            = b.AC_OU_ID AND
         a.CCY                 = b.CCY AND
         a.ALT_TP_ID           = b.ALT_TP_ID AND
         b.CDR_YR              = CUR_YEAR -1 AND
         b.CDR_MTH             = 12 AND
         a.LN_INVST_DIRC_TP_ID = b.LN_INVST_DIRC_TP_ID 
 				------------------------Start on 20091216--------------------------
	       and a.LN_AR_TP_ID         = b.LN_AR_TP_ID
	     	------------------------End on 20091216--------------------------			                                                                
         ;--
  
  ELSEIF CUR_MONTH IN (4, 7, 10) THEN
      SET SMY_STEPDESC = '插入季初数据';--
      INSERT INTO SMY.CST_LN_BAL_MTHLY_SMY
				(CST_ID
				,LN_GNT_TP_ID
				,TM_MAT_SEG_ID
				,LN_TERM_TP_ID
				,LN_LCS_STS_TP_ID
				,LN_FIVE_RTG_STS
				,LN_PPS_TP_ID
				,IDY_CL_ID
				,LN_FNC_STS_TP_ID
				,LN_CGY_TP_ID
				,FND_SRC_TP_ID
				,PD_GRP_CD
				,PD_SUB_CD
				,AC_OU_ID
				,LN_INVST_DIRC_TP_ID
				,ALT_TP_ID
				,CCY
				,CDR_YR
				,CDR_MTH
				,NOCLD_IN_MTH
				,NOD_IN_MTH
				,NOCLD_IN_QTR
				,NOD_IN_QTR
				,NOCLD_IN_YEAR
				,NOD_IN_YEAR
				,ACG_DT
				,ENT_IDV_IND
				,LN_BAL
				,LST_DAY_BAL
				,NBR_LN
				,ACR_INT_RCVD
				,ACR_INT_RCVB
				,MTD_ACML_LN_BAL_AMT
				,TOT_MTD_NBR_LN_DRDWNS
				,TOT_MTD_NBR_LN_REPYMT_RCVD
				,TOT_MTD_WRTOF_AMT_RCVD
				,TOT_MTD_WRTOF_AMT
				,TOT_MTD_AMT_RCVD_OF_AST_RPLC
				,TOT_MTD_LN_DRDWN_AMT
				,TOT_MTD_AMT_LN_REPYMT_RCVD
				,QTD_ACML_LN_BAL_AMT
				,TOT_QTD_NBR_LN_DRDWN
				,TOT_QTD_NBR_LN_RPYMT_RCVD
				,TOT_QTD_WRTOF_AMT_RCVD
				,TOT_QTD_AMT_RCVD_OF_AST_RPLC
				,TOT_QTD_LN_DRDWN_AMT
				,TOT_QTD_WRTOF_AMT
				,TOT_QTD_AMT_LN_RPYMT_RCVD
				,YTD_ACML_LN_BAL_AMT
				,TOT_YTD_NBR_LN_DRDWN
				,TOT_YTD_NBR_LN_REPYMT_RCVD
				,TOT_YTD_WRTOF_AMT_RCVD
				,TOT_YTD_AMT_RCVD_OF_AST_RPLC
				,TOT_YTD_LN_DRDWN_AMT
				,TOT_YTD_WRTOF_AMT
				,TOT_YTD_AMT_LN_REPYMT_RCVD
				,ON_BST_INT_RCVB
				,OFF_BST_INT_RCVB
				,OFF_BST_INT_RCVB_WRTOF
				,OFF_BST_INT_RCVB_RPLC
				,YTD_On_BST_INT_AMT_RCVD
 				------------------------Start on 20091216--------------------------
	      ,LN_AR_TP_ID                  	
	     	------------------------End on 20091216--------------------------			                                                       				
				)
			SELECT 
         a.CST_ID                             --CST_ID                            
        ,a.LN_GNT_TP_ID                       --LN_GNT_TP_ID                      
        ,a.TM_MAT_SEG_ID                      --TM_MAT_SEG_ID                     
        ,a.LN_TERM_TP_ID                      --LN_TERM_TP_ID                     
        ,a.LN_LCS_STS_TP_ID                   --LN_LCS_STS_TP_ID                  
        ,a.LN_FIVE_RTG_STS                    --LN_FIVE_RTG_STS                   
        ,a.LN_PPS_TP_ID                       --LN_PPS_TP_ID                      
        ,a.IDY_CL_ID                          --IDY_CL_ID                         
        ,a.LN_FNC_STS_TP_ID                   --LN_FNC_STS_TP_ID                  
        ,a.LN_CGY_TP_ID                       --LN_CGY_TP_ID                      
        ,a.FND_SRC_TP_ID                      --FND_SRC_TP_ID                     
        ,a.PD_GRP_CD                          --PD_GRP_CD                         
        ,a.PD_SUB_CD                          --PD_SUB_CD                         
        ,a.AC_OU_ID                           --AC_OU_ID                          
        ,a.LN_INVST_DIRC_TP_ID                --LN_INVST_DIRC_TP_ID               
        ,a.ALT_TP_ID                          --ALT_TP_ID                         
        ,a.CCY                                --CCY                               
        ,CUR_YEAR                             --CDR_YR                            
        ,CUR_MONTH                            --CDR_MTH                           
        ,MONTH_DAY                            --NOCLD_IN_MTH                      
        ,case when a.NBR_LN =0 then 0 else 1  end                                    --NOD_IN_MTH                        
        ,QTR_DAY                              --NOCLD_IN_QTR                      
        ,case when a.NBR_LN =0 then 0 else 1  end                                    --NOD_IN_QTR                        
        ,YR_DAY                               --NOCLD_IN_YEAR                     
        ,case when a.NBR_LN =0 then b.NOD_In_Year  else b.NOD_In_Year + 1   end                    --NOD_IN_YEAR                       
        ,SMY_DATE                             --ACG_DT                            
        ,a.ENT_IDV_IND                        --ENT_IDV_IND                       
        ,a.LN_BAL                             --LN_BAL                            
        ,COALESCE(b.LN_BAL,0.00)              --LST_DAY_BAL                       
        ,a.NBR_LN                             --NBR_LN                            
        ,a.ACR_INT_RCVD                       --ACR_INT_RCVD                      
        ,a.ACR_INT_RCVB                       --ACR_INT_RCVB                      
        ,a.MTD_ACML_LN_BAL_AMT                --MTD_ACML_LN_BAL_AMT               
        ,a.TOT_MTD_NBR_LN_DRDWNS              --TOT_MTD_NBR_LN_DRDWNS             
        ,a.TOT_MTD_NBR_LN_REPYMT_RCVD         --TOT_MTD_NBR_LN_REPYMT_RCVD        
        ,a.TOT_MTD_WRTOF_AMT_RCVD             --TOT_MTD_WRTOF_AMT_RCVD            
        ,a.TOT_MTD_WRTOF_AMT                  --TOT_MTD_WRTOF_AMT                 
        ,a.TOT_MTD_AMT_RCVD_OF_AST_RPLC       --TOT_MTD_AMT_RCVD_OF_AST_RPLC      
        ,a.TOT_MTD_LN_DRDWN_AMT               --TOT_MTD_LN_DRDWN_AMT              
        ,a.TOT_MTD_AMT_LN_REPYMT_RCVD         --TOT_MTD_AMT_LN_REPYMT_RCVD        
        ,a.QTD_ACML_LN_BAL_AMT                --QTD_ACML_LN_BAL_AMT               
        ,a.TOT_QTD_NBR_LN_DRDWN               --TOT_QTD_NBR_LN_DRDWN              
        ,a.TOT_QTD_NBR_LN_RPYMT_RCVD          --TOT_QTD_NBR_LN_RPYMT_RCVD         
        ,a.TOT_QTD_WRTOF_AMT_RCVD             --TOT_QTD_WRTOF_AMT_RCVD            
        ,a.TOT_QTD_AMT_RCVD_OF_AST_RPLC       --TOT_QTD_AMT_RCVD_OF_AST_RPLC      
        ,a.TOT_QTD_LN_DRDWN_AMT               --TOT_QTD_LN_DRDWN_AMT              
        ,a.TOT_QTD_WRTOF_AMT                  --TOT_QTD_WRTOF_AMT                 
        ,a.TOT_QTD_AMT_LN_RPYMT_RCVD          --TOT_QTD_AMT_LN_RPYMT_RCVD         
        ,a.YTD_ACML_LN_BAL_AMT                --YTD_ACML_LN_BAL_AMT               
        ,a.TOT_YTD_NBR_LN_DRDWN               --TOT_YTD_NBR_LN_DRDWN              
        ,a.TOT_YTD_NBR_LN_REPYMT_RCVD         --TOT_YTD_NBR_LN_REPYMT_RCVD        
        ,a.TOT_YTD_WRTOF_AMT_RCVD             --TOT_YTD_WRTOF_AMT_RCVD            
        ,a.TOT_YTD_AMT_RCVD_OF_AST_RPLC       --TOT_YTD_AMT_RCVD_OF_AST_RPLC      
        ,a.TOT_YTD_LN_DRDWN_AMT               --TOT_YTD_LN_DRDWN_AMT              
        ,a.TOT_YTD_WRTOF_AMT                  --TOT_YTD_WRTOF_AMT                 
        ,a.TOT_YTD_AMT_LN_REPYMT_RCVD         --TOT_YTD_AMT_LN_REPYMT_RCVD        
        ,a.ON_BST_INT_RCVB                    --ON_BST_INT_RCVB                   
        ,a.OFF_BST_INT_RCVB                   --OFF_BST_INT_RCVB                  
        ,a.OFF_BST_INT_RCVB_WRTOF             --OFF_BST_INT_RCVB_WRTOF            
        ,a.OFF_BST_INT_RCVB_RPLC              --OFF_BST_INT_RCVB_RPLC             
        ,a.YTD_On_BST_INT_AMT_RCVD            --YTD_On_BST_INT_AMT_RCVD)          
 				------------------------Start on 20091216--------------------------
	      ,a.LN_AR_TP_ID                  	
	     	------------------------End on 20091216--------------------------			                                                               
      FROM SESSION.TMP_CST_LN_BAL_MTHLY_SMY a LEFT JOIN SMY.CST_LN_BAL_MTHLY_SMY b
      ON a.CST_ID              = b.CST_ID AND
         a.LN_GNT_TP_ID        = b.LN_GNT_TP_ID AND
         a.TM_MAT_SEG_ID       = b.TM_MAT_SEG_ID AND
         a.LN_TERM_TP_ID       = b.LN_TERM_TP_ID AND
         a.LN_LCS_STS_TP_ID    = b.LN_LCS_STS_TP_ID AND
         a.LN_FIVE_RTG_STS     = b.LN_FIVE_RTG_STS AND
         a.LN_PPS_TP_ID        = b.LN_PPS_TP_ID AND
         a.IDY_CL_ID           = b.IDY_CL_ID AND
         a.LN_FNC_STS_TP_ID    = b.LN_FNC_STS_TP_ID AND
         a.LN_CGY_TP_ID        = b.LN_CGY_TP_ID AND
         a.FND_SRC_TP_ID       = b.FND_SRC_TP_ID AND
         a.PD_GRP_CD           = b.PD_GRP_CD AND
         a.PD_SUB_CD           = b.PD_SUB_CD AND
         a.AC_OU_ID            = b.AC_OU_ID AND
         a.CCY                 = b.CCY AND
         a.ALT_TP_ID           = b.ALT_TP_ID AND
         b.CDR_YR              = CUR_YEAR  AND
         b.CDR_MTH             = CUR_MONTH - 1 AND
         a.LN_INVST_DIRC_TP_ID = b.LN_INVST_DIRC_TP_ID  
         ------------------------Start on 20091216--------------------------
	       and a.LN_AR_TP_ID         = b.LN_AR_TP_ID
	     	------------------------End on 20091216--------------------------			                                                                
         ;--
  
    ELSE
    	SET SMY_STEPDESC = '插入非年初或季初的月初数据';--
      INSERT INTO SMY.CST_LN_BAL_MTHLY_SMY
				(CST_ID                           
				,LN_GNT_TP_ID                     
				,TM_MAT_SEG_ID                    
				,LN_TERM_TP_ID                    
				,LN_LCS_STS_TP_ID                 
				,LN_FIVE_RTG_STS                  
				,LN_PPS_TP_ID                     
				,IDY_CL_ID                        
				,LN_FNC_STS_TP_ID                 
				,LN_CGY_TP_ID                     
				,FND_SRC_TP_ID                    
				,PD_GRP_CD                        
				,PD_SUB_CD                        
				,AC_OU_ID                         
				,LN_INVST_DIRC_TP_ID              
				,ALT_TP_ID                        
				,CCY                              
				,CDR_YR                           
				,CDR_MTH                          
				,NOCLD_IN_MTH                     
				,NOD_IN_MTH                       
				,NOCLD_IN_QTR                     
				,NOD_IN_QTR                       
				,NOCLD_IN_YEAR                    
				,NOD_IN_YEAR                      
				,ACG_DT                           
				,ENT_IDV_IND                      
				,LN_BAL                           
				,LST_DAY_BAL                      
				,NBR_LN                           
				,ACR_INT_RCVD                     
				,ACR_INT_RCVB                     
				,MTD_ACML_LN_BAL_AMT              
				,TOT_MTD_NBR_LN_DRDWNS            
				,TOT_MTD_NBR_LN_REPYMT_RCVD       
				,TOT_MTD_WRTOF_AMT_RCVD           
				,TOT_MTD_WRTOF_AMT                
				,TOT_MTD_AMT_RCVD_OF_AST_RPLC     
				,TOT_MTD_LN_DRDWN_AMT             
				,TOT_MTD_AMT_LN_REPYMT_RCVD       
				,QTD_ACML_LN_BAL_AMT              
				,TOT_QTD_NBR_LN_DRDWN             
				,TOT_QTD_NBR_LN_RPYMT_RCVD        
				,TOT_QTD_WRTOF_AMT_RCVD           
				,TOT_QTD_AMT_RCVD_OF_AST_RPLC     
				,TOT_QTD_LN_DRDWN_AMT             
				,TOT_QTD_WRTOF_AMT                
				,TOT_QTD_AMT_LN_RPYMT_RCVD        
				,YTD_ACML_LN_BAL_AMT              
				,TOT_YTD_NBR_LN_DRDWN             
				,TOT_YTD_NBR_LN_REPYMT_RCVD       
				,TOT_YTD_WRTOF_AMT_RCVD           
				,TOT_YTD_AMT_RCVD_OF_AST_RPLC     
				,TOT_YTD_LN_DRDWN_AMT             
				,TOT_YTD_WRTOF_AMT                
				,TOT_YTD_AMT_LN_REPYMT_RCVD       
				,ON_BST_INT_RCVB                  
				,OFF_BST_INT_RCVB                 
				,OFF_BST_INT_RCVB_WRTOF           
				,OFF_BST_INT_RCVB_RPLC            
				,YTD_On_BST_INT_AMT_RCVD 
				------------------------Start on 20091216--------------------------
	      ,LN_AR_TP_ID  
	     	------------------------End on 20091216--------------------------			                                                                				         
			)
			SELECT 
         a.CST_ID                              --CST_ID                             
        ,a.LN_GNT_TP_ID                        --LN_GNT_TP_ID                       
        ,a.TM_MAT_SEG_ID                       --TM_MAT_SEG_ID                      
        ,a.LN_TERM_TP_ID                       --LN_TERM_TP_ID                      
        ,a.LN_LCS_STS_TP_ID                    --LN_LCS_STS_TP_ID                   
        ,a.LN_FIVE_RTG_STS                     --LN_FIVE_RTG_STS                    
        ,a.LN_PPS_TP_ID                        --LN_PPS_TP_ID                       
        ,a.IDY_CL_ID                           --IDY_CL_ID                          
        ,a.LN_FNC_STS_TP_ID                    --LN_FNC_STS_TP_ID                   
        ,a.LN_CGY_TP_ID                        --LN_CGY_TP_ID                       
        ,a.FND_SRC_TP_ID                       --FND_SRC_TP_ID                      
        ,a.PD_GRP_CD                           --PD_GRP_CD                          
        ,a.PD_SUB_CD                           --PD_SUB_CD                          
        ,a.AC_OU_ID                            --AC_OU_ID                           
        ,a.LN_INVST_DIRC_TP_ID                 --LN_INVST_DIRC_TP_ID                
        ,a.ALT_TP_ID                           --ALT_TP_ID                          
        ,a.CCY                                 --CCY                                
        ,CUR_YEAR                              --CDR_YR                             
        ,CUR_MONTH                             --CDR_MTH                            
        ,MONTH_DAY                             --NOCLD_IN_MTH                       
        ,case when a.NBR_LN = 0 then 0 else 1  end                                      --NOD_IN_MTH                         
        ,QTR_DAY                               --NOCLD_IN_QTR                       
        ,case when a.NBR_LN = 0 then b.NOD_In_QTR  else b.NOD_In_QTR + 1  end                       --NOD_IN_QTR                         
        ,YR_DAY                                --NOCLD_IN_YEAR                      
        ,case when a.NBR_LN = 0 then b.NOD_In_Year else b.NOD_In_Year + 1 end                      --NOD_IN_YEAR                        
        ,SMY_DATE                              --ACG_DT                             
        ,a.ENT_IDV_IND                         --ENT_IDV_IND                        
        ,a.LN_BAL                              --LN_BAL                             
        ,COALESCE(b.LN_BAL,0.00)               --LST_DAY_BAL                        
        ,a.NBR_LN                              --NBR_LN                             
        ,a.ACR_INT_RCVD                        --ACR_INT_RCVD                       
        ,a.ACR_INT_RCVB                        --ACR_INT_RCVB                       
        ,a.MTD_ACML_LN_BAL_AMT                 --MTD_ACML_LN_BAL_AMT                
        ,a.TOT_MTD_NBR_LN_DRDWNS               --TOT_MTD_NBR_LN_DRDWNS              
        ,a.TOT_MTD_NBR_LN_REPYMT_RCVD          --TOT_MTD_NBR_LN_REPYMT_RCVD         
        ,a.TOT_MTD_WRTOF_AMT_RCVD              --TOT_MTD_WRTOF_AMT_RCVD             
        ,a.TOT_MTD_WRTOF_AMT                   --TOT_MTD_WRTOF_AMT                  
        ,a.TOT_MTD_AMT_RCVD_OF_AST_RPLC        --TOT_MTD_AMT_RCVD_OF_AST_RPLC       
        ,a.TOT_MTD_LN_DRDWN_AMT                --TOT_MTD_LN_DRDWN_AMT               
        ,a.TOT_MTD_AMT_LN_REPYMT_RCVD          --TOT_MTD_AMT_LN_REPYMT_RCVD         
        ,a.QTD_ACML_LN_BAL_AMT                 --QTD_ACML_LN_BAL_AMT                
        ,a.TOT_QTD_NBR_LN_DRDWN                --TOT_QTD_NBR_LN_DRDWN               
        ,a.TOT_QTD_NBR_LN_RPYMT_RCVD           --TOT_QTD_NBR_LN_RPYMT_RCVD          
        ,a.TOT_QTD_WRTOF_AMT_RCVD              --TOT_QTD_WRTOF_AMT_RCVD             
        ,a.TOT_QTD_AMT_RCVD_OF_AST_RPLC        --TOT_QTD_AMT_RCVD_OF_AST_RPLC       
        ,a.TOT_QTD_LN_DRDWN_AMT                --TOT_QTD_LN_DRDWN_AMT               
        ,a.TOT_QTD_WRTOF_AMT                   --TOT_QTD_WRTOF_AMT                  
        ,a.TOT_QTD_AMT_LN_RPYMT_RCVD           --TOT_QTD_AMT_LN_RPYMT_RCVD          
        ,a.YTD_ACML_LN_BAL_AMT                 --YTD_ACML_LN_BAL_AMT                
        ,a.TOT_YTD_NBR_LN_DRDWN                --TOT_YTD_NBR_LN_DRDWN               
        ,a.TOT_YTD_NBR_LN_REPYMT_RCVD          --TOT_YTD_NBR_LN_REPYMT_RCVD         
        ,a.TOT_YTD_WRTOF_AMT_RCVD              --TOT_YTD_WRTOF_AMT_RCVD             
        ,a.TOT_YTD_AMT_RCVD_OF_AST_RPLC        --TOT_YTD_AMT_RCVD_OF_AST_RPLC       
        ,a.TOT_YTD_LN_DRDWN_AMT                --TOT_YTD_LN_DRDWN_AMT               
        ,a.TOT_YTD_WRTOF_AMT                   --TOT_YTD_WRTOF_AMT                  
        ,a.TOT_YTD_AMT_LN_REPYMT_RCVD          --TOT_YTD_AMT_LN_REPYMT_RCVD         
        ,a.ON_BST_INT_RCVB                     --ON_BST_INT_RCVB                    
        ,a.OFF_BST_INT_RCVB                    --OFF_BST_INT_RCVB                   
        ,a.OFF_BST_INT_RCVB_WRTOF              --OFF_BST_INT_RCVB_WRTOF             
        ,a.OFF_BST_INT_RCVB_RPLC               --OFF_BST_INT_RCVB_RPLC              
        ,a.YTD_On_BST_INT_AMT_RCVD             --YTD_On_BST_INT_AMT_RCVD)           
				------------------------Start on 20091216--------------------------
	      ,a.LN_AR_TP_ID        
	     	------------------------End on 20091216--------------------------			                                                                        
      FROM SESSION.TMP_CST_LN_BAL_MTHLY_SMY a LEFT JOIN SMY.CST_LN_BAL_MTHLY_SMY b
      ON a.CST_ID              = b.CST_ID AND
         a.LN_GNT_TP_ID        = b.LN_GNT_TP_ID AND
         a.TM_MAT_SEG_ID       = b.TM_MAT_SEG_ID AND
         a.LN_TERM_TP_ID       = b.LN_TERM_TP_ID AND
         a.LN_LCS_STS_TP_ID    = b.LN_LCS_STS_TP_ID AND
         a.LN_FIVE_RTG_STS     = b.LN_FIVE_RTG_STS AND
         a.LN_PPS_TP_ID        = b.LN_PPS_TP_ID AND
         a.IDY_CL_ID           = b.IDY_CL_ID AND
         a.LN_FNC_STS_TP_ID    = b.LN_FNC_STS_TP_ID AND
         a.LN_CGY_TP_ID        = b.LN_CGY_TP_ID AND
         a.FND_SRC_TP_ID       = b.FND_SRC_TP_ID AND
         a.PD_GRP_CD           = b.PD_GRP_CD AND
         a.PD_SUB_CD           = b.PD_SUB_CD AND
         a.AC_OU_ID            = b.AC_OU_ID AND
         a.CCY                 = b.CCY AND
         a.ALT_TP_ID           = b.ALT_TP_ID AND
         b.CDR_YR              = CUR_YEAR  AND
         b.CDR_MTH             = CUR_MONTH - 1 AND
         a.LN_INVST_DIRC_TP_ID = b.LN_INVST_DIRC_TP_ID     	
				------------------------Start on 20091216--------------------------
	       and a.LN_AR_TP_ID         = b.LN_AR_TP_ID
	     	------------------------End on 20091216--------------------------			                                                                         
         ;--
    	
  END IF;--

ELSE
	SET SMY_STEPDESC = 'merge非月初数据';--
  MERGE INTO SMY.CST_LN_BAL_MTHLY_SMY TAG
  USING SESSION.TMP_CST_LN_BAL_MTHLY_SMY SOC
  ON     TAG.CST_ID              = SOC.CST_ID AND
         TAG.LN_GNT_TP_ID        = SOC.LN_GNT_TP_ID AND
         TAG.TM_MAT_SEG_ID       = SOC.TM_MAT_SEG_ID AND
         TAG.LN_TERM_TP_ID       = SOC.LN_TERM_TP_ID AND
         TAG.LN_LCS_STS_TP_ID    = SOC.LN_LCS_STS_TP_ID AND
         TAG.LN_FIVE_RTG_STS     = SOC.LN_FIVE_RTG_STS AND
         TAG.LN_PPS_TP_ID        = SOC.LN_PPS_TP_ID AND
         TAG.IDY_CL_ID           = SOC.IDY_CL_ID AND
         TAG.LN_FNC_STS_TP_ID    = SOC.LN_FNC_STS_TP_ID AND
         TAG.LN_CGY_TP_ID        = SOC.LN_CGY_TP_ID AND
         TAG.FND_SRC_TP_ID       = SOC.FND_SRC_TP_ID AND
         TAG.PD_GRP_CD           = SOC.PD_GRP_CD AND
         TAG.PD_SUB_CD           = SOC.PD_SUB_CD AND
         TAG.AC_OU_ID            = SOC.AC_OU_ID AND
         TAG.CCY                 = SOC.CCY AND
         TAG.ALT_TP_ID           = SOC.ALT_TP_ID AND
         TAG.CDR_YR              = CUR_YEAR  AND
         TAG.CDR_MTH             = CUR_MONTH AND
         TAG.LN_INVST_DIRC_TP_ID = SOC.LN_INVST_DIRC_TP_ID 
         AND TAG.LN_AR_TP_ID     = SOC.LN_AR_TP_ID       
	WHEN MATCHED THEN
	     UPDATE SET 
				(CST_ID                                   
				,LN_GNT_TP_ID                             
				,TM_MAT_SEG_ID                            
				,LN_TERM_TP_ID                            
				,LN_LCS_STS_TP_ID                         
				,LN_FIVE_RTG_STS                          
				,LN_PPS_TP_ID                             
				,IDY_CL_ID                                
				,LN_FNC_STS_TP_ID                         
				,LN_CGY_TP_ID                             
				,FND_SRC_TP_ID                            
				,PD_GRP_CD                                
				,PD_SUB_CD                                
				,AC_OU_ID                                 
				,LN_INVST_DIRC_TP_ID                      
				,ALT_TP_ID                                
				,CCY                                      
				,CDR_YR                                   
				,CDR_MTH                                  
				,NOCLD_IN_MTH                             
				,NOD_IN_MTH                               
				,NOCLD_IN_QTR                             
				,NOD_IN_QTR                               
				,NOCLD_IN_YEAR                            
				,NOD_IN_YEAR                              
				,ACG_DT                                   
				,ENT_IDV_IND                              
				,LN_BAL                                   
				,LST_DAY_BAL                              
				,NBR_LN                                   
				,ACR_INT_RCVD                             
				,ACR_INT_RCVB                             
				,MTD_ACML_LN_BAL_AMT                      
				,TOT_MTD_NBR_LN_DRDWNS                    
				,TOT_MTD_NBR_LN_REPYMT_RCVD               
				,TOT_MTD_WRTOF_AMT_RCVD                   
				,TOT_MTD_WRTOF_AMT                        
				,TOT_MTD_AMT_RCVD_OF_AST_RPLC             
				,TOT_MTD_LN_DRDWN_AMT                     
				,TOT_MTD_AMT_LN_REPYMT_RCVD               
				,QTD_ACML_LN_BAL_AMT                      
				,TOT_QTD_NBR_LN_DRDWN                     
				,TOT_QTD_NBR_LN_RPYMT_RCVD                
				,TOT_QTD_WRTOF_AMT_RCVD                   
				,TOT_QTD_AMT_RCVD_OF_AST_RPLC             
				,TOT_QTD_LN_DRDWN_AMT                     
				,TOT_QTD_WRTOF_AMT                        
				,TOT_QTD_AMT_LN_RPYMT_RCVD                
				,YTD_ACML_LN_BAL_AMT                      
				,TOT_YTD_NBR_LN_DRDWN                     
				,TOT_YTD_NBR_LN_REPYMT_RCVD               
				,TOT_YTD_WRTOF_AMT_RCVD                   
				,TOT_YTD_AMT_RCVD_OF_AST_RPLC             
				,TOT_YTD_LN_DRDWN_AMT                     
				,TOT_YTD_WRTOF_AMT                        
				,TOT_YTD_AMT_LN_REPYMT_RCVD               
				,ON_BST_INT_RCVB                          
				,OFF_BST_INT_RCVB                         
				,OFF_BST_INT_RCVB_WRTOF                   
				,OFF_BST_INT_RCVB_RPLC                    
				,YTD_On_BST_INT_AMT_RCVD                				
			)
			=	(TAG.CST_ID                                     --CST_ID                         
        ,TAG.LN_GNT_TP_ID                               --LN_GNT_TP_ID                   
        ,TAG.TM_MAT_SEG_ID                              --TM_MAT_SEG_ID                  
        ,TAG.LN_TERM_TP_ID                              --LN_TERM_TP_ID                  
        ,TAG.LN_LCS_STS_TP_ID                           --LN_LCS_STS_TP_ID               
        ,TAG.LN_FIVE_RTG_STS                            --LN_FIVE_RTG_STS                
        ,TAG.LN_PPS_TP_ID                               --LN_PPS_TP_ID                   
        ,TAG.IDY_CL_ID                                  --IDY_CL_ID                      
        ,TAG.LN_FNC_STS_TP_ID                           --LN_FNC_STS_TP_ID               
        ,TAG.LN_CGY_TP_ID                               --LN_CGY_TP_ID                   
        ,TAG.FND_SRC_TP_ID                              --FND_SRC_TP_ID                  
        ,TAG.PD_GRP_CD                                  --PD_GRP_CD                      
        ,TAG.PD_SUB_CD                                  --PD_SUB_CD                      
        ,TAG.AC_OU_ID                                   --AC_OU_ID                       
        ,TAG.LN_INVST_DIRC_TP_ID                        --LN_INVST_DIRC_TP_ID            
        ,TAG.ALT_TP_ID                                  --ALT_TP_ID                      
        ,TAG.CCY                                        --CCY                            
        ,CUR_YEAR                                       --CDR_YR                         
        ,CUR_MONTH                                      --CDR_MTH                        
        ,MONTH_DAY                                      --NOCLD_IN_MTH                   
        --,TAG.NOCLD_IN_MTH + 1                         
        ,case when SOC.NBR_LN = 0 then TAG.NOD_IN_MTH else TAG.NOD_IN_MTH + 1  end                              --NOD_IN_MTH                     
        ,QTR_DAY                                        --NOCLD_IN_QTR                   
        ,case when SOC.NBR_LN = 0 then TAG.NOD_In_QTR else TAG.NOD_In_QTR + 1  end                             --NOD_IN_QTR                     
        ,YR_DAY                                         --NOCLD_IN_YEAR                  
        ,case when SOC.NBR_LN = 0 then TAG.NOD_In_Year else TAG.NOD_In_Year + 1  end                            --NOD_IN_YEAR                    
        ,SMY_DATE                                       --ACG_DT                         
        ,TAG.ENT_IDV_IND                                --ENT_IDV_IND                    
        ,SOC.LN_BAL                                     --LN_BAL                         
        ,TAG.LN_BAL                                     --LST_DAY_BAL                    
        ,SOC.NBR_LN                                     --NBR_LN                         
        ,SOC.ACR_INT_RCVD                               --ACR_INT_RCVD                   
        ,SOC.ACR_INT_RCVB                               --ACR_INT_RCVB                   
        ,SOC.MTD_ACML_LN_BAL_AMT                        --MTD_ACML_LN_BAL_AMT            
        ,SOC.TOT_MTD_NBR_LN_DRDWNS                      --TOT_MTD_NBR_LN_DRDWNS          
        ,SOC.TOT_MTD_NBR_LN_REPYMT_RCVD                 --TOT_MTD_NBR_LN_REPYMT_RCVD     
        ,SOC.TOT_MTD_WRTOF_AMT_RCVD                     --TOT_MTD_WRTOF_AMT_RCVD         
        ,SOC.TOT_MTD_WRTOF_AMT                          --TOT_MTD_WRTOF_AMT              
        ,SOC.TOT_MTD_AMT_RCVD_OF_AST_RPLC               --TOT_MTD_AMT_RCVD_OF_AST_RPLC   
        ,SOC.TOT_MTD_LN_DRDWN_AMT                       --TOT_MTD_LN_DRDWN_AMT           
        ,SOC.TOT_MTD_AMT_LN_REPYMT_RCVD                 --TOT_MTD_AMT_LN_REPYMT_RCVD     
        ,SOC.QTD_ACML_LN_BAL_AMT                        --QTD_ACML_LN_BAL_AMT            
        ,SOC.TOT_QTD_NBR_LN_DRDWN                       --TOT_QTD_NBR_LN_DRDWN           
        ,SOC.TOT_QTD_NBR_LN_RPYMT_RCVD                  --TOT_QTD_NBR_LN_RPYMT_RCVD      
        ,SOC.TOT_QTD_WRTOF_AMT_RCVD                     --TOT_QTD_WRTOF_AMT_RCVD         
        ,SOC.TOT_QTD_AMT_RCVD_OF_AST_RPLC               --TOT_QTD_AMT_RCVD_OF_AST_RPLC   
        ,SOC.TOT_QTD_LN_DRDWN_AMT                       --TOT_QTD_LN_DRDWN_AMT           
        ,SOC.TOT_QTD_WRTOF_AMT                          --TOT_QTD_WRTOF_AMT              
        ,SOC.TOT_QTD_AMT_LN_RPYMT_RCVD                  --TOT_QTD_AMT_LN_RPYMT_RCVD      
        ,SOC.YTD_ACML_LN_BAL_AMT                        --YTD_ACML_LN_BAL_AMT            
        ,SOC.TOT_YTD_NBR_LN_DRDWN                       --TOT_YTD_NBR_LN_DRDWN           
        ,SOC.TOT_YTD_NBR_LN_REPYMT_RCVD                 --TOT_YTD_NBR_LN_REPYMT_RCVD     
        ,SOC.TOT_YTD_WRTOF_AMT_RCVD                     --TOT_YTD_WRTOF_AMT_RCVD         
        ,SOC.TOT_YTD_AMT_RCVD_OF_AST_RPLC               --TOT_YTD_AMT_RCVD_OF_AST_RPLC   
        ,SOC.TOT_YTD_LN_DRDWN_AMT                       --TOT_YTD_LN_DRDWN_AMT           
        ,SOC.TOT_YTD_WRTOF_AMT                          --TOT_YTD_WRTOF_AMT              
        ,SOC.TOT_YTD_AMT_LN_REPYMT_RCVD                 --TOT_YTD_AMT_LN_REPYMT_RCVD     
        ,SOC.ON_BST_INT_RCVB                            --ON_BST_INT_RCVB                
        ,SOC.OFF_BST_INT_RCVB                           --OFF_BST_INT_RCVB               
        ,SOC.OFF_BST_INT_RCVB_WRTOF                     --OFF_BST_INT_RCVB_WRTOF         
        ,SOC.OFF_BST_INT_RCVB_RPLC                      --OFF_BST_INT_RCVB_RPLC          
        ,SOC.YTD_On_BST_INT_AMT_RCVD                    --YTD_On_BST_INT_AMT_RCVD				
       )        
;
  INSERT INTO SMY.CST_LN_BAL_MTHLY_SMY
  (
        CST_ID
				,LN_GNT_TP_ID
				,TM_MAT_SEG_ID
				,LN_TERM_TP_ID
				,LN_LCS_STS_TP_ID
				,LN_FIVE_RTG_STS
				,LN_PPS_TP_ID
				,IDY_CL_ID
				,LN_FNC_STS_TP_ID
				,LN_CGY_TP_ID
				,FND_SRC_TP_ID
				,PD_GRP_CD
				,PD_SUB_CD
				,AC_OU_ID
				,LN_INVST_DIRC_TP_ID
				,ALT_TP_ID
				,CCY
				,CDR_YR
				,CDR_MTH
				,NOCLD_IN_MTH
				,NOD_IN_MTH
				,NOCLD_IN_QTR
				,NOD_IN_QTR
				,NOCLD_IN_YEAR
				,NOD_IN_YEAR
				,ACG_DT
				,ENT_IDV_IND
				,LN_BAL
				,LST_DAY_BAL
				,NBR_LN
				,ACR_INT_RCVD
				,ACR_INT_RCVB
				,MTD_ACML_LN_BAL_AMT
				,TOT_MTD_NBR_LN_DRDWNS
				,TOT_MTD_NBR_LN_REPYMT_RCVD
				,TOT_MTD_WRTOF_AMT_RCVD
				,TOT_MTD_WRTOF_AMT
				,TOT_MTD_AMT_RCVD_OF_AST_RPLC
				,TOT_MTD_LN_DRDWN_AMT
				,TOT_MTD_AMT_LN_REPYMT_RCVD
				,QTD_ACML_LN_BAL_AMT
				,TOT_QTD_NBR_LN_DRDWN
				,TOT_QTD_NBR_LN_RPYMT_RCVD
				,TOT_QTD_WRTOF_AMT_RCVD
				,TOT_QTD_AMT_RCVD_OF_AST_RPLC
				,TOT_QTD_LN_DRDWN_AMT
				,TOT_QTD_WRTOF_AMT
				,TOT_QTD_AMT_LN_RPYMT_RCVD
				,YTD_ACML_LN_BAL_AMT
				,TOT_YTD_NBR_LN_DRDWN
				,TOT_YTD_NBR_LN_REPYMT_RCVD
				,TOT_YTD_WRTOF_AMT_RCVD
				,TOT_YTD_AMT_RCVD_OF_AST_RPLC
				,TOT_YTD_LN_DRDWN_AMT
				,TOT_YTD_WRTOF_AMT
				,TOT_YTD_AMT_LN_REPYMT_RCVD
				,ON_BST_INT_RCVB
				,OFF_BST_INT_RCVB
				,OFF_BST_INT_RCVB_WRTOF
				,OFF_BST_INT_RCVB_RPLC
				,YTD_On_BST_INT_AMT_RCVD  
	      ,LN_AR_TP_ID      
  )
  SELECT
        SOC.CST_ID
        ,SOC.LN_GNT_TP_ID
        ,SOC.TM_MAT_SEG_ID
        ,SOC.LN_TERM_TP_ID
        ,SOC.LN_LCS_STS_TP_ID
        ,SOC.LN_FIVE_RTG_STS
        ,SOC.LN_PPS_TP_ID
        ,SOC.IDY_CL_ID
        ,SOC.LN_FNC_STS_TP_ID
        ,SOC.LN_CGY_TP_ID
        ,SOC.FND_SRC_TP_ID
        ,SOC.PD_GRP_CD
        ,SOC.PD_SUB_CD
        ,SOC.AC_OU_ID
        ,SOC.LN_INVST_DIRC_TP_ID
        ,SOC.ALT_TP_ID
        ,SOC.CCY
        ,CUR_YEAR
        ,CUR_MONTH
        ,MONTH_DAY
        ,case when SOC.NBR_LN = 0 then 0 else 1  end  
        ,QTR_DAY
        ,case when SOC.NBR_LN = 0 then 0 else 1  end  
        ,YR_DAY
        ,case when SOC.NBR_LN = 0 then 0 else 1  end  
        ,SMY_DATE
        ,SOC.ENT_IDV_IND
        ,SOC.LN_BAL
        ,0.00
        ,SOC.NBR_LN
        ,SOC.ACR_INT_RCVD
        ,SOC.ACR_INT_RCVB
        ,SOC.MTD_ACML_LN_BAL_AMT
        ,SOC.TOT_MTD_NBR_LN_DRDWNS
        ,SOC.TOT_MTD_NBR_LN_REPYMT_RCVD
        ,SOC.TOT_MTD_WRTOF_AMT_RCVD
        ,SOC.TOT_MTD_WRTOF_AMT
        ,SOC.TOT_MTD_AMT_RCVD_OF_AST_RPLC
        ,SOC.TOT_MTD_LN_DRDWN_AMT
        ,SOC.TOT_MTD_AMT_LN_REPYMT_RCVD
        ,SOC.QTD_ACML_LN_BAL_AMT
        ,SOC.TOT_QTD_NBR_LN_DRDWN
        ,SOC.TOT_QTD_NBR_LN_RPYMT_RCVD
        ,SOC.TOT_QTD_WRTOF_AMT_RCVD
        ,SOC.TOT_QTD_AMT_RCVD_OF_AST_RPLC
        ,SOC.TOT_QTD_LN_DRDWN_AMT
        ,SOC.TOT_QTD_WRTOF_AMT
        ,SOC.TOT_QTD_AMT_LN_RPYMT_RCVD
        ,SOC.YTD_ACML_LN_BAL_AMT
        ,SOC.TOT_YTD_NBR_LN_DRDWN
        ,SOC.TOT_YTD_NBR_LN_REPYMT_RCVD
        ,SOC.TOT_YTD_WRTOF_AMT_RCVD
        ,SOC.TOT_YTD_AMT_RCVD_OF_AST_RPLC
        ,SOC.TOT_YTD_LN_DRDWN_AMT
        ,SOC.TOT_YTD_WRTOF_AMT
        ,SOC.TOT_YTD_AMT_LN_REPYMT_RCVD
        ,SOC.ON_BST_INT_RCVB
        ,SOC.OFF_BST_INT_RCVB
        ,SOC.OFF_BST_INT_RCVB_WRTOF
        ,SOC.OFF_BST_INT_RCVB_RPLC
        ,SOC.YTD_On_BST_INT_AMT_RCVD   
	      ,SOC.LN_AR_TP_ID      
  FROM SESSION.TMP_CST_LN_BAL_MTHLY_SMY as SOC
    WHERE NOT EXISTS (
         SELECT 1 FROM SMY.CST_LN_BAL_MTHLY_SMY TAG
         WHERE TAG.CST_ID              = SOC.CST_ID AND
               TAG.LN_GNT_TP_ID        = SOC.LN_GNT_TP_ID AND
               TAG.TM_MAT_SEG_ID       = SOC.TM_MAT_SEG_ID AND
               TAG.LN_TERM_TP_ID       = SOC.LN_TERM_TP_ID AND
               TAG.LN_LCS_STS_TP_ID    = SOC.LN_LCS_STS_TP_ID AND
               TAG.LN_FIVE_RTG_STS     = SOC.LN_FIVE_RTG_STS AND
               TAG.LN_PPS_TP_ID        = SOC.LN_PPS_TP_ID AND
               TAG.IDY_CL_ID           = SOC.IDY_CL_ID AND
               TAG.LN_FNC_STS_TP_ID    = SOC.LN_FNC_STS_TP_ID AND
               TAG.LN_CGY_TP_ID        = SOC.LN_CGY_TP_ID AND
               TAG.FND_SRC_TP_ID       = SOC.FND_SRC_TP_ID AND
               TAG.PD_GRP_CD           = SOC.PD_GRP_CD AND
               TAG.PD_SUB_CD           = SOC.PD_SUB_CD AND
               TAG.AC_OU_ID            = SOC.AC_OU_ID AND
               TAG.CCY                 = SOC.CCY AND
               TAG.ALT_TP_ID           = SOC.ALT_TP_ID AND
               TAG.CDR_YR              = CUR_YEAR  AND
               TAG.CDR_MTH             = CUR_MONTH AND
               TAG.LN_INVST_DIRC_TP_ID = SOC.LN_INVST_DIRC_TP_ID AND 
               TAG.LN_AR_TP_ID     = SOC.LN_AR_TP_ID
    )
	;
/*
  WHEN NOT MATCHED THEN
  INSERT
				(CST_ID
				,LN_GNT_TP_ID
				,TM_MAT_SEG_ID
				,LN_TERM_TP_ID
				,LN_LCS_STS_TP_ID
				,LN_FIVE_RTG_STS
				,LN_PPS_TP_ID
				,IDY_CL_ID
				,LN_FNC_STS_TP_ID
				,LN_CGY_TP_ID
				,FND_SRC_TP_ID
				,PD_GRP_CD
				,PD_SUB_CD
				,AC_OU_ID
				,LN_INVST_DIRC_TP_ID
				,ALT_TP_ID
				,CCY
				,CDR_YR
				,CDR_MTH
				,NOCLD_IN_MTH
				,NOD_IN_MTH
				,NOCLD_IN_QTR
				,NOD_IN_QTR
				,NOCLD_IN_YEAR
				,NOD_IN_YEAR
				,ACG_DT
				,ENT_IDV_IND
				,LN_BAL
				,LST_DAY_BAL
				,NBR_LN
				,ACR_INT_RCVD
				,ACR_INT_RCVB
				,MTD_ACML_LN_BAL_AMT
				,TOT_MTD_NBR_LN_DRDWNS
				,TOT_MTD_NBR_LN_REPYMT_RCVD
				,TOT_MTD_WRTOF_AMT_RCVD
				,TOT_MTD_WRTOF_AMT
				,TOT_MTD_AMT_RCVD_OF_AST_RPLC
				,TOT_MTD_LN_DRDWN_AMT
				,TOT_MTD_AMT_LN_REPYMT_RCVD
				,QTD_ACML_LN_BAL_AMT
				,TOT_QTD_NBR_LN_DRDWN
				,TOT_QTD_NBR_LN_RPYMT_RCVD
				,TOT_QTD_WRTOF_AMT_RCVD
				,TOT_QTD_AMT_RCVD_OF_AST_RPLC
				,TOT_QTD_LN_DRDWN_AMT
				,TOT_QTD_WRTOF_AMT
				,TOT_QTD_AMT_LN_RPYMT_RCVD
				,YTD_ACML_LN_BAL_AMT
				,TOT_YTD_NBR_LN_DRDWN
				,TOT_YTD_NBR_LN_REPYMT_RCVD
				,TOT_YTD_WRTOF_AMT_RCVD
				,TOT_YTD_AMT_RCVD_OF_AST_RPLC
				,TOT_YTD_LN_DRDWN_AMT
				,TOT_YTD_WRTOF_AMT
				,TOT_YTD_AMT_LN_REPYMT_RCVD
				,ON_BST_INT_RCVB
				,OFF_BST_INT_RCVB
				,OFF_BST_INT_RCVB_WRTOF
				,OFF_BST_INT_RCVB_RPLC
				,YTD_On_BST_INT_AMT_RCVD  
				------------------------Start on 20091216--------------------------
	      ,LN_AR_TP_ID      
	     	------------------------End on 20091216--------------------------			                                                                								
				)
	VALUES  	         
        (SOC.CST_ID
        ,SOC.LN_GNT_TP_ID
        ,SOC.TM_MAT_SEG_ID
        ,SOC.LN_TERM_TP_ID
        ,SOC.LN_LCS_STS_TP_ID
        ,SOC.LN_FIVE_RTG_STS
        ,SOC.LN_PPS_TP_ID
        ,SOC.IDY_CL_ID
        ,SOC.LN_FNC_STS_TP_ID
        ,SOC.LN_CGY_TP_ID
        ,SOC.FND_SRC_TP_ID
        ,SOC.PD_GRP_CD
        ,SOC.PD_SUB_CD
        ,SOC.AC_OU_ID
        ,SOC.LN_INVST_DIRC_TP_ID
        ,SOC.ALT_TP_ID
        ,SOC.CCY
        ,CUR_YEAR
        ,CUR_MONTH
        ,MONTH_DAY
        ,case when SOC.NBR_LN = 0 then 0 else 1  end  
        ,QTR_DAY
        ,case when SOC.NBR_LN = 0 then 0 else 1  end  
        ,YR_DAY
        ,case when SOC.NBR_LN = 0 then 0 else 1  end  
        ,SMY_DATE
        ,SOC.ENT_IDV_IND
        ,SOC.LN_BAL
        ,0.00
        ,SOC.NBR_LN
        ,SOC.ACR_INT_RCVD
        ,SOC.ACR_INT_RCVB
        ,SOC.MTD_ACML_LN_BAL_AMT
        ,SOC.TOT_MTD_NBR_LN_DRDWNS
        ,SOC.TOT_MTD_NBR_LN_REPYMT_RCVD
        ,SOC.TOT_MTD_WRTOF_AMT_RCVD
        ,SOC.TOT_MTD_WRTOF_AMT
        ,SOC.TOT_MTD_AMT_RCVD_OF_AST_RPLC
        ,SOC.TOT_MTD_LN_DRDWN_AMT
        ,SOC.TOT_MTD_AMT_LN_REPYMT_RCVD
        ,SOC.QTD_ACML_LN_BAL_AMT
        ,SOC.TOT_QTD_NBR_LN_DRDWN
        ,SOC.TOT_QTD_NBR_LN_RPYMT_RCVD
        ,SOC.TOT_QTD_WRTOF_AMT_RCVD
        ,SOC.TOT_QTD_AMT_RCVD_OF_AST_RPLC
        ,SOC.TOT_QTD_LN_DRDWN_AMT
        ,SOC.TOT_QTD_WRTOF_AMT
        ,SOC.TOT_QTD_AMT_LN_RPYMT_RCVD
        ,SOC.YTD_ACML_LN_BAL_AMT
        ,SOC.TOT_YTD_NBR_LN_DRDWN
        ,SOC.TOT_YTD_NBR_LN_REPYMT_RCVD
        ,SOC.TOT_YTD_WRTOF_AMT_RCVD
        ,SOC.TOT_YTD_AMT_RCVD_OF_AST_RPLC
        ,SOC.TOT_YTD_LN_DRDWN_AMT
        ,SOC.TOT_YTD_WRTOF_AMT
        ,SOC.TOT_YTD_AMT_LN_REPYMT_RCVD
        ,SOC.ON_BST_INT_RCVB
        ,SOC.OFF_BST_INT_RCVB
        ,SOC.OFF_BST_INT_RCVB_WRTOF
        ,SOC.OFF_BST_INT_RCVB_RPLC
        ,SOC.YTD_On_BST_INT_AMT_RCVD   
				------------------------Start on 20091216--------------------------
	      ,SOC.LN_AR_TP_ID      
	     	------------------------End on 20091216--------------------------			                                                                				        
        )	    ;     --
*/
END IF;--

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
 
                            	
END@