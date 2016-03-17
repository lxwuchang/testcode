CREATE PROCEDURE SMY.PROC_CR_MGR_LN_BAL_MTHLY_SMY(ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_CR_MGR_LN_BAL_MTHLY_SMY.sql
-- Procedure name: 			SMY.PROC_CR_MGR_LN_BAL_MTHLY_SMY
-- Source Table:				SOR.LN_RSPL_RGST,SMY.LOAN_AR_SMY,SOR.TELLER,SMY.LN_AR_INT_MTHLY_SMY,SMY.CST_INF
-- Target Table: 				SMY.CR_MGR_LN_BAL_MTHLY_SMY
-- Project:             ZJ RCCB EDW
-- Note                 Delete and Insert and Update
-- Purpose:             
--
--=============================================================================
-- Creation Date:       2009.11.13
-- Origin Author:       Peng Jie
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2009-11-13   Peng Jie     Create SP File	
-- 2009-12-04   Xu Yan       Rename the history table	
-- 2009-12-09   Xu Yan       Updated the SMY_LOG table insert statements, increase the step number just before inserting.
-- 2010-01-08   Xu Yan       Update all 'RSPL_*' related columns.
-- 2010-01-15   Xu Yan       Added new columns RSPL_ON_BST_INT_RCVB, RSPL_OFF_BST_INT_RCVB
-- 2010-01-16   Xu Yan       Updated 'CUR_Year_NPERF_FNC_STS_CHG_F','CUR_Year_NPERF_FR_RSLT_CHG_F'
-- 2010-04-20   Peng Yi tao  Add 'Del_F'
-- 2010-06-26   Peng Yi tao  Modify SOR.CR_MGR_EP_INF to SOR.TELLER
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
DECLARE MAX_ACG_DT DATE;--
DECLARE YR_FIRST_DAY DATE;--
DECLARE QTR_FIRST_DAY DATE;--
DECLARE YR_DAY SMALLINT;--
DECLARE QTR_DAY SMALLINT;--

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
	SET SMY_SQLCODE = SQLCODE;--
	SET SMY_STEPNUM = SMY_STEPNUM+1;--
  ROLLBACK;--
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
  COMMIT;--
  RESIGNAL;--
END;--
DECLARE CONTINUE HANDLER FOR SQLWARNING
BEGIN
  SET SMY_SQLCODE = SQLCODE;--
  SET SMY_STEPNUM = SMY_STEPNUM+1;--
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
  COMMIT;--
END;--

/*变量赋值*/
SET SMY_PROCNM = 'PROC_CR_MGR_LN_BAL_MTHLY_SMY';--
SET SMY_DATE=ACCOUNTING_DATE;--
SET CUR_YEAR=YEAR(ACCOUNTING_DATE);  --取当前年份
SET CUR_MONTH=MONTH(ACCOUNTING_DATE); --取当前月份
SET CUR_DAY=DAY(ACCOUNTING_DATE);     --取当日
SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');--
SET YR_DAY=DAYS(ACCOUNTING_DATE)-DAYS(YR_FIRST_DAY)+1;--
IF CUR_MONTH IN (1,2,3) THEN 
   SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');--
ELSEIF CUR_MONTH IN (4,5,6) THEN 
       SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-04-01');--
    ELSEIF CUR_MONTH IN (7,8,9) THEN 
           SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-07-01');--
        ELSE
            SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-10-01');--
END IF;--
SET QTR_DAY=DAYS(ACCOUNTING_DATE)-DAYS(QTR_FIRST_DAY)+1;--
SELECT MAX(ACG_DT) INTO MAX_ACG_DT FROM SMY.CR_MGR_LN_BAL_MTHLY_SMY;--

/*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE,并插入新的起始标志*/
DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
COMMIT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, 'PROCEDURE START.', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT; --

/*数据恢复与备份*/
IF MAX_ACG_DT=ACCOUNTING_DATE THEN
   DELETE FROM SMY.CR_MGR_LN_BAL_MTHLY_SMY WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;--
   COMMIT;--
   IF CUR_DAY<>1 THEN
      INSERT INTO SMY.CR_MGR_LN_BAL_MTHLY_SMY SELECT * FROM HIS.CR_MGR_LN_BAL_MTHLY_SMY;--
      COMMIT;--
   END IF;--
ELSE
   DELETE FROM HIS.CR_MGR_LN_BAL_MTHLY_SMY;--
   COMMIT;--
   INSERT INTO HIS.CR_MGR_LN_BAL_MTHLY_SMY SELECT * FROM SMY.CR_MGR_LN_BAL_MTHLY_SMY WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;--
   COMMIT;--
END IF;--

SET SMY_STEPDESC = '创建临时表,并把当日数据插入';--

DECLARE GLOBAL TEMPORARY TABLE TMP_CR_MGR_LN_BAL_MTHLY_SMY(CR_MGR_ID                        CHARACTER(18)
																													 ,LN_GNT_TP_ID                    INTEGER
																													 ,LN_PPS_TP_ID                    INTEGER
																													 ,CTR_CGY_ID                    INTEGER
																													 ,FND_SRC_TP_ID                   INTEGER
																													 ,LN_FIVE_RTG_STS                 INTEGER
																													 ,LN_FNC_STS_TP_ID                INTEGER
																													 ,IDY_CL_ID                       INTEGER
																													 ,PD_GRP_CD                       CHARACTER(2)
																													 ,PD_SUB_CD                       CHARACTER(3)
																													 ,ENT_IDV_IND                     INTEGER
																													 ,RSPL_TP_ID                      INTEGER
																													 ,CUR_Year_NPERF_FNC_STS_CHG_F    SMALLINT
																													 ,CUR_Year_NPERF_FR_RSLT_CHG_F    SMALLINT
																													 ,CCY                             CHARACTER(3)
																													 ,CDR_YR                          SMALLINT
																													 ,CDR_MTH                         SMALLINT
																													 ,OU_ID                           CHARACTER(18)
																													 ,LN_BAL                          DECIMAL(17,2)
																													 ,LN_PNP                          DECIMAL(17,2)
																													 ,YTD_INT_INCM_OF_LN              DECIMAL(17,2)
																													 ,NBR_LN_AR                       INTEGER
																													 ,NBR_CST                         INTEGER
																													 ,RSPL_AMT_LN_BAL                 DECIMAL(17,2)
																													 ,RSPL_AMT_LN_PNP                 DECIMAL(17,2)
																													 ,RSPL_AMT_LN_INT_INCM            DECIMAL(17,2)
																													 ,ON_BST_INT_RCVB                 DECIMAL(17,2)
																													 ,OFF_BST_INT_RCVB                DECIMAL(17,2)
																													 ,MTD_ACML_LN_BAL_AMT             DECIMAL(17,2)
																													 ,QTD_ACML_LN_BAL_AMT             DECIMAL(17,2)
																													 ,YTD_ACML_LN_BAL_AMT             DECIMAL(17,2)
																													 ,NBR_NEW_CST_CRN_YEAR            INTEGER
																													 ,NBR_NEW_AC_in_CUR_Year          INTEGER
																													 ,TOT_YTD_NBR_LN_DRDWN            INTEGER
																													 ,TOT_YTD_NBR_LN_REPYMT_RCVD      INTEGER
																													 ,TOT_YTD_LN_DRDWN_AMT            DECIMAL(17,2)
																													 ,TOT_YTD_AMT_LN_RPYMT_RCVD       DECIMAL(17,2)
																													 ,RSPL_AMT_MTD_ACML_LN_BAL        DECIMAL(17,2)
																													 ,RSPL_AMT_QTD_ACML_LN_BAL        DECIMAL(17,2)
																													 ,RSPL_AMT_YTD_ACML_LN_BAL        DECIMAL(17,2)
																													 ,RSPL_AMT_YTD_LN_DRDWN_AMT       DECIMAL(17,2)
																													 ,RSPL_AMT_YTD_LN_RPYMT_AMT_RCVD  DECIMAL(17,2)
																													 ,RSPL_ON_BST_INT_RCVB  NUMERIC(17,2)
																													 ,RSPL_OFF_BST_INT_RCVB  NUMERIC(17,2)
																													 ,PD_UN_CODE  CHARACTER(1)
																													 )
ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE   IN TS_USR_TMP32K PARTITIONING KEY(CR_MGR_ID); --

INSERT INTO SESSION.TMP_CR_MGR_LN_BAL_MTHLY_SMY(CR_MGR_ID                        
																				,LN_GNT_TP_ID                    
																				,LN_PPS_TP_ID                    
																				,CTR_CGY_ID                    
																				,FND_SRC_TP_ID                   
																				,LN_FIVE_RTG_STS                 
																				,LN_FNC_STS_TP_ID                
																				,IDY_CL_ID                       
																				,PD_GRP_CD                       
																				,PD_SUB_CD                       
																				,ENT_IDV_IND                     
																				,RSPL_TP_ID      
																				,CUR_Year_NPERF_FNC_STS_CHG_F
																				,CUR_Year_NPERF_FR_RSLT_CHG_F                
																				,CCY                             
																				,CDR_YR                          
																				,CDR_MTH                         
																				,OU_ID                           
																				,LN_BAL                          
																				,LN_PNP                          
																				,YTD_INT_INCM_OF_LN              
																				,NBR_LN_AR                       
																				,NBR_CST                         
																				,RSPL_AMT_LN_BAL                 
																				,RSPL_AMT_LN_PNP                 
																				,RSPL_AMT_LN_INT_INCM            
																				,ON_BST_INT_RCVB                 
																				,OFF_BST_INT_RCVB                
																				,MTD_ACML_LN_BAL_AMT             
																				,QTD_ACML_LN_BAL_AMT             
																				,YTD_ACML_LN_BAL_AMT             
																				,NBR_NEW_CST_CRN_YEAR 
																				,NBR_NEW_AC_in_CUR_Year           
																				,TOT_YTD_NBR_LN_DRDWN            
																				,TOT_YTD_NBR_LN_REPYMT_RCVD      
																				,TOT_YTD_LN_DRDWN_AMT            
																				,TOT_YTD_AMT_LN_RPYMT_RCVD       
																				,RSPL_AMT_MTD_ACML_LN_BAL        
																				,RSPL_AMT_QTD_ACML_LN_BAL        
																				,RSPL_AMT_YTD_ACML_LN_BAL        
																				,RSPL_AMT_YTD_LN_DRDWN_AMT       
																				,RSPL_AMT_YTD_LN_RPYMT_AMT_RCVD  
																				,RSPL_ON_BST_INT_RCVB
																				,RSPL_OFF_BST_INT_RCVB
																				,PD_UN_CODE
																				)
WITH     TMP_LOAN_AR_SMY AS  ( SELECT
                              CTR_AR_ID
                             ,CTR_ITM_ORDR_ID  
														 --,CTR_AR_ID
														 ,CLT_TP_ID
														 ,LN_PPS_TP_ID
														 ,CTR_CGY_ID
														 ,FND_SRC_DST_TP_ID
														 ,LN_FR_RSLT_TP_ID
														 ,AR_FNC_ST_TP_ID
														 ,CNRL_BNK_IDY_CL_ID
														 ,PD_GRP_CODE
														 ,PD_SUB_CODE
														 ,ENT_IDV_IND
														 -----------------------------------------Start on 20100116------------------------------------------------------------
  													 --,(CASE WHEN DAYS(NPERF_FNC_STS_CHG_DT) - DAYS(YR_FIRST_DAY) > 0 THEN 1 ELSE 0 END) AS CUR_Year_NPERF_FNC_STS_CHG_F
  													 ,(CASE WHEN YEAR(NPERF_FNC_STS_CHG_DT) = CUR_YEAR THEN 1 ELSE 0 END) AS CUR_Year_NPERF_FNC_STS_CHG_F
  													 --,(CASE WHEN DAYS(NPERF_FR_RSLT_CHG_DT) - DAYS(YR_FIRST_DAY) > 0 THEN 1 ELSE 0 END) AS CUR_Year_NPERF_FR_RSLT_CHG_F
  													 ,(CASE WHEN YEAR(NPERF_FR_RSLT_CHG_DT) = CUR_YEAR THEN 1 ELSE 0 END) AS CUR_Year_NPERF_FR_RSLT_CHG_F
  													 -----------------------------------------End on 20100116------------------------------------------------------------	
                             ,DNMN_CCY_ID
														 ,SUM(coalesce(LN_BAL,0.00)) AS LN_BAL
														 ,SUM(coalesce(LN_DRDWN_AMT,0.00)) AS LN_DRDWN_AMT
														 ,COUNT(DISTINCT LN_AR_ID) AS LN_AR_ID
														 ,COUNT(DISTINCT PRIM_CST_ID) AS PRIM_CST_ID
														 ,PD_UN_CODE
													FROM SMY.LOAN_AR_SMY
													GROUP BY 
													   CTR_AR_ID
													   ,CTR_ITM_ORDR_ID
													   ,PD_UN_CODE
														 --,CTR_AR_ID
														 ,CLT_TP_ID
														 ,LN_PPS_TP_ID
														 ,CTR_CGY_ID
														 ,FND_SRC_DST_TP_ID
														 ,LN_FR_RSLT_TP_ID
														 ,AR_FNC_ST_TP_ID
														 ,CNRL_BNK_IDY_CL_ID
														 ,PD_GRP_CODE
														 ,PD_SUB_CODE
														 ,ENT_IDV_IND
														 -----------------------------------------Start on 20100116------------------------------------------------------------
  													 --,(CASE WHEN DAYS(NPERF_FNC_STS_CHG_DT) - DAYS(YR_FIRST_DAY) > 0 THEN 1 ELSE 0 END) AS CUR_Year_NPERF_FNC_STS_CHG_F
  													 ,(CASE WHEN YEAR(NPERF_FNC_STS_CHG_DT) = CUR_YEAR THEN 1 ELSE 0 END) --AS CUR_Year_NPERF_FNC_STS_CHG_F
  													 --,(CASE WHEN DAYS(NPERF_FR_RSLT_CHG_DT) - DAYS(YR_FIRST_DAY) > 0 THEN 1 ELSE 0 END) AS CUR_Year_NPERF_FR_RSLT_CHG_F
  													 ,(CASE WHEN YEAR(NPERF_FR_RSLT_CHG_DT) = CUR_YEAR THEN 1 ELSE 0 END) --AS CUR_Year_NPERF_FR_RSLT_CHG_F
  													 -----------------------------------------End on 20100116------------------------------------------------------------	

                             ,DNMN_CCY_ID),
     TMP_LN_AR_INT_MTHLY_SMY AS ( SELECT 
                                    CTR_AR_ID
                                    ,CTR_ITM_ORDR_ID
                                    ,CDR_YR
                                    ,CDR_MTH
                                    ,SUM(TOT_YTD_AMT_Of_INT_INCM) AS TOT_YTD_AMT_Of_INT_INCM
																		,SUM(ON_BST_INT_RCVB) AS ON_BST_INT_RCVB
																		,SUM(OFF_BST_INT_RCVB) AS OFF_BST_INT_RCVB
																		--,SUM(MTD_ACML_BAL_AMT) AS MTD_ACML_BAL_AMT
																		--,SUM(QTD_ACML_BAL_AMT) AS QTD_ACML_BAL_AMT
																		--,SUM(YTD_ACML_BAL_AMT) AS YTD_ACML_BAL_AMT
																		,SUM(TOT_YTD_NBR_LN_DRDWN_TXN) AS TOT_YTD_NBR_LN_DRDWN_TXN
																		,SUM(TOT_YTD_NBR_LN_RCVD_TXN) AS TOT_YTD_NBR_LN_RCVD_TXN
																		--,SUM(TOT_YTD_LN_DRDWN_AMT) AS TOT_YTD_LN_DRDWN_AMT
																		--,SUM(TOT_YTD_AMT_LN_REPYMT_RCVD) AS TOT_YTD_AMT_LN_REPYMT_RCVD
																		--,SUM(TOT_YTD_AMT_Of_INT_INCM) AS TOT_YTD_AMT_Of_INT_INCM
																		,SUM(MTD_ACML_BAL_AMT) AS MTD_ACML_BAL_AMT
																		,SUM(QTD_ACML_BAL_AMT) AS QTD_ACML_BAL_AMT
																		,SUM(YTD_ACML_BAL_AMT) AS YTD_ACML_BAL_AMT
																		,SUM(TOT_YTD_LN_DRDWN_AMT) AS TOT_YTD_LN_DRDWN_AMT
																		,SUM(TOT_YTD_AMT_LN_REPYMT_RCVD) AS TOT_YTD_AMT_LN_REPYMT_RCVD
																  FROM SMY.LN_AR_INT_MTHLY_SMY
																  GROUP BY 
                                    CTR_AR_ID            
                                    ,CTR_ITM_ORDR_ID
                                    ,CDR_YR
                                    ,CDR_MTH)
      
SELECT 
   a.RSPL_CST_MGR_IP_ID
  ,b.CLT_TP_ID
  ,b.LN_PPS_TP_ID
  ,b.CTR_CGY_ID
  ,b.FND_SRC_DST_TP_ID
  ,b.LN_FR_RSLT_TP_ID
  ,b.AR_FNC_ST_TP_ID
  ,b.CNRL_BNK_IDY_CL_ID
  ,b.PD_GRP_CODE
  ,b.PD_SUB_CODE
  ,b.ENT_IDV_IND
  ,a.RSPL_TP_ID
  ,b.CUR_Year_NPERF_FNC_STS_CHG_F
  ,b.CUR_Year_NPERF_FR_RSLT_CHG_F
  ,b.DNMN_CCY_ID
  ,CUR_YEAR
  ,CUR_MONTH
  -----------------------------------------Start on 20100626------------------------------------------------------------
  ,c.RPT_OU_IP_ID
  -----------------------------------------End on 20100626------------------------------------------------------------
  ,SUM(COALESCE(b.LN_BAL,0.00))
  ,SUM(COALESCE(b.LN_DRDWN_AMT,0.00))
  ,SUM(COALESCE(TOT_YTD_AMT_Of_INT_INCM,0))
  ,sum(COALESCE( b.LN_AR_ID,0))
  ,sum(COALESCE( b.PRIM_CST_ID, 0))
  --------------------------Start on 20100108------------------------------------------------------------------------
  /*
  ,(sum(COALESCE(b.LN_BAL,0.00) * COALESCE(RTO_RSPL_PNP,0))) AS RSPL_AMT_LN_BAL
  ,(sum(COALESCE(b.LN_DRDWN_AMT,0.00) *COALESCE(RTO_RSPL_PNP,0))) AS RSPL_AMT_LN_PNP
  ,(sum(COALESCE(d.TOT_YTD_AMT_Of_INT_INCM,0)* COALESCE(RTO_RSPL_INT,0)) ) AS RSPL_AMT_LN_INT_INCM
  */
  ,(sum(COALESCE(b.LN_BAL,0.00) * COALESCE(RTO_RSPL_PNP,0))/100) AS RSPL_AMT_LN_BAL
  ,(sum(COALESCE(b.LN_DRDWN_AMT,0.00) *COALESCE(RTO_RSPL_PNP,0))/100) AS RSPL_AMT_LN_PNP
  ,(sum(COALESCE(d.TOT_YTD_AMT_Of_INT_INCM,0)* COALESCE(RTO_RSPL_INT,0))/100) AS RSPL_AMT_LN_INT_INCM
  --------------------------End on 20100108------------------------------------------------------------------------
  ,SUM(COALESCE(ON_BST_INT_RCVB,0.00))
  ,SUM(COALESCE(OFF_BST_INT_RCVB,0.00))
  ,SUM(COALESCE(MTD_ACML_BAL_AMT,0.00))
  ,SUM(COALESCE(QTD_ACML_BAL_AMT,0.00))
  ,SUM(COALESCE(YTD_ACML_BAL_AMT,0.00))
  -------------------------Start on 20100420------------------------------------------------------------------------ 
  --,(select count(distinct a.PRIM_CST_ID) from SOR.LN_RSPL_RGST a left join SMY.CST_INF b on a.PRIM_CST_ID = b.CST_ID where SUBSTR(TRIM(CHAR(b.EFF_CST_DT)),1,4) = CHAR(CUR_YEAR))
  ,(select count(distinct a.PRIM_CST_ID) from SOR.LN_RSPL_RGST a left join SMY.CST_INF b on a.PRIM_CST_ID = b.CST_ID where a.DEL_F=0 and SUBSTR(TRIM(CHAR(b.EFF_CST_DT)),1,4) = CHAR(CUR_YEAR))
  --------------------------End on 20100420------------------------------------------------------------------------
  ,(SELECT COUNT(DISTINCT a.LN_AR_ID) FROM SMY.LOAN_AR_SMY a WHERE SUBSTR(TRIM(CHAR(LN_DRDWN_DT)),1,4) = CHAR(CUR_YEAR))
  ,SUM(COALESCE(TOT_YTD_NBR_LN_DRDWN_TXN,0))
  ,SUM(COALESCE(TOT_YTD_NBR_LN_RCVD_TXN, 0))
  ,SUM(COALESCE(TOT_YTD_LN_DRDWN_AMT,0.00))
  ,SUM(COALESCE(TOT_YTD_AMT_LN_REPYMT_RCVD,0))
  --------------------------Start on 20100108------------------------------------------------------------------------
  /*,(SUM(COALESCE(MTD_ACML_BAL_AMT,0.00) * COALESCE(RTO_RSPL_INT,0))) AS RSPL_AMT_MTD_ACML_LN_BAL
  ,(SUM(COALESCE(QTD_ACML_BAL_AMT,0.00) * COALESCE(RTO_RSPL_INT,0))) AS RSPL_AMT_QTD_ACML_LN_BAL
  ,(SUM(COALESCE(YTD_ACML_BAL_AMT,0.00) * COALESCE(RTO_RSPL_INT,0))) AS RSPL_AMT_YTD_ACML_LN_BAL
  ,(SUM(COALESCE(TOT_YTD_LN_DRDWN_AMT,0.00) * COALESCE(RTO_RSPL_INT,0))) AS RSPL_AMT_YTD_LN_DRDWN_AMT
  ,(SUM(COALESCE(TOT_YTD_AMT_LN_REPYMT_RCVD,0) * COALESCE(RTO_RSPL_INT,0))) AS RSPL_AMT_YTD_LN_RPYMT_AMT_RCVD
  */
  ,(SUM(COALESCE(MTD_ACML_BAL_AMT,0.00) * COALESCE(RTO_RSPL_PNP,0))/100) AS RSPL_AMT_MTD_ACML_LN_BAL
  ,(SUM(COALESCE(QTD_ACML_BAL_AMT,0.00) * COALESCE(RTO_RSPL_PNP,0))/100) AS RSPL_AMT_QTD_ACML_LN_BAL
  ,(SUM(COALESCE(YTD_ACML_BAL_AMT,0.00) * COALESCE(RTO_RSPL_PNP,0))/100) AS RSPL_AMT_YTD_ACML_LN_BAL
  ,(SUM(COALESCE(TOT_YTD_LN_DRDWN_AMT,0.00) * COALESCE(RTO_RSPL_PNP,0))/100) AS RSPL_AMT_YTD_LN_DRDWN_AMT
  ,(SUM(COALESCE(TOT_YTD_AMT_LN_REPYMT_RCVD,0) * COALESCE(RTO_RSPL_PNP,0))/100) AS RSPL_AMT_YTD_LN_RPYMT_AMT_RCVD
  --------------------------End on 20100108------------------------------------------------------------------------
  ,SUM(COALESCE(ON_BST_INT_RCVB,0.00)*COALESCE(RTO_RSPL_INT,0)) AS RSPL_ON_BST_INT_RCVB
  ,SUM(COALESCE(OFF_BST_INT_RCVB,0.00)*COALESCE(RTO_RSPL_INT,0)) AS RSPL_OFF_BST_INT_RCVB
  ,b.PD_UN_CODE
  FROM SOR.LN_RSPL_RGST a	 INNER JOIN TMP_LOAN_AR_SMY b ON a.LN_CTR_NO=b.CTR_AR_ID
  -----------------------------------------Start on 20100626------------------------------------------------------------
                           INNER JOIN SOR.TELLER c ON a.RSPL_CST_MGR_IP_ID =c.TELLER_ID 
  -----------------------------------------End on 20100626------------------------------------------------------------
                           INNER JOIN TMP_LN_AR_INT_MTHLY_SMY d ON d.CTR_AR_ID = b.CTR_AR_ID  AND d.CTR_ITM_ORDR_ID = b.CTR_ITM_ORDR_ID AND d.CDR_YR = CUR_YEAR AND d.CDR_MTH = CUR_MONTH
   --------------------------Start on 20100420------------------------------------------------------------------------                        
                           WHERE a.DEL_F=0
   --------------------------End on  2010420------------------------------------------------------------------------                       
GROUP BY 
   a.RSPL_CST_MGR_IP_ID
  ,b.CLT_TP_ID
  ,b.LN_PPS_TP_ID
  ,b.CTR_CGY_ID
  ,b.FND_SRC_DST_TP_ID
  ,b.LN_FR_RSLT_TP_ID
  ,b.AR_FNC_ST_TP_ID
  ,b.CNRL_BNK_IDY_CL_ID
  ,b.PD_GRP_CODE
  ,b.PD_SUB_CODE
  ,b.ENT_IDV_IND
  ,a.RSPL_TP_ID
  ,b.PD_UN_CODE
  ,b.CUR_Year_NPERF_FNC_STS_CHG_F
  ,b.CUR_Year_NPERF_FR_RSLT_CHG_F
  ,b.DNMN_CCY_ID
  ,CUR_YEAR
  ,CUR_MONTH
  -----------------------------------------Start on 20100626------------------------------------------------------------
  ,c.RPT_OU_IP_ID
  -----------------------------------------End on 20100626------------------------------------------------------------
;--
  
GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
SET SMY_STEPNUM = SMY_STEPNUM+1;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--


IF CUR_DAY = 1 THEN
   IF CUR_MONTH = 1 THEN
   SET SMY_STEPDESC = '插入年初数据';--
        INSERT INTO SMY.CR_MGR_LN_BAL_MTHLY_SMY
          (CR_MGR_ID                                   ,--信贷员
          LN_GNT_TP_ID                                ,--贷款担保类型
          LN_PPS_TP_ID                                ,--贷款用途类型
          CTR_CGY_ID                                ,--业务品种
          FND_SRC_TP_ID                               ,--资金来源
          LN_FIVE_RTG_STS                             ,--贷款五级形态类型
          LN_FNC_STS_TP_ID                            ,--贷款四级形态类型
          IDY_CL_ID                                   ,--行业代码
          PD_GRP_CD                                   ,--产品组代码
          PD_SUB_CD                                   ,--产品字代码
          ENT_IDV_IND                                 ,--企业/个人标志
          RSPL_TP_Id                                  ,--责任类型
          CUR_Year_NPERF_FNC_STS_CHG_F                ,
          CUR_Year_NPERF_FR_RSLT_CHG_F                ,
          CCY                                         ,--币种
          CDR_YR                                      ,--年份YYYY
          CDR_MTH                                     ,--月份MM
          OU_ID                                       ,--信贷员归属机构号
          NOCLD_In_MTH                                ,--当月日历天数
          NOD_In_MTH                                  ,--当月有效天数
          NOCLD_In_QTR                                ,--当季日历天数
          NOD_In_QTR                                  ,--当季有效天数
          NOCLD_In_Year                               ,--当年日历天数
          NOD_In_Year                                 ,--当年有效天数
          ACG_DT                                      ,--日期YYYY-MM-DD
          LN_BAL                                      ,--贷款余额
          LN_PNP                                      ,--贷款本金
          YTD_INT_INCM_of_LN                          ,--当年贷款利息收入
          NBR_LN_AR                                   ,--贷款账户数
          NBR_CST                                     ,--客户数量
          RSPL_AMT_LN_BAL                             ,--贷款余额责任比
          RSPL_AMT_LN_PNP                             ,--贷款本金责任比
          RSPL_AMT_LN_INT_INCM                        ,--贷款利息责任比
          ON_BST_INT_RCVB                             ,--表内应收利息余额
          OFF_BST_INT_RCVB                            ,--表外应收利息余额
          MTD_ACML_LN_BAL_AMT                         ,--月贷款累计余额
          QTD_ACML_LN_BAL_AMT                         ,--季贷款累计余额
          YTD_ACML_LN_BAL_AMT                         ,--年贷款累计余额
          NBR_NEW_CST_CRN_YEAR                        ,--当年新增客户数
          NBR_NEW_AC_in_CUR_Year                      ,
          TOT_YTD_NBR_LN_DRDWN                        ,--年累计贷款发放笔数
          TOT_YTD_NBR_LN_REPYMT_RCVD                  ,--年累计收回贷款笔数
          TOT_YTD_LN_DRDWN_AMT                        ,--当年贷款累放金额
          TOT_YTD_AMT_LN_RPYMT_RCVD                   ,--当年贷款累收金额
          RSPL_AMT_MTD_ACML_LN_BAL                    ,--月累计余额责任比金额
          RSPL_AMT_QTD_ACML_LN_BAL                    ,--季累计余额责任比金额
          RSPL_AMT_YTD_ACML_LN_BAL                    ,--年累计余额责任比金额
          RSPL_AMT_YTD_LN_DRDWN_AMT                   ,--年累放金额责任比金额
          RSPL_AMT_YTD_LN_RPYMT_AMT_RCVD              --年累收金额的责任比金额
          ,RSPL_ON_BST_INT_RCVB                       --表内应收利息余额责任比   
					,RSPL_OFF_BST_INT_RCVB                      --表外应收利息余额责任比
					,PD_UN_CODE																	--产品联社代码
        )
        SELECT 
          CR_MGR_ID                      
          ,LN_GNT_TP_ID                  
          ,LN_PPS_TP_ID                  
          ,CTR_CGY_ID                  
          ,FND_SRC_TP_ID                 
          ,LN_FIVE_RTG_STS               
          ,LN_FNC_STS_TP_ID              
          ,IDY_CL_ID                     
          ,PD_GRP_CD                     
          ,PD_SUB_CD                     
          ,ENT_IDV_IND                   
          ,RSPL_TP_ID 
          ,CUR_Year_NPERF_FNC_STS_CHG_F
          ,CUR_Year_NPERF_FR_RSLT_CHG_F
          ,CCY                           
          ,CDR_YR                        
          ,CDR_MTH                       
          ,OU_ID 
          ,CUR_DAY
          ,CUR_DAY
          ,CUR_DAY
          ,CUR_DAY
          ,CUR_DAY
          ,CUR_DAY
          ,SMY_DATE
          ,LN_BAL                        
          ,LN_PNP                        
          ,YTD_INT_INCM_OF_LN            
          ,NBR_LN_AR                     
          ,NBR_CST                       
          ,RSPL_AMT_LN_BAL               
          ,RSPL_AMT_LN_PNP               
          ,RSPL_AMT_LN_INT_INCM          
          ,ON_BST_INT_RCVB               
          ,OFF_BST_INT_RCVB              
          ,MTD_ACML_LN_BAL_AMT           
          ,QTD_ACML_LN_BAL_AMT           
          ,YTD_ACML_LN_BAL_AMT           
          ,NBR_NEW_CST_CRN_YEAR  
          ,NBR_NEW_AC_in_CUR_Year        
          ,TOT_YTD_NBR_LN_DRDWN          
          ,TOT_YTD_NBR_LN_REPYMT_RCVD    
          ,TOT_YTD_LN_DRDWN_AMT          
          ,TOT_YTD_AMT_LN_RPYMT_RCVD     
          ,RSPL_AMT_MTD_ACML_LN_BAL      
          ,RSPL_AMT_QTD_ACML_LN_BAL      
          ,RSPL_AMT_YTD_ACML_LN_BAL      
          ,RSPL_AMT_YTD_LN_DRDWN_AMT     
          ,RSPL_AMT_YTD_LN_RPYMT_AMT_RCVD
          ,RSPL_ON_BST_INT_RCVB                       --表内应收利息余额责任比   
					,RSPL_OFF_BST_INT_RCVB                      --表外应收利息余额责任比   
					,PD_UN_CODE																	--产品联社代码  
        FROM SESSION.TMP_CR_MGR_LN_BAL_MTHLY_SMY;--

  ELSEIF CUR_MONTH IN (4, 7, 10) THEN
  SET SMY_STEPDESC = '插入季初数据';--
        INSERT INTO SMY.CR_MGR_LN_BAL_MTHLY_SMY
          (CR_MGR_ID                                   ,--信贷员
          LN_GNT_TP_ID                                ,--贷款担保类型
          LN_PPS_TP_ID                                ,--贷款用途类型
          CTR_CGY_ID                                ,--业务品种
          FND_SRC_TP_ID                               ,--资金来源
          LN_FIVE_RTG_STS                             ,--贷款五级形态类型
          LN_FNC_STS_TP_ID                            ,--贷款四级形态类型
          IDY_CL_ID                                   ,--行业代码
          PD_GRP_CD                                   ,--产品组代码
          PD_SUB_CD                                   ,--产品字代码
          ENT_IDV_IND                                 ,--企业/个人标志
          RSPL_TP_Id                                  ,--责任类型
          CUR_Year_NPERF_FNC_STS_CHG_F                ,
          CUR_Year_NPERF_FR_RSLT_CHG_F                ,
          CCY                                         ,--币种
          CDR_YR                                      ,--年份YYYY
          CDR_MTH                                     ,--月份MM
          OU_ID                                       ,--信贷员归属机构号
          NOCLD_In_MTH                                ,--当月日历天数
          NOD_In_MTH                                  ,--当月有效天数
          NOCLD_In_QTR                                ,--当季日历天数
          NOD_In_QTR                                  ,--当季有效天数
          NOCLD_In_Year                               ,--当年日历天数
          NOD_In_Year                                 ,--当年有效天数
          ACG_DT                                      ,--日期YYYY-MM-DD
          LN_BAL                                      ,--贷款余额
          LN_PNP                                      ,--贷款本金
          YTD_INT_INCM_of_LN                          ,--当年贷款利息收入
          NBR_LN_AR                                   ,--贷款账户数
          NBR_CST                                     ,--客户数量
          RSPL_AMT_LN_BAL                             ,--贷款余额责任比
          RSPL_AMT_LN_PNP                             ,--贷款本金责任比
          RSPL_AMT_LN_INT_INCM                        ,--贷款利息责任比
          ON_BST_INT_RCVB                             ,--表内应收利息余额
          OFF_BST_INT_RCVB                            ,--表外应收利息余额
          MTD_ACML_LN_BAL_AMT                         ,--月贷款累计余额
          QTD_ACML_LN_BAL_AMT                         ,--季贷款累计余额
          YTD_ACML_LN_BAL_AMT                         ,--年贷款累计余额
          NBR_NEW_CST_CRN_YEAR                        ,--当年新增客户数
          NBR_NEW_AC_in_CUR_Year                      ,
          TOT_YTD_NBR_LN_DRDWN                        ,--年累计贷款发放笔数
          TOT_YTD_NBR_LN_REPYMT_RCVD                  ,--年累计收回贷款笔数
          TOT_YTD_LN_DRDWN_AMT                        ,--当年贷款累放金额
          TOT_YTD_AMT_LN_RPYMT_RCVD                   ,--当年贷款累收金额
          RSPL_AMT_MTD_ACML_LN_BAL                    ,--月累计余额责任比金额
          RSPL_AMT_QTD_ACML_LN_BAL                    ,--季累计余额责任比金额
          RSPL_AMT_YTD_ACML_LN_BAL                    ,--年累计余额责任比金额
          RSPL_AMT_YTD_LN_DRDWN_AMT                   ,--年累放金额责任比金额
          RSPL_AMT_YTD_LN_RPYMT_AMT_RCVD              --年累收金额的责任比金额
          ,RSPL_ON_BST_INT_RCVB                       --表内应收利息余额责任比   
					,RSPL_OFF_BST_INT_RCVB                      --表外应收利息余额责任比
					,PD_UN_CODE																	--产品联社代码  
				)
        SELECT 
           a.CR_MGR_ID                      
          ,a.LN_GNT_TP_ID                  
          ,a.LN_PPS_TP_ID                  
          ,a.CTR_CGY_ID                  
          ,a.FND_SRC_TP_ID                 
          ,a.LN_FIVE_RTG_STS               
          ,a.LN_FNC_STS_TP_ID              
          ,a.IDY_CL_ID                     
          ,a.PD_GRP_CD                     
          ,a.PD_SUB_CD                     
          ,a.ENT_IDV_IND                   
          ,a.RSPL_TP_ID
          ,a.CUR_Year_NPERF_FNC_STS_CHG_F
          ,a.CUR_Year_NPERF_FR_RSLT_CHG_F
          ,a.CCY                           
          ,a.CDR_YR                        
          ,a.CDR_MTH                       
          ,a.OU_ID 
          ,CUR_DAY
          ,CUR_DAY
          ,CUR_DAY
          ,CUR_DAY
          ,YR_DAY
          ,COALESCE(b.NOD_In_Year,0) + CUR_DAY
          ,SMY_DATE
          ,a.LN_BAL                        
          ,a.LN_PNP                        
          ,a.YTD_INT_INCM_OF_LN            
          ,a.NBR_LN_AR                     
          ,a.NBR_CST                       
          ,a.RSPL_AMT_LN_BAL               
          ,a.RSPL_AMT_LN_PNP               
          ,a.RSPL_AMT_LN_INT_INCM          
          ,a.ON_BST_INT_RCVB               
          ,a.OFF_BST_INT_RCVB              
          ,a.MTD_ACML_LN_BAL_AMT           
          ,a.QTD_ACML_LN_BAL_AMT           
          ,a.YTD_ACML_LN_BAL_AMT           
          ,a.NBR_NEW_CST_CRN_YEAR 
          ,a.NBR_NEW_AC_in_CUR_Year         
          ,a.TOT_YTD_NBR_LN_DRDWN          
          ,a.TOT_YTD_NBR_LN_REPYMT_RCVD    
          ,a.TOT_YTD_LN_DRDWN_AMT          
          ,a.TOT_YTD_AMT_LN_RPYMT_RCVD     
          ,a.RSPL_AMT_MTD_ACML_LN_BAL      
          ,a.RSPL_AMT_QTD_ACML_LN_BAL      
          ,a.RSPL_AMT_YTD_ACML_LN_BAL      
          ,a.RSPL_AMT_YTD_LN_DRDWN_AMT     
          ,a.RSPL_AMT_YTD_LN_RPYMT_AMT_RCVD
          ,a.RSPL_ON_BST_INT_RCVB                       --表内应收利息余额责任比   
					,a.RSPL_OFF_BST_INT_RCVB                      --表外应收利息余额责任比
					,a.PD_UN_CODE																	--产品联社代码 
        FROM SESSION.TMP_CR_MGR_LN_BAL_MTHLY_SMY a LEFT JOIN SMY.CR_MGR_LN_BAL_MTHLY_SMY b
          ON a.CR_MGR_ID        = b.CR_MGR_ID AND
             a.LN_GNT_TP_ID     = b.LN_GNT_TP_ID AND
             a.LN_PPS_TP_ID     = b.LN_PPS_TP_ID AND
             a.CTR_CGY_ID     = b.CTR_CGY_ID AND
             a.FND_SRC_TP_ID    = b.FND_SRC_TP_ID AND
             a.LN_FIVE_RTG_STS  = b.LN_FIVE_RTG_STS AND
             a.LN_FNC_STS_TP_ID = b.LN_FNC_STS_TP_ID AND
             a.IDY_CL_ID        = b.IDY_CL_ID AND
             a.PD_GRP_CD        = b.PD_GRP_CD AND
             a.PD_SUB_CD        = b.PD_SUB_CD AND
             a.ENT_IDV_IND      = b.ENT_IDV_IND AND
             a.CCY              = b.CCY AND
             a.CDR_YR           = b.CDR_YR AND
             a.CDR_MTH - 1      = b.CDR_MTH AND
             a.RSPL_TP_ID       = b.RSPL_TP_ID AND
             a.PD_UN_CODE       = b.PD_UN_CODE AND
             a.CUR_Year_NPERF_FNC_STS_CHG_F = b.CUR_Year_NPERF_FNC_STS_CHG_F AND
             a.CUR_Year_NPERF_FR_RSLT_CHG_F = b.CUR_Year_NPERF_FR_RSLT_CHG_F;--
             

    ELSE
       SET SMY_STEPDESC = '插入非年初或季初的月初数据';--
        INSERT INTO SMY.CR_MGR_LN_BAL_MTHLY_SMY
          (CR_MGR_ID                                   ,--信贷员
          LN_GNT_TP_ID                                ,--贷款担保类型
          LN_PPS_TP_ID                                ,--贷款用途类型
          CTR_CGY_ID                                ,--业务品种
          FND_SRC_TP_ID                               ,--资金来源
          LN_FIVE_RTG_STS                             ,--贷款五级形态类型
          LN_FNC_STS_TP_ID                            ,--贷款四级形态类型
          IDY_CL_ID                                   ,--行业代码
          PD_GRP_CD                                   ,--产品组代码
          PD_SUB_CD                                   ,--产品字代码
          ENT_IDV_IND                                 ,--企业/个人标志
          RSPL_TP_Id                                  ,--责任类型
          CUR_Year_NPERF_FNC_STS_CHG_F                ,
          CUR_Year_NPERF_FR_RSLT_CHG_F                ,          
          CCY                                         ,--币种
          CDR_YR                                      ,--年份YYYY
          CDR_MTH                                     ,--月份MM
          OU_ID                                       ,--信贷员归属机构号
          NOCLD_In_MTH                                ,--当月日历天数
          NOD_In_MTH                                  ,--当月有效天数
          NOCLD_In_QTR                                ,--当季日历天数
          NOD_In_QTR                                  ,--当季有效天数
          NOCLD_In_Year                               ,--当年日历天数
          NOD_In_Year                                 ,--当年有效天数
          ACG_DT                                      ,--日期YYYY-MM-DD
          LN_BAL                                      ,--贷款余额
          LN_PNP                                      ,--贷款本金
          YTD_INT_INCM_of_LN                          ,--当年贷款利息收入
          NBR_LN_AR                                   ,--贷款账户数
          NBR_CST                                     ,--客户数量
          RSPL_AMT_LN_BAL                             ,--贷款余额责任比
          RSPL_AMT_LN_PNP                             ,--贷款本金责任比
          RSPL_AMT_LN_INT_INCM                        ,--贷款利息责任比
          ON_BST_INT_RCVB                             ,--表内应收利息余额
          OFF_BST_INT_RCVB                            ,--表外应收利息余额
          MTD_ACML_LN_BAL_AMT                         ,--月贷款累计余额
          QTD_ACML_LN_BAL_AMT                         ,--季贷款累计余额
          YTD_ACML_LN_BAL_AMT                         ,--年贷款累计余额
          NBR_NEW_CST_CRN_YEAR                        ,--当年新增客户数
          NBR_NEW_AC_in_CUR_Year                      ,
          TOT_YTD_NBR_LN_DRDWN                        ,--年累计贷款发放笔数
          TOT_YTD_NBR_LN_REPYMT_RCVD                  ,--年累计收回贷款笔数
          TOT_YTD_LN_DRDWN_AMT                        ,--当年贷款累放金额
          TOT_YTD_AMT_LN_RPYMT_RCVD                   ,--当年贷款累收金额
          RSPL_AMT_MTD_ACML_LN_BAL                    ,--月累计余额责任比金额
          RSPL_AMT_QTD_ACML_LN_BAL                    ,--季累计余额责任比金额
          RSPL_AMT_YTD_ACML_LN_BAL                    ,--年累计余额责任比金额
          RSPL_AMT_YTD_LN_DRDWN_AMT                   ,--年累放金额责任比金额
          RSPL_AMT_YTD_LN_RPYMT_AMT_RCVD              --年累收金额的责任比金额
          ,RSPL_ON_BST_INT_RCVB                       --表内应收利息余额责任比   
					,RSPL_OFF_BST_INT_RCVB                      --表外应收利息余额责任比 
					,PD_UN_CODE																	--产品联社代码  
				)
        SELECT 
           a.CR_MGR_ID                      
          ,a.LN_GNT_TP_ID                  
          ,a.LN_PPS_TP_ID                  
          ,a.CTR_CGY_ID                  
          ,a.FND_SRC_TP_ID                 
          ,a.LN_FIVE_RTG_STS               
          ,a.LN_FNC_STS_TP_ID              
          ,a.IDY_CL_ID                     
          ,a.PD_GRP_CD                     
          ,a.PD_SUB_CD                     
          ,a.ENT_IDV_IND                   
          ,a.RSPL_TP_ID
          ,a.CUR_Year_NPERF_FNC_STS_CHG_F
          ,a.CUR_Year_NPERF_FR_RSLT_CHG_F
          ,a.CCY                           
          ,a.CDR_YR                        
          ,a.CDR_MTH                       
          ,a.OU_ID 
          ,CUR_DAY
          ,CUR_DAY
          ,QTR_DAY
          ,COALESCE(b.NOD_In_QTR,0) + CUR_DAY
          ,YR_DAY
          ,COALESCE(b.NOD_In_Year,0) + CUR_DAY
          ,SMY_DATE
          ,a.LN_BAL                        
          ,a.LN_PNP                        
          ,a.YTD_INT_INCM_OF_LN            
          ,a.NBR_LN_AR                     
          ,a.NBR_CST                       
          ,a.RSPL_AMT_LN_BAL               
          ,a.RSPL_AMT_LN_PNP               
          ,a.RSPL_AMT_LN_INT_INCM          
          ,a.ON_BST_INT_RCVB               
          ,a.OFF_BST_INT_RCVB              
          ,a.MTD_ACML_LN_BAL_AMT           
          ,a.QTD_ACML_LN_BAL_AMT           
          ,a.YTD_ACML_LN_BAL_AMT           
          ,a.NBR_NEW_CST_CRN_YEAR  
          ,a.NBR_NEW_AC_in_CUR_Year        
          ,a.TOT_YTD_NBR_LN_DRDWN          
          ,a.TOT_YTD_NBR_LN_REPYMT_RCVD    
          ,a.TOT_YTD_LN_DRDWN_AMT          
          ,a.TOT_YTD_AMT_LN_RPYMT_RCVD     
          ,a.RSPL_AMT_MTD_ACML_LN_BAL      
          ,a.RSPL_AMT_QTD_ACML_LN_BAL      
          ,a.RSPL_AMT_YTD_ACML_LN_BAL      
          ,a.RSPL_AMT_YTD_LN_DRDWN_AMT     
          ,a.RSPL_AMT_YTD_LN_RPYMT_AMT_RCVD
          ,a.RSPL_ON_BST_INT_RCVB                       --表内应收利息余额责任比   
					,a.RSPL_OFF_BST_INT_RCVB                      --表外应收利息余额责任比
					,a.PD_UN_CODE																	--产品联社代码   
        FROM SESSION.TMP_CR_MGR_LN_BAL_MTHLY_SMY a LEFT JOIN SMY.CR_MGR_LN_BAL_MTHLY_SMY b
          ON a.CR_MGR_ID        = b.CR_MGR_ID AND
             a.LN_GNT_TP_ID     = b.LN_GNT_TP_ID AND
             a.LN_PPS_TP_ID     = b.LN_PPS_TP_ID AND
             a.CTR_CGY_ID     = b.CTR_CGY_ID AND
             a.FND_SRC_TP_ID    = b.FND_SRC_TP_ID AND
             a.LN_FIVE_RTG_STS  = b.LN_FIVE_RTG_STS AND
             a.LN_FNC_STS_TP_ID = b.LN_FNC_STS_TP_ID AND
             a.IDY_CL_ID        = b.IDY_CL_ID AND
             a.PD_GRP_CD        = b.PD_GRP_CD AND
             a.PD_SUB_CD        = b.PD_SUB_CD AND
             a.ENT_IDV_IND      = b.ENT_IDV_IND AND
             a.CCY              = b.CCY AND
             a.CDR_YR           = b.CDR_YR AND
             a.CDR_MTH - 1      = b.CDR_MTH AND
             a.RSPL_TP_ID       = b.RSPL_TP_ID AND
             a.PD_UN_CODE       = b.PD_UN_CODE AND
             a.CUR_Year_NPERF_FNC_STS_CHG_F = b.CUR_Year_NPERF_FNC_STS_CHG_F AND
             a.CUR_Year_NPERF_FR_RSLT_CHG_F = b.CUR_Year_NPERF_FR_RSLT_CHG_F;--

           

   END IF;--
ELSE
  SET SMY_STEPDESC = 'merge非月初数据';--
  MERGE INTO 	SMY.CR_MGR_LN_BAL_MTHLY_SMY TAG
  USING SESSION.TMP_CR_MGR_LN_BAL_MTHLY_SMY SOC
          ON TAG.CR_MGR_ID        = SOC.CR_MGR_ID AND
             TAG.LN_GNT_TP_ID     = SOC.LN_GNT_TP_ID AND
             TAG.LN_PPS_TP_ID     = SOC.LN_PPS_TP_ID AND
             TAG.CTR_CGY_ID     =   SOC.CTR_CGY_ID AND
             TAG.FND_SRC_TP_ID    = SOC.FND_SRC_TP_ID AND
             TAG.LN_FIVE_RTG_STS  = SOC.LN_FIVE_RTG_STS AND
             TAG.LN_FNC_STS_TP_ID = SOC.LN_FNC_STS_TP_ID AND
             TAG.IDY_CL_ID        = SOC.IDY_CL_ID AND
             TAG.PD_GRP_CD        = SOC.PD_GRP_CD AND
             TAG.PD_SUB_CD        = SOC.PD_SUB_CD AND
             TAG.ENT_IDV_IND      = SOC.ENT_IDV_IND AND
             TAG.CCY              = SOC.CCY AND
             TAG.CDR_YR           = SOC.CDR_YR AND
             TAG.CDR_MTH          = SOC.CDR_MTH AND
             TAG.RSPL_TP_ID       = SOC.RSPL_TP_ID AND
             TAG.PD_UN_CODE       = SOC.PD_UN_CODE AND
             TAG.CUR_Year_NPERF_FNC_STS_CHG_F = SOC.CUR_Year_NPERF_FNC_STS_CHG_F AND
             TAG.CUR_Year_NPERF_FR_RSLT_CHG_F = SOC.CUR_Year_NPERF_FR_RSLT_CHG_F

	WHEN MATCHED THEN
	    UPDATE SET 
          (CR_MGR_ID                                   ,--信贷员
          LN_GNT_TP_ID                                ,--贷款担保类型
          LN_PPS_TP_ID                                ,--贷款用途类型
          CTR_CGY_ID                                ,--业务品种
          FND_SRC_TP_ID                               ,--资金来源
          LN_FIVE_RTG_STS                             ,--贷款五级形态类型
          LN_FNC_STS_TP_ID                            ,--贷款四级形态类型
          IDY_CL_ID                                   ,--行业代码
          PD_GRP_CD                                   ,--产品组代码
          PD_SUB_CD                                   ,--产品字代码
          ENT_IDV_IND                                 ,--企业/个人标志
          RSPL_TP_Id                                  ,--责任类型
          CUR_Year_NPERF_FNC_STS_CHG_F                ,
          CUR_Year_NPERF_FR_RSLT_CHG_F                , 
          CCY                                         ,--币种
          CDR_YR                                      ,--年份YYYY
          CDR_MTH                                     ,--月份MM
          OU_ID                                       ,--信贷员归属机构号
          NOCLD_In_MTH                                ,--当月日历天数
          NOD_In_MTH                                  ,--当月有效天数
          NOCLD_In_QTR                                ,--当季日历天数
          NOD_In_QTR                                  ,--当季有效天数
          NOCLD_In_Year                               ,--当年日历天数
          NOD_In_Year                                 ,--当年有效天数
          ACG_DT                                      ,--日期YYYY-MM-DD
          LN_BAL                                      ,--贷款余额
          LN_PNP                                      ,--贷款本金
          YTD_INT_INCM_of_LN                          ,--当年贷款利息收入
          NBR_LN_AR                                   ,--贷款账户数
          NBR_CST                                     ,--客户数量
          RSPL_AMT_LN_BAL                             ,--贷款余额责任比
          RSPL_AMT_LN_PNP                             ,--贷款本金责任比
          RSPL_AMT_LN_INT_INCM                        ,--贷款利息责任比
          ON_BST_INT_RCVB                             ,--表内应收利息余额
          OFF_BST_INT_RCVB                            ,--表外应收利息余额
          MTD_ACML_LN_BAL_AMT                         ,--月贷款累计余额
          QTD_ACML_LN_BAL_AMT                         ,--季贷款累计余额
          YTD_ACML_LN_BAL_AMT                         ,--年贷款累计余额
          NBR_NEW_CST_CRN_YEAR                        ,--当年新增客户数
          NBR_NEW_AC_in_CUR_Year                      ,
          TOT_YTD_NBR_LN_DRDWN                        ,--年累计贷款发放笔数
          TOT_YTD_NBR_LN_REPYMT_RCVD                  ,--年累计收回贷款笔数
          TOT_YTD_LN_DRDWN_AMT                        ,--当年贷款累放金额
          TOT_YTD_AMT_LN_RPYMT_RCVD                   ,--当年贷款累收金额
          RSPL_AMT_MTD_ACML_LN_BAL                    ,--月累计余额责任比金额
          RSPL_AMT_QTD_ACML_LN_BAL                    ,--季累计余额责任比金额
          RSPL_AMT_YTD_ACML_LN_BAL                    ,--年累计余额责任比金额
          RSPL_AMT_YTD_LN_DRDWN_AMT                   ,--年累放金额责任比金额
          RSPL_AMT_YTD_LN_RPYMT_AMT_RCVD              --年累收金额的责任比金额
          ,RSPL_ON_BST_INT_RCVB                       --表内应收利息余额责任比   
					,RSPL_OFF_BST_INT_RCVB                      --表外应收利息余额责任比 
					,PD_UN_CODE																	--产品联社代码  
				)
         =(TAG.CR_MGR_ID                      
          ,TAG.LN_GNT_TP_ID                  
          ,TAG.LN_PPS_TP_ID                  
          ,TAG.CTR_CGY_ID                  
          ,TAG.FND_SRC_TP_ID                 
          ,TAG.LN_FIVE_RTG_STS               
          ,TAG.LN_FNC_STS_TP_ID              
          ,TAG.IDY_CL_ID                     
          ,TAG.PD_GRP_CD                     
          ,TAG.PD_SUB_CD                     
          ,TAG.ENT_IDV_IND                   
          ,TAG.RSPL_TP_ID
          ,TAG.CUR_Year_NPERF_FNC_STS_CHG_F
          ,TAG.CUR_Year_NPERF_FR_RSLT_CHG_F           
          ,TAG.CCY                           
          ,TAG.CDR_YR                        
          ,TAG.CDR_MTH                       
          ,SOC.OU_ID 
          ,CUR_DAY
          ,COALESCE(TAG.NOD_In_MTH,0) + 1
          ,QTR_DAY
          ,COALESCE(TAG.NOD_In_QTR,0) + 1
          ,YR_DAY
          ,COALESCE(TAG.NOD_In_Year,0) + 1
          ,SMY_DATE
          ,SOC.LN_BAL                        
          ,SOC.LN_PNP                        
          ,SOC.YTD_INT_INCM_OF_LN            
          ,SOC.NBR_LN_AR                     
          ,SOC.NBR_CST                       
          ,SOC.RSPL_AMT_LN_BAL               
          ,SOC.RSPL_AMT_LN_PNP               
          ,SOC.RSPL_AMT_LN_INT_INCM          
          ,SOC.ON_BST_INT_RCVB               
          ,SOC.OFF_BST_INT_RCVB              
          ,SOC.MTD_ACML_LN_BAL_AMT           
          ,SOC.QTD_ACML_LN_BAL_AMT           
          ,SOC.YTD_ACML_LN_BAL_AMT           
          ,SOC.NBR_NEW_CST_CRN_YEAR  
          ,SOC.NBR_NEW_AC_in_CUR_Year        
          ,SOC.TOT_YTD_NBR_LN_DRDWN          
          ,SOC.TOT_YTD_NBR_LN_REPYMT_RCVD    
          ,SOC.TOT_YTD_LN_DRDWN_AMT          
          ,SOC.TOT_YTD_AMT_LN_RPYMT_RCVD     
          ,SOC.RSPL_AMT_MTD_ACML_LN_BAL      
          ,SOC.RSPL_AMT_QTD_ACML_LN_BAL      
          ,SOC.RSPL_AMT_YTD_ACML_LN_BAL      
          ,SOC.RSPL_AMT_YTD_LN_DRDWN_AMT     
          ,SOC.RSPL_AMT_YTD_LN_RPYMT_AMT_RCVD
          ,SOC.RSPL_ON_BST_INT_RCVB                       --表内应收利息余额责任比   
					,SOC.RSPL_OFF_BST_INT_RCVB                      --表外应收利息余额责任比   
					,SOC.PD_UN_CODE																	--产品联社代码
          )

  WHEN NOT MATCHED THEN
      INSERT
         (CR_MGR_ID                                   ,--信贷员               
         LN_GNT_TP_ID                                ,--贷款担保类型          
         LN_PPS_TP_ID                                ,--贷款用途类型          
         CTR_CGY_ID                                ,--业务品种              
         FND_SRC_TP_ID                               ,--资金来源              
         LN_FIVE_RTG_STS                             ,--贷款五级形态类型      
         LN_FNC_STS_TP_ID                            ,--贷款四级形态类型      
         IDY_CL_ID                                   ,--行业代码              
         PD_GRP_CD                                   ,--产品组代码            
         PD_SUB_CD                                   ,--产品字代码            
         ENT_IDV_IND                                 ,--企业/个人标志         
         RSPL_TP_Id                                  ,--责任类型   
         CUR_Year_NPERF_FNC_STS_CHG_F                ,
         CUR_Year_NPERF_FR_RSLT_CHG_F                ,                   
         CCY                                         ,--币种                  
         CDR_YR                                      ,--年份YYYY              
         CDR_MTH                                     ,--月份MM                
         OU_ID                                       ,--信贷员归属机构号      
         NOCLD_In_MTH                                ,--当月日历天数          
         NOD_In_MTH                                  ,--当月有效天数          
         NOCLD_In_QTR                                ,--当季日历天数          
         NOD_In_QTR                                  ,--当季有效天数          
         NOCLD_In_Year                               ,--当年日历天数          
         NOD_In_Year                                 ,--当年有效天数          
         ACG_DT                                      ,--日期YYYY-MM-DD        
         LN_BAL                                      ,--贷款余额              
         LN_PNP                                      ,--贷款本金              
         YTD_INT_INCM_of_LN                          ,--当年贷款利息收入      
         NBR_LN_AR                                   ,--贷款账户数            
         NBR_CST                                     ,--客户数量              
         RSPL_AMT_LN_BAL                             ,--贷款余额责任比        
         RSPL_AMT_LN_PNP                             ,--贷款本金责任比        
         RSPL_AMT_LN_INT_INCM                        ,--贷款利息责任比        
         ON_BST_INT_RCVB                             ,--表内应收利息余额      
         OFF_BST_INT_RCVB                            ,--表外应收利息余额      
         MTD_ACML_LN_BAL_AMT                         ,--月贷款累计余额        
         QTD_ACML_LN_BAL_AMT                         ,--季贷款累计余额        
         YTD_ACML_LN_BAL_AMT                         ,--年贷款累计余额        
         NBR_NEW_CST_CRN_YEAR                        ,--当年新增客户数   
         NBR_NEW_AC_in_CUR_Year                      ,     
         TOT_YTD_NBR_LN_DRDWN                        ,--年累计贷款发放笔数    
         TOT_YTD_NBR_LN_REPYMT_RCVD                  ,--年累计收回贷款笔数    
         TOT_YTD_LN_DRDWN_AMT                        ,--当年贷款累放金额      
         TOT_YTD_AMT_LN_RPYMT_RCVD                   ,--当年贷款累收金额      
         RSPL_AMT_MTD_ACML_LN_BAL                    ,--月累计余额责任比金额  
         RSPL_AMT_QTD_ACML_LN_BAL                    ,--季累计余额责任比金额  
         RSPL_AMT_YTD_ACML_LN_BAL                    ,--年累计余额责任比金额  
         RSPL_AMT_YTD_LN_DRDWN_AMT                   ,--年累放金额责任比金额  
         RSPL_AMT_YTD_LN_RPYMT_AMT_RCVD              --年累收金额的责任比金额
         ,RSPL_ON_BST_INT_RCVB                       --表内应收利息余额责任比   
				 ,RSPL_OFF_BST_INT_RCVB                      --表外应收利息余额责任比   
				 ,PD_UN_CODE																 --产品联社代码
       )
     VALUES
          (CR_MGR_ID                      
          ,LN_GNT_TP_ID                  
          ,LN_PPS_TP_ID                  
          ,CTR_CGY_ID                  
          ,FND_SRC_TP_ID                 
          ,LN_FIVE_RTG_STS               
          ,LN_FNC_STS_TP_ID              
          ,IDY_CL_ID                     
          ,PD_GRP_CD                     
          ,PD_SUB_CD                     
          ,ENT_IDV_IND                   
          ,RSPL_TP_ID  
          ,CUR_Year_NPERF_FNC_STS_CHG_F
          ,CUR_Year_NPERF_FR_RSLT_CHG_F 
          ,CCY                           
          ,CDR_YR                        
          ,CDR_MTH                       
          ,OU_ID 
          ,CUR_DAY
          ,1
          ,QTR_DAY
          ,1
          ,YR_DAY
          ,1
          ,SMY_DATE
          ,LN_BAL                        
          ,LN_PNP                        
          ,YTD_INT_INCM_OF_LN            
          ,NBR_LN_AR                     
          ,NBR_CST                       
          ,RSPL_AMT_LN_BAL               
          ,RSPL_AMT_LN_PNP               
          ,RSPL_AMT_LN_INT_INCM          
          ,ON_BST_INT_RCVB               
          ,OFF_BST_INT_RCVB              
          ,MTD_ACML_LN_BAL_AMT           
          ,QTD_ACML_LN_BAL_AMT           
          ,YTD_ACML_LN_BAL_AMT           
          ,NBR_NEW_CST_CRN_YEAR  
          ,NBR_NEW_AC_in_CUR_Year        
          ,TOT_YTD_NBR_LN_DRDWN          
          ,TOT_YTD_NBR_LN_REPYMT_RCVD    
          ,TOT_YTD_LN_DRDWN_AMT          
          ,TOT_YTD_AMT_LN_RPYMT_RCVD     
          ,RSPL_AMT_MTD_ACML_LN_BAL      
          ,RSPL_AMT_QTD_ACML_LN_BAL      
          ,RSPL_AMT_YTD_ACML_LN_BAL      
          ,RSPL_AMT_YTD_LN_DRDWN_AMT     
          ,RSPL_AMT_YTD_LN_RPYMT_AMT_RCVD
          ,RSPL_ON_BST_INT_RCVB                       --表内应收利息余额责任比   
					,RSPL_OFF_BST_INT_RCVB                      --表外应收利息余额责任比 
					,PD_UN_CODE																	--产品联社代码  
          );--

  
END IF ;--

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
SET SMY_STEPNUM = SMY_STEPNUM+1;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
 

END@