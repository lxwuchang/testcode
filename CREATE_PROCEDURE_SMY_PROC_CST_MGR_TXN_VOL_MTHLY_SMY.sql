CREATE PROCEDURE SMY.PROC_CST_MGR_TXN_VOL_MTHLY_SMY(ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_DB_CRD_DEP_INCM_MTHLY_SMY.sql
-- Procedure name: 			SMY.PROC_DB_CRD_DEP_INCM_MTHLY_SMY
-- Source Table:				SMY.CR_CRD_SMY,SOR.CST_MGR_PRFT_PCT_INF,SMY.DEP_AR_SMY,SOR.ONLINE_TXN_RUN,SMY.DB_CRD_SMY,SMY.LOAN_AR_SMY
-- Target Table: 				SMY.CST_MGR_TXN_VOL_MTHLY_SMY
-- Project:             ZJ RCCB EDW
-- Note                 Delete and Insert and Update
-- Purpose:             
--
--=============================================================================
-- Creation Date:       2009.11.09
-- Origin Author:       Peng Jie
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2009-11-09   Peng Jie     Create SP File
-- 2009-12-04   Xu Yan       Rename the history table		
-------------------------------------------------------------------------------
LANGUAGE SQL
BEGIN
/*声明异常处理使用变量*/
DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
DECLARE SMY_STEPNUM INT DEFAULT 1;                     --过程内部位置标记
DECLARE SMY_STEPDESC VARCHAR(100) DEFAULT '';          --过程内部位置描述
DECLARE SMY_DATE DATE;                                 --临时日期变量
DECLARE SMY_RCOUNT INT;                                --DML语句作用记录数
DECLARE SMY_PROCNM VARCHAR(100);    
DECLARE CUR_YEAR SMALLINT;
DECLARE CUR_MONTH SMALLINT;
DECLARE CUR_DAY INTEGER;
DECLARE MAX_ACG_DT DATE;

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
SET SMY_PROCNM = 'PROC_CST_MGR_TXN_VOL_MTHLY_SMY';
SET SMY_DATE=ACCOUNTING_DATE;
SET CUR_YEAR=YEAR(ACCOUNTING_DATE);  --取当前年份
SET CUR_MONTH=MONTH(ACCOUNTING_DATE); --取当前月份
SET CUR_DAY=DAY(ACCOUNTING_DATE);     --取当日
SELECT MAX(ACG_DT) INTO MAX_ACG_DT FROM SMY.MTHLY_CR_CRD_AC_ACML_BAL_AMT;	

/*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE,并插入新的起始标志*/
DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;
COMMIT;
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, 'PROCEDURE START.', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);
COMMIT;
	
/*数据恢复与备份*/
IF MAX_ACG_DT=ACCOUNTING_DATE THEN
   DELETE FROM SMY.CST_MGR_TXN_VOL_MTHLY_SMY WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;
   COMMIT;
   IF CUR_DAY<>1 THEN
      INSERT INTO SMY.CST_MGR_TXN_VOL_MTHLY_SMY SELECT * FROM HIS.CST_MGR_TXN_VOL_MTHLY_SMY;
      COMMIT;
   END IF;
ELSE
   DELETE FROM HIS.CST_MGR_TXN_VOL_MTHLY_SMY;
   COMMIT;
   INSERT INTO HIS.CST_MGR_TXN_VOL_MTHLY_SMY SELECT * FROM SMY.CST_MGR_TXN_VOL_MTHLY_SMY WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;
   COMMIT;
END IF;

SET SMY_STEPNUM = SMY_STEPNUM+1;
SET SMY_STEPDESC = '创建临时表,并把当日数据插入';

DECLARE GLOBAL TEMPORARY TABLE TMP_CST_MGR_TXN_VOL_MTHLY_SMY(CST_MGR_ID              CHARACTER(18)
																												 ,AC_OU_ID               CHARACTER(18)
																												 ,OU_ID                  CHARACTER(18)
																												 ,CSH_TFR_TP_ID          INTEGER
																												 ,TXN_CD                 CHARACTER(6)
																												 ,ACG_TXN_RUN_TP_ID      INTEGER
																												 ,CCY                    CHARACTER(3)
																												 ,CDR_YR                 SMALLINT
																												 ,CDR_MTH                SMALLINT
																												 ,ACG_DT                 DATE
																												 ,NBR_TXN                INTEGER
																												 ,TXN_AMT                DECIMAL(17,2))
ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(CST_MGR_ID); 																													 


INSERT INTO SESSION.TMP_CST_MGR_TXN_VOL_MTHLY_SMY
  (CST_MGR_ID
   ,AC_OU_ID
   ,OU_ID
   ,CSH_TFR_TP_ID
   ,TXN_CD
   ,ACG_TXN_RUN_TP_ID
   ,CCY
   ,CDR_YR
   ,CDR_MTH
   ,ACG_DT
   ,NBR_TXN
   ,TXN_AMT)
WITH TMP_ONLINE_TXN_RUN AS ( SELECT 
                               TXN_AR_ID
                               ,CASH_TFR_TP_ID
                               ,SUB_TXN_CODE_SEP_CODE
                               ,ACG_TXN_RUN_TP_ID
                               ,TXN_DNMN_CCY_ID
                               ,COUNT(*) AS NBR_TXN
                               ,sum(TXN_AMT) AS TXN_AMT
                             FROM SOR.ONLINE_TXN_RUN
                             WHERE TXN_DT = SMY_DATE AND TXN_RED_BLUE_TP_ID = 15200001 AND PST_RVRSL_TP_ID = 15130001
                             GROUP BY 
                               TXN_AR_ID
                               ,CASH_TFR_TP_ID
                               ,SUB_TXN_CODE_SEP_CODE
                               ,ACG_TXN_RUN_TP_ID
                               ,TXN_DNMN_CCY_ID),
  TMP_CST_MGR_PRFT_PCT_INF_DEP  AS ( SELECT 
                                      AC_NO                  ,
                                      CR_MGR_ID              --,
                                      --CTR_OU_ID              
                                    FROM SOR.CST_MGR_PRFT_PCT_INF 
                                    WHERE DEL_F=0 AND AC_AR_TP_ID = 15840002),
  TMP_CST_MGR_PRFT_PCT_INF_CAD AS ( SELECT 
                                      AC_NO                  ,
                                      CR_MGR_ID              --,
                                      --CTR_OU_ID              
                                    FROM SOR.CST_MGR_PRFT_PCT_INF 
                                    WHERE DEL_F=0 AND AC_AR_TP_ID = 15840003),                                                                  
  TMP_CST_MGR_PRFT_PCT_INF_LN  AS ( SELECT 
                                      AC_NO                  ,
                                      CR_MGR_ID              --,
                                      --CTR_OU_ID              
                                    FROM SOR.CST_MGR_PRFT_PCT_INF 
                                    WHERE DEL_F=0 AND AC_AR_TP_ID = 15840001),
                       TMP_CRD AS ( SELECT 
                                      CRD_NO                 ,
                                      AC_OU_ID               
                                    FROM
                                   (SELECT 
                                      CRD_NO                 ,
                                      AC_OU_ID
                                    FROM SMY.DB_CRD_SMY
                                    UNION ALL
                                    SELECT 
                                      CRD_NO                 ,
                                      ISSU_CRD_OU_ID AS AC_OU_ID
                                    FROM SMY.CR_CRD_SMY) AS A)
SELECT 
  CR_MGR_ID                  ,
  RPRG_OU_IP_ID              ,
  RPT_OU_IP_ID                  ,
  CASH_TFR_TP_ID             ,
  SUB_TXN_CODE_SEP_CODE      ,
  ACG_TXN_RUN_TP_ID          ,
  TXN_DNMN_CCY_ID            ,
  CUR_YEAR                   ,
  CUR_MONTH                  ,
  SMY_DATE                   ,
  SUM(NBR_TXN)               ,
  SUM(TXN_AMT)               
FROM
(SELECT 
  a.CR_MGR_ID                   ,
  b.RPRG_OU_IP_ID              ,
  d.RPT_OU_IP_ID                  ,
  COALESCE(c.CASH_TFR_TP_ID, -1)     CASH_TFR_TP_ID        ,
  COALESCE(c.SUB_TXN_CODE_SEP_CODE, '')   SUB_TXN_CODE_SEP_CODE   ,
  COALESCE(c.ACG_TXN_RUN_TP_ID ,-1)     ACG_TXN_RUN_TP_ID   ,
  COALESCE(c.TXN_DNMN_CCY_ID ,'')     TXN_DNMN_CCY_ID      ,
  CUR_YEAR      as CUR_YEAR             ,
  CUR_MONTH     as CUR_MONTH             ,
  SMY_DATE      as SMY_DATE             ,
  SUM(COALESCE(NBR_TXN, 0))     AS  NBR_TXN       ,
  SUM(COALESCE(TXN_AMT, 0.00))  AS  TXN_AMT     
FROM TMP_CST_MGR_PRFT_PCT_INF_DEP a INNER JOIN SMY.DEP_AR_SMY b ON b.DEP_AR_ID=a.AC_NO
                                    LEFT JOIN TMP_ONLINE_TXN_RUN c ON a.AC_NO = c.TXN_AR_ID
                                    LEFT JOIN SOR.TELLER d ON a.CR_MGR_ID = d.TELLER_ID
GROUP BY       
  CR_MGR_ID                  ,
  RPRG_OU_IP_ID              ,
  RPT_OU_IP_ID                  ,
  CASH_TFR_TP_ID             ,
  SUB_TXN_CODE_SEP_CODE      ,
  ACG_TXN_RUN_TP_ID          ,
  TXN_DNMN_CCY_ID            ,
  CUR_YEAR                   ,
  CUR_MONTH                  ,
  SMY_DATE         
UNION ALL
SELECT 
  a.CR_MGR_ID                  ,
  b.AC_OU_ID      as RPRG_OU_IP_ID             ,
  d.RPT_OU_IP_ID                  ,
  COALESCE(c.CASH_TFR_TP_ID , -1)   CASH_TFR_TP_ID         ,
  COALESCE(c.SUB_TXN_CODE_SEP_CODE, '')  SUB_TXN_CODE_SEP_CODE    ,
  COALESCE(c.ACG_TXN_RUN_TP_ID, -1)    ACG_TXN_RUN_TP_ID      ,
  COALESCE(c.TXN_DNMN_CCY_ID, '')   TXN_DNMN_CCY_ID         ,
  CUR_YEAR     as CUR_YEAR              ,
  CUR_MONTH    as CUR_MONTH              ,
  SMY_DATE     as SMY_DATE              ,
  SUM(COALESCE(NBR_TXN, 0))     AS  NBR_TXN       ,
  SUM(COALESCE(TXN_AMT, 0.00))  AS  TXN_AMT  
FROM  TMP_CST_MGR_PRFT_PCT_INF_CAD a INNER JOIN TMP_CRD b ON b.CRD_NO=a.AC_NO
                                     LEFT JOIN TMP_ONLINE_TXN_RUN c ON a.AC_NO = c.TXN_AR_ID
                                     LEFT JOIN SOR.TELLER d ON a.CR_MGR_ID = d.TELLER_ID
GROUP BY       
  CR_MGR_ID                  ,
  AC_OU_ID                   ,
  RPT_OU_IP_ID                  ,
  CASH_TFR_TP_ID             ,
  SUB_TXN_CODE_SEP_CODE      ,
  ACG_TXN_RUN_TP_ID          ,
  TXN_DNMN_CCY_ID            ,
  CUR_YEAR                   ,
  CUR_MONTH                  ,
  SMY_DATE
UNION ALL
SELECT 
  a.CR_MGR_ID                  ,
  b.RPRG_OU_IP_ID              ,
  d.RPT_OU_IP_ID                  ,
  COALESCE(c.CASH_TFR_TP_ID, -1)    CASH_TFR_TP_ID         ,
  COALESCE(c.SUB_TXN_CODE_SEP_CODE, '') SUB_TXN_CODE_SEP_CODE     ,
  COALESCE(c.ACG_TXN_RUN_TP_ID , -1)   ACG_TXN_RUN_TP_ID      ,
  COALESCE(c.TXN_DNMN_CCY_ID , '')       TXN_DNMN_CCY_ID     ,
  CUR_YEAR        as CUR_YEAR           ,
  CUR_MONTH       as CUR_MONTH           ,
  SMY_DATE        as SMY_DATE           ,
  SUM(COALESCE(NBR_TXN, 0))     AS  NBR_TXN       ,
  SUM(COALESCE(TXN_AMT, 0.00))  AS  TXN_AMT  
FROM TMP_CST_MGR_PRFT_PCT_INF_LN a INNER JOIN SMY.LOAN_AR_SMY b ON b.LN_AR_ID=a.AC_NO
                                   LEFT JOIN TMP_ONLINE_TXN_RUN c ON a.AC_NO = c.TXN_AR_ID
                                   LEFT JOIN SOR.TELLER d ON a.CR_MGR_ID = d.TELLER_ID
GROUP BY       
  CR_MGR_ID                  ,
  RPRG_OU_IP_ID              ,
  RPT_OU_IP_ID                  ,
  CASH_TFR_TP_ID             ,
  SUB_TXN_CODE_SEP_CODE      ,
  ACG_TXN_RUN_TP_ID          ,
  TXN_DNMN_CCY_ID            ,
  CUR_YEAR                   ,
  CUR_MONTH                  ,
  SMY_DATE) AS A
GROUP BY 
  CR_MGR_ID                  ,
  RPRG_OU_IP_ID              ,
  RPT_OU_IP_ID                  ,
  CASH_TFR_TP_ID             ,
  SUB_TXN_CODE_SEP_CODE      ,
  ACG_TXN_RUN_TP_ID          ,
  TXN_DNMN_CCY_ID            ,
  CUR_YEAR                   ,
  CUR_MONTH                  ,
  SMY_DATE;

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);

SET SMY_STEPNUM = SMY_STEPNUM+1;
 
IF CUR_DAY = 1 THEN
   IF CUR_MONTH = 1 THEN
      SET SMY_STEPDESC = '插入年初数据';
      INSERT INTO SMY.CST_MGR_TXN_VOL_MTHLY_SMY(CST_MGR_ID         
																							 ,AC_OU_ID           
																							 ,OU_ID              
																							 ,CSH_TFR_TP_ID      
																							 ,TXN_CD             
																							 ,ACG_TXN_RUN_TP_ID  
																							 ,CCY                
																							 ,CDR_YR             
																							 ,CDR_MTH            
																							 ,ACG_DT             
																							 ,NBR_TXN            
																							 ,TXN_AMT            
                                               ,TOT_MTD_NBR_TXN
                                               ,TOT_MTD_AMT
                                               ,TOT_QTD_AMT
                                               ,TOT_QTD_NBR_TXN
                                               ,TOT_YTD_AMT
                                               ,TOT_YTD_NBR_TXN)
      SELECT 
         CST_MGR_ID         
				,AC_OU_ID           
				,OU_ID              
				,CSH_TFR_TP_ID      
				,TXN_CD             
				,ACG_TXN_RUN_TP_ID  
				,CCY                
				,CDR_YR             
				,CDR_MTH            
				,ACG_DT             
				,NBR_TXN            
				,TXN_AMT
				,NBR_TXN
				,TXN_AMT        
				,TXN_AMT
				,NBR_TXN   
				,TXN_AMT
				,NBR_TXN
			FROM SESSION.TMP_CST_MGR_TXN_VOL_MTHLY_SMY;
			
  ELSEIF CUR_MONTH IN (4, 7, 10) THEN
      SET SMY_STEPDESC = '插入季初数据';
      INSERT INTO SMY.CST_MGR_TXN_VOL_MTHLY_SMY(CST_MGR_ID         
																							 ,AC_OU_ID           
																							 ,OU_ID              
																							 ,CSH_TFR_TP_ID      
																							 ,TXN_CD             
																							 ,ACG_TXN_RUN_TP_ID  
																							 ,CCY                
																							 ,CDR_YR             
																							 ,CDR_MTH            
																							 ,ACG_DT             
																							 ,NBR_TXN            
																							 ,TXN_AMT            
                                               ,TOT_MTD_NBR_TXN
                                               ,TOT_MTD_AMT
                                               ,TOT_QTD_AMT
                                               ,TOT_QTD_NBR_TXN
                                               ,TOT_YTD_AMT
                                               ,TOT_YTD_NBR_TXN)
      SELECT 
         a.CST_MGR_ID         
				,a.AC_OU_ID           
				,a.OU_ID              
				,a.CSH_TFR_TP_ID      
				,a.TXN_CD             
				,a.ACG_TXN_RUN_TP_ID  
				,a.CCY                
				,a.CDR_YR             
				,a.CDR_MTH            
				,a.ACG_DT             
				,a.NBR_TXN
				,a.TXN_AMT
				,a.NBR_TXN
				,a.TXN_AMT				     
				,a.TXN_AMT
				,a.NBR_TXN  
        ,COALESCE(b.TOT_YTD_AMT,0.00) + a.TXN_AMT
        ,COALESCE(b.TOT_YTD_NBR_TXN,0) + a.NBR_TXN
      FROM SESSION.TMP_CST_MGR_TXN_VOL_MTHLY_SMY a LEFT JOIN SMY.CST_MGR_TXN_VOL_MTHLY_SMY b
      ON a.CST_MGR_ID        = b.CST_MGR_ID AND
         a.AC_OU_ID          = b.AC_OU_ID AND
         a.CSH_TFR_TP_ID     = b.CSH_TFR_TP_ID AND
         a.TXN_CD            = b.TXN_CD AND
         a.ACG_TXN_RUN_TP_ID = b.ACG_TXN_RUN_TP_ID AND
         a.CCY               = b.CCY AND
         a.CDR_YR            = b.CDR_YR AND
         a.CDR_MTH           = b.CDR_MTH;

    ELSE
    	SET SMY_STEPDESC = '插入非年初或季初的月初数据';
      INSERT INTO SMY.CST_MGR_TXN_VOL_MTHLY_SMY(CST_MGR_ID         
																							 ,AC_OU_ID           
																							 ,OU_ID              
																							 ,CSH_TFR_TP_ID      
																							 ,TXN_CD             
																							 ,ACG_TXN_RUN_TP_ID  
																							 ,CCY                
																							 ,CDR_YR             
																							 ,CDR_MTH            
																							 ,ACG_DT             
																							 ,NBR_TXN            
																							 ,TXN_AMT            
                                               ,TOT_MTD_NBR_TXN
                                               ,TOT_MTD_AMT
                                               ,TOT_QTD_AMT
                                               ,TOT_QTD_NBR_TXN
                                               ,TOT_YTD_AMT
                                               ,TOT_YTD_NBR_TXN)
      SELECT 
         a.CST_MGR_ID         
				,a.AC_OU_ID           
				,a.OU_ID              
				,a.CSH_TFR_TP_ID      
				,a.TXN_CD             
				,a.ACG_TXN_RUN_TP_ID  
				,a.CCY                
				,a.CDR_YR             
				,a.CDR_MTH            
				,a.ACG_DT             
				,a.NBR_TXN
				,a.TXN_AMT
				,a.NBR_TXN
				,a.TXN_AMT				     
				,COALESCE(b.TOT_QTD_AMT, 0.00) + a.TXN_AMT
				,COALESCE(b.TOT_QTD_NBR_TXN, 0) + a.NBR_TXN  
        ,COALESCE(b.TOT_YTD_AMT, 0.00) + a.TXN_AMT
        ,COALESCE(b.TOT_YTD_NBR_TXN, 0) + a.NBR_TXN
      FROM SESSION.TMP_CST_MGR_TXN_VOL_MTHLY_SMY a LEFT JOIN SMY.CST_MGR_TXN_VOL_MTHLY_SMY b
      ON a.CST_MGR_ID        = b.CST_MGR_ID AND
         a.AC_OU_ID          = b.AC_OU_ID AND
         a.CSH_TFR_TP_ID     = b.CSH_TFR_TP_ID AND
         a.TXN_CD            = b.TXN_CD AND
         a.ACG_TXN_RUN_TP_ID = b.ACG_TXN_RUN_TP_ID AND
         a.CCY               = b.CCY AND
         a.CDR_YR            = b.CDR_YR AND
         a.CDR_MTH           = b.CDR_MTH;

   END IF;
ELSE
	SET SMY_STEPDESC = 'merge非月初数据';
  MERGE INTO 	SMY.CST_MGR_TXN_VOL_MTHLY_SMY TAG
  USING SESSION.TMP_CST_MGR_TXN_VOL_MTHLY_SMY SOC	
      ON TAG.CST_MGR_ID        = SOC.CST_MGR_ID AND
         TAG.AC_OU_ID          = SOC.AC_OU_ID AND
         TAG.CSH_TFR_TP_ID     = SOC.CSH_TFR_TP_ID AND
         TAG.TXN_CD            = SOC.TXN_CD AND
         TAG.ACG_TXN_RUN_TP_ID = SOC.ACG_TXN_RUN_TP_ID AND
         TAG.CCY               = SOC.CCY AND
         TAG.CDR_YR            = SOC.CDR_YR AND
         TAG.CDR_MTH           = SOC.CDR_MTH
	WHEN MATCHED THEN
	         UPDATE SET 
                (CST_MGR_ID         
								 ,AC_OU_ID           
								 ,OU_ID              
								 ,CSH_TFR_TP_ID      
								 ,TXN_CD             
								 ,ACG_TXN_RUN_TP_ID  
								 ,CCY                
								 ,CDR_YR             
								 ,CDR_MTH            
								 ,ACG_DT             
								 ,NBR_TXN            
								 ,TXN_AMT            
                 ,TOT_MTD_NBR_TXN
                 ,TOT_MTD_AMT
                 ,TOT_QTD_AMT
                 ,TOT_QTD_NBR_TXN
                 ,TOT_YTD_AMT
                 ,TOT_YTD_NBR_TXN)
               =( TAG.CST_MGR_ID         
								 ,TAG.AC_OU_ID           
								 ,TAG.OU_ID              
								 ,TAG.CSH_TFR_TP_ID      
								 ,TAG.TXN_CD             
								 ,TAG.ACG_TXN_RUN_TP_ID  
								 ,TAG.CCY                
								 ,TAG.CDR_YR             
								 ,TAG.CDR_MTH            
								 ,TAG.ACG_DT             
								 ,SOC.NBR_TXN            
								 ,SOC.TXN_AMT
								 ,TAG.TOT_MTD_NBR_TXN + SOC.NBR_TXN
								 ,TAG.TOT_MTD_AMT + SOC.TXN_AMT
								 ,TAG.TOT_QTD_AMT + SOC.TXN_AMT
								 ,TAG.TOT_QTD_NBR_TXN + SOC.NBR_TXN
								 ,TAG.TOT_YTD_AMT + SOC.TXN_AMT
								 ,TAG.TOT_YTD_NBR_TXN + SOC.NBR_TXN)
  WHEN NOT MATCHED THEN
              INSERT 								 
                (CST_MGR_ID         
								 ,AC_OU_ID           
								 ,OU_ID              
								 ,CSH_TFR_TP_ID      
								 ,TXN_CD             
								 ,ACG_TXN_RUN_TP_ID  
								 ,CCY                
								 ,CDR_YR             
								 ,CDR_MTH            
								 ,ACG_DT             
								 ,NBR_TXN            
								 ,TXN_AMT            
                 ,TOT_MTD_NBR_TXN
                 ,TOT_MTD_AMT
                 ,TOT_QTD_AMT
                 ,TOT_QTD_NBR_TXN
                 ,TOT_YTD_AMT
                 ,TOT_YTD_NBR_TXN)		
               VALUES
                 (SOC.CST_MGR_ID         
				         ,SOC.AC_OU_ID           
				         ,SOC.OU_ID              
				         ,SOC.CSH_TFR_TP_ID      
				         ,SOC.TXN_CD             
				         ,SOC.ACG_TXN_RUN_TP_ID  
				         ,SOC.CCY                
				         ,SOC.CDR_YR             
				         ,SOC.CDR_MTH            
				         ,SOC.ACG_DT             
				         ,SOC.NBR_TXN            
				         ,SOC.TXN_AMT
				         ,SOC.NBR_TXN
				         ,SOC.TXN_AMT        
				         ,SOC.TXN_AMT
				         ,SOC.NBR_TXN   
				         ,SOC.TXN_AMT
				         ,SOC.NBR_TXN  );               						 
END IF ;


GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);
 




END
@