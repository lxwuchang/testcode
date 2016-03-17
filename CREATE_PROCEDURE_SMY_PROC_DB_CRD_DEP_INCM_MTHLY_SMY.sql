CREATE PROCEDURE SMY.PROC_DB_CRD_DEP_INCM_MTHLY_SMY(ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_DB_CRD_DEP_INCM_MTHLY_SMY.sql
-- Procedure name: 			SMY.PROC_DB_CRD_DEP_INCM_MTHLY_SMY
-- Source Table:				SOR.DB_CRD,SOR.CRD,SOR.ONLINE_TXN_RUN
-- Target Table: 				SMY.DB_CRD_DEP_INCM_MTHLY_SMY
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
-- 2010-01-05   Xu Yan       Replaced the 'DEAL_OU_IP_ID' with 'RPRG_OU_IP_ID'
-- 2010-01-20   Xu Yan       Inserted the records of the last month into current month to make the set complete.
-- 2010-02-02   Xu Yan       Updatd a previous bug
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
SET SMY_PROCNM = 'PROC_DB_CRD_DEP_INCM_MTHLY_SMY';
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
   DELETE FROM SMY.DB_CRD_DEP_INCM_MTHLY_SMY WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;
   COMMIT;
   IF CUR_DAY<>1 THEN
      INSERT INTO SMY.DB_CRD_DEP_INCM_MTHLY_SMY SELECT * FROM HIS.DB_CRD_DEP_INCM_MTHLY_SMY;
      COMMIT;
   END IF;
ELSE
   DELETE FROM HIS.DB_CRD_DEP_INCM_MTHLY_SMY;
   COMMIT;
   INSERT INTO HIS.DB_CRD_DEP_INCM_MTHLY_SMY SELECT * FROM SMY.DB_CRD_DEP_INCM_MTHLY_SMY WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;
   COMMIT;
END IF;

SET SMY_STEPNUM = SMY_STEPNUM+1;
SET SMY_STEPDESC = '创建临时表,并把当日数据插入';

DECLARE GLOBAL TEMPORARY TABLE TMP_DB_CRD_DEP_INCM_MTHLY_SMY(OU_ID                 CHARACTER(18)   ,
                                                         		 CRD_TP_ID             INTEGER         ,
                                                         		 PSBK_RLTD_F           SMALLINT        ,
                                                         		 IS_NONGXIN_CRD_F      INTEGER         ,
                                                         		 ENT_IDV_IND           INTEGER     ,
                                                         		 DB_CR_IND             INTEGER         ,
                                                         		 CCY                   CHARACTER(3)    ,
                                                         		 ACG_SBJ_ID            CHARACTER(10)   ,
                                                         		 CDR_YR                SMALLINT        ,
                                                         		 CDR_MTH               SMALLINT        ,
                                                         		 CUR_DAY_INCM_AMT      DECIMAL(17,2))  
ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(OU_ID); 	

INSERT INTO SESSION.TMP_DB_CRD_DEP_INCM_MTHLY_SMY
(OU_ID              ,
 CRD_TP_ID          ,
 PSBK_RLTD_F        ,
 IS_NONGXIN_CRD_F   ,
 ENT_IDV_IND        ,
 DB_CR_IND          ,
 CCY                ,
 ACG_SBJ_ID        ,
 CDR_YR             ,
 CDR_MTH            ,
 CUR_DAY_INCM_AMT   )
WITH TMP_DMD_DEP_AR_TXN_DTL AS 
( SELECT DMD_DEP_AR_ID  ,
         DB_CR_IND      ,
         DNMN_CCY_ID
  FROM SOR.DMD_DEP_AR_TXN_DTL TXN
  WHERE TXN.TXN_DT=SMY_DATE and TXN.TXN_TP_ID=20460007) ,
     TMP_ONLINE_TXN_RUN AS 
( SELECT TXN_AR_ID      ,
         ACG_SBJ_ID     ,
         TXN_AMT  
  FROM SOR.ONLINE_TXN_RUN
  WHERE TXN_DT =  SMY_DATE AND DEL_F = 0 and ACG_TXN_RUN_TP_ID = 15150002 AND TXN_RED_BLUE_TP_ID = 15200001 and PST_RVRSL_TP_ID = 15130001 )   
     
SELECT 
 --a.DEAL_OU_IP_ID                               ,
 COALESCE(b.RPRG_OU_IP_ID, '-1')									 ,
 COALESCE(b.CRD_TP_ID, -1)                     ,
 COALESCE(a.PSBK_RLTD_F, -1)                   ,
 CASE WHEN LEFT(b.CRD_NO, 6) = '940036' then 1 ELSE 0 END  , 
 COALESCE(b.ENT_IDV_CST_IND, -1)                   ,
 COALESCE(c.DB_CR_IND, -1)                     ,
 COALESCE(c.DNMN_CCY_ID, '')                   ,
 COALESCE(d.ACG_SBJ_ID, '')                    ,
 CUR_YEAR                 ,
 CUR_MONTH                ,
 SUM(COALESCE(d.TXN_AMT,0.00))                 
FROM SOR.DB_CRD a LEFT JOIN SOR.CRD b on a.DB_CRD_NO = b.CRD_NO
                  LEFT JOIN TMP_DMD_DEP_AR_TXN_DTL c ON b.AC_AR_ID=c.DMD_DEP_AR_ID
                  LEFT JOIN TMP_ONLINE_TXN_RUN d ON b.AC_AR_ID = d.TXN_AR_ID
GROUP BY  
-- a.DEAL_OU_IP_ID                               ,
 COALESCE(b.RPRG_OU_IP_ID, '-1')                             ,
 COALESCE(b.CRD_TP_ID, -1)                     ,
 COALESCE(a.PSBK_RLTD_F, -1)                   ,
 CASE WHEN LEFT(b.CRD_NO, 6) = '940036' then 1 ELSE 0 END  ,
 COALESCE(b.ENT_IDV_CST_IND, -1)                   ,
 COALESCE(c.DB_CR_IND, -1)                     ,
 COALESCE(c.DNMN_CCY_ID, '')                   ,
 COALESCE(d.ACG_SBJ_ID, '')                    ,
 CUR_YEAR                 ,
 CUR_MONTH ;

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);

SET SMY_STEPNUM = SMY_STEPNUM+1;
 
IF CUR_DAY = 1 THEN
   IF CUR_MONTH = 1 THEN
       SET SMY_STEPDESC = '插入年初数据';
       INSERT INTO SMY.DB_CRD_DEP_INCM_MTHLY_SMY
                    (                    OU_ID
										,CRD_TP_ID
										,PSBK_RLTD_F
										,IS_NONGXIN_CRD_F
										,ENT_IDV_IND
										,DB_CR_IND
										,CCY
										,ACG_SBJ_ID
										,CDR_YR
										,CDR_MTH
										,ACG_DT
										,CUR_DAY_INCM_AMT
										,TOT_MTD_INCM_AMT
										,TOT_QTD_INCM_AMT
										,TOT_YTD_INCM_AMT)
			 SELECT 
				  OU_ID                 ,
				  CRD_TP_ID             ,
				  PSBK_RLTD_F           ,
				  IS_NONGXIN_CRD_F      ,
				  ENT_IDV_IND           ,
				  DB_CR_IND             ,
				  CCY                   ,
				  ACG_SBJ_ID            ,
				  CDR_YR                ,
				  CDR_MTH               ,
				  SMY_DATE              ,
				  CUR_DAY_INCM_AMT      ,
				  CUR_DAY_INCM_AMT      ,
				  CUR_DAY_INCM_AMT      ,
				  CUR_DAY_INCM_AMT      
			 FROM   SESSION.TMP_DB_CRD_DEP_INCM_MTHLY_SMY  ;
   
  ELSEIF CUR_MONTH IN (4, 7, 10) THEN
       SET SMY_STEPDESC = '插入季初数据';
       INSERT INTO  SMY.DB_CRD_DEP_INCM_MTHLY_SMY
                    (OU_ID
										,CRD_TP_ID
										,PSBK_RLTD_F
										,IS_NONGXIN_CRD_F
										,ENT_IDV_IND
										,DB_CR_IND
										,CCY
										,ACG_SBJ_ID
										,CDR_YR
										,CDR_MTH
										,ACG_DT
										,CUR_DAY_INCM_AMT
										,TOT_MTD_INCM_AMT
										,TOT_QTD_INCM_AMT
										,TOT_YTD_INCM_AMT)
			 SELECT 
				  a.OU_ID                 ,
				  a.CRD_TP_ID             ,
				  a.PSBK_RLTD_F           ,
				  a.IS_NONGXIN_CRD_F      ,
				  a.ENT_IDV_IND           ,
				  a.DB_CR_IND             ,
				  a.CCY                   ,
				  a.ACG_SBJ_ID            ,
				  a.CDR_YR                ,
				  a.CDR_MTH               ,
				  SMY_DATE              ,
				  COALESCE(a.CUR_DAY_INCM_AMT, 0.00)      ,
				  COALESCE(a.CUR_DAY_INCM_AMT, 0.00)      ,
				  COALESCE(a.CUR_DAY_INCM_AMT, 0.00)      ,
				  COALESCE(b.TOT_YTD_INCM_AMT, 0.00) + COALESCE(a.CUR_DAY_INCM_AMT ,0.00) 			                         
       FROM SESSION.TMP_DB_CRD_DEP_INCM_MTHLY_SMY a left join SMY.DB_CRD_DEP_INCM_MTHLY_SMY b
       ON a.OU_ID = b.OU_ID and
          a.CRD_TP_ID = b.CRD_TP_ID and
          a.PSBK_RLTD_F = b.PSBK_RLTD_F and
          a.IS_NONGXIN_CRD_F = b.IS_NONGXIN_CRD_F and
          a.ENT_IDV_IND = b.ENT_IDV_IND and
          a.DB_CR_IND = b.DB_CR_IND AND
          a.CCY  = b.CCY  and
          a.ACG_SBJ_ID = b.ACG_SBJ_ID and
          a.CDR_YR = b.CDR_YR and
          a.CDR_MTH -1 = b.CDR_MTH;
    
    --插入上月存在而本月不存在的数据 
    INSERT INTO  SMY.DB_CRD_DEP_INCM_MTHLY_SMY
                    (OU_ID
										,CRD_TP_ID
										,PSBK_RLTD_F
										,IS_NONGXIN_CRD_F
										,ENT_IDV_IND
										,DB_CR_IND
										,CCY
										,ACG_SBJ_ID
										,CDR_YR
										,CDR_MTH
										,ACG_DT
										,CUR_DAY_INCM_AMT
										,TOT_MTD_INCM_AMT
										,TOT_QTD_INCM_AMT
										,TOT_YTD_INCM_AMT
			) SELECT   
						         OU_ID
										,CRD_TP_ID
										,PSBK_RLTD_F
										,IS_NONGXIN_CRD_F
										,ENT_IDV_IND
										,DB_CR_IND
										,CCY
										,ACG_SBJ_ID
										,CUR_YEAR
										,CUR_MONTH
										,SMY_DATE
										,0
										,0
										,0
										,TOT_YTD_INCM_AMT
			 FROM SMY.DB_CRD_DEP_INCM_MTHLY_SMY b
			 WHERE CDR_YR = CUR_YEAR 
			       AND
			       -----------Start on 20100201----------------
			       --CDR_MTH = CUR_MONTH
			       CDR_MTH = CUR_MONTH - 1
			       -----------End on 20100201----------------
			       AND
			       NOT EXISTS (
			          SELECT 1 FROM SESSION.TMP_DB_CRD_DEP_INCM_MTHLY_SMY a 
			            where a.OU_ID = b.OU_ID and
							          a.CRD_TP_ID = b.CRD_TP_ID and
							          a.PSBK_RLTD_F = b.PSBK_RLTD_F and
							          a.IS_NONGXIN_CRD_F = b.IS_NONGXIN_CRD_F and
							          a.ENT_IDV_IND = b.ENT_IDV_IND and
							          a.DB_CR_IND = b.DB_CR_IND AND
							          a.CCY  = b.CCY  and
							          a.ACG_SBJ_ID = b.ACG_SBJ_ID and
							          a.CDR_YR = b.CDR_YR and
							          a.CDR_MTH -1 = b.CDR_MTH
			       )
         ; 
    ELSE
    	 SET SMY_STEPDESC = '插入非年初或季初的月初数据';
       INSERT INTO  SMY.DB_CRD_DEP_INCM_MTHLY_SMY
                    (OU_ID
										,CRD_TP_ID
										,PSBK_RLTD_F
										,IS_NONGXIN_CRD_F
										,ENT_IDV_IND
										,DB_CR_IND
										,CCY
										,ACG_SBJ_ID
										,CDR_YR
										,CDR_MTH
										,ACG_DT
										,CUR_DAY_INCM_AMT
										,TOT_MTD_INCM_AMT
										,TOT_QTD_INCM_AMT
										,TOT_YTD_INCM_AMT)
			 SELECT 
				  a.OU_ID                 ,
				  a.CRD_TP_ID             ,
				  a.PSBK_RLTD_F           ,
				  a.IS_NONGXIN_CRD_F      ,
				  a.ENT_IDV_IND           ,
				  a.DB_CR_IND             ,
				  a.CCY                   ,
				  a.ACG_SBJ_ID            ,
				  a.CDR_YR                ,
				  a.CDR_MTH               ,
				  SMY_DATE              ,
				  COALESCE(a.CUR_DAY_INCM_AMT, 0.00)      ,
				  COALESCE(a.CUR_DAY_INCM_AMT, 0.00)      ,
				  COALESCE(b.TOT_QTD_INCM_AMT, 0.00) + COALESCE(a.CUR_DAY_INCM_AMT ,0.00)      ,
				  COALESCE(b.TOT_YTD_INCM_AMT, 0.00) + COALESCE(a.CUR_DAY_INCM_AMT ,0.00) 			                         
       FROM SESSION.TMP_DB_CRD_DEP_INCM_MTHLY_SMY a left join SMY.DB_CRD_DEP_INCM_MTHLY_SMY b
       ON a.OU_ID = b.OU_ID and
          a.CRD_TP_ID = b.CRD_TP_ID and
          a.PSBK_RLTD_F = b.PSBK_RLTD_F and
          a.IS_NONGXIN_CRD_F = b.IS_NONGXIN_CRD_F and
          a.ENT_IDV_IND = b.ENT_IDV_IND and
          a.DB_CR_IND = b.DB_CR_IND AND
          a.CCY  = b.CCY  and
          a.ACG_SBJ_ID = b.ACG_SBJ_ID and
          a.CDR_YR = b.CDR_YR and
          a.CDR_MTH -1 = b.CDR_MTH   ; 	
    	
    	    --插入上月存在而本月不存在的数据 
    INSERT INTO  SMY.DB_CRD_DEP_INCM_MTHLY_SMY
                    (OU_ID
										,CRD_TP_ID
										,PSBK_RLTD_F
										,IS_NONGXIN_CRD_F
										,ENT_IDV_IND
										,DB_CR_IND
										,CCY
										,ACG_SBJ_ID
										,CDR_YR
										,CDR_MTH
										,ACG_DT
										,CUR_DAY_INCM_AMT
										,TOT_MTD_INCM_AMT
										,TOT_QTD_INCM_AMT
										,TOT_YTD_INCM_AMT
			) SELECT   
						         OU_ID
										,CRD_TP_ID
										,PSBK_RLTD_F
										,IS_NONGXIN_CRD_F
										,ENT_IDV_IND
										,DB_CR_IND
										,CCY
										,ACG_SBJ_ID
										,CUR_YEAR
										,CUR_MONTH
										,SMY_DATE
										,0
										,0
										,TOT_QTD_INCM_AMT
										,TOT_YTD_INCM_AMT
			 FROM SMY.DB_CRD_DEP_INCM_MTHLY_SMY b
			 WHERE CDR_YR = CUR_YEAR 
			       AND
			       -----------Start on 20100201----------------
			       --CDR_MTH = CUR_MONTH
			       CDR_MTH = CUR_MONTH - 1
			       -----------End on 20100201----------------			       
			       AND
			       NOT EXISTS (
			          SELECT 1 FROM SESSION.TMP_DB_CRD_DEP_INCM_MTHLY_SMY a 
			            where a.OU_ID = b.OU_ID and
							          a.CRD_TP_ID = b.CRD_TP_ID and
							          a.PSBK_RLTD_F = b.PSBK_RLTD_F and
							          a.IS_NONGXIN_CRD_F = b.IS_NONGXIN_CRD_F and
							          a.ENT_IDV_IND = b.ENT_IDV_IND and
							          a.DB_CR_IND = b.DB_CR_IND AND
							          a.CCY  = b.CCY  and
							          a.ACG_SBJ_ID = b.ACG_SBJ_ID and
							          a.CDR_YR = b.CDR_YR and
							          a.CDR_MTH -1 = b.CDR_MTH
			       )
         ; 
    	
   END IF;
ELSE
	SET SMY_STEPDESC = 'merge非月初数据';
  MERGE INTO 	SMY.DB_CRD_DEP_INCM_MTHLY_SMY TAG
  USING SESSION.TMP_DB_CRD_DEP_INCM_MTHLY_SMY SOC
  ON      TAG.OU_ID = SOC.OU_ID and
          TAG.CRD_TP_ID = SOC.CRD_TP_ID and
          TAG.PSBK_RLTD_F = SOC.PSBK_RLTD_F and
          TAG.IS_NONGXIN_CRD_F = SOC.IS_NONGXIN_CRD_F and
          TAG.ENT_IDV_IND = SOC.ENT_IDV_IND and
          TAG.DB_CR_IND = SOC.DB_CR_IND AND
          TAG.CCY  = SOC.CCY  and
          TAG.ACG_SBJ_ID = SOC.ACG_SBJ_ID and
          TAG.CDR_YR = SOC.CDR_YR and
          TAG.CDR_MTH = SOC.CDR_MTH
	WHEN MATCHED THEN
	         UPDATE SET 
                    (OU_ID
										,CRD_TP_ID
										,PSBK_RLTD_F
										,IS_NONGXIN_CRD_F
										,ENT_IDV_IND
										,DB_CR_IND
										,CCY
										,ACG_SBJ_ID
										,CDR_YR
										,CDR_MTH
										,ACG_DT
										,CUR_DAY_INCM_AMT
										,TOT_MTD_INCM_AMT
										,TOT_QTD_INCM_AMT
										,TOT_YTD_INCM_AMT)
						=(OU_ID
										,CRD_TP_ID
										,PSBK_RLTD_F
										,IS_NONGXIN_CRD_F
										,ENT_IDV_IND
										,DB_CR_IND
										,CCY
										,ACG_SBJ_ID
										,CDR_YR
										,CDR_MTH
										,SMY_DATE
										,COALESCE(CUR_DAY_INCM_AMT, 0.00)
                    ,COALESCE(SOC.CUR_DAY_INCM_AMT,0.00) + COALESCE(TAG.TOT_MTD_INCM_AMT, 0.00)
                    ,COALESCE(SOC.CUR_DAY_INCM_AMT,0.00) + COALESCE(TAG.TOT_QTD_INCM_AMT, 0.00)
                    ,COALESCE(SOC.CUR_DAY_INCM_AMT,0.00) + COALESCE(TAG.TOT_YTD_INCM_AMT, 0.00)    )
  WHEN NOT MATCHED THEN
  INSERT            (OU_ID
										,CRD_TP_ID
										,PSBK_RLTD_F
										,IS_NONGXIN_CRD_F
										,ENT_IDV_IND
										,DB_CR_IND
										,CCY
										,ACG_SBJ_ID
										,CDR_YR
										,CDR_MTH
										,ACG_DT
										,CUR_DAY_INCM_AMT
										,TOT_MTD_INCM_AMT
										,TOT_QTD_INCM_AMT
										,TOT_YTD_INCM_AMT)
			VALUES (       OU_ID
										,SOC.CRD_TP_ID
										,SOC.PSBK_RLTD_F
										,SOC.IS_NONGXIN_CRD_F
										,SOC.ENT_IDV_IND
										,SOC.DB_CR_IND
										,SOC.CCY
										,SOC.ACG_SBJ_ID
										,SOC.CDR_YR
										,SOC.CDR_MTH
										,SMY_DATE
										,COALESCE(SOC.CUR_DAY_INCM_AMT, 0.00)
										,COALESCE(SOC.CUR_DAY_INCM_AMT, 0.00)
										,COALESCE(SOC.CUR_DAY_INCM_AMT, 0.00)
										,COALESCE(SOC.CUR_DAY_INCM_AMT, 0.00));	
	
END IF ;
GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);
      

END@