CREATE PROCEDURE SMY.PROC_CR_CRD_INCM_MTHLY_SMY(ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_CR_CRD_INCM_MTHLY_SMY.sql
-- Procedure name: 			SMY.PROC_CR_CRD_INCM_MTHLY_SMY
-- Source Table:				SOR.DB_CRD,SOR.CRD,SOR.ONLINE_TXN_RUN,SOR.SOR.CC_AC_AR
-- Target Table: 				SMY.CR_CRD_INCM_MTHLY_SMY
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
-- 2010-01-20   Xu Yan       Inserted the records of the last month into current month to make the set complete.
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
SET SMY_PROCNM = 'PROC_CR_CRD_INCM_MTHLY_SMY';
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
   DELETE FROM SMY.CR_CRD_INCM_MTHLY_SMY WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;
   COMMIT;
   IF CUR_DAY<>1 THEN
      INSERT INTO SMY.CR_CRD_INCM_MTHLY_SMY SELECT * FROM HIS.CR_CRD_INCM_MTHLY_SMY;
      COMMIT;
   END IF;
ELSE
   DELETE FROM HIS.CR_CRD_INCM_MTHLY_SMY;
   COMMIT;
   INSERT INTO HIS.CR_CRD_INCM_MTHLY_SMY SELECT * FROM SMY.CR_CRD_INCM_MTHLY_SMY WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;
   COMMIT;
END IF;

SET SMY_STEPNUM = SMY_STEPNUM+1;
SET SMY_STEPDESC = '创建临时表,并把当日数据插入';

DECLARE GLOBAL TEMPORARY TABLE TMP_CR_CRD_INCM_MTHLY_SMY(APL_ACPT_OU_IP_ID     CHARACTER(18) 
																												,CC_TP_ID              INTEGER
																												,CRD_BRND_TP_ID        INTEGER
																												,CRD_PRVL_TP_ID        INTEGER
																												,ENT_IDV_CST_IND       INTEGER
																												,MST_CRD_IND           INTEGER
																												,NGO_CRD_IND           INTEGER
																												,MULTI_CCY_F           SMALLINT
																												,AST_RSK_ASES_RTG_TP_CD CHARACTER(2)
																												,LN_FR_RSLT_TP_ID      INTEGER
																												,PD_GRP_CD             CHARACTER(2)
																												,PD_SUB_CD             CHARACTER(3)
																												,DB_CR_IND             INT
																												,PRIM_CCY_ID           CHARACTER(3) 
																												,ACG_SBJ_ID            CHARACTER(10)
																												,CRD_ISSU_OU_IP_ID     CHARACTER(18)
																												,CDR_YR                INT
																												,CDR_MTH               INT
																												,TXN_AMT               DECIMAL(17,2))
ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE PARTITIONING KEY(APL_ACPT_OU_IP_ID);    

INSERT INTO SESSION.TMP_CR_CRD_INCM_MTHLY_SMY
(APL_ACPT_OU_IP_ID      
,CC_TP_ID              
,CRD_BRND_TP_ID        
,CRD_PRVL_TP_ID        
,ENT_IDV_CST_IND       
,MST_CRD_IND           
,NGO_CRD_IND           
,MULTI_CCY_F           
,AST_RSK_ASES_RTG_TP_CD
,LN_FR_RSLT_TP_ID      
,PD_GRP_CD             
,PD_SUB_CD             
,DB_CR_IND
,PRIM_CCY_ID         
,ACG_SBJ_ID            
--,CRD_ISSU_OU_IP_ID 
,CDR_YR
,CDR_MTH    
,TXN_AMT )
WITH TMP_ONLINE_TXN_RUN AS 
( SELECT TXN_AR_ID              ,
         DB_CR_TP_ID            ,
         ACG_SBJ_ID             ,
          TXN_DNMN_CCY_ID, 
         sum(TXN_AMT) AS TXN_AMT
  FROM SOR.ONLINE_TXN_RUN 
  WHERE TXN_DT =  SMY_DATE AND DEL_F = 0 and ACG_TXN_RUN_TP_ID = 15150002 AND TXN_RED_BLUE_TP_ID = 15200001 and PST_RVRSL_TP_ID = 15130001
  GROUP BY
    TXN_AR_ID              ,
    DB_CR_TP_ID            ,
    ACG_SBJ_ID,
    TXN_DNMN_CCY_ID)
SELECT 
  a.APL_ACPT_OU_IP_ID                               ,
  COALESCE(a.CC_TP_ID, -1)                          ,
  COALESCE(b.CRD_BRND_TP_ID, -1)                    ,
  COALESCE(a.CRD_PRVL_TP_ID, -1)                    ,
  COALESCE(b.ENT_IDV_CST_IND, -1)                   ,
  COALESCE(a.MST_CRD_IND, -1)                       ,
  COALESCE(b.NGO_CRD_IND, -1)                       ,
  COALESCE(b.MULTI_CCY_F, -1)                       ,
  COALESCE(c.AST_RSK_ASES_RTG_TP_CD, '')                          ,
  COALESCE(c.LN_FR_RSLT_TP_ID, -1)                  ,
  COALESCE(b.PD_GRP_CD, '')                                      ,
  COALESCE(b.PD_SUB_CD, '')                                       ,
  COALESCE(d.DB_CR_TP_ID, -1)                                     ,
  COALESCE(a.PRIM_CCY_ID, '')                                    ,
  COALESCE(d.ACG_SBJ_ID, '')                                      ,
 -- a.ISSU_CRD_OU_IP_ID                               ,
  CUR_YEAR                                          ,
  CUR_MONTH                                         ,
  sum(COALESCE(d.TXN_AMT, 0.00))                      
FROM SOR.CR_CRD a LEFT JOIN SOR.CRD b ON a.CC_NO = b.CRD_NO      
                  LEFT JOIN SOR.CC_AC_AR c ON b.AC_AR_ID = c.CC_AC_AR_ID AND a.PRIM_CCY_ID =C.DNMN_CCY_ID
                  LEFT JOIN TMP_ONLINE_TXN_RUN d ON b.AC_AR_ID = d.TXN_AR_ID  AND a.PRIM_CCY_ID =D.TXN_DNMN_CCY_ID
GROUP BY 
  a.APL_ACPT_OU_IP_ID                               ,
  COALESCE(a.CC_TP_ID, -1)                          ,
  COALESCE(b.CRD_BRND_TP_ID, -1)                    ,
  COALESCE(a.CRD_PRVL_TP_ID, -1)                    ,
  COALESCE(b.ENT_IDV_CST_IND, -1)                   ,
  COALESCE(a.MST_CRD_IND, -1)                       ,
  COALESCE(b.NGO_CRD_IND, -1)                       ,
  COALESCE(b.MULTI_CCY_F, -1)                       ,
  c.AST_RSK_ASES_RTG_TP_CD                          ,
  COALESCE(c.LN_FR_RSLT_TP_ID, -1)                  ,
  b.PD_GRP_CD                                       ,
  b.PD_SUB_CD                                       ,
  d.DB_CR_TP_ID                                     ,
  a.PRIM_CCY_ID                                     ,
  d.ACG_SBJ_ID                                      ,
 -- a.ISSU_CRD_OU_IP_ID                               ,
  CUR_YEAR                                          ,
  CUR_MONTH;

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);

SET SMY_STEPNUM = SMY_STEPNUM+1;
 
IF CUR_DAY = 1 THEN
   IF CUR_MONTH = 1 THEN
      SET SMY_STEPDESC = '插入年初数据'; 
      INSERT INTO SMY.CR_CRD_INCM_MTHLY_SMY
       (OU_ID                                 ,--受理机构号
        CRD_TP_ID                             ,--卡类型
        CRD_Brand_TP_Id                       ,--卡品牌类型
        CRD_PRVL_TP_ID                        ,--卡级别
        ENT_IDV_IND                           ,--卡对象
        MST_CRD_IND                           ,--主/副卡标志       
				NGO_CRD_IND                           ,--协议卡类型        
				MULT_CCY_F                            ,--双币卡标志        
				AST_RSK_ASES_RTG_TP_CD                ,--资产风险分类      
				LN_FIVE_RTG_STS                       ,--贷款五级形态类型  
				PD_GRP_CD                             ,--产品类            
				PD_SUB_CD                             ,--产品子代码        
				DB_CR_IND                             ,--借贷标志  
				CCY                                   ,--币种        
				ACG_SBJ_ID                            ,--收费科目（核算码）
				CDR_YR                                ,--年份YYYY          
				CDR_MTH                               ,--月份MM            
				ACG_DT                                ,--日期YYYY-MM-DD    
				ISSU_CRD_OU_Id                        ,--发卡机构号        
				CUR_Day_INCM_AMT                      ,--当日收入          
				TOT_MTD_INCM_AMT                      ,--累计月收入汇总    
				TOT_QTD_INCM_AMT                      ,--累计季收入汇总    
				TOT_YTD_INCM_AMT                      )--累计年收入汇总
      SELECT 
        APL_ACPT_OU_IP_ID      
        ,CC_TP_ID              
        ,CRD_BRND_TP_ID        
        ,CRD_PRVL_TP_ID        
        ,ENT_IDV_CST_IND       
        ,MST_CRD_IND           
        ,NGO_CRD_IND           
        ,MULTI_CCY_F           
        ,AST_RSK_ASES_RTG_TP_CD
        ,LN_FR_RSLT_TP_ID      
        ,PD_GRP_CD             
        ,PD_SUB_CD             
        ,DB_CR_IND  
        ,PRIM_CCY_ID         
        ,ACG_SBJ_ID 
        ,CDR_YR
        ,CDR_MTH
        ,SMY_DATE
        ,CRD_ISSU_OU_IP_ID
        ,TXN_AMT
        ,TXN_AMT
        ,TXN_AMT
        ,TXN_AMT
      FROM  SESSION.TMP_CR_CRD_INCM_MTHLY_SMY a ;
        
  ELSEIF CUR_MONTH IN (4, 7, 10) THEN
      SET SMY_STEPDESC = '插入季初数据';
      INSERT INTO SMY.CR_CRD_INCM_MTHLY_SMY
       (OU_ID                                 ,--受理机构号
        CRD_TP_ID                             ,--卡类型
        CRD_Brand_TP_Id                       ,--卡品牌类型
        CRD_PRVL_TP_ID                        ,--卡级别
        ENT_IDV_IND                           ,--卡对象
        MST_CRD_IND                           ,--主/副卡标志       
				NGO_CRD_IND                           ,--协议卡类型        
				MULT_CCY_F                            ,--双币卡标志        
				AST_RSK_ASES_RTG_TP_CD                ,--资产风险分类      
				LN_FIVE_RTG_STS                       ,--贷款五级形态类型  
				PD_GRP_CD                             ,--产品类            
				PD_SUB_CD                             ,--产品子代码        
				DB_CR_IND                             ,--借贷标志       
				CCY                                   ,   
				ACG_SBJ_ID                            ,--收费科目（核算码）
				CDR_YR                                ,--年份YYYY          
				CDR_MTH                               ,--月份MM            
				ACG_DT                                ,--日期YYYY-MM-DD    
				ISSU_CRD_OU_Id                        ,--发卡机构号        
				CUR_Day_INCM_AMT                      ,--当日收入          
				TOT_MTD_INCM_AMT                      ,--累计月收入汇总    
				TOT_QTD_INCM_AMT                      ,--累计季收入汇总    
				TOT_YTD_INCM_AMT                      )--累计年收入汇总
			SELECT
        a.APL_ACPT_OU_IP_ID      
        ,a.CC_TP_ID              
        ,a.CRD_BRND_TP_ID        
        ,a.CRD_PRVL_TP_ID        
        ,a.ENT_IDV_CST_IND       
        ,a.MST_CRD_IND           
        ,a.NGO_CRD_IND           
        ,a.MULTI_CCY_F           
        ,a.AST_RSK_ASES_RTG_TP_CD
        ,a.LN_FR_RSLT_TP_ID      
        ,a.PD_GRP_CD             
        ,a.PD_SUB_CD             
        ,a.DB_CR_IND           
        ,a.PRIM_CCY_ID
        ,a.ACG_SBJ_ID 
        ,a.CDR_YR
        ,a.CDR_MTH
        ,SMY_DATE
        ,a.CRD_ISSU_OU_IP_ID
        ,COALESCE(TXN_AMT, 0.00)
        ,COALESCE(TXN_AMT, 0.00)
        ,COALESCE(TXN_AMT, 0.00)
        ,COALESCE(b.TOT_YTD_INCM_AMT,0.00) + COALESCE(TXN_AMT, 0.00)			
      FROM SESSION.TMP_CR_CRD_INCM_MTHLY_SMY a LEFT JOIN SMY.CR_CRD_INCM_MTHLY_SMY b
			ON a.APL_ACPT_OU_IP_ID = b.OU_ID AND
			   a.CC_TP_ID = b.CRD_TP_ID and
			   a.CRD_BRND_TP_ID = b.CRD_Brand_TP_Id and
			   a.CRD_PRVL_TP_ID = b.CRD_PRVL_TP_ID and
			   a.ENT_IDV_CST_IND = b.ENT_IDV_IND and
			   a.MST_CRD_IND = b.MST_CRD_IND and
			   a.NGO_CRD_IND = b.NGO_CRD_IND and
			   a.MULTI_CCY_F = b.MULT_CCY_F and
			   a.AST_RSK_ASES_RTG_TP_CD = b.AST_RSK_ASES_RTG_TP_CD and
			   a.LN_FR_RSLT_TP_ID = b.LN_FIVE_RTG_STS and
			   a.PD_GRP_CD = b.PD_GRP_CD and
			   a.PD_SUB_CD = b.PD_SUB_CD and
			   a.DB_CR_IND = b.DB_CR_IND and
			   a.PRIM_CCY_ID = b.CCY AND
			   a.ACG_SBJ_ID = b.ACG_SBJ_ID and
			   b.CDR_YR = a.CDR_YR  and
			   b.CDR_MTH = a.CDR_MTH -1;
		
		insert into SMY.CR_CRD_INCM_MTHLY_SMY
       (OU_ID                                 ,--受理机构号
        CRD_TP_ID                             ,--卡类型
        CRD_Brand_TP_Id                       ,--卡品牌类型
        CRD_PRVL_TP_ID                        ,--卡级别
        ENT_IDV_IND                           ,--卡对象
        MST_CRD_IND                           ,--主/副卡标志       
				NGO_CRD_IND                           ,--协议卡类型        
				MULT_CCY_F                            ,--双币卡标志        
				AST_RSK_ASES_RTG_TP_CD                ,--资产风险分类      
				LN_FIVE_RTG_STS                       ,--贷款五级形态类型  
				PD_GRP_CD                             ,--产品类            
				PD_SUB_CD                             ,--产品子代码        
				DB_CR_IND                             ,--借贷标志       
				CCY                                   ,   
				ACG_SBJ_ID                            ,--收费科目（核算码）
				CDR_YR                                ,--年份YYYY          
				CDR_MTH                               ,--月份MM            
				ACG_DT                                ,--日期YYYY-MM-DD    
				ISSU_CRD_OU_Id                        ,--发卡机构号        
				CUR_Day_INCM_AMT                      ,--当日收入          
				TOT_MTD_INCM_AMT                      ,--累计月收入汇总    
				TOT_QTD_INCM_AMT                      ,--累计季收入汇总    
				TOT_YTD_INCM_AMT                      )--累计年收入汇总
			SELECT
				OU_ID                                 ,--受理机构号
        CRD_TP_ID                             ,--卡类型
        CRD_Brand_TP_Id                       ,--卡品牌类型
        CRD_PRVL_TP_ID                        ,--卡级别
        ENT_IDV_IND                           ,--卡对象
        MST_CRD_IND                           ,--主/副卡标志       
				NGO_CRD_IND                           ,--协议卡类型        
				MULT_CCY_F                            ,--双币卡标志        
				AST_RSK_ASES_RTG_TP_CD                ,--资产风险分类      
				LN_FIVE_RTG_STS                       ,--贷款五级形态类型  
				PD_GRP_CD                             ,--产品类            
				PD_SUB_CD                             ,--产品子代码        
				DB_CR_IND                             ,--借贷标志       
				CCY                                   ,   
				ACG_SBJ_ID                            ,--收费科目（核算码）
				CUR_YEAR                                ,--年份YYYY          
				CUR_MONTH                               ,--月份MM            
				SMY_DATE                                ,--日期YYYY-MM-DD  
				ISSU_CRD_OU_Id                        ,--发卡机构号        
				0                      ,--当日收入          
				0                      ,--累计月收入汇总    
				0                      ,--累计季收入汇总    
				TOT_YTD_INCM_AMT 			                 --累计年收入汇总
			from 	SMY.CR_CRD_INCM_MTHLY_SMY b
			where CDR_YR = CUR_YEAR 
			      AND
			      CDR_MTH = CUR_MONTH - 1
			      AND NOT EXISTS (
			          SELECT 1 
			          FROM SESSION.TMP_CR_CRD_INCM_MTHLY_SMY a 
			          where 
			             a.APL_ACPT_OU_IP_ID = b.OU_ID AND
								   a.CC_TP_ID = b.CRD_TP_ID and
								   a.CRD_BRND_TP_ID = b.CRD_Brand_TP_Id and
								   a.CRD_PRVL_TP_ID = b.CRD_PRVL_TP_ID and
								   a.ENT_IDV_CST_IND = b.ENT_IDV_IND and
								   a.MST_CRD_IND = b.MST_CRD_IND and
								   a.NGO_CRD_IND = b.NGO_CRD_IND and
								   a.MULTI_CCY_F = b.MULT_CCY_F and
								   a.AST_RSK_ASES_RTG_TP_CD = b.AST_RSK_ASES_RTG_TP_CD and
								   a.LN_FR_RSLT_TP_ID = b.LN_FIVE_RTG_STS and
								   a.PD_GRP_CD = b.PD_GRP_CD and
								   a.PD_SUB_CD = b.PD_SUB_CD and
								   a.DB_CR_IND = b.DB_CR_IND and
								   a.PRIM_CCY_ID = b.CCY AND
								   a.ACG_SBJ_ID = b.ACG_SBJ_ID and
								   b.CDR_YR = a.CDR_YR  and
								   b.CDR_MTH + 1 = a.CDR_MTH  
			      )
			;
    ELSE
    	SET SMY_STEPDESC = '插入非年初或季初的月初数据';
      INSERT INTO SMY.CR_CRD_INCM_MTHLY_SMY
       (OU_ID                                 ,--受理机构号
        CRD_TP_ID                             ,--卡类型
        CRD_Brand_TP_Id                       ,--卡品牌类型
        CRD_PRVL_TP_ID                        ,--卡级别
        ENT_IDV_IND                           ,--卡对象
        MST_CRD_IND                           ,--主/副卡标志       
				NGO_CRD_IND                           ,--协议卡类型        
				MULT_CCY_F                            ,--双币卡标志        
				AST_RSK_ASES_RTG_TP_CD                ,--资产风险分类      
				LN_FIVE_RTG_STS                       ,--贷款五级形态类型  
				PD_GRP_CD                             ,--产品类            
				PD_SUB_CD                             ,--产品子代码        
				DB_CR_IND                             ,--借贷标志 
				CCY                                   ,         
				ACG_SBJ_ID                            ,--收费科目（核算码）
				CDR_YR                                ,--年份YYYY          
				CDR_MTH                               ,--月份MM            
				ACG_DT                                ,--日期YYYY-MM-DD    
				ISSU_CRD_OU_Id                        ,--发卡机构号        
				CUR_Day_INCM_AMT                      ,--当日收入          
				TOT_MTD_INCM_AMT                      ,--累计月收入汇总    
				TOT_QTD_INCM_AMT                      ,--累计季收入汇总    
				TOT_YTD_INCM_AMT                      )--累计年收入汇总
			SELECT
        a.APL_ACPT_OU_IP_ID      
        ,a.CC_TP_ID              
        ,a.CRD_BRND_TP_ID        
        ,a.CRD_PRVL_TP_ID        
        ,a.ENT_IDV_CST_IND       
        ,a.MST_CRD_IND           
        ,a.NGO_CRD_IND           
        ,a.MULTI_CCY_F           
        ,a.AST_RSK_ASES_RTG_TP_CD
        ,a.LN_FR_RSLT_TP_ID      
        ,a.PD_GRP_CD             
        ,a.PD_SUB_CD             
        ,a.DB_CR_IND 
        ,a.PRIM_CCY_ID          
        ,a.ACG_SBJ_ID 
        ,a.CDR_YR
        ,a.CDR_MTH
        ,SMY_DATE
        ,a.CRD_ISSU_OU_IP_ID
        ,COALESCE(TXN_AMT, 0.00)
        ,COALESCE(TXN_AMT, 0.00)
        ,COALESCE(b.TOT_QTD_INCM_AMT,0.00) + COALESCE(TXN_AMT, 0.00)
        ,COALESCE(b.TOT_YTD_INCM_AMT,0.00) + COALESCE(TXN_AMT, 0.00)			
      FROM SESSION.TMP_CR_CRD_INCM_MTHLY_SMY a LEFT JOIN SMY.CR_CRD_INCM_MTHLY_SMY b
			ON a.APL_ACPT_OU_IP_ID = b.OU_ID AND
			   a.CC_TP_ID = b.CRD_TP_ID and
			   a.CRD_BRND_TP_ID = b.CRD_Brand_TP_Id and
			   a.CRD_PRVL_TP_ID = b.CRD_PRVL_TP_ID and
			   a.ENT_IDV_CST_IND = b.ENT_IDV_IND and
			   a.MST_CRD_IND = b.MST_CRD_IND and
			   a.NGO_CRD_IND = b.NGO_CRD_IND and
			   a.MULTI_CCY_F = b.MULT_CCY_F and
			   a.AST_RSK_ASES_RTG_TP_CD = b.AST_RSK_ASES_RTG_TP_CD and
			   a.LN_FR_RSLT_TP_ID = b.LN_FIVE_RTG_STS and
			   a.PD_GRP_CD = b.PD_GRP_CD and
			   a.PD_SUB_CD = b.PD_SUB_CD and
			   a.DB_CR_IND = b.DB_CR_IND and
			   a.PRIM_CCY_ID = b.CCY and
			   a.ACG_SBJ_ID = b.ACG_SBJ_ID and
			   b.CDR_YR = a.CDR_YR  and
			   b.CDR_MTH = a.CDR_MTH -1;   
			   
		--插入上个月中存在，本月中不存在的数据  
     insert into SMY.CR_CRD_INCM_MTHLY_SMY
       (OU_ID                                 ,--受理机构号
        CRD_TP_ID                             ,--卡类型
        CRD_Brand_TP_Id                       ,--卡品牌类型
        CRD_PRVL_TP_ID                        ,--卡级别
        ENT_IDV_IND                           ,--卡对象
        MST_CRD_IND                           ,--主/副卡标志       
				NGO_CRD_IND                           ,--协议卡类型        
				MULT_CCY_F                            ,--双币卡标志        
				AST_RSK_ASES_RTG_TP_CD                ,--资产风险分类      
				LN_FIVE_RTG_STS                       ,--贷款五级形态类型  
				PD_GRP_CD                             ,--产品类            
				PD_SUB_CD                             ,--产品子代码        
				DB_CR_IND                             ,--借贷标志       
				CCY                                   ,   
				ACG_SBJ_ID                            ,--收费科目（核算码）
				CDR_YR                                ,--年份YYYY          
				CDR_MTH                               ,--月份MM            
				ACG_DT                                ,--日期YYYY-MM-DD    
				ISSU_CRD_OU_Id                        ,--发卡机构号        
				CUR_Day_INCM_AMT                      ,--当日收入          
				TOT_MTD_INCM_AMT                      ,--累计月收入汇总    
				TOT_QTD_INCM_AMT                      ,--累计季收入汇总    
				TOT_YTD_INCM_AMT                      )--累计年收入汇总
			SELECT
				OU_ID                                 ,--受理机构号
        CRD_TP_ID                             ,--卡类型
        CRD_Brand_TP_Id                       ,--卡品牌类型
        CRD_PRVL_TP_ID                        ,--卡级别
        ENT_IDV_IND                           ,--卡对象
        MST_CRD_IND                           ,--主/副卡标志       
				NGO_CRD_IND                           ,--协议卡类型        
				MULT_CCY_F                            ,--双币卡标志        
				AST_RSK_ASES_RTG_TP_CD                ,--资产风险分类      
				LN_FIVE_RTG_STS                       ,--贷款五级形态类型  
				PD_GRP_CD                             ,--产品类            
				PD_SUB_CD                             ,--产品子代码        
				DB_CR_IND                             ,--借贷标志       
				CCY                                   ,   
				ACG_SBJ_ID                            ,--收费科目（核算码）
				CUR_YEAR                                ,--年份YYYY          
				CUR_MONTH                               ,--月份MM            
				SMY_DATE                                ,--日期YYYY-MM-DD    
				ISSU_CRD_OU_Id                        ,--发卡机构号        
				0                      ,--当日收入          
				0                      ,--累计月收入汇总    
				TOT_QTD_INCM_AMT                      ,--累计季收入汇总    
				TOT_YTD_INCM_AMT 			                 --累计年收入汇总
			from 	SMY.CR_CRD_INCM_MTHLY_SMY b
			where CDR_YR = CUR_YEAR 
			      AND
			      CDR_MTH = CUR_MONTH - 1
			      AND NOT EXISTS (
			          SELECT 1 
			          FROM SESSION.TMP_CR_CRD_INCM_MTHLY_SMY a 
			          where 
			             a.APL_ACPT_OU_IP_ID = b.OU_ID AND
								   a.CC_TP_ID = b.CRD_TP_ID and
								   a.CRD_BRND_TP_ID = b.CRD_Brand_TP_Id and
								   a.CRD_PRVL_TP_ID = b.CRD_PRVL_TP_ID and
								   a.ENT_IDV_CST_IND = b.ENT_IDV_IND and
								   a.MST_CRD_IND = b.MST_CRD_IND and
								   a.NGO_CRD_IND = b.NGO_CRD_IND and
								   a.MULTI_CCY_F = b.MULT_CCY_F and
								   a.AST_RSK_ASES_RTG_TP_CD = b.AST_RSK_ASES_RTG_TP_CD and
								   a.LN_FR_RSLT_TP_ID = b.LN_FIVE_RTG_STS and
								   a.PD_GRP_CD = b.PD_GRP_CD and
								   a.PD_SUB_CD = b.PD_SUB_CD and
								   a.DB_CR_IND = b.DB_CR_IND and
								   a.PRIM_CCY_ID = b.CCY AND
								   a.ACG_SBJ_ID = b.ACG_SBJ_ID and
								   b.CDR_YR = a.CDR_YR  and
								   b.CDR_MTH + 1 = a.CDR_MTH  
			      )
			;			    	
    
   END IF;
ELSE
	SET SMY_STEPDESC = 'merge 非月初数据';
  MERGE INTO 	SMY.CR_CRD_INCM_MTHLY_SMY TAG
  USING SESSION.TMP_CR_CRD_INCM_MTHLY_SMY SOC
  ON     SOC.APL_ACPT_OU_IP_ID = TAG.OU_ID AND
			   SOC.CC_TP_ID = TAG.CRD_TP_ID and
			   SOC.CRD_BRND_TP_ID = TAG.CRD_Brand_TP_Id and
			   SOC.CRD_PRVL_TP_ID = TAG.CRD_PRVL_TP_ID and
			   SOC.ENT_IDV_CST_IND = TAG.ENT_IDV_IND and
			   SOC.MST_CRD_IND = TAG.MST_CRD_IND and
			   SOC.NGO_CRD_IND = TAG.NGO_CRD_IND and
			   SOC.MULTI_CCY_F = TAG.MULT_CCY_F and
			   SOC.AST_RSK_ASES_RTG_TP_CD = TAG.AST_RSK_ASES_RTG_TP_CD and
			   SOC.LN_FR_RSLT_TP_ID = TAG.LN_FIVE_RTG_STS and
			   SOC.PD_GRP_CD = TAG.PD_GRP_CD and
			   SOC.PD_SUB_CD = TAG.PD_SUB_CD and
			   SOC.DB_CR_IND = TAG.DB_CR_IND and
			   SOC.PRIM_CCY_ID = TAG.CCY and
			   SOC.ACG_SBJ_ID = TAG.ACG_SBJ_ID and
			   TAG.CDR_YR = CUR_MONTH  and
			   TAG.CDR_MTH = CUR_MONTH 
	WHEN MATCHED THEN
	UPDATE SET (OU_ID                                 ,--受理机构号
        			CRD_TP_ID                             ,--卡类型
        			CRD_Brand_TP_Id                       ,--卡品牌类型
        			CRD_PRVL_TP_ID                        ,--卡级别
        			ENT_IDV_IND                           ,--卡对象
        			MST_CRD_IND                           ,--主/副卡标志       
							NGO_CRD_IND                           ,--协议卡类型        
							MULT_CCY_F                            ,--双币卡标志        
							AST_RSK_ASES_RTG_TP_CD                ,--资产风险分类      
							LN_FIVE_RTG_STS                       ,--贷款五级形态类型  
							PD_GRP_CD                             ,--产品类            
							PD_SUB_CD                             ,--产品子代码        
							DB_CR_IND                             ,--借贷标志 
							CCY                                   ,         
							ACG_SBJ_ID                            ,--收费科目（核算码）
							CDR_YR                                ,--年份YYYY          
							CDR_MTH                               ,--月份MM            
							ACG_DT                                ,--日期YYYY-MM-DD    
							ISSU_CRD_OU_Id                        ,--发卡机构号        
							CUR_Day_INCM_AMT                      ,--当日收入          
							TOT_MTD_INCM_AMT                      ,--累计月收入汇总    
							TOT_QTD_INCM_AMT                      ,--累计季收入汇总    
							TOT_YTD_INCM_AMT                      )--累计年收入汇总
						=(TAG.OU_ID                             ,
						  TAG.CRD_TP_ID                         ,
						  TAG.CRD_Brand_TP_Id                   ,
						  TAG.CRD_PRVL_TP_ID                    ,                      
						  TAG.ENT_IDV_IND                       ,                      
              TAG.MST_CRD_IND                       ,
              TAG.NGO_CRD_IND                       ,
              TAG.MULT_CCY_F                        ,
              TAG.AST_RSK_ASES_RTG_TP_CD            ,
              TAG.LN_FIVE_RTG_STS                   ,
              TAG.PD_GRP_CD                         ,
              TAG.PD_SUB_CD                         ,
              TAG.DB_CR_IND                         ,
              TAG.CCY                               ,
              TAG.ACG_SBJ_ID                        ,
              TAG.CDR_YR                            ,
              TAG.CDR_MTH                           ,
              TAG.ACG_DT                            ,
              TAG.ISSU_CRD_OU_Id                    ,
              SOC.TXN_AMT                           ,
              SOC.TXN_AMT + TAG.TOT_MTD_INCM_AMT    ,
              SOC.TXN_AMT + TAG.TOT_QTD_INCM_AMT    ,
              SOC.TXN_AMT + TAG.TOT_YTD_INCM_AMT    )
  WHEN NOT MATCHED THEN
  INSERT  (OU_ID                                 ,--受理机构号
        			CRD_TP_ID                             ,--卡类型
        			CRD_Brand_TP_Id                       ,--卡品牌类型
        			CRD_PRVL_TP_ID                        ,--卡级别
        			ENT_IDV_IND                           ,--卡对象
        			MST_CRD_IND                           ,--主/副卡标志       
							NGO_CRD_IND                           ,--协议卡类型        
							MULT_CCY_F                            ,--双币卡标志        
							AST_RSK_ASES_RTG_TP_CD                ,--资产风险分类      
							LN_FIVE_RTG_STS                       ,--贷款五级形态类型  
							PD_GRP_CD                             ,--产品类            
							PD_SUB_CD                             ,--产品子代码        
							DB_CR_IND                             ,--借贷标志 
							CCY                                   ,         
							ACG_SBJ_ID                            ,--收费科目（核算码）
							CDR_YR                                ,--年份YYYY          
							CDR_MTH                               ,--月份MM            
							ACG_DT                                ,--日期YYYY-MM-DD    
							ISSU_CRD_OU_Id                        ,--发卡机构号        
							CUR_Day_INCM_AMT                      ,--当日收入          
							TOT_MTD_INCM_AMT                      ,--累计月收入汇总    
							TOT_QTD_INCM_AMT                      ,--累计季收入汇总    
							TOT_YTD_INCM_AMT                      )--累计年收入汇总
			VALUES( SOC.APL_ACPT_OU_IP_ID      
             ,SOC.CC_TP_ID              
             ,SOC.CRD_BRND_TP_ID        
             ,SOC.CRD_PRVL_TP_ID        
             ,SOC.ENT_IDV_CST_IND       
             ,SOC.MST_CRD_IND           
             ,SOC.NGO_CRD_IND           
             ,SOC.MULTI_CCY_F           
             ,SOC.AST_RSK_ASES_RTG_TP_CD
             ,SOC.LN_FR_RSLT_TP_ID      
             ,SOC.PD_GRP_CD             
             ,SOC.PD_SUB_CD             
             ,SOC.DB_CR_IND
             ,SOC.PRIM_CCY_ID         
             ,SOC.ACG_SBJ_ID
             ,SOC.CDR_YR 
             ,SOC.CDR_MTH 
             ,SMY_DATE     
             ,SOC.CRD_ISSU_OU_IP_ID 
             ,SOC.TXN_AMT
             ,SOC.TXN_AMT 
             ,SOC.TXN_AMT 
             ,SOC.TXN_AMT);
              
END IF ;
GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);
      

END@