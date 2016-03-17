CREATE PROCEDURE SMY.PROC_CR_CRD_BAL_OU_DLY_SMY(ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_CR_CRD_BAL_OU_DLY_SMY.sql
-- Procedure name: 			SMY.PROC_CR_CRD_BAL_OU_DLY_SMY
-- Source Table:				SOR.CR_CRD,SOR.CRD,SOR.CC_AC_AR,SOR.CC_REPYMT_TXN_DTL,SMY.MTHLY_CR_CRD_AC_ACML_BAL_AMT
-- Target Table: 				SMY.CR_CRD_BAL_OU_DLY_SMY
-- Project:             ZJ RCCB EDW
--
-- Purpose:             
--
--=============================================================================
-- Creation Date:       2009.11.11
-- Origin Author:       Wang Youbing
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2009-10-28   Wang Youbing     Create SP File		
-- 2009-12-01   Xu Yan           Added a new column 'NBR_OD_AC'
-- 2009-12-16   Xu Yan           Updated the 'CR_LMT', 'TMP_CRED_LMT'
-- 2010-01-13   Xu Yan           Included 11920004 --已收卡
-- 2010-01-19   Xu Yan           Updated the accumulated amount getting logic from the ar level
-- 2010-01-21   Xu Yan           Handled the '作废卡' problem, excluded them
-------------------------------------------------------------------------------
LANGUAGE SQL
BEGIN
/*声明异常处理使用变量*/
DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
DECLARE SMY_STEPNUM INT DEFAULT 1;                     --过程内部位置标记
DECLARE SMY_STEPDESC VARCHAR(100) DEFAULT '';          --过程内部位置描述
DECLARE SMY_DATE DATE;                                 --临时日期变量
DECLARE SMY_RCOUNT INT;                                --DML语句作用记录数
DECLARE SMY_PROCNM VARCHAR(100);                        --存储过程名称
DECLARE at_end SMALLINT DEFAULT 0;--
/*声明存储过程使用变量*/
DECLARE CUR_YEAR SMALLINT;                             --年
DECLARE CUR_MONTH SMALLINT;                            --月
DECLARE CUR_DAY INTEGER;                               --日
DECLARE LAST_YR_MONTH VARCHAR(6);                      --上月
DECLARE LAST_ACG_DT DATE;                              --上一日
DECLARE YR_FIRST_DAY DATE;                             --年初1月1日
DECLARE QTR_FIRST_DAY DATE;                            --每季度第1日
DECLARE YR_DAY SMALLINT;                               --当年天数
DECLARE QTR_DAY SMALLINT;                              --当季天数
DECLARE NEXT_DAY SMALLINT;                             --下一天
--DECLARE MAX_ACG_DT DATE;                               --最大会计日期
--DECLARE DELETE_SQL VARCHAR(200);                       --删除历史表动态SQL

/*1.定义针对SQL异常情况的句柄(EXIT方式).
  2.将出现SQL异常时在存储过程中的位置(SMY_STEPNUM),位置描述(SMY_STEPDESC)，SQLCODE(SMY_SQLCODE)记入表SMY_LOG中作调试用.
  3.调用RESIGNAL重新引发异常,跳出存储过程执行体,对引发SQL异常之前存储过程体中所完成的操作进行回滚.*/
DECLARE CONTINUE HANDLER FOR NOT FOUND
SET at_end=1;--
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
	SET SMY_SQLCODE = SQLCODE;--
  ROLLBACK;--
  SET SMY_STEPNUM=SMY_STEPNUM+1;--
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
  COMMIT;--
  RESIGNAL;--
END;--
DECLARE CONTINUE HANDLER FOR SQLWARNING
BEGIN
  SET SMY_SQLCODE = SQLCODE;--
  SET SMY_STEPNUM=SMY_STEPNUM+1;--
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
  COMMIT;--
END;--
/*变量赋值*/
SET SMY_PROCNM = 'PROC_CR_CRD_BAL_OU_DLY_SMY';--
SET SMY_DATE=ACCOUNTING_DATE;--
SET CUR_YEAR=YEAR(ACCOUNTING_DATE);  --取当前年份
SET CUR_MONTH=MONTH(ACCOUNTING_DATE); --取当前月份
SET CUR_DAY=DAY(ACCOUNTING_DATE);     --取当日
IF CUR_MONTH=1 THEN 
   SET LAST_YR_MONTH=TRIM(CHAR(CUR_YEAR-1))||'12';--
ELSE
   SET LAST_YR_MONTH=TRIM(CHAR(CUR_YEAR))||RIGHT('0'||TRIM(CHAR(CUR_MONTH-1)),2);--
END IF;--
SET LAST_ACG_DT=ACCOUNTING_DATE - 1 DAYS;--
SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');    --当年第1天
SET YR_DAY=DAYS(ACCOUNTING_DATE)-DAYS(YR_FIRST_DAY) + 1;    --当前会计日期属于当年第几天
IF CUR_MONTH IN (1,2,3) THEN                              --当季第1天
   SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');--
ELSEIF CUR_MONTH IN (4,5,6) THEN 
       SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-04-01');--
    ELSEIF CUR_MONTH IN (7,8,9) THEN 
           SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-07-01');--
        ELSE
            SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-10-01');--
END IF;--
SET QTR_DAY=DAYS(ACCOUNTING_DATE)-DAYS(QTR_FIRST_DAY) + 1;  --当前会计日期属于当季第几天
SET NEXT_DAY=DAY(DATE(ACCOUNTING_DATE)+1 DAYS);--
--SELECT MAX(ACG_DT) INTO MAX_ACG_DT FROM SMY.CR_CRD_BAL_OU_DLY_SMY;          --当前汇总表总存放数据的最大会计日期
--SET DELETE_SQL='ALTER TABLE SMY.HIST_CR_CRD_BAL_OU_DLY_SMY ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE';--

/*Delete日志表,条件SMY_PROCNM=当前存储过程名字,SMY_DATE=ACCOUNTING_DATE,并插入新的起始标志*/
DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
COMMIT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, 'PROCEDURE START.', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

/*数据恢复与备份*/

DELETE FROM SMY.CR_CRD_BAL_OU_DLY_SMY WHERE ACG_DT=ACCOUNTING_DATE;--
COMMIT;--



INSERT INTO SMY.CR_CRD_BAL_OU_DLY_SMY(
				 OU_ID                            --受理机构号              
        ,CRD_TP_ID                        --卡类型                  
        ,CRD_Brand_TP_Id                  --卡品牌类型              
        ,CRD_PRVL_TP_ID                   --卡级别                  
        ,ENT_IDV_IND                      --卡对象                  
        ,MST_CRD_IND                      --主/副卡标志             
        ,NGO_CRD_IND                      --协议卡类型              
        ,MULT_CCY_F                       --双币卡标志              
        ,AST_RSK_ASES_RTG_TP_CD           --资产风险分类            
        ,LN_FIVE_RTG_STS                  --贷款五级形态类型        
        ,PD_GRP_CD                        --产品类                  
        ,PD_SUB_CD                        --产品子代码              
        ,BYND_LMT_F                       --超限标志                
        ,CCY                              --币种                    
        ,ACG_DT                           --日期YYYY-MM-DD          
        ,CDR_YR                           --年份YYYY                
        ,CDR_MTH                          --月份MM                  
        ,NOD_In_MTH                       --当月天数                
        ,NOD_In_QTR                       --当季日历日天数          
        ,NOD_In_Year                      --当年日历日天数          
        ,ISSU_CRD_OU_Id                   --发卡机构号              
        ,AC_BAL_AMT                       --账户余额                
        ,LST_Day_AC_BAL                   --昨日账户余额            
        ,DEP_BAL_CRD                      --银行卡存款余额          
        ,OTSND_AMT_RCVB                   --应收账款余额            
        ,OTSND_INT_BRG_DUE_AMT            --生息应收账款余额        
        ,NBR_AC                           --账户个数
        ------------------------Start of 2009-12-01-------------------------                           
        ,NBR_OD_AC                        --透支账户个数
        ------------------------End of 2009-12-01-------------------------
        ,CR_LMT                           --授信额度                
        ,TMP_CRED_LMT                     --临时授信金额            
        ,OD_BAL_AMT                       --透支余额                
        ,AMT_PNP_ARS                      --透支本金                
        ,INT_RCVB                         --应收利息                
        ,FEE_RCVB                         --应收费用                
        ,OFFSET_AMT                       --冲销金额                
        ,OTSND_LOSS_ALOW_CRD_FRD_AVY      --伪冒损失准备余额        
        ,OTSND_LOSS_ALOW_CRD_NON_FRDAVY   --非伪冒损失准备余额      
        ,ACT_LOSS_AMT_CRD_FRD             --年累计伪冒损失金额      
        ,AMT_RCVD_For_LST_TM_OD           --本期还的上个月之前的金额
        ,AMT_RCVD                         --已还款金额              
        ,MTD_ACML_DEP_BAL_AMT             --月累计存款余额                
        ,MTD_ACML_OFFSET_AMT              --月累计冲销余额                
        ,MTD_ACML_OD_BAL_AMT              --月累计透支余额                
        ,TOT_MTD_ACT_LOSS_AMT_CRD_FRD     --月累计伪冒损失金额        
        ,TOT_MTD_AMT_RCVD_For_LST_TM_OD   --月累计本期还前期欠费金额  
        ,TOT_MTD_AMT_RCVD                 --月累计已还款金额          
        ,QTD_ACML_DEP_BAL_AMT             --季累计存款余额            
        ,QTD_ACML_OFFSET_AMT              --季累计冲销余额            
        ,QTD_ACML_OD_BAL_AMT              --季累计透支余额          
        ,TOT_QTD_ACT_LOSS_AMT_CRD_FRD     --季累计伪冒损失金额        
        ,TOT_QTD_AMT_RCVD_For_LST_TM_OD   --季累计本期还前期欠费金额  
        ,TOT_QTD_AMT_RCVD                 --季累计已还款金额                
        ,YTD_ACML_DEP_BAL_AMT             --年累计存款余额            
        ,YTD_ACML_OFFSET_AMT              --年累计冲销金额            
        ,YTD_ACML_OD_BAL_AMT              --年累计透支余额            
        ,TOT_YTD_ACT_LOSS_AMT_CRD_FRD     --年累计伪冒损失金额        
        ,TOT_YTD_AMT_RCVD_For_LST_TM_OD   --年累计本期还前期欠费金额  
        ,TOT_YTD_AMT_RCVD                 --年累计已还款金额  
        ,INT_RCVB_EXCPT_OFF_BST      
)
--------------------------------------Start on 20100113--------------------------------------------------------------
/*
WITH TMP AS (
		Select 
				  OU_ID                                                  --受理机构号                                                 
         ,CR_CRD_TP_ID AS CRD_TP_ID                              --卡类型                                                     
         ,CRD_BRAND_TP_ID AS CRD_Brand_TP_Id                     --卡品牌类型                                                 
         ,CRD_PRVL_TP_ID                                         --卡级别                                                     
         ,ENT_IDV_IND                                            --卡对象                                                     
         ,MST_CRD_IND                                            --主/副卡标志                                                
         ,NGO_CRD_IND                                            --协议卡类型                                                 
         ,MULT_CCY_F                                             --双币卡标志                                                 
         ,AST_RSK_ASES_RTG_TP_CD                                 --资产风险分类                                               
         ,LN_FIVE_RTG_STS                                        --贷款五级形态类型                                           
         ,PD_GRP_CD                                              --产品类                                                     
         ,PD_SUB_CD                                              --产品子代码                                                 
         ,AC_BYND_LMT_F AS BYND_LMT_F                            --超限标志                                                   
         ,CCY                                                    --币种  
         ,value(ISSU_CRD_OU_Id, '') as ISSU_CRD_OU_Id              --发卡机构号                                                          
         ,SUM(CR_LMT) AS CR_LMT                                  --授信额度                            
         ,SUM(TMP_CRED_LMT) AS TMP_CRED_LMT                      --临时授信金额                        
    from SMY.CR_CRD_SMY
    where CRD_LCS_TP_ID = 11920004   --已收卡
    GROUP BY 
    					OU_ID,
              CR_CRD_TP_ID,
              CRD_BRAND_TP_ID,
              CRD_PRVL_TP_ID,
              ENT_IDV_IND,
              MST_CRD_IND,
              NGO_CRD_IND,
              MULT_CCY_F,
              AST_RSK_ASES_RTG_TP_CD,
              LN_FIVE_RTG_STS,
              PD_GRP_CD,
              PD_SUB_CD,
              AC_BYND_LMT_F,
              CCY,
              value(ISSU_CRD_OU_Id,'')                        
  )           , 
*/
--------------------------------------End on 20100113--------------------------------------------------------------
With TMP1 AS (
		Select 
				  OU_ID                                                  --受理机构号                                                 
         ,CR_CRD_TP_ID AS CRD_TP_ID                              --卡类型                                                     
         ,CRD_BRAND_TP_ID AS CRD_Brand_TP_Id                     --卡品牌类型                                                 
         ,CRD_PRVL_TP_ID                                         --卡级别                                                     
         ,ENT_IDV_IND                                            --卡对象                                                     
         ,MST_CRD_IND                                            --主/副卡标志                                                
         ,NGO_CRD_IND                                            --协议卡类型                                                 
         ,MULT_CCY_F                                             --双币卡标志                                                 
         ,AST_RSK_ASES_RTG_TP_CD                                 --资产风险分类                                               
         ,LN_FIVE_RTG_STS                                        --贷款五级形态类型                                           
         ,PD_GRP_CD                                              --产品类                                                     
         ,PD_SUB_CD                                              --产品子代码                                                 
         ,CRD.AC_BYND_LMT_F AS BYND_LMT_F                            --超限标志                                                   
         ,CRD.CCY  as CCY                                              --币种                                                       
         ,ACCOUNTING_DATE AS ACG_DT                              --日期YYYY-MM-DD                                             
         ,CUR_YEAR AS CDR_YR                                     --年份YYYY                                                   
         ,CUR_MONTH AS CDR_MTH                                   --月份MM                                                     
         ,CUR_DAY AS NOD_In_MTH                                  --当月天数                                                   
         ,QTR_DAY AS NOD_In_QTR                                  --当季日历日天数                                             
         ,YR_DAY AS NOD_In_Year                                  --当年日历日天数                                             
         ,value(ISSU_CRD_OU_Id, '') as ISSU_CRD_OU_Id              --发卡机构号                                                 
         ,SUM(AC_BAL_AMT) AS AC_BAL_AMT                          --账户余额                                                   
         ,SUM(CRD.DEP_BAL_CRD) AS DEP_BAL_CRD                        --银行卡存款余额                                             
         ,SUM(OTSND_AMT_RCVB) AS OTSND_AMT_RCVB                  --应收账款余额                                               
         ,SUM(OTSND_AMT_RCVB) AS OTSND_INT_BRG_DUE_AMT           --生息应收账款余额                                           
         --,COUNT(AC_AR_ID) AS NBR_AC                              --账户个数                                                   
         ,sum(case when CRD_LCS_TP_ID in(
		                        11920001  --正常
		              				 ,11920002  --新开卡未启用
		              				 ,11920003  --新换卡未启用              				 
		              				 ,11920004	--已收卡 
              				 )
              		 then 1 else 0 end ) AS NBR_AC                              --账户个数                                                   
         ---------------------------Start of 2009-12-01---------------------------------
         ,SUM( case when CRD_LCS_TP_ID in(
		                        11920001  --正常
		              				 ,11920002  --新开卡未启用
		              				 ,11920003  --新换卡未启用              				 
		              				 ,11920004	--已收卡 
              				   )
                         and CRD.OD_BAL_AMT>0 
                    then 1 else 0 end) AS NBR_OD_AC   --透支账户个数
         ---------------------------End of 2009-12-01---------------------------------
         ,SUM( case when CRD_LCS_TP_ID in(
		                        11920001  --正常
		              				 ,11920002  --新开卡未启用
		              				 ,11920003  --新换卡未启用              				 
		              				 ,11920004	--已收卡 
              				   )
              		  then  CR_LMT else 0 end ) AS CR_LMT                                  --授信额度                            
         ,SUM( case when CRD_LCS_TP_ID in(
		                        11920001  --正常
		              				 ,11920002  --新开卡未启用
		              				 ,11920003  --新换卡未启用              				 
		              				 ,11920004	--已收卡 
              				   )
              		  then TMP_CRED_LMT else 0 end) AS TMP_CRED_LMT                      --临时授信金额                        
         ,SUM(CRD.OD_BAL_AMT) AS OD_BAL_AMT                          --透支余额                            
         ,SUM(AMT_PNP_ARS) AS AMT_PNP_ARS                        --透支本金                            
         ,SUM(INT_RCVB) AS INT_RCVB                              --应收利息                            
         ,SUM(FEE_RCVB) AS FEE_RCVB                              --应收费用                            
         ,0 AS OFFSET_AMT                                        --冲销金额                            
         ,0 AS OTSND_LOSS_ALOW_CRD_FRD_AVY                       --伪冒损失准备余额                    
         ,0 AS OTSND_LOSS_ALOW_CRD_NON_FRDAVY                    --非伪冒损失准备余额                  
         ,0 AS ACT_LOSS_AMT_CRD_FRD                              --年累计伪冒损失金额                  
         ,SUM(AC.AMT_RCVD_For_LST_TM_OD) AS AMT_RCVD_For_LST_TM_OD  --本期还的上个月之前的金额            
         ,SUM(AC.AMT_RCVD) AS AMT_RCVD                              --已还款金额  
         ,SUM(AC.MTD_ACML_DEP_BAL_AMT) AS MTD_ACML_DEP_BAL_AMT  
				,SUM(AC.MTD_ACML_OD_BAL_AMT  ) AS MTD_ACML_OD_BAL_AMT  				
				,SUM(AC.QTD_ACML_OD_BAL_AMT  ) AS QTD_ACML_OD_BAL_AMT  
				,SUM(AC.QTD_ACML_DEP_BAL_AMT ) AS QTD_ACML_DEP_BAL_AMT 
				,SUM(AC.YTD_ACML_OD_BAL_AMT  ) AS YTD_ACML_OD_BAL_AMT  
				,SUM(AC.YTD_ACML_DEP_BAL_AMT ) AS YTD_ACML_DEP_BAL_AMT   
				,SUM(TOT_MTD_AMT_RCVD_For_LST_TM_OD) AS TOT_MTD_AMT_RCVD_For_LST_TM_OD   
				,SUM(TOT_MTD_AMT_RCVD              ) AS TOT_MTD_AMT_RCVD               
				,SUM(TOT_QTD_AMT_RCVD_For_LST_TM_OD) AS TOT_QTD_AMT_RCVD_For_LST_TM_OD   
				,SUM(TOT_QTD_AMT_RCVD              ) AS TOT_QTD_AMT_RCVD               
				,SUM(TOT_YTD_AMT_RCVD_For_LST_TM_OD) AS TOT_YTD_AMT_RCVD_For_LST_TM_OD   
				,SUM(TOT_YTD_AMT_RCVD              ) AS TOT_YTD_AMT_RCVD
				,SUM(INT_RCVB_EXCPT_OFF_BST)  AS INT_RCVB_EXCPT_OFF_BST                           
    from SMY.CR_CRD_SMY CRD
    -------------------Start on 20100119------------------------------------------------------
    /*
    where CRD_LCS_TP_ID in (
              					11920001  --正常
              				 ,11920002  --新开卡未启用
              				 ,11920003  --新换卡未启用
              				 -------------Start on 20100113-----------
              				 ,11920004	--已收卡
              				 -------------End on 20100113-----------
     			)   
    */
    join SMY.MTHLY_CR_CRD_AC_ACML_BAL_AMT AC 
       on CRD.AC_AR_ID = AC.AC_AR_ID 
          AND
          CRD.CCY = AC.CCY
          AND
          AC.CDR_YR = CUR_YEAR
          AND
          AC.CDR_MTH = CUR_MONTH
    -------------------End on 20100119------------------------------------------------------   
    -----------------Start on 20100121------------------------
    where CRD_LCS_TP_ID <> 11920006	   --作废卡
    -----------------End on 20100121------------------------  			
    GROUP BY 
    					OU_ID,
              CR_CRD_TP_ID,
              CRD_BRAND_TP_ID,
              CRD_PRVL_TP_ID,
              ENT_IDV_IND,
              MST_CRD_IND,
              NGO_CRD_IND,
              MULT_CCY_F,
              AST_RSK_ASES_RTG_TP_CD,
              LN_FIVE_RTG_STS,
              PD_GRP_CD,
              PD_SUB_CD,
              CRD.AC_BYND_LMT_F,
              CRD.CCY,
              value(ISSU_CRD_OU_Id,'')
)              
SELECT 
			 S.OU_ID                                          --受理机构号           
      , S.CRD_TP_ID                                     --卡类型               
      , S.CRD_Brand_TP_Id                               --卡品牌类型           
      , S.CRD_PRVL_TP_ID                                --卡级别               
      , S.ENT_IDV_IND                                   --卡对象               
      , S.MST_CRD_IND                                   --主/副卡标志          
      , S.NGO_CRD_IND                                   --协议卡类型           
      , S.MULT_CCY_F                                    --双币卡标志           
      , S.AST_RSK_ASES_RTG_TP_CD                        --资产风险分类         
      , S.LN_FIVE_RTG_STS                               --贷款五级形态类型     
      , S.PD_GRP_CD                                     --产品类               
      , S.PD_SUB_CD                                     --产品子代码           
      , S.BYND_LMT_F                                    --超限标志             
      , S.CCY                                           --币种                 
      , S.ACG_DT                                        --日期YYYY-MM-DD       
      , S.CDR_YR                                        --年份YYYY             
      , S.CDR_MTH                                       --月份MM               
      , S.NOD_In_MTH                                    --当月天数             
      , S.NOD_In_QTR                                    --当季日历日天数       
      , S.NOD_In_Year                                   --当年日历日天数       
      , S.ISSU_CRD_OU_Id                                --发卡机构号           
      , S.AC_BAL_AMT                                    --账户余额             
      , COALESCE(T.AC_BAL_AMT,0) AS LST_Day_AC_BAL      --昨日账户余额         
      , S.DEP_BAL_CRD                                   --银行卡存款余额       
      , S.OTSND_AMT_RCVB                                --应收账款余额         
      , S.OTSND_INT_BRG_DUE_AMT                         --生息应收账款余额     
      , S.NBR_AC                                        --账户个数             
      , S.NBR_OD_AC                                     --透支账户个数
      ---------------------------Start on 20100113-----------------------------------
      --, S.CR_LMT + VALUE(TMP.CR_LMT,0)                  --授信额度                 
      , S.CR_LMT                  --授信额度                 
      --, S.TMP_CRED_LMT + VALUE(TMP.TMP_CRED_LMT,0)      --临时授信金额             
      , S.TMP_CRED_LMT       --临时授信金额             
      ---------------------------End on 20100113-----------------------------------
      , S.OD_BAL_AMT                                    --透支余额                 
      , S.AMT_PNP_ARS                                   --透支本金                 
      , S.INT_RCVB                                      --应收利息                 
      , S.FEE_RCVB                                      --应收费用                 
      , S.OFFSET_AMT                                    --冲销金额                 
      , S.OTSND_LOSS_ALOW_CRD_FRD_AVY                   --伪冒损失准备余额         
      , S.OTSND_LOSS_ALOW_CRD_NON_FRDAVY                --非伪冒损失准备余额       
      , S.ACT_LOSS_AMT_CRD_FRD                          --年累计伪冒损失金额       
      , S.AMT_RCVD_For_LST_TM_OD                        --本期还的上个月之前的金额 
      , S.AMT_RCVD                                      --已还款金额 
      /*              
      ,(CASE WHEN CUR_DAY=1 THEN S.AC_BAL_AMT ELSE COALESCE(S.AC_BAL_AMT+T.MTD_ACML_DEP_BAL_AMT,S.AC_BAL_AMT) END) AS MTD_ACML_DEP_BAL_AMT
      ,(CASE WHEN CUR_DAY=1 THEN S.OFFSET_AMT ELSE COALESCE(S.OFFSET_AMT+T.MTD_ACML_OFFSET_AMT,S.OFFSET_AMT) END) AS MTD_ACML_OFFSET_AMT
      ,(CASE WHEN CUR_DAY=1 THEN S.OD_BAL_AMT ELSE COALESCE(S.OD_BAL_AMT+T.MTD_ACML_OD_BAL_AMT,S.OD_BAL_AMT) END) AS MTD_ACML_OD_BAL_AMT
      ,(CASE WHEN CUR_DAY=1 THEN S.ACT_LOSS_AMT_CRD_FRD ELSE COALESCE(S.ACT_LOSS_AMT_CRD_FRD+T.TOT_MTD_ACT_LOSS_AMT_CRD_FRD,S.ACT_LOSS_AMT_CRD_FRD) END) AS TOT_MTD_ACT_LOSS_AMT_CRD_FRD
      ,(CASE WHEN CUR_DAY=1 THEN S.AMT_RCVD_For_LST_TM_OD ELSE COALESCE(S.AMT_RCVD_For_LST_TM_OD+T.TOT_MTD_AMT_RCVD_For_LST_TM_OD,S.AMT_RCVD_For_LST_TM_OD) END) AS TOT_MTD_AMT_RCVD_For_LST_TM_OD
      ,(CASE WHEN CUR_DAY=1 THEN S.AMT_RCVD ELSE COALESCE(S.AMT_RCVD+T.TOT_MTD_AMT_RCVD,S.AMT_RCVD) END) AS TOT_MTD_AMT_RCVD
      ,(CASE WHEN CUR_DAY=1 AND CUR_MONTH IN (1,4,7,10) THEN S.AC_BAL_AMT ELSE COALESCE(S.AC_BAL_AMT+T.QTD_ACML_DEP_BAL_AMT,S.AC_BAL_AMT) END) AS QTD_ACML_DEP_BAL_AMT
      ,(CASE WHEN CUR_DAY=1 AND CUR_MONTH IN (1,4,7,10) THEN S.OFFSET_AMT ELSE COALESCE(S.OFFSET_AMT+T.QTD_ACML_OFFSET_AMT,S.OFFSET_AMT) END) AS QTD_ACML_OFFSET_AMT
      ,(CASE WHEN CUR_DAY=1 AND CUR_MONTH IN (1,4,7,10) THEN S.OD_BAL_AMT ELSE COALESCE(S.OD_BAL_AMT+T.QTD_ACML_OD_BAL_AMT,S.OD_BAL_AMT) END) AS QTD_ACML_OD_BAL_AMT
      ,(CASE WHEN CUR_DAY=1 AND CUR_MONTH IN (1,4,7,10) THEN S.ACT_LOSS_AMT_CRD_FRD ELSE COALESCE(S.ACT_LOSS_AMT_CRD_FRD+T.TOT_QTD_ACT_LOSS_AMT_CRD_FRD,S.ACT_LOSS_AMT_CRD_FRD) END) AS TOT_QTD_ACT_LOSS_AMT_CRD_FRD
      ,(CASE WHEN CUR_DAY=1 AND CUR_MONTH IN (1,4,7,10) THEN S.AMT_RCVD_For_LST_TM_OD ELSE COALESCE(S.AMT_RCVD_For_LST_TM_OD+T.TOT_QTD_AMT_RCVD_For_LST_TM_OD,S.AMT_RCVD_For_LST_TM_OD) END) AS TOT_QTD_AMT_RCVD_For_LST_TM_OD
      ,(CASE WHEN CUR_DAY=1 AND CUR_MONTH IN (1,4,7,10) THEN S.AMT_RCVD ELSE COALESCE(S.AMT_RCVD+T.TOT_QTD_AMT_RCVD,S.AMT_RCVD) END) AS TOT_QTD_AMT_RCVD
      ,(CASE WHEN CUR_DAY=1 AND CUR_MONTH=1 THEN S.AC_BAL_AMT ELSE COALESCE(S.AC_BAL_AMT+T.YTD_ACML_DEP_BAL_AMT,S.AC_BAL_AMT) END) AS YTD_ACML_DEP_BAL_AMT
      ,(CASE WHEN CUR_DAY=1 AND CUR_MONTH=1 THEN S.OFFSET_AMT ELSE COALESCE(S.OFFSET_AMT+T.YTD_ACML_OFFSET_AMT,S.OFFSET_AMT) END) AS YTD_ACML_OFFSET_AMT
      ,(CASE WHEN CUR_DAY=1 AND CUR_MONTH=1 THEN S.OD_BAL_AMT ELSE COALESCE(S.OD_BAL_AMT+T.YTD_ACML_OD_BAL_AMT,S.OD_BAL_AMT) END) AS YTD_ACML_OD_BAL_AMT
      ,(CASE WHEN CUR_DAY=1 AND CUR_MONTH=1 THEN S.ACT_LOSS_AMT_CRD_FRD ELSE COALESCE(S.ACT_LOSS_AMT_CRD_FRD+T.TOT_YTD_ACT_LOSS_AMT_CRD_FRD,S.ACT_LOSS_AMT_CRD_FRD) END) AS TOT_YTD_ACT_LOSS_AMT_CRD_FRD
      ,(CASE WHEN CUR_DAY=1 AND CUR_MONTH=1 THEN S.AMT_RCVD_For_LST_TM_OD ELSE COALESCE(S.AMT_RCVD_For_LST_TM_OD+T.TOT_YTD_AMT_RCVD_For_LST_TM_OD,S.AMT_RCVD_For_LST_TM_OD) END) AS TOT_YTD_AMT_RCVD_For_LST_TM_OD
      ,(CASE WHEN CUR_DAY=1 AND CUR_MONTH=1 THEN S.AMT_RCVD ELSE COALESCE(S.AMT_RCVD+T.TOT_YTD_AMT_RCVD,S.AMT_RCVD) END) AS TOT_YTD_AMT_RCVD
      */
      ,S.MTD_ACML_DEP_BAL_AMT 									AS MTD_ACML_DEP_BAL_AMT
      ,0 																				AS MTD_ACML_OFFSET_AMT
      ,S.MTD_ACML_OD_BAL_AMT 										AS MTD_ACML_OD_BAL_AMT
      ,0 																				AS TOT_MTD_ACT_LOSS_AMT_CRD_FRD
      ,S.TOT_MTD_AMT_RCVD_For_LST_TM_OD 				AS TOT_MTD_AMT_RCVD_For_LST_TM_OD
      ,S.TOT_MTD_AMT_RCVD 											AS TOT_MTD_AMT_RCVD
      ,S.QTD_ACML_DEP_BAL_AMT 									AS QTD_ACML_DEP_BAL_AMT
      ,0 																				AS QTD_ACML_OFFSET_AMT
      ,S.QTD_ACML_OD_BAL_AMT 										AS QTD_ACML_OD_BAL_AMT
      ,0 																				AS TOT_QTD_ACT_LOSS_AMT_CRD_FRD
      ,S.TOT_QTD_AMT_RCVD_For_LST_TM_OD 				AS TOT_QTD_AMT_RCVD_For_LST_TM_OD
      ,S.TOT_QTD_AMT_RCVD 											AS TOT_QTD_AMT_RCVD
      ,S.YTD_ACML_DEP_BAL_AMT 									AS YTD_ACML_DEP_BAL_AMT
      ,0 																				AS YTD_ACML_OFFSET_AMT
      ,S.YTD_ACML_OD_BAL_AMT 										AS YTD_ACML_OD_BAL_AMT
      ,0 																				AS TOT_YTD_ACT_LOSS_AMT_CRD_FRD
      ,S.TOT_YTD_AMT_RCVD_For_LST_TM_OD 				AS TOT_YTD_AMT_RCVD_For_LST_TM_OD
      ,S.TOT_YTD_AMT_RCVD 											AS TOT_YTD_AMT_RCVD
      ,S.INT_RCVB_EXCPT_OFF_BST                 AS INT_RCVB_EXCPT_OFF_BST
FROM TMP1 S
----------------------------------------------------------------------Start on 20100113---------------------------------------------------------------------
/*
LEFT JOIN TMP                   --如果'已收卡'中存在'有效卡'没有的维度，则该部分卡的授信额度无法统计，因此建议应用从账户层进行授信额度的统计。
	ON S.OU_ID=TMP.OU_ID
	AND S.CRD_TP_ID=TMP.CRD_TP_ID
	AND S.CRD_Brand_TP_Id=TMP.CRD_Brand_TP_Id
	AND S.CRD_PRVL_TP_ID=TMP.CRD_PRVL_TP_ID
	AND S.ENT_IDV_IND=TMP.ENT_IDV_IND
	AND S.MST_CRD_IND=TMP.MST_CRD_IND
	AND S.NGO_CRD_IND=TMP.NGO_CRD_IND
	AND S.MULT_CCY_F=TMP.MULT_CCY_F
	AND S.AST_RSK_ASES_RTG_TP_CD=TMP.AST_RSK_ASES_RTG_TP_CD
	AND S.LN_FIVE_RTG_STS=TMP.LN_FIVE_RTG_STS
	AND S.PD_GRP_CD=TMP.PD_GRP_CD
	AND S.PD_SUB_CD=TMP.PD_SUB_CD
	AND S.BYND_LMT_F=TMP.BYND_LMT_F
	AND S.CCY=TMP.CCY	
	AND S.ISSU_CRD_OU_Id=TMP.ISSU_CRD_OU_Id
*/	
----------------------------------------------------------------------End on 20100113---------------------------------------------------------------------
LEFT JOIN SMY.CR_CRD_BAL_OU_DLY_SMY T
	ON S.OU_ID=T.OU_ID
	AND S.CRD_TP_ID=T.CRD_TP_ID
	AND S.CRD_Brand_TP_Id=T.CRD_Brand_TP_Id
	AND S.CRD_PRVL_TP_ID=T.CRD_PRVL_TP_ID
	AND S.ENT_IDV_IND=T.ENT_IDV_IND
	AND S.MST_CRD_IND=T.MST_CRD_IND
	AND S.NGO_CRD_IND=T.NGO_CRD_IND
	AND S.MULT_CCY_F=T.MULT_CCY_F
	AND S.AST_RSK_ASES_RTG_TP_CD=T.AST_RSK_ASES_RTG_TP_CD
	AND S.LN_FIVE_RTG_STS=T.LN_FIVE_RTG_STS
	AND S.PD_GRP_CD=T.PD_GRP_CD
	AND S.PD_SUB_CD=T.PD_SUB_CD
	AND S.BYND_LMT_F=T.BYND_LMT_F
	AND S.CCY=T.CCY
	AND T.ACG_DT=LAST_ACG_DT
	AND S.ISSU_CRD_OU_Id=T.ISSU_CRD_OU_Id;--

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
SET SMY_STEPNUM = SMY_STEPNUM+1;--
SET SMY_STEPDESC = '插入汇总表当日交易数据.';--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);   --
COMMIT;--

IF NEXT_DAY=1 THEN
   DELETE FROM SMY.CR_CRD_BAL_OU_MTHLY_SMY WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;--
   INSERT INTO SMY.CR_CRD_BAL_OU_MTHLY_SMY(OU_ID,
                                           CRD_TP_ID,
                                           CRD_Brand_TP_Id,
                                           CRD_PRVL_TP_ID,
                                           ENT_IDV_IND,
                                           MST_CRD_IND,
                                           NGO_CRD_IND,
                                           MULT_CCY_F,
                                           AST_RSK_ASES_RTG_TP_CD,
                                           LN_FIVE_RTG_STS,
                                           PD_GRP_CD,
                                           PD_SUB_CD,
                                           BYND_LMT_F,
                                           CCY,
                                           ACG_DT,
                                           CDR_YR,
                                           CDR_MTH,
                                           NOD_In_MTH,
                                           NOD_In_QTR,
                                           NOD_In_Year,
                                           ISSU_CRD_OU_Id,
                                           AC_BAL_AMT,
                                           LST_Day_AC_BAL,
                                           DEP_BAL_CRD,
                                           OTSND_AMT_RCVB,
                                           OTSND_INT_BRG_DUE_AMT,
                                           NBR_AC,
                                           NBR_OD_AC,
                                           CR_LMT,
                                           TMP_CRED_LMT,
                                           OD_BAL_AMT,
                                           AMT_PNP_ARS,
                                           INT_RCVB,
                                           FEE_RCVB,
                                           OFFSET_AMT,
                                           OTSND_LOSS_ALOW_CRD_FRD_AVY,
                                           OTSND_LOSS_ALOW_CRD_NON_FRDAVY,
                                           ACT_LOSS_AMT_CRD_FRD,
                                           AMT_RCVD_For_LST_TM_OD,
                                           AMT_RCVD,
                                           MTD_ACML_DEP_BAL_AMT,
                                           MTD_ACML_OFFSET_AMT,
                                           MTD_ACML_OD_BAL_AMT,
                                           TOT_MTD_ACT_LOSS_AMT_CRD_FRD,
                                           TOT_MTD_AMT_RCVD_For_LST_TM_OD,
                                           TOT_MTD_AMT_RCVD,
                                           QTD_ACML_DEP_BAL_AMT,
                                           QTD_ACML_OFFSET_AMT,
                                           QTD_ACML_OD_BAL_AMT,
                                           TOT_QTD_ACT_LOSS_AMT_CRD_FRD,
                                           TOT_QTD_AMT_RCVD_For_LST_TM_OD,
                                           TOT_QTD_AMT_RCVD,
                                           YTD_ACML_DEP_BAL_AMT,
                                           YTD_ACML_OFFSET_AMT,
                                           YTD_ACML_OD_BAL_AMT,
                                           TOT_YTD_ACT_LOSS_AMT_CRD_FRD,
                                           TOT_YTD_AMT_RCVD_For_LST_TM_OD,
                                           TOT_YTD_AMT_RCVD,
                                           INT_RCVB_EXCPT_OFF_BST)
   SELECT OU_ID,
          CRD_TP_ID,
          CRD_Brand_TP_Id,
          CRD_PRVL_TP_ID,
          ENT_IDV_IND,
          MST_CRD_IND,
          NGO_CRD_IND,
          MULT_CCY_F,
          AST_RSK_ASES_RTG_TP_CD,
          LN_FIVE_RTG_STS,
          PD_GRP_CD,
          PD_SUB_CD,
          BYND_LMT_F,
          CCY,
          ACG_DT,
          CDR_YR,
          CDR_MTH,
          NOD_In_MTH,
          NOD_In_QTR,
          NOD_In_Year,
          ISSU_CRD_OU_Id,
          AC_BAL_AMT,
          LST_Day_AC_BAL,
          DEP_BAL_CRD,
          OTSND_AMT_RCVB,
          OTSND_INT_BRG_DUE_AMT,
          NBR_AC,
          NBR_OD_AC,
          CR_LMT,
          TMP_CRED_LMT,
          OD_BAL_AMT,
          AMT_PNP_ARS,
          INT_RCVB,
          FEE_RCVB,
          OFFSET_AMT,
          OTSND_LOSS_ALOW_CRD_FRD_AVY,
          OTSND_LOSS_ALOW_CRD_NON_FRDAVY,
          ACT_LOSS_AMT_CRD_FRD,
          AMT_RCVD_For_LST_TM_OD,
          AMT_RCVD,
          MTD_ACML_DEP_BAL_AMT,
          MTD_ACML_OFFSET_AMT,
          MTD_ACML_OD_BAL_AMT,
          TOT_MTD_ACT_LOSS_AMT_CRD_FRD,
          TOT_MTD_AMT_RCVD_For_LST_TM_OD,
          TOT_MTD_AMT_RCVD,
          QTD_ACML_DEP_BAL_AMT,
          QTD_ACML_OFFSET_AMT,
          QTD_ACML_OD_BAL_AMT,
          TOT_QTD_ACT_LOSS_AMT_CRD_FRD,
          TOT_QTD_AMT_RCVD_For_LST_TM_OD,
          TOT_QTD_AMT_RCVD,
          YTD_ACML_DEP_BAL_AMT,
          YTD_ACML_OFFSET_AMT,
          YTD_ACML_OD_BAL_AMT,
          TOT_YTD_ACT_LOSS_AMT_CRD_FRD,
          TOT_YTD_AMT_RCVD_For_LST_TM_OD,
          TOT_YTD_AMT_RCVD,
          INT_RCVB_EXCPT_OFF_BST
  FROM SMY.CR_CRD_BAL_OU_DLY_SMY
  WHERE ACG_DT=ACCOUNTING_DATE;--
END IF;--

SET SMY_STEPNUM=SMY_STEPNUM+1 ;--
SET SMY_STEPDESC = '存储过程结束!';--

INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
VALUES(SMY_PROCNM, SMY_DATE, 0, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

END@