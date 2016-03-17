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
-- 2010-01-13   Xu Yan           Included 11920004 --���տ�
-- 2010-01-19   Xu Yan           Updated the accumulated amount getting logic from the ar level
-- 2010-01-21   Xu Yan           Handled the '���Ͽ�' problem, excluded them
-------------------------------------------------------------------------------
LANGUAGE SQL
BEGIN
/*�����쳣����ʹ�ñ���*/
DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
DECLARE SMY_STEPNUM INT DEFAULT 1;                     --�����ڲ�λ�ñ��
DECLARE SMY_STEPDESC VARCHAR(100) DEFAULT '';          --�����ڲ�λ������
DECLARE SMY_DATE DATE;                                 --��ʱ���ڱ���
DECLARE SMY_RCOUNT INT;                                --DML������ü�¼��
DECLARE SMY_PROCNM VARCHAR(100);                        --�洢��������
DECLARE at_end SMALLINT DEFAULT 0;--
/*�����洢����ʹ�ñ���*/
DECLARE CUR_YEAR SMALLINT;                             --��
DECLARE CUR_MONTH SMALLINT;                            --��
DECLARE CUR_DAY INTEGER;                               --��
DECLARE LAST_YR_MONTH VARCHAR(6);                      --����
DECLARE LAST_ACG_DT DATE;                              --��һ��
DECLARE YR_FIRST_DAY DATE;                             --���1��1��
DECLARE QTR_FIRST_DAY DATE;                            --ÿ���ȵ�1��
DECLARE YR_DAY SMALLINT;                               --��������
DECLARE QTR_DAY SMALLINT;                              --��������
DECLARE NEXT_DAY SMALLINT;                             --��һ��
--DECLARE MAX_ACG_DT DATE;                               --���������
--DECLARE DELETE_SQL VARCHAR(200);                       --ɾ����ʷ��̬SQL

/*1.�������SQL�쳣����ľ��(EXIT��ʽ).
  2.������SQL�쳣ʱ�ڴ洢�����е�λ��(SMY_STEPNUM),λ������(SMY_STEPDESC)��SQLCODE(SMY_SQLCODE)�����SMY_LOG����������.
  3.����RESIGNAL���������쳣,�����洢����ִ����,������SQL�쳣֮ǰ�洢������������ɵĲ������лع�.*/
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
/*������ֵ*/
SET SMY_PROCNM = 'PROC_CR_CRD_BAL_OU_DLY_SMY';--
SET SMY_DATE=ACCOUNTING_DATE;--
SET CUR_YEAR=YEAR(ACCOUNTING_DATE);  --ȡ��ǰ���
SET CUR_MONTH=MONTH(ACCOUNTING_DATE); --ȡ��ǰ�·�
SET CUR_DAY=DAY(ACCOUNTING_DATE);     --ȡ����
IF CUR_MONTH=1 THEN 
   SET LAST_YR_MONTH=TRIM(CHAR(CUR_YEAR-1))||'12';--
ELSE
   SET LAST_YR_MONTH=TRIM(CHAR(CUR_YEAR))||RIGHT('0'||TRIM(CHAR(CUR_MONTH-1)),2);--
END IF;--
SET LAST_ACG_DT=ACCOUNTING_DATE - 1 DAYS;--
SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');    --�����1��
SET YR_DAY=DAYS(ACCOUNTING_DATE)-DAYS(YR_FIRST_DAY) + 1;    --��ǰ����������ڵ���ڼ���
IF CUR_MONTH IN (1,2,3) THEN                              --������1��
   SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');--
ELSEIF CUR_MONTH IN (4,5,6) THEN 
       SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-04-01');--
    ELSEIF CUR_MONTH IN (7,8,9) THEN 
           SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-07-01');--
        ELSE
            SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-10-01');--
END IF;--
SET QTR_DAY=DAYS(ACCOUNTING_DATE)-DAYS(QTR_FIRST_DAY) + 1;  --��ǰ����������ڵ����ڼ���
SET NEXT_DAY=DAY(DATE(ACCOUNTING_DATE)+1 DAYS);--
--SELECT MAX(ACG_DT) INTO MAX_ACG_DT FROM SMY.CR_CRD_BAL_OU_DLY_SMY;          --��ǰ���ܱ��ܴ�����ݵ����������
--SET DELETE_SQL='ALTER TABLE SMY.HIST_CR_CRD_BAL_OU_DLY_SMY ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE';--

/*Delete��־��,����SMY_PROCNM=��ǰ�洢��������,SMY_DATE=ACCOUNTING_DATE,�������µ���ʼ��־*/
DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
COMMIT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, 'PROCEDURE START.', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

/*���ݻָ��뱸��*/

DELETE FROM SMY.CR_CRD_BAL_OU_DLY_SMY WHERE ACG_DT=ACCOUNTING_DATE;--
COMMIT;--



INSERT INTO SMY.CR_CRD_BAL_OU_DLY_SMY(
				 OU_ID                            --���������              
        ,CRD_TP_ID                        --������                  
        ,CRD_Brand_TP_Id                  --��Ʒ������              
        ,CRD_PRVL_TP_ID                   --������                  
        ,ENT_IDV_IND                      --������                  
        ,MST_CRD_IND                      --��/������־             
        ,NGO_CRD_IND                      --Э�鿨����              
        ,MULT_CCY_F                       --˫�ҿ���־              
        ,AST_RSK_ASES_RTG_TP_CD           --�ʲ����շ���            
        ,LN_FIVE_RTG_STS                  --�����弶��̬����        
        ,PD_GRP_CD                        --��Ʒ��                  
        ,PD_SUB_CD                        --��Ʒ�Ӵ���              
        ,BYND_LMT_F                       --���ޱ�־                
        ,CCY                              --����                    
        ,ACG_DT                           --����YYYY-MM-DD          
        ,CDR_YR                           --���YYYY                
        ,CDR_MTH                          --�·�MM                  
        ,NOD_In_MTH                       --��������                
        ,NOD_In_QTR                       --��������������          
        ,NOD_In_Year                      --��������������          
        ,ISSU_CRD_OU_Id                   --����������              
        ,AC_BAL_AMT                       --�˻����                
        ,LST_Day_AC_BAL                   --�����˻����            
        ,DEP_BAL_CRD                      --���п�������          
        ,OTSND_AMT_RCVB                   --Ӧ���˿����            
        ,OTSND_INT_BRG_DUE_AMT            --��ϢӦ���˿����        
        ,NBR_AC                           --�˻�����
        ------------------------Start of 2009-12-01-------------------------                           
        ,NBR_OD_AC                        --͸֧�˻�����
        ------------------------End of 2009-12-01-------------------------
        ,CR_LMT                           --���Ŷ��                
        ,TMP_CRED_LMT                     --��ʱ���Ž��            
        ,OD_BAL_AMT                       --͸֧���                
        ,AMT_PNP_ARS                      --͸֧����                
        ,INT_RCVB                         --Ӧ����Ϣ                
        ,FEE_RCVB                         --Ӧ�շ���                
        ,OFFSET_AMT                       --�������                
        ,OTSND_LOSS_ALOW_CRD_FRD_AVY      --αð��ʧ׼�����        
        ,OTSND_LOSS_ALOW_CRD_NON_FRDAVY   --��αð��ʧ׼�����      
        ,ACT_LOSS_AMT_CRD_FRD             --���ۼ�αð��ʧ���      
        ,AMT_RCVD_For_LST_TM_OD           --���ڻ����ϸ���֮ǰ�Ľ��
        ,AMT_RCVD                         --�ѻ�����              
        ,MTD_ACML_DEP_BAL_AMT             --���ۼƴ�����                
        ,MTD_ACML_OFFSET_AMT              --���ۼƳ������                
        ,MTD_ACML_OD_BAL_AMT              --���ۼ�͸֧���                
        ,TOT_MTD_ACT_LOSS_AMT_CRD_FRD     --���ۼ�αð��ʧ���        
        ,TOT_MTD_AMT_RCVD_For_LST_TM_OD   --���ۼƱ��ڻ�ǰ��Ƿ�ѽ��  
        ,TOT_MTD_AMT_RCVD                 --���ۼ��ѻ�����          
        ,QTD_ACML_DEP_BAL_AMT             --���ۼƴ�����            
        ,QTD_ACML_OFFSET_AMT              --���ۼƳ������            
        ,QTD_ACML_OD_BAL_AMT              --���ۼ�͸֧���          
        ,TOT_QTD_ACT_LOSS_AMT_CRD_FRD     --���ۼ�αð��ʧ���        
        ,TOT_QTD_AMT_RCVD_For_LST_TM_OD   --���ۼƱ��ڻ�ǰ��Ƿ�ѽ��  
        ,TOT_QTD_AMT_RCVD                 --���ۼ��ѻ�����                
        ,YTD_ACML_DEP_BAL_AMT             --���ۼƴ�����            
        ,YTD_ACML_OFFSET_AMT              --���ۼƳ������            
        ,YTD_ACML_OD_BAL_AMT              --���ۼ�͸֧���            
        ,TOT_YTD_ACT_LOSS_AMT_CRD_FRD     --���ۼ�αð��ʧ���        
        ,TOT_YTD_AMT_RCVD_For_LST_TM_OD   --���ۼƱ��ڻ�ǰ��Ƿ�ѽ��  
        ,TOT_YTD_AMT_RCVD                 --���ۼ��ѻ�����  
        ,INT_RCVB_EXCPT_OFF_BST      
)
--------------------------------------Start on 20100113--------------------------------------------------------------
/*
WITH TMP AS (
		Select 
				  OU_ID                                                  --���������                                                 
         ,CR_CRD_TP_ID AS CRD_TP_ID                              --������                                                     
         ,CRD_BRAND_TP_ID AS CRD_Brand_TP_Id                     --��Ʒ������                                                 
         ,CRD_PRVL_TP_ID                                         --������                                                     
         ,ENT_IDV_IND                                            --������                                                     
         ,MST_CRD_IND                                            --��/������־                                                
         ,NGO_CRD_IND                                            --Э�鿨����                                                 
         ,MULT_CCY_F                                             --˫�ҿ���־                                                 
         ,AST_RSK_ASES_RTG_TP_CD                                 --�ʲ����շ���                                               
         ,LN_FIVE_RTG_STS                                        --�����弶��̬����                                           
         ,PD_GRP_CD                                              --��Ʒ��                                                     
         ,PD_SUB_CD                                              --��Ʒ�Ӵ���                                                 
         ,AC_BYND_LMT_F AS BYND_LMT_F                            --���ޱ�־                                                   
         ,CCY                                                    --����  
         ,value(ISSU_CRD_OU_Id, '') as ISSU_CRD_OU_Id              --����������                                                          
         ,SUM(CR_LMT) AS CR_LMT                                  --���Ŷ��                            
         ,SUM(TMP_CRED_LMT) AS TMP_CRED_LMT                      --��ʱ���Ž��                        
    from SMY.CR_CRD_SMY
    where CRD_LCS_TP_ID = 11920004   --���տ�
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
				  OU_ID                                                  --���������                                                 
         ,CR_CRD_TP_ID AS CRD_TP_ID                              --������                                                     
         ,CRD_BRAND_TP_ID AS CRD_Brand_TP_Id                     --��Ʒ������                                                 
         ,CRD_PRVL_TP_ID                                         --������                                                     
         ,ENT_IDV_IND                                            --������                                                     
         ,MST_CRD_IND                                            --��/������־                                                
         ,NGO_CRD_IND                                            --Э�鿨����                                                 
         ,MULT_CCY_F                                             --˫�ҿ���־                                                 
         ,AST_RSK_ASES_RTG_TP_CD                                 --�ʲ����շ���                                               
         ,LN_FIVE_RTG_STS                                        --�����弶��̬����                                           
         ,PD_GRP_CD                                              --��Ʒ��                                                     
         ,PD_SUB_CD                                              --��Ʒ�Ӵ���                                                 
         ,CRD.AC_BYND_LMT_F AS BYND_LMT_F                            --���ޱ�־                                                   
         ,CRD.CCY  as CCY                                              --����                                                       
         ,ACCOUNTING_DATE AS ACG_DT                              --����YYYY-MM-DD                                             
         ,CUR_YEAR AS CDR_YR                                     --���YYYY                                                   
         ,CUR_MONTH AS CDR_MTH                                   --�·�MM                                                     
         ,CUR_DAY AS NOD_In_MTH                                  --��������                                                   
         ,QTR_DAY AS NOD_In_QTR                                  --��������������                                             
         ,YR_DAY AS NOD_In_Year                                  --��������������                                             
         ,value(ISSU_CRD_OU_Id, '') as ISSU_CRD_OU_Id              --����������                                                 
         ,SUM(AC_BAL_AMT) AS AC_BAL_AMT                          --�˻����                                                   
         ,SUM(CRD.DEP_BAL_CRD) AS DEP_BAL_CRD                        --���п�������                                             
         ,SUM(OTSND_AMT_RCVB) AS OTSND_AMT_RCVB                  --Ӧ���˿����                                               
         ,SUM(OTSND_AMT_RCVB) AS OTSND_INT_BRG_DUE_AMT           --��ϢӦ���˿����                                           
         --,COUNT(AC_AR_ID) AS NBR_AC                              --�˻�����                                                   
         ,sum(case when CRD_LCS_TP_ID in(
		                        11920001  --����
		              				 ,11920002  --�¿���δ����
		              				 ,11920003  --�»���δ����              				 
		              				 ,11920004	--���տ� 
              				 )
              		 then 1 else 0 end ) AS NBR_AC                              --�˻�����                                                   
         ---------------------------Start of 2009-12-01---------------------------------
         ,SUM( case when CRD_LCS_TP_ID in(
		                        11920001  --����
		              				 ,11920002  --�¿���δ����
		              				 ,11920003  --�»���δ����              				 
		              				 ,11920004	--���տ� 
              				   )
                         and CRD.OD_BAL_AMT>0 
                    then 1 else 0 end) AS NBR_OD_AC   --͸֧�˻�����
         ---------------------------End of 2009-12-01---------------------------------
         ,SUM( case when CRD_LCS_TP_ID in(
		                        11920001  --����
		              				 ,11920002  --�¿���δ����
		              				 ,11920003  --�»���δ����              				 
		              				 ,11920004	--���տ� 
              				   )
              		  then  CR_LMT else 0 end ) AS CR_LMT                                  --���Ŷ��                            
         ,SUM( case when CRD_LCS_TP_ID in(
		                        11920001  --����
		              				 ,11920002  --�¿���δ����
		              				 ,11920003  --�»���δ����              				 
		              				 ,11920004	--���տ� 
              				   )
              		  then TMP_CRED_LMT else 0 end) AS TMP_CRED_LMT                      --��ʱ���Ž��                        
         ,SUM(CRD.OD_BAL_AMT) AS OD_BAL_AMT                          --͸֧���                            
         ,SUM(AMT_PNP_ARS) AS AMT_PNP_ARS                        --͸֧����                            
         ,SUM(INT_RCVB) AS INT_RCVB                              --Ӧ����Ϣ                            
         ,SUM(FEE_RCVB) AS FEE_RCVB                              --Ӧ�շ���                            
         ,0 AS OFFSET_AMT                                        --�������                            
         ,0 AS OTSND_LOSS_ALOW_CRD_FRD_AVY                       --αð��ʧ׼�����                    
         ,0 AS OTSND_LOSS_ALOW_CRD_NON_FRDAVY                    --��αð��ʧ׼�����                  
         ,0 AS ACT_LOSS_AMT_CRD_FRD                              --���ۼ�αð��ʧ���                  
         ,SUM(AC.AMT_RCVD_For_LST_TM_OD) AS AMT_RCVD_For_LST_TM_OD  --���ڻ����ϸ���֮ǰ�Ľ��            
         ,SUM(AC.AMT_RCVD) AS AMT_RCVD                              --�ѻ�����  
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
              					11920001  --����
              				 ,11920002  --�¿���δ����
              				 ,11920003  --�»���δ����
              				 -------------Start on 20100113-----------
              				 ,11920004	--���տ�
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
    where CRD_LCS_TP_ID <> 11920006	   --���Ͽ�
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
			 S.OU_ID                                          --���������           
      , S.CRD_TP_ID                                     --������               
      , S.CRD_Brand_TP_Id                               --��Ʒ������           
      , S.CRD_PRVL_TP_ID                                --������               
      , S.ENT_IDV_IND                                   --������               
      , S.MST_CRD_IND                                   --��/������־          
      , S.NGO_CRD_IND                                   --Э�鿨����           
      , S.MULT_CCY_F                                    --˫�ҿ���־           
      , S.AST_RSK_ASES_RTG_TP_CD                        --�ʲ����շ���         
      , S.LN_FIVE_RTG_STS                               --�����弶��̬����     
      , S.PD_GRP_CD                                     --��Ʒ��               
      , S.PD_SUB_CD                                     --��Ʒ�Ӵ���           
      , S.BYND_LMT_F                                    --���ޱ�־             
      , S.CCY                                           --����                 
      , S.ACG_DT                                        --����YYYY-MM-DD       
      , S.CDR_YR                                        --���YYYY             
      , S.CDR_MTH                                       --�·�MM               
      , S.NOD_In_MTH                                    --��������             
      , S.NOD_In_QTR                                    --��������������       
      , S.NOD_In_Year                                   --��������������       
      , S.ISSU_CRD_OU_Id                                --����������           
      , S.AC_BAL_AMT                                    --�˻����             
      , COALESCE(T.AC_BAL_AMT,0) AS LST_Day_AC_BAL      --�����˻����         
      , S.DEP_BAL_CRD                                   --���п�������       
      , S.OTSND_AMT_RCVB                                --Ӧ���˿����         
      , S.OTSND_INT_BRG_DUE_AMT                         --��ϢӦ���˿����     
      , S.NBR_AC                                        --�˻�����             
      , S.NBR_OD_AC                                     --͸֧�˻�����
      ---------------------------Start on 20100113-----------------------------------
      --, S.CR_LMT + VALUE(TMP.CR_LMT,0)                  --���Ŷ��                 
      , S.CR_LMT                  --���Ŷ��                 
      --, S.TMP_CRED_LMT + VALUE(TMP.TMP_CRED_LMT,0)      --��ʱ���Ž��             
      , S.TMP_CRED_LMT       --��ʱ���Ž��             
      ---------------------------End on 20100113-----------------------------------
      , S.OD_BAL_AMT                                    --͸֧���                 
      , S.AMT_PNP_ARS                                   --͸֧����                 
      , S.INT_RCVB                                      --Ӧ����Ϣ                 
      , S.FEE_RCVB                                      --Ӧ�շ���                 
      , S.OFFSET_AMT                                    --�������                 
      , S.OTSND_LOSS_ALOW_CRD_FRD_AVY                   --αð��ʧ׼�����         
      , S.OTSND_LOSS_ALOW_CRD_NON_FRDAVY                --��αð��ʧ׼�����       
      , S.ACT_LOSS_AMT_CRD_FRD                          --���ۼ�αð��ʧ���       
      , S.AMT_RCVD_For_LST_TM_OD                        --���ڻ����ϸ���֮ǰ�Ľ�� 
      , S.AMT_RCVD                                      --�ѻ����� 
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
LEFT JOIN TMP                   --���'���տ�'�д���'��Ч��'û�е�ά�ȣ���ò��ֿ������Ŷ���޷�ͳ�ƣ���˽���Ӧ�ô��˻���������Ŷ�ȵ�ͳ�ơ�
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
SET SMY_STEPDESC = '������ܱ��ս�������.';--
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
SET SMY_STEPDESC = '�洢���̽���!';--

INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
VALUES(SMY_PROCNM, SMY_DATE, 0, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

END@