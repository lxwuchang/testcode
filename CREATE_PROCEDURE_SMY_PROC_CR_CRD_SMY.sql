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

/*�����쳣����ʹ�ñ���*/
		DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
		DECLARE SMY_STEPNUM INT DEFAULT 1;                     --�����ڲ�λ�ñ��
		DECLARE SMY_STEPDESC VARCHAR(100) DEFAULT '';          --�����ڲ�λ������
		DECLARE SMY_DATE DATE;        --��ʱ���ڱ���
		DECLARE SMY_RCOUNT INT;       --DML������ü�¼��
		DECLARE SMY_PROCNM VARCHAR(100);                        --�洢��������

/*�����洢����ʹ�ñ���*/
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
	1.�������SQL�쳣����ľ��(EXIT��ʽ).
  2.������SQL�쳣ʱ�ڴ洢�����е�λ��(SMY_STEPNUM),λ������(SMY_STEPDESC),SQLCODE(SMY_SQLCODE)�����SMY_LOG����������.
  3.����RESIGNAL���������쳣,�����洢����ִ����,������SQL�쳣֮ǰ�洢������������ɵĲ������лع�.
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

   /*������ֵ*/
    SET SMY_PROCNM  ='PROC_CR_CRD_SMY';--
    SET SMY_DATE    =ACCOUNTING_DATE;    --
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --ȡ��ǰ���
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --ȡ��ǰ�·�
    SET CUR_DAY     =DAY(ACCOUNTING_DATE);     --ȡ�µڼ���
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- ȡ�����
    SET C_YR_DAY      =DAYOFYEAR(DATE(TRIM(CHAR(YEAR(ACCOUNTING_DATE)))||'-12-31')); --ȡ����ڼ���
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --��ǰ����
    SET MTH_FIRST_DAY=DATE(TRIM(CHAR(TRIM(CHAR(CUR_YEAR))||'-'||RIGHT('0'||TRIM(CHAR(CUR_MONTH)),2)||'-01'))); --ȡ���³���
    SET MON_DAY     =DAY(ACCOUNTING_DATE);     --ȡ�µڼ���
   
    SET LAST_SMY_DATE=DATE(SMY_DATE) - 1 DAYS ;--

    SET LAST_MONTH = MONTH(LAST_SMY_DATE);--

    --��������������
    SET C_MON_DAY = DAYS(MTH_FIRST_DAY + 1 MONTH ) - DAYS(MTH_FIRST_DAY);    --
  
  --���㼾����������
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

  /*ȡ������������*/ 
  	SET C_QTR_DAY = DAYS(QTR_LAST_DAY) - DAYS(QTR_FIRST_DAY) + 1 ;--
	

/*Delete��־��,����SMY_PROCNM=��ǰ�洢��������,SMY_DATE=ACCOUNTING_DATE,�������µ���ʼ��־*/
		DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
			COMMIT;--
		
		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		
		SET SMY_STEPDESC = 	'�洢���̿�ʼ����' ;--
		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
				VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--


	   SET EMP_SQL= 'Alter TABLE SMY.CR_CRD_SMY ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE' ;--
		
		  EXECUTE IMMEDIATE EMP_SQL;       --
      
      COMMIT;--

SET SMY_STEPNUM = 2 ;--
SET SMY_STEPDESC = '�����û���ʱ��,���SOR.CC_REPYMT_TXN_DTL ��ʱ����';--

	/*�����û���ʱ��*/
	
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
 /* �ռ�������Ϣ */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	 --

 
		SET SMY_STEPNUM = 3 ;--
		SET SMY_STEPDESC = '������ʱ��, ��Ŵ�SOR.CC_AC_AR ���ܺ������'; 		 --
 
	DECLARE GLOBAL TEMPORARY TABLE T_CC_AC_AR 
   AS 
   (
   		SELECT
   				 CC_AC_AR_ID                AS CC_AC_AR_ID
          ,AST_RSK_ASES_RTG_TP_CD     AS AST_RSK_ASES_RTG_TP_CD  --
          ,LN_FR_RSLT_TP_ID           AS LN_FIVE_RTG_STS         --     
          ,BAL_AMT                    AS AC_BAL_AMT              --�˻����
          ,(CASE WHEN BAL_AMT <=0 THEN 0 ELSE BAL_AMT END)  AS DEP_BAL_CRD  --���п������� 
          ,(CASE WHEN BAL_AMT > 0 THEN 0 ELSE ABS(BAL_AMT-ODUE_INT_AMT-FEE_AMT_DUE) END)  AS OD_BAL_AMT --͸֧���
          ,(CASE WHEN BAL_AMT >=0 then 0 ELSE ABS(BAL_AMT) END )  AS AMT_PNP_ARS   --͸֧����
          ,(CASE WHEN BAL_AMT-ODUE_INT_AMT-FEE_AMT_DUE < 0 THEN BAL_AMT-ODUE_INT_AMT-FEE_AMT_DUE ELSE 0 END) AS OTSND_AMT_RCVB --Ӧ���˿����
          ,(FEE_AMT_DUE)                           AS FEE_RCVB  --Ӧ�շ���
          ,(ODUE_INT_AMT)                          AS INT_RCVB  --Ӧ����Ϣ
          ,TMP_CRED_LMT_AMT                        AS TMP_CRED_LMT        --��ʱ���Ž��
          ,CR_LMT                                      --���Ŷ��    
          ,DEP_ACG_SBJ_ID                 --����Ŀ
          ,OD_ACG_SBJ_ID              --͸֧��Ŀ
                   	 
   		FROM SOR.CC_AC_AR 
      )  DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE
     PARTITIONING KEY(CC_AC_AR_ID) IN TS_USR_TMP32K;  	--
     
 INSERT INTO SESSION.T_CC_AC_AR 
       SELECT  
   				 CC_AC_AR_ID                AS CC_AC_AR_ID
          ,AST_RSK_ASES_RTG_TP_CD     AS AST_RSK_ASES_RTG_TP_CD  --
          ,LN_FR_RSLT_TP_ID           AS LN_FIVE_RTG_STS         --     
          ,BAL_AMT                    AS AC_BAL_AMT              --�˻����
          ,(CASE WHEN BAL_AMT <=0 THEN 0 ELSE BAL_AMT END)  AS DEP_BAL_CRD  --���п������� 
          ,(CASE WHEN BAL_AMT > 0 THEN 0 ELSE ABS(BAL_AMT-ODUE_INT_AMT-FEE_AMT_DUE) END)  AS OD_BAL_AMT --͸֧���
          ,(CASE WHEN BAL_AMT >=0 then 0 ELSE ABS(BAL_AMT) END )  AS AMT_PNP_ARS   --͸֧����
          ,(CASE WHEN BAL_AMT-ODUE_INT_AMT-FEE_AMT_DUE < 0 THEN BAL_AMT-ODUE_INT_AMT-FEE_AMT_DUE ELSE 0 END) AS OTSND_AMT_RCVB --Ӧ���˿����
          ,(FEE_AMT_DUE)                           AS FEE_RCVB  --Ӧ�շ���
          ,(ODUE_INT_AMT)                          AS INT_RCVB  --Ӧ����Ϣ
          ,TMP_CRED_LMT_AMT                        AS TMP_CRED_LMT                 --��ʱ���Ž��
          ,CR_LMT                                               --���Ŷ��    
          ,DEP_ACG_SBJ_ID             --����Ŀ
          ,OD_ACG_SBJ_ID              --͸֧��Ŀ
          	 
   		FROM SOR.CC_AC_AR 
   		-------------------------Start on 20110421--------------------------------------
   		WHERE DEL_F=0
   		-------------------------End on 20110421----------------------------------------
 		
 		;     --
 /* �ռ�������Ϣ */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

-----Start On 20110421---
		SET SMY_STEPNUM = 4 ;--
		SET SMY_STEPDESC = '������ʱ��, ��Ŵ�SOR.CC_INT_RCVB_RGST���ܺ������'; 		 --	
		
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
		SET SMY_STEPDESC = '�����ܱ�CR_CRD_SMY,���뵱�յ�����'; 	 --

  INSERT INTO SMY.CR_CRD_SMY
  
  (
          CRD_NO                  --����                    
         ,CR_CRD_TP_ID            --������                  
         ,CRD_Brand_TP_Id         --��Ʒ������              
         ,CRD_PRVL_TP_ID          --������                  
         ,ENT_IDV_IND             --������                  
         ,MST_CRD_IND             --��/������־             
         ,NGO_CRD_IND             --Э�鿨����              
         ,MULT_CCY_F              --˫�ҿ���־              
         ,AST_RSK_ASES_RTG_TP_CD  --�ʲ����շ���            
         ,LN_FIVE_RTG_STS         --�����弶��̬����        
         ,PD_GRP_CD               --��Ʒ��                  
         ,PD_SUB_CD               --��Ʒ�Ӵ���              
         ,CRD_LCS_TP_ID           --��״̬                  
         ,OU_ID                   --���������              
         ,CCY                     --����                    
         ,ISSU_CRD_OU_Id          --����������              
         ,TMP_CRED_LMT            --��ʱ���Ž��            
         ,CR_LMT                  --���Ŷ��                
         ,AC_AR_Id                --����˺�                
         ,AC_BAL_AMT              --�˻����                
         ,DEP_BAL_CRD             --���п�������          
         ,OD_BAL_AMT              --͸֧���                
         ,AMT_PNP_ARS             --͸֧����                
         ,OTSND_AMT_RCVB          --Ӧ���˿����            
         ,FEE_RCVB                --Ӧ�շ���                
         ,INT_RCVB                --Ӧ����Ϣ                
         ,AMT_RCVD_For_LST_TM_OD  --���ڻ����ϸ���֮ǰ�Ľ��
         ,AMT_RCVD                --�ѻ�����              
         ,EFF_DT                  --����������              
         ,END_DT                  --��������                
         ,CST_ID                  --�ͻ�����                
         ,LST_CST_AVY_DT          --�ͻ��������        
         ,EXP_MTH_YEAR            --��������                
         ,CRD_CHG_DT              --��������                
         ,CRD_DLVD_DT             --��������                
         ,BIZ_CGY_TP_ID           --ҵ�����
         ,CST_NO	                --�ͻ���
         ,CST_NM	                --�ͻ�����
         ,CST_CR_RSK_RTG_ID	      --�ͻ����ŵȼ�               
------------------------------Start of modification on 2009-11-19----------------------------------------------
	       ,AC_RVL_LMT_AC_F         --ѭ���˻���־
	       ,AC_BYND_LMT_F           --�����˻���־
         ,DEP_ACG_SBJ_ID          --����Ŀ
         ,OD_ACG_SBJ_ID           --͸֧��Ŀ
------------------------------End of modification on 2009-11-19----------------------------------------------
----------------------------Start on 2010-05-21--------------------------------------------------
         ,DRMT_CRD_F              --˯�߿���־
----------------------------End on 2010-05-21--------------------------------------------------        
-------------------------Start On 20110421-----------------
         ,INT_RCVB_EXCPT_OFF_BST                          
-------------------------End On 20110421-----------------  
  )
  	SELECT 
          CR_CRD.CC_NO                              --����                        
         ,CR_CRD.CC_TP_ID                           --������                      
         ,CRD.CRD_BRND_TP_ID                        --��Ʒ������                  
         ,CR_CRD.CRD_PRVL_TP_ID                     --������                      
         ,CRD.ENT_IDV_CST_IND                       --������                      
         ,CR_CRD.MST_CRD_IND                        --��/������־                 
         ,CRD.NGO_CRD_IND                           --Э�鿨����                  
         ,CRD.MULTI_CCY_F                           --˫�ҿ���־                  
         ,COALESCE(T_CC_AC_AR.AST_RSK_ASES_RTG_TP_CD,'')         --�ʲ����շ���                
         ,COALESCE(T_CC_AC_AR.LN_FIVE_RTG_STS,-1)               --�����弶��̬����            
         ,CRD.PD_GRP_CD                             --��Ʒ��                      
         ,CRD.PD_SUB_CD                             --��Ʒ�Ӵ���                  
         ,CRD.CRD_LCS_TP_ID                         --��״̬                      
         ,CR_CRD.APL_ACPT_OU_IP_ID                  --���������                  
         ,CR_CRD.PRIM_CCY_ID                        --����                        
         ,CR_CRD.ISSU_CRD_OU_IP_ID                  --����������                  
         ,COALESCE(T_CC_AC_AR.TMP_CRED_LMT,0)                   --��ʱ���Ž��                
         ,COALESCE(T_CC_AC_AR.CR_LMT ,0)                        --���Ŷ��                    
         ,CRD.AC_AR_ID                              --����˺�                    
         ,COALESCE(T_CC_AC_AR.AC_BAL_AMT               ,0)      --�˻����                    
         ,COALESCE(T_CC_AC_AR.DEP_BAL_CRD              ,0)      --���п�������              
         ,COALESCE(T_CC_AC_AR.OD_BAL_AMT               ,0)      --͸֧���                    
         ,COALESCE(T_CC_AC_AR.AMT_PNP_ARS              ,0)      --͸֧����                    
         ,COALESCE(T_CC_AC_AR.OTSND_AMT_RCVB           ,0)      --Ӧ���˿����                
         ,COALESCE(T_CC_AC_AR.FEE_RCVB                 ,0)      --Ӧ�շ���                    
         ,COALESCE(T_CC_AC_AR.INT_RCVB                 ,0)      --Ӧ����Ϣ                    
         ,COALESCE(T_REPYMT_AMT.AMT_RCVD_FOR_LST_TM_OD ,0)      --���ڻ����ϸ���֮ǰ�Ľ��    
         ,COALESCE(T_REPYMT_AMT.AMT_RCVD               ,0)      --�ѻ�����                  
         ,CRD.EFF_DT                                --����������                  
         ,CRD.END_DT                                --��������                    
         ,CRD.PRIM_CST_ID                           --�ͻ�����                    
         ,CRD.LAST_CST_AVY_DT                       --�ͻ��������            
         ,CR_CRD.EXP_MTH_YEAR                       --��������                    
         ,CR_CRD.CRD_CHG_DT                         --��������                    
         ,CR_CRD.CRD_DLVD_DT                        --��������                    
         ,CRD.BIZ_TP_ID                             --ҵ�����                    
         ,COALESCE(CST_INF.CST_NO ,'')                           --�ͻ���                      
         ,COALESCE(CST_INF.CST_NM ,'')                           --�ͻ�����                    
         ,COALESCE(CST_INF.CST_CR_RSK_RTG_ID,-1)               --�ͻ����ŵȼ�                
------------------------------Start of modification on 2009-11-19----------------------------------------------
	       ,COALESCE(CC_AC_SMY.RVL_LMT_AC_F     , -1)       --ѭ���˻���־
         ,COALESCE(CC_AC_SMY.BYND_LMT_F       , -1)      --�����˻���־
         ,COALESCE(T_CC_AC_AR.DEP_ACG_SBJ_ID  , '')       --����Ŀ
         ,COALESCE(T_CC_AC_AR.OD_ACG_SBJ_ID   , '')      --͸֧��Ŀ
------------------------------End of modification on 2009-11-19----------------------------------------------
----------------------------Start on 2010-05-21--------------------------------------------------
         ,case when DAYS(SMY_DATE) - DAYS(LAST_CST_AVY_DT) >= 180   				
  									and DAYS(SMY_DATE) - DAYS(CRD.EFF_DT) >= 180
  									and DAYS(SMY_DATE) - DAYS(CRD_DLVD_DT) >= 180   
  									and CRD_LCS_TP_ID in ( 11920001 --����
  																				,11920002 --�·���δ����
  																				,11920003 --�»���δ����
  																			 )
  			  then 1 else 0 end as DRMT_CRD_F              --˯�߿���־
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
 /* �ռ�������Ϣ */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
	 
	 COMMIT;--
END@