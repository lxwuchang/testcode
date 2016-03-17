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
-- 2009-11-24   JAMES SHANG     �����±���		
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
-- 2011-05-31   Chen XiaoWen    1��������ʱ��TMP�ķ�����
--                              2�������ʱ��T_CUR_TMP�Ȼ����������,�������ʱ��group by
--                              3���޸��±�����߼�
-- 2011-09-06   Li ShenYu       Add PD_UN_CODE for test environment only
-- 2012-02-28   Chen XiaoWen    1��ȥ����ʱ��TMP_LOAN_AR_SMY,ֱ�Ӵ�ԭ���ȡ
--                              2���޸�LN_AR_INT_MTHLY_SMY��ѯ����,��Ϊʹ��ACG_DT������
--                              3��������ʱ��T_CUR_TMP����
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
		DECLARE CUR_QTR SMALLINT;    --
		-- ���������µ����һ��
		DECLARE MTH_LAST_DAY DATE; --
	
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
      SET SMY_STEPNUM = SMY_STEPNUM + 1;--
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
      COMMIT;--
      RESIGNAL;--
    END;--
    

   /*������ֵ*/
    SET SMY_PROCNM  ='PROC_OU_LN_DLY_SMY';--
    SET SMY_DATE    =ACCOUNTING_DATE;    --
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --ȡ��ǰ���
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --ȡ��ǰ�·�
    SET CUR_DAY     =DAY(ACCOUNTING_DATE);     --ȡ�µڼ���
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- ȡ�����
    SET C_YR_DAY      =DAYOFYEAR(ACCOUNTING_DATE); --ȡ����ڼ���
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --��ǰ����
    SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,ISO),1,7)||'-01'); --ȡ���³���
    
    VALUES(ACCOUNTING_DATE - 1 DAY) INTO LAST_SMY_DATE;--
    VALUES(MTH_FIRST_DAY + 1 MONTH - 1 DAY) INTO MTH_LAST_DAY ;      --
    --��������������
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

  /*ȡ������������*/ 
  	SET C_QTR_DAY = DAYS(SMY_DATE) - DAYS(QTR_FIRST_DAY) + 1 ;--
		
--------------------------start on 2010-08-09------------------------------------------------------------------------	
		SET YR_DAY=DAYS(ACCOUNTING_DATE)-DAYS(YR_FIRST_DAY)+1;    --��������
		SET QTR_DAY=DAYS(ACCOUNTING_DATE)-DAYS(QTR_FIRST_DAY)+1;    --��������
--------------------------end   on 2010-08-09------------------------------------------------------------------------			
		SELECT COALESCE(MAX(ACG_DT),'1900-01-01') INTO MAX_ACG_DT FROM SMY.OU_LN_DLY_SMY;--

/*Delete��־��,����SMY_PROCNM=��ǰ�洢��������,SMY_DATE=ACCOUNTING_DATE,�������µ���ʼ��־*/
		DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
			COMMIT;--
		
		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		
		SET SMY_STEPDESC = 	'�洢���̿�ʼ����' ;--
		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
				VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);		 --

/*���ݻָ��뱸��*/
    IF MAX_ACG_DT = ACCOUNTING_DATE THEN
       DELETE FROM SMY.OU_LN_DLY_SMY WHERE ACG_DT = ACCOUNTING_DATE ;       --
       COMMIT;--
    END IF;--
/*�±�Ļָ�*/
   IF ACCOUNTING_DATE = MTH_LAST_DAY THEN 
   		DELETE FROM SMY.OU_LN_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = CUR_MONTH ;--
   		COMMIT;--
   	END IF;    --
   

--SET SMY_STEPNUM = 2 ;--
SET SMY_STEPDESC = '�����û���ʱ��,�������SMY����';--

	/*�����û���ʱ��*/
	
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
					,CST_Area_LVL1_TP_Id               --�ͻ���������1
					,CST_Area_LVL2_TP_Id               --�ͻ���������2
					,CST_Area_LVL3_TP_Id               --�ͻ���������3          
          ,ENT_IDV_IND  										 --������ҵ��־                  
          ,LN_INVST_DIRC_TP_ID               --��ҵͶ��
     FROM SMY.OU_LN_DLY_SMY
		)
	definition only ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(ACG_OU_IP_ID,ACG_SBJ_ID);--

 /*��������һ�ղ���Ҫ����*/

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
					,CST_Area_LVL1_TP_Id               --�ͻ���������1
					,CST_Area_LVL2_TP_Id               --�ͻ���������2
					,CST_Area_LVL3_TP_Id               --�ͻ���������3
          ----------------------End on 2009-11-30-----------------------------------------------
          ----------------------Start on 2009-1-30-----------------------------------------------
          ,ENT_IDV_IND  										 --������ҵ��־
          ----------------------End on 2009-11-30-----------------------------------------------
          ,LN_INVST_DIRC_TP_ID               --��ҵͶ��
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
					,CST_Area_LVL1_TP_Id               --�ͻ���������1
					,CST_Area_LVL2_TP_Id               --�ͻ���������2
					,CST_Area_LVL3_TP_Id               --�ͻ���������3
          ----------------------End on 2009-11-30-----------------------------------------------          
          ----------------------Start on 2009-1-30-----------------------------------------------
          ,ENT_IDV_IND  										 --������ҵ��־
          ----------------------End on 2009-11-30-----------------------------------------------          
          ,LN_INVST_DIRC_TP_ID               --��ҵͶ��
     FROM SMY.OU_LN_DLY_SMY WHERE ACG_DT = LAST_SMY_DATE ;  --
     
   END IF;     --
      
      
 /** �ռ�������Ϣ */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	SET SMY_STEPNUM = SMY_STEPNUM + 1;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	           --

CREATE INDEX SESSION.IDX_TMP ON SESSION.TMP(ACG_OU_IP_ID,ACG_SBJ_ID,LN_CTR_TP_ID,LN_GNT_TP_ID,LN_PPS_TP_ID);
	
		SET SMY_STEPDESC = '�����û���ʱ���Ż������Ϊ���������';--
		
		
DECLARE GLOBAL TEMPORARY TABLE T_CUR AS 
 (		
					SELECT                                                                                                         																					
               SMY_LOAN_AR.RPRG_OU_IP_ID         AS ACG_OU_IP_ID     --�������
              ,SMY_LOAN_AR.ACG_SBJ_ID            AS ACG_SBJ_ID       --����Ŀ(������)
              ---------------------------Start of 20091127--------------------------------------------
              ,SMY_LOAN_AR.NEW_ACG_SBJ_ID        AS NEW_ACG_SBJ_ID   --�¿�Ŀ
              ---------------------------End of 20091127--------------------------------------------
              ,SMY_LOAN_AR.LN_CGY_TP_ID          AS LN_CTR_TP_ID     --ҵ��Ʒ��
              ,SMY_LOAN_AR.CLT_TP_ID             AS LN_GNT_TP_ID     --�������ʽ
              ,SMY_LOAN_AR.LN_PPS_TP_ID          AS LN_PPS_TP_ID     --������;����
              ,SMY_LOAN_AR.FND_SRC_DST_TP_ID     AS FND_SRC_TP_ID    --�ʽ���Դ
              ,SMY_LOAN_AR.LN_TERM_TP_ID         AS LN_TERM_TP_ID     --������������
              ,SMY_LOAN_AR.TM_MAT_SEG_ID         AS TM_MAT_SEG_ID   --������������
              --,SMY_CST_INF.ENT_IDV_IND           AS LN_CST_TP_ID           --����ͻ�����
              ,SMY_LOAN_AR.ALT_TP_ID           AS LN_CST_TP_ID           --����ͻ�����
              ,SMY_CST_INF.FARMER_TP_ID          AS Farmer_TP_Id           --ũ�����
              ,SMY_LOAN_AR.AR_LCS_TP_ID          AS LN_LCS_STS_TP_ID       --������������
              ,SMY_LOAN_AR.CNRL_BNK_IDY_CL_ID    AS IDY_CL_ID              --��ҵ����
              ,SMY_LOAN_AR.LN_FR_RSLT_TP_ID      AS LN_FIVE_RTG_STS        --��/�弶��̬����
              ,SMY_LOAN_AR.AR_FNC_ST_TP_ID       AS LN_FNC_STS_TP_ID       --�����ļ���̬����
              ,SMY_CST_INF.CST_CR_RSK_RTG_ID     AS CST_CR_RSK_RTG_ID      --�ͻ����ŵȼ�
              ,SMY_LOAN_AR.PD_GRP_CODE           AS PD_GRP_CD              --��Ʒ�����
              ,SMY_LOAN_AR.PD_SUB_CODE           AS PD_SUB_CD              --��Ʒ�ִ���
              ,SMY_CST_INF.ORG_SCALE_TP_ID       AS CST_Scale_TP_Id        --��ҵ��ģ����
              ,SMY_LOAN_AR.DNMN_CCY_ID           AS CCY                    --����
              ,SMY_LOAN_AR.PD_UN_CODE            AS PD_UN_CODE             --��Ʒ����
              ,(SMY_LOAN_AR.LN_BAL)              AS LN_BAL                 --�������
              ,1         AS NBR_CST           --�ͻ�����
              ,1           AS NBR_AC             --�˻���
              ,1           AS NBR_NEW_AC    --�����˻���
              ,1             AS NBR_NEW_CST   --�����ͻ���
              ,1            AS NBR_AC_CLS    --���������˻���  13360005:����
              ,SMY_LN_AR_INT_MTHLY.YTD_ON_BST_INT_AMT_RCVD   AS YTD_ON_BST_INT_AMT_RCVD         --����ʵ����Ϣ            
              ,SMY_LN_AR_INT_MTHLY.YTD_OFF_BST_INT_AMT_RCVD   AS YTD_OFF_BST_INT_AMT_RCVD        --����ʵ����Ϣ                       
              ,SMY_LN_AR_INT_MTHLY.TOT_YTD_AMT_OF_INT_INCM   AS TOT_YTD_AMT_OF_INT_INCM        --��Ϣ����            
              ,SMY_LN_AR_INT_MTHLY.ON_BST_INT_RCVB        AS ON_BST_INT_RCVB             --����Ӧ��δ����Ϣ        
              ,SMY_LN_AR_INT_MTHLY.OFF_BST_INT_RCVB       AS OFF_BST_INT_RCVB            --����Ӧ��δ����Ϣ
              ,(CASE WHEN SMY_LN_AR_INT_MTHLY.LN_DRDWN_AMT > 0 THEN 1 ELSE 0 END )  AS NBR_LN_DRDWN_AC        --�����۷��˻���
              ,SMY_LN_AR_INT_MTHLY.TOT_MTD_LN_DRDWN_AMT             AS TOT_MTD_LN_DRDWN_AMT           --�´����ۼƷ��Ž��
              ,SMY_LN_AR_INT_MTHLY.TOT_MTD_AMT_LN_REPYMT_RCVD       AS TOT_MTD_AMT_LN_REPYMT_RCVD     --���ۼ��ջش�����
              ,(CASE WHEN SMY_LN_AR_INT_MTHLY.AMT_LN_REPYMT_RCVD > 0 THEN 1 ELSE 0 END )  AS NBR_LN_REPYMT_RCVD_AC  --���������˻���                     
              ,SMY_LN_AR_INT_MTHLY.TOT_QTD_LN_DRDWN_AMT             AS TOT_QTD_LN_DRDWN_AMT           --���ȴ����ۼƷ��Ž��
              ,SMY_LN_AR_INT_MTHLY.TOT_QTD_AMT_LN_RPYMT_RCVD        AS TOT_QTD_AMT_LN_RPYMT_RCVD      --�����ۼ��ջش�����
              ,SMY_LN_AR_INT_MTHLY.TOT_YTD_LN_DRDWN_AMT             AS TOT_YTD_LN_DRDWN_AMT           --��ȴ����ۼƷ��Ž��
              ,SMY_LN_AR_INT_MTHLY.TOT_YTD_AMT_LN_REPYMT_RCVD       AS TOT_YTD_AMT_LN_REPYMT_RCVD     --����ۼ��ջش�����                  
              ,SMY_LN_AR_INT_MTHLY.CUR_CR_AMT             AS CUR_CR_AMT    			--����������      
              ,SMY_LN_AR_INT_MTHLY.CUR_DB_AMT             AS CUR_DB_AMT         --�跽������      
              ,SMY_LN_AR_INT_MTHLY.TOT_MTD_CR_AMT         AS TOT_MTD_CR_AMT     --���ۼƴ���������
              ,SMY_LN_AR_INT_MTHLY.TOT_MTD_DB_AMT         AS TOT_MTD_DB_AMT     --���ۼƽ跽������
              ,SMY_LN_AR_INT_MTHLY.TOT_QTD_DB_AMT         AS TOT_QTD_DB_AMT     --���ۼƴ���������
              ,SMY_LN_AR_INT_MTHLY.TOT_QTD_CR_AMT         AS TOT_QTD_CR_AMT     --���ۼƽ跽������
              ,SMY_LN_AR_INT_MTHLY.TOT_YTD_CR_AMT         AS TOT_YTD_CR_AMT     --���ۼƴ���������
              ,SMY_LN_AR_INT_MTHLY.TOT_YTD_DB_AMT         AS TOT_YTD_DB_AMT     --���ۼƽ跽������
              ,SMY_LN_AR_INT_MTHLY.OFF_BST_INT_RCVB_WRTOF          AS  OFF_BST_INT_RCVB_WRTOF           --����Ӧ����Ϣ�������
              ,SMY_LN_AR_INT_MTHLY.OFF_BST_INT_RCVB_RPLC	         AS  OFF_BST_INT_RCVB_RPLC	           --����Ӧ����Ϣ�û����
              ,SMY_LN_AR_INT_MTHLY.TOT_YTD_INT_INCM_AMT_DEBT_AST	 AS  TOT_YTD_INT_INCM_AMT_DEBT_AST		 --��ծ�ʲ���ծ��Ϣ����
					    ,SMY_LN_AR_INT_MTHLY.TOT_YTD_INT_INCM_RTND_WRTOF_LN	 AS  TOT_YTD_INT_INCM_RTND_WRTOF_LN   --���������ջ���Ϣ
              ,SMY_LN_AR_INT_MTHLY.TOT_MTD_NBR_LN_DRDWNTXN         AS  TOT_MTD_NBR_LN_DRDWNTXN     --���ۼƷ��Ŵ������
              ,SMY_LN_AR_INT_MTHLY.TOT_QTD_NBR_LN_DRDWN_TXN        AS  TOT_QTD_NBR_LN_DRDWN_TXN    --���ۼƷ��Ŵ������
              ,SMY_LN_AR_INT_MTHLY.TOT_YTD_NBR_LN_DRDWN_TXN        AS  TOT_YTD_NBR_LN_DRDWN_TXN    --���ۼƷ��Ŵ������
              ,SMY_LN_AR_INT_MTHLY.AMT_LN_REPYMT_RCVD  					   AS  AMT_LN_REPYMT_RCVD  			               
              ----------------------Start on 2009-1-30-----------------------------------------------
							,VALUE(SMY_CST_INF.Area_LVL1_TP_Id, -1)             AS CST_Area_LVL1_TP_Id   --�ͻ���������1
							,VALUE(SMY_CST_INF.Area_LVL2_TP_Id, -1)             AS CST_Area_LVL2_TP_Id   --�ͻ���������2
							,VALUE(SMY_CST_INF.Area_LVL3_TP_Id, -1)             AS CST_Area_LVL3_TP_Id   --�ͻ���������3
		          ----------------------End on 2009-11-30-----------------------------------------------
		          ----------------------Start on 2009-1-30-----------------------------------------------
		          ,SMY_LOAN_AR.ENT_IDV_IND  									AS ENT_IDV_IND	 --������ҵ��־
		          ----------------------End on 2009-11-30-----------------------------------------------
		          ,SMY_LOAN_AR.LN_INVST_DIRC_TP_ID               --��ҵͶ��
		          ,1                                          AS CUR_AR_FLAG --�˻��Ƿ�����
		          ,SMY_LN_AR_INT_MTHLY.LN_DRDWN_AMT AS LN_DRDWN_AMT
							,MTD_ACML_BAL_AMT
							,QTD_ACML_BAL_AMT
							,YTD_ACML_BAL_AMT
		          ,1 TOT_MTD_NBR_NEW_CST                  --�����ۼ������ͻ��� 
		          ,1 TOT_MTD_NBR_AC_CLS                   --�����ۼ������˻���                   
		          ,1 TOT_MTD_NBR_LN_DRDWN_AC              --�¶��۷��˻���     
		          ,1 TOT_MTD_NBR_LN_REPYMT_RCVD_AC        --�¶������˻��� 
		          ,1 TOT_MTD_NBR_NEW_AC                   --�¶������˻���   
		          ,1 TOT_QTD_NBR_NEW_CST                  --�����ۼ������ͻ���               
		          ,1 TOT_QTD_NBR_AC_CLS                   --�����ۼ������˻���               
		          ,1 TOT_QTD_NBR_LN_DRDWN_AC              --�����۷��˻���                                                       
		          ,1 TOT_QTD_NBR_LN_REPYMT_RCVD_AC        --���������˻���  
		          ,1 TOT_QTD_NBR_NEW_AC                   --���������˻���                                       
		          ,1 TOT_YTD_NBR_NEW_CST                  --�����ۼ������ͻ���   
		          ,1 TOT_YTD_NBR_AC_CLS                   --�����ۼ������˻���   
		          ,1 TOT_YTD_NBR_LN_DRDWN_AC              --�¶��۷��˻���             
		          ,1 TOT_YTD_NBR_LN_REPYMT_RCVD_AC        --�¶������˻���       
		          ,1 TOT_YTD_NBR_NEW_AC                   --��������˻���      
        FROM   
														SMY.LOAN_AR_SMY              AS SMY_LOAN_AR
            INNER JOIN SMY.LN_AR_INT_MTHLY_SMY      AS SMY_LN_AR_INT_MTHLY ON SMY_LOAN_AR.LN_AR_ID     = SMY_LN_AR_INT_MTHLY.LN_AR_ID
        		LEFT OUTER JOIN SMY.CST_INF                  AS SMY_CST_INF   	    ON SMY_LOAN_AR.PRIM_CST_ID 	= SMY_CST_INF.CST_ID 														                  
) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE
     IN TS_USR_TMP32K 
     PARTITIONING KEY(ACG_OU_IP_ID,ACG_SBJ_ID) ;--

-----------------------------------------Start on 2010-01-26---------------------------------------------
/*����LOAN_AR_SMY ��ʱ�������*/
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
               SMY_LOAN_AR.RPRG_OU_IP_ID                       AS ACG_OU_IP_ID     --�������
              ,SMY_LOAN_AR.ACG_SBJ_ID                          AS ACG_SBJ_ID       --����Ŀ(������)
              ,VALUE(SMY_LOAN_AR.NEW_ACG_SBJ_ID,'')        							 AS NEW_ACG_SBJ_ID   --�¿�Ŀ
              ,SMY_LOAN_AR.LN_CGY_TP_ID                        AS LN_CTR_TP_ID     --ҵ��Ʒ��
              ,SMY_LOAN_AR.CLT_TP_ID                           AS LN_GNT_TP_ID     --�������ʽ
              ,SMY_LOAN_AR.LN_PPS_TP_ID                        AS LN_PPS_TP_ID     --������;����
              ,SMY_LOAN_AR.FND_SRC_DST_TP_ID                   AS FND_SRC_TP_ID    --�ʽ���Դ
              ,SMY_LOAN_AR.LN_TERM_TP_ID                       AS LN_TERM_TP_ID     --������������
              ,COALESCE(SMY_LOAN_AR.TM_MAT_SEG_ID,-1)                      AS TM_MAT_SEG_ID   --������������
              ,COALESCE(SMY_LOAN_AR.ALT_TP_ID  ,-1)         AS LN_CST_TP_ID           --����ͻ�����
              ,COALESCE(SMY_CST_INF.FARMER_TP_ID ,-1)         AS Farmer_TP_Id           --ũ�����
              ,COALESCE(SMY_LOAN_AR.AR_LCS_TP_ID          ,-1)             AS LN_LCS_STS_TP_ID       --������������
              ,COALESCE(SMY_LOAN_AR.CNRL_BNK_IDY_CL_ID    ,-1)             AS IDY_CL_ID              --��ҵ����
              ,COALESCE(SMY_LOAN_AR.LN_FR_RSLT_TP_ID      ,-1)             AS LN_FIVE_RTG_STS        --��/�弶��̬����
              ,COALESCE(SMY_LOAN_AR.AR_FNC_ST_TP_ID       ,-1)             AS LN_FNC_STS_TP_ID       --�����ļ���̬����
              ,COALESCE(SMY_CST_INF.CST_CR_RSK_RTG_ID ,-1)     AS CST_CR_RSK_RTG_ID      --�ͻ����ŵȼ�
              ,COALESCE(SMY_LOAN_AR.PD_GRP_CODE  ,' ')                       AS PD_GRP_CD              --��Ʒ�����
              ,COALESCE(SMY_LOAN_AR.PD_SUB_CODE  ,' ')                       AS PD_SUB_CD              --��Ʒ�ִ���
              ,COALESCE(SMY_CST_INF.ORG_SCALE_TP_ID,-1)       AS CST_SCALE_TP_ID        --��ҵ��ģ����
              ,SMY_LOAN_AR.DNMN_CCY_ID                      AS CCY                    --����
              ,COALESCE(SMY_LOAN_AR.PD_UN_CODE ,' ')       AS PD_UN_CODE             --��Ʒ����
              ,SMY_LOAN_AR.LN_BAL                        AS LN_BAL                 --�������
              ,SMY_LOAN_AR.PRIM_CST_ID               AS NBR_CST           --�ͻ�����
              ,case when SMY_LOAN_AR.AR_LCS_TP_ID = 13360003 then 1 else 0 end AS NBR_AC            --�˻���
              ,CASE WHEN SMY_LOAN_AR.LN_DRDWN_DT='1900-01-01' THEN SMY_LOAN_AR.LN_AR_ID ELSE '0' END AS NBR_NEW_AC --�����˻���
              ,CASE WHEN SMY_CST_INF.EFF_CST_DT='1900-01-01' THEN  COALESCE(SMY_CST_INF.CST_ID,'0') ELSE '0' END AS NBR_NEW_CST   --�����ͻ���
              ,CASE WHEN SMY_LOAN_AR.END_DT='1900-01-01' AND SMY_LOAN_AR.AR_LCS_TP_ID=13360005 THEN 1 ELSE 0 END AS NBR_AC_CLS --���������˻���13360005:����
              ,COALESCE(SMY_LN_AR_INT_MTHLY.YTD_ON_BST_INT_AMT_RCVD  ,0)  AS YTD_ON_BST_INT_AMT_RCVD --����ʵ����Ϣ            
              ,COALESCE(SMY_LN_AR_INT_MTHLY.YTD_OFF_BST_INT_AMT_RCVD ,0)  AS YTD_OFF_BST_INT_AMT_RCVD        --����ʵ����Ϣ                       
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_AMT_OF_INT_INCM  ,0)  AS TOT_YTD_AMT_OF_INT_INCM        --��Ϣ����            
              ,COALESCE(SMY_LN_AR_INT_MTHLY.ON_BST_INT_RCVB          ,0)  AS ON_BST_INT_RCVB             --����Ӧ��δ����Ϣ        
              ,COALESCE(SMY_LN_AR_INT_MTHLY.OFF_BST_INT_RCVB         ,0)  AS OFF_BST_INT_RCVB            --����Ӧ��δ����Ϣ
              ,CASE WHEN COALESCE(SMY_LN_AR_INT_MTHLY.LN_DRDWN_AMT,0) > 0 THEN 1 ELSE 0 END AS NBR_LN_DRDWN_AC        --�����۷��˻���
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_MTD_LN_DRDWN_AMT ,0)            AS TOT_MTD_LN_DRDWN_AMT           --�´����ۼƷ��Ž��
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_MTD_AMT_LN_REPYMT_RCVD ,0)      AS TOT_MTD_AMT_LN_REPYMT_RCVD     --���ۼ��ջش�����
              ,CASE WHEN COALESCE(SMY_LN_AR_INT_MTHLY.AMT_LN_REPYMT_RCVD,0) > 0 THEN 1 ELSE 0 END   AS NBR_LN_REPYMT_RCVD_AC  --���������˻���                     
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_QTD_LN_DRDWN_AMT          ,0)  AS TOT_QTD_LN_DRDWN_AMT           --���ȴ����ۼƷ��Ž��
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_QTD_AMT_LN_RPYMT_RCVD     ,0)  AS TOT_QTD_AMT_LN_RPYMT_RCVD      --�����ۼ��ջش�����
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_LN_DRDWN_AMT          ,0)  AS TOT_YTD_LN_DRDWN_AMT           --��ȴ����ۼƷ��Ž��
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_AMT_LN_REPYMT_RCVD    ,0)  AS TOT_YTD_AMT_LN_REPYMT_RCVD     --����ۼ��ջش�����                  
              ,COALESCE(SMY_LN_AR_INT_MTHLY.CUR_CR_AMT                    ,0)  AS CUR_CR_AMT    			--����������      
              ,COALESCE(SMY_LN_AR_INT_MTHLY.CUR_DB_AMT                    ,0)  AS CUR_DB_AMT         --�跽������      
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_MTD_CR_AMT                ,0)  AS TOT_MTD_CR_AMT     --���ۼƴ���������
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_MTD_DB_AMT                ,0)  AS TOT_MTD_DB_AMT     --���ۼƽ跽������
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_QTD_DB_AMT                ,0)  AS TOT_QTD_DB_AMT     --���ۼƴ���������
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_QTD_CR_AMT                ,0)  AS TOT_QTD_CR_AMT     --���ۼƽ跽������
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_CR_AMT                ,0)  AS TOT_YTD_CR_AMT     --���ۼƴ���������
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_DB_AMT                ,0)  AS TOT_YTD_DB_AMT     --���ۼƽ跽������
              ,COALESCE(SMY_LN_AR_INT_MTHLY.OFF_BST_INT_RCVB_WRTOF        ,0) AS  OFF_BST_INT_RCVB_WRTOF           --����Ӧ����Ϣ�������
              ,COALESCE(SMY_LN_AR_INT_MTHLY.OFF_BST_INT_RCVB_RPLC	        ,0) AS  OFF_BST_INT_RCVB_RPLC	           --����Ӧ����Ϣ�û����
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_INT_INCM_AMT_DEBT_AST	,0) AS  TOT_YTD_INT_INCM_AMT_DEBT_AST		 --��ծ�ʲ���ծ��Ϣ����
					    ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_INT_INCM_RTND_WRTOF_LN,0) AS  TOT_YTD_INT_INCM_RTND_WRTOF_LN   --���������ջ���Ϣ
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_MTD_NBR_LN_DRDWNTXN       ,0) AS  TOT_MTD_NBR_LN_DRDWNTXN     --���ۼƷ��Ŵ������
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_QTD_NBR_LN_DRDWN_TXN      ,0) AS  TOT_QTD_NBR_LN_DRDWN_TXN    --���ۼƷ��Ŵ������
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_NBR_LN_DRDWN_TXN      ,0) AS  TOT_YTD_NBR_LN_DRDWN_TXN    --���ۼƷ��Ŵ������
              ,COALESCE(SMY_LN_AR_INT_MTHLY.AMT_LN_REPYMT_RCVD  					,0) AS  AMT_LN_REPYMT_RCVD  			               
							,VALUE(SMY_CST_INF.Area_LVL1_TP_Id, -1)             AS CST_Area_LVL1_TP_Id   --�ͻ���������1
							,VALUE(SMY_CST_INF.Area_LVL2_TP_Id, -1)             AS CST_Area_LVL2_TP_Id   --�ͻ���������2
							,VALUE(SMY_CST_INF.Area_LVL3_TP_Id, -1)             AS CST_Area_LVL3_TP_Id   --�ͻ���������3
		          ,SMY_LOAN_AR.ENT_IDV_IND  									AS ENT_IDV_IND	 --������ҵ��־
		          ,SMY_LOAN_AR.LN_INVST_DIRC_TP_ID AS LN_INVST_DIRC_TP_ID               --��ҵͶ��
		          ,case when SMY_LOAN_AR.AR_LCS_TP_ID = 13360003 then 1 else 0 end AS CUR_AR_FLAG --�˻��Ƿ�����
		          ,COALESCE(SMY_LN_AR_INT_MTHLY.LN_DRDWN_AMT  					,0)   AS LN_DRDWN_AMT
							,COALESCE(SMY_LN_AR_INT_MTHLY.MTD_ACML_BAL_AMT  ,0) AS MTD_ACML_BAL_AMT
							,COALESCE(SMY_LN_AR_INT_MTHLY.QTD_ACML_BAL_AMT  ,0) AS QTD_ACML_BAL_AMT
							,COALESCE(SMY_LN_AR_INT_MTHLY.YTD_ACML_BAL_AMT  ,0) AS YTD_ACML_BAL_AMT
		          ,CASE WHEN year(SMY_CST_INF.EFF_CST_DT)=2011 and month(SMY_CST_INF.EFF_CST_DT)= 5
		                    THEN  COALESCE(SMY_CST_INF.CST_ID,'0') 
		                    ELSE '0' END AS TOT_MTD_NBR_NEW_CST                  --�����ۼ������ͻ��� 
		          ,CASE WHEN year(SMY_LOAN_AR.END_DT)=2011 and month(SMY_LOAN_AR.END_DT) = 5  
		                          AND 
		                          SMY_LOAN_AR.AR_LCS_TP_ID= 13360005 
		                          THEN 1 ELSE 0 END AS TOT_MTD_NBR_AC_CLS                   --�����ۼ������˻��� 
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_MTD_LN_DRDWN_AMT > 0 
		                         THEN 1 ELSE 0 END AS TOT_MTD_NBR_LN_DRDWN_AC              --�¶��۷��˻���   
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_MTD_AMT_LN_REPYMT_RCVD > 0 
		                         THEN 1 ELSE 0 END AS TOT_MTD_NBR_LN_REPYMT_RCVD_AC        --�¶������˻��� 
		          ,CASE WHEN year(SMY_LOAN_AR.LN_DRDWN_DT)=2011 and month(SMY_LOAN_AR.LN_DRDWN_DT) = 5 
		                         THEN 1 ELSE 0 END AS TOT_MTD_NBR_NEW_AC                   --�¶������˻��� 
		          ,CASE WHEN year(SMY_CST_INF.EFF_CST_DT)=2011 and quarter(SMY_CST_INF.EFF_CST_DT)= 2
		                    THEN  COALESCE(SMY_CST_INF.CST_ID,'0') 
		                    ELSE '0' END AS TOT_QTD_NBR_NEW_CST                  --�����ۼ������ͻ��� 
		          ,CASE WHEN year(SMY_LOAN_AR.END_DT)=5 and quarter(SMY_LOAN_AR.END_DT) = 2  
		                          AND 
		                          SMY_LOAN_AR.AR_LCS_TP_ID= 13360005 
		                          THEN 1 ELSE 0 END AS TOT_QTD_NBR_AC_CLS                   --�����ۼ������˻���
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_QTD_LN_DRDWN_AMT > 0 
		                         THEN 1 ELSE 0 END AS TOT_QTD_NBR_LN_DRDWN_AC              --�����۷��˻���  
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_QTD_AMT_LN_RPYMT_RCVD > 0 
		                         THEN 1 ELSE 0 END AS TOT_QTD_NBR_LN_REPYMT_RCVD_AC        --���������˻���  
		          ,CASE WHEN year(SMY_LOAN_AR.LN_DRDWN_DT)=2011 and quarter(SMY_LOAN_AR.LN_DRDWN_DT) = 2 
		                         THEN 1 ELSE 0 END AS TOT_QTD_NBR_NEW_AC                   --���������˻���   
		          ,CASE WHEN year(SMY_CST_INF.EFF_CST_DT)= 2011
		                    THEN  COALESCE(SMY_CST_INF.CST_ID,'0') 
		                    ELSE '0' END AS TOT_YTD_NBR_NEW_CST                  --�����ۼ������ͻ���   
		          ,CASE WHEN year(SMY_LOAN_AR.END_DT) = 2011  
		                          AND 
		                          SMY_LOAN_AR.AR_LCS_TP_ID= 13360005 
		                          THEN 1 ELSE 0 END AS TOT_YTD_NBR_AC_CLS                   --�����ۼ������˻���  
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_YTD_LN_DRDWN_AMT > 0 
		                         THEN 1 ELSE 0 END AS TOT_YTD_NBR_LN_DRDWN_AC              --����۷��˻���
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_YTD_AMT_LN_REPYMT_RCVD > 0 
		                         THEN 1 ELSE 0 END AS TOT_YTD_NBR_LN_REPYMT_RCVD_AC        --��������˻���       
		          ,CASE WHEN year(SMY_LOAN_AR.LN_DRDWN_DT) = 2011 
		                         THEN 1 ELSE 0 END AS TOT_YTD_NBR_NEW_AC                   --��������˻��� 							
        FROM SMY.LOAN_AR_SMY AS SMY_LOAN_AR
            INNER JOIN SESSION.TMP_LN_AR_INT_MTHLY_SMY AS SMY_LN_AR_INT_MTHLY ON SMY_LOAN_AR.CTR_AR_ID = SMY_LN_AR_INT_MTHLY.CTR_AR_ID AND SMY_LOAN_AR.CTR_ITM_ORDR_ID = SMY_LN_AR_INT_MTHLY.CTR_ITM_ORDR_ID
        		LEFT OUTER JOIN SMY.CST_INF AS SMY_CST_INF ON SMY_LOAN_AR.PRIM_CST_ID = SMY_CST_INF.CST_ID
    )DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(ACG_OU_IP_ID,ACG_SBJ_ID);
		
		INSERT INTO SESSION.T_CUR_TMP
		SELECT                                                                                                         																					
               SMY_LOAN_AR.RPRG_OU_IP_ID                       AS ACG_OU_IP_ID     --�������
              ,SMY_LOAN_AR.ACG_SBJ_ID                          AS ACG_SBJ_ID       --����Ŀ(������)
              ,VALUE(SMY_LOAN_AR.NEW_ACG_SBJ_ID,'')        							 AS NEW_ACG_SBJ_ID   --�¿�Ŀ
              ,SMY_LOAN_AR.LN_CGY_TP_ID                        AS LN_CTR_TP_ID     --ҵ��Ʒ��
              ,SMY_LOAN_AR.CLT_TP_ID                           AS LN_GNT_TP_ID     --�������ʽ
              ,SMY_LOAN_AR.LN_PPS_TP_ID                        AS LN_PPS_TP_ID     --������;����
              ,SMY_LOAN_AR.FND_SRC_DST_TP_ID                   AS FND_SRC_TP_ID    --�ʽ���Դ
              ,SMY_LOAN_AR.LN_TERM_TP_ID                       AS LN_TERM_TP_ID     --������������
              ,COALESCE(SMY_LOAN_AR.TM_MAT_SEG_ID,-1)                      AS TM_MAT_SEG_ID   --������������
              ,COALESCE(SMY_LOAN_AR.ALT_TP_ID  ,-1)         AS LN_CST_TP_ID           --����ͻ�����
              ,COALESCE(SMY_CST_INF.FARMER_TP_ID ,-1)         AS Farmer_TP_Id           --ũ�����
              ,COALESCE(SMY_LOAN_AR.AR_LCS_TP_ID          ,-1)             AS LN_LCS_STS_TP_ID       --������������
              ,COALESCE(SMY_LOAN_AR.CNRL_BNK_IDY_CL_ID    ,-1)             AS IDY_CL_ID              --��ҵ����
              ,COALESCE(SMY_LOAN_AR.LN_FR_RSLT_TP_ID      ,-1)             AS LN_FIVE_RTG_STS        --��/�弶��̬����
              ,COALESCE(SMY_LOAN_AR.AR_FNC_ST_TP_ID       ,-1)             AS LN_FNC_STS_TP_ID       --�����ļ���̬����
              ,COALESCE(SMY_CST_INF.CST_CR_RSK_RTG_ID ,-1)     AS CST_CR_RSK_RTG_ID      --�ͻ����ŵȼ�
              ,COALESCE(SMY_LOAN_AR.PD_GRP_CODE  ,' ')                       AS PD_GRP_CD              --��Ʒ�����
              ,COALESCE(SMY_LOAN_AR.PD_SUB_CODE  ,' ')                       AS PD_SUB_CD              --��Ʒ�ִ���
              ,COALESCE(SMY_CST_INF.ORG_SCALE_TP_ID,-1)       AS CST_SCALE_TP_ID        --��ҵ��ģ����
              ,SMY_LOAN_AR.DNMN_CCY_ID                      AS CCY                    --����
              ,COALESCE(SMY_LOAN_AR.PD_UN_CODE  ,' ')        AS PD_UN_CODE             --��Ʒ����
              ,SMY_LOAN_AR.LN_BAL                        AS LN_BAL                 --�������
              ,SMY_LOAN_AR.PRIM_CST_ID               AS NBR_CST           --�ͻ�����
              ,case when SMY_LOAN_AR.AR_LCS_TP_ID = 13360003 then 1 else 0 end AS NBR_AC            --�˻���
              ,CASE WHEN SMY_LOAN_AR.LN_DRDWN_DT=ACCOUNTING_DATE THEN SMY_LOAN_AR.LN_AR_ID ELSE '0' END --�����˻���
              ,CASE WHEN SMY_CST_INF.EFF_CST_DT=ACCOUNTING_DATE THEN COALESCE(SMY_CST_INF.CST_ID,'0') ELSE '0' END --�����ͻ���
              ,CASE WHEN SMY_LOAN_AR.END_DT=ACCOUNTING_DATE AND SMY_LOAN_AR.AR_LCS_TP_ID=13360005 THEN 1 ELSE 0 END --���������˻���13360005:����
              ,COALESCE(SMY_LN_AR_INT_MTHLY.YTD_ON_BST_INT_AMT_RCVD  ,0)  AS YTD_ON_BST_INT_AMT_RCVD --����ʵ����Ϣ            
              ,COALESCE(SMY_LN_AR_INT_MTHLY.YTD_OFF_BST_INT_AMT_RCVD ,0)  AS YTD_OFF_BST_INT_AMT_RCVD        --����ʵ����Ϣ                       
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_AMT_OF_INT_INCM  ,0)  AS TOT_YTD_AMT_OF_INT_INCM        --��Ϣ����            
              ,COALESCE(SMY_LN_AR_INT_MTHLY.ON_BST_INT_RCVB          ,0)  AS ON_BST_INT_RCVB             --����Ӧ��δ����Ϣ        
              ,COALESCE(SMY_LN_AR_INT_MTHLY.OFF_BST_INT_RCVB         ,0)  AS OFF_BST_INT_RCVB            --����Ӧ��δ����Ϣ
              ,CASE WHEN COALESCE(SMY_LN_AR_INT_MTHLY.LN_DRDWN_AMT,0) > 0 THEN 1 ELSE 0 END   AS NBR_LN_DRDWN_AC        --�����۷��˻���
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_MTD_LN_DRDWN_AMT ,0)            AS TOT_MTD_LN_DRDWN_AMT           --�´����ۼƷ��Ž��
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_MTD_AMT_LN_REPYMT_RCVD ,0)      AS TOT_MTD_AMT_LN_REPYMT_RCVD     --���ۼ��ջش�����
              ,CASE WHEN COALESCE(SMY_LN_AR_INT_MTHLY.AMT_LN_REPYMT_RCVD,0) > 0 THEN 1 ELSE 0 END   AS NBR_LN_REPYMT_RCVD_AC  --���������˻���                     
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_QTD_LN_DRDWN_AMT          ,0)  AS TOT_QTD_LN_DRDWN_AMT           --���ȴ����ۼƷ��Ž��
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_QTD_AMT_LN_RPYMT_RCVD     ,0)  AS TOT_QTD_AMT_LN_RPYMT_RCVD      --�����ۼ��ջش�����
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_LN_DRDWN_AMT          ,0)  AS TOT_YTD_LN_DRDWN_AMT           --��ȴ����ۼƷ��Ž��
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_AMT_LN_REPYMT_RCVD    ,0)  AS TOT_YTD_AMT_LN_REPYMT_RCVD     --����ۼ��ջش�����                  
              ,COALESCE(SMY_LN_AR_INT_MTHLY.CUR_CR_AMT                    ,0)  AS CUR_CR_AMT    			--����������      
              ,COALESCE(SMY_LN_AR_INT_MTHLY.CUR_DB_AMT                    ,0)  AS CUR_DB_AMT         --�跽������      
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_MTD_CR_AMT                ,0)  AS TOT_MTD_CR_AMT     --���ۼƴ���������
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_MTD_DB_AMT                ,0)  AS TOT_MTD_DB_AMT     --���ۼƽ跽������
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_QTD_DB_AMT                ,0)  AS TOT_QTD_DB_AMT     --���ۼƴ���������
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_QTD_CR_AMT                ,0)  AS TOT_QTD_CR_AMT     --���ۼƽ跽������
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_CR_AMT                ,0)  AS TOT_YTD_CR_AMT     --���ۼƴ���������
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_DB_AMT                ,0)  AS TOT_YTD_DB_AMT     --���ۼƽ跽������
              ,COALESCE(SMY_LN_AR_INT_MTHLY.OFF_BST_INT_RCVB_WRTOF        ,0) AS  OFF_BST_INT_RCVB_WRTOF           --����Ӧ����Ϣ�������
              ,COALESCE(SMY_LN_AR_INT_MTHLY.OFF_BST_INT_RCVB_RPLC	        ,0) AS  OFF_BST_INT_RCVB_RPLC	           --����Ӧ����Ϣ�û����
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_INT_INCM_AMT_DEBT_AST	,0) AS  TOT_YTD_INT_INCM_AMT_DEBT_AST		 --��ծ�ʲ���ծ��Ϣ����
					    ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_INT_INCM_RTND_WRTOF_LN,0)	 AS  TOT_YTD_INT_INCM_RTND_WRTOF_LN   --���������ջ���Ϣ
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_MTD_NBR_LN_DRDWNTXN       ,0)  AS  TOT_MTD_NBR_LN_DRDWNTXN     --���ۼƷ��Ŵ������
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_QTD_NBR_LN_DRDWN_TXN      ,0)  AS  TOT_QTD_NBR_LN_DRDWN_TXN    --���ۼƷ��Ŵ������
              ,COALESCE(SMY_LN_AR_INT_MTHLY.TOT_YTD_NBR_LN_DRDWN_TXN      ,0)  AS  TOT_YTD_NBR_LN_DRDWN_TXN    --���ۼƷ��Ŵ������
              ,COALESCE(SMY_LN_AR_INT_MTHLY.AMT_LN_REPYMT_RCVD  					,0)  AS  AMT_LN_REPYMT_RCVD  			               
							,VALUE(SMY_CST_INF.Area_LVL1_TP_Id, -1)             AS CST_Area_LVL1_TP_Id   --�ͻ���������1
							,VALUE(SMY_CST_INF.Area_LVL2_TP_Id, -1)             AS CST_Area_LVL2_TP_Id   --�ͻ���������2
							,VALUE(SMY_CST_INF.Area_LVL3_TP_Id, -1)             AS CST_Area_LVL3_TP_Id   --�ͻ���������3
		          ,SMY_LOAN_AR.ENT_IDV_IND  									AS ENT_IDV_IND	 --������ҵ��־
		          ,SMY_LOAN_AR.LN_INVST_DIRC_TP_ID               --��ҵͶ��
		          ,case when SMY_LOAN_AR.AR_LCS_TP_ID = 13360003 then 1 else 0 end --�˻��Ƿ�����
		          ,COALESCE(SMY_LN_AR_INT_MTHLY.LN_DRDWN_AMT,0)   AS LN_DRDWN_AMT
							,COALESCE(SMY_LN_AR_INT_MTHLY.MTD_ACML_BAL_AMT  ,0) AS MTD_ACML_BAL_AMT
							,COALESCE(SMY_LN_AR_INT_MTHLY.QTD_ACML_BAL_AMT  ,0) AS QTD_ACML_BAL_AMT
							,COALESCE(SMY_LN_AR_INT_MTHLY.YTD_ACML_BAL_AMT  ,0) AS YTD_ACML_BAL_AMT
		          ,CASE WHEN year(SMY_CST_INF.EFF_CST_DT)=cur_year and month(SMY_CST_INF.EFF_CST_DT)= cur_month
		                    THEN  COALESCE(SMY_CST_INF.CST_ID,'0') ELSE '0' END --�����ۼ������ͻ��� 
		          ,CASE WHEN year(SMY_LOAN_AR.END_DT)=cur_year and month(SMY_LOAN_AR.END_DT) = cur_month AND SMY_LOAN_AR.AR_LCS_TP_ID= 13360005 
		                    THEN 1 ELSE 0 END --�����ۼ������˻��� 
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_MTD_LN_DRDWN_AMT > 0 
		                    THEN 1 ELSE 0 END --�¶��۷��˻���   
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_MTD_AMT_LN_REPYMT_RCVD > 0 
		                    THEN 1 ELSE 0 END --�¶������˻��� 
		          ,CASE WHEN year(SMY_LOAN_AR.LN_DRDWN_DT)=cur_year and month(SMY_LOAN_AR.LN_DRDWN_DT) = cur_month 
		                    THEN 1 ELSE 0 END --�¶������˻��� 
		          ,CASE WHEN year(SMY_CST_INF.EFF_CST_DT)=cur_year and quarter(SMY_CST_INF.EFF_CST_DT)= cur_qtr
		                    THEN  COALESCE(SMY_CST_INF.CST_ID,'0') ELSE '0' END --�����ۼ������ͻ��� 
		          ,CASE WHEN year(SMY_LOAN_AR.END_DT)=cur_year and quarter(SMY_LOAN_AR.END_DT) = cur_qtr AND SMY_LOAN_AR.AR_LCS_TP_ID= 13360005 
		                    THEN 1 ELSE 0 END --�����ۼ������˻���
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_QTD_LN_DRDWN_AMT > 0 
		                    THEN 1 ELSE 0 END --�����۷��˻���
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_QTD_AMT_LN_RPYMT_RCVD > 0 
		                    THEN 1 ELSE 0 END --���������˻���
		          ,CASE WHEN year(SMY_LOAN_AR.LN_DRDWN_DT)=cur_year and quarter(SMY_LOAN_AR.LN_DRDWN_DT) = cur_qtr 
		                    THEN 1 ELSE 0 END --���������˻���
		          ,CASE WHEN year(SMY_CST_INF.EFF_CST_DT)= cur_year
		                    THEN  COALESCE(SMY_CST_INF.CST_ID,'0') ELSE '0' END --�����ۼ������ͻ���
		          ,CASE WHEN year(SMY_LOAN_AR.END_DT) = cur_year AND SMY_LOAN_AR.AR_LCS_TP_ID= 13360005 
		                    THEN 1 ELSE 0 END --�����ۼ������˻���	 
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_YTD_LN_DRDWN_AMT > 0 
		                    THEN 1 ELSE 0 END --����۷��˻���    	             
		          ,CASE WHEN SMY_LN_AR_INT_MTHLY.TOT_YTD_AMT_LN_REPYMT_RCVD > 0 THEN 1 ELSE 0 END --��������˻���
		          ,CASE WHEN year(SMY_LOAN_AR.LN_DRDWN_DT) = cur_year THEN 1 ELSE 0 END --��������˻���
        FROM SMY.LOAN_AR_SMY AS SMY_LOAN_AR 
        INNER JOIN SESSION.TMP_LN_AR_INT_MTHLY_SMY AS SMY_LN_AR_INT_MTHLY ON SMY_LOAN_AR.CTR_AR_ID = SMY_LN_AR_INT_MTHLY.CTR_AR_ID AND SMY_LOAN_AR.CTR_ITM_ORDR_ID = SMY_LN_AR_INT_MTHLY.CTR_ITM_ORDR_ID 
        LEFT OUTER JOIN SMY.CST_INF AS SMY_CST_INF ON SMY_LOAN_AR.PRIM_CST_ID = SMY_CST_INF.CST_ID
    ;
    
    CREATE INDEX SESSION.T_CUR_TMP_GB ON SESSION.T_CUR_TMP(ACG_OU_IP_ID,ACG_SBJ_ID,NEW_ACG_SBJ_ID,LN_CTR_TP_ID,LN_GNT_TP_ID,LN_PPS_TP_ID,FND_SRC_TP_ID,LN_TERM_TP_ID,TM_MAT_SEG_ID,LN_CST_TP_ID,Farmer_TP_Id,LN_LCS_STS_TP_ID,IDY_CL_ID,LN_FIVE_RTG_STS,LN_FNC_STS_TP_ID,CST_CR_RSK_RTG_ID,PD_GRP_CD,PD_SUB_CD,CST_SCALE_TP_ID,CCY,CST_Area_LVL1_TP_Id,CST_Area_LVL2_TP_Id,CST_Area_LVL3_TP_Id,ENT_IDV_IND,LN_INVST_DIRC_TP_ID,PD_UN_CODE);
		
		INSERT INTO SESSION.T_CUR 
					SELECT                                                                                                         																					
               ACG_OU_IP_ID                              --�������
              ,ACG_SBJ_ID                                --����Ŀ(������)
              ,NEW_ACG_SBJ_ID                            --�¿�Ŀ
              ,LN_CTR_TP_ID                              --ҵ��Ʒ��
              ,LN_GNT_TP_ID                              --�������ʽ
              ,LN_PPS_TP_ID                              --������;����
              ,FND_SRC_TP_ID                             --�ʽ���Դ
              ,LN_TERM_TP_ID                             --������������
              ,TM_MAT_SEG_ID                             --������������
              ,LN_CST_TP_ID                              --����ͻ�����
              ,Farmer_TP_Id                              --ũ�����
              ,LN_LCS_STS_TP_ID                          --������������
              ,IDY_CL_ID                                 --��ҵ����
              ,LN_FIVE_RTG_STS                           --��/�弶��̬����
              ,LN_FNC_STS_TP_ID                          --�����ļ���̬����
              ,CST_CR_RSK_RTG_ID                         --�ͻ����ŵȼ�
              ,PD_GRP_CD                                 --��Ʒ�����
              ,PD_SUB_CD                                 --��Ʒ�ִ���
              ,CST_SCALE_TP_ID                           --��ҵ��ģ����
              ,CCY                                       --����
              ,PD_UN_CODE                                --��Ʒ����
              ,SUM(LN_BAL)                               --�������
              ,COUNT(distinct NBR_CST)                   --�ͻ�����
              ,SUM(NBR_AC)                               --�˻���
              ,COUNT(distinct NBR_NEW_AC) -1             --�����˻���
              ,COUNT(distinct NBR_NEW_CST) -1            --�����ͻ���
              ,SUM(NBR_AC_CLS)                           --���������˻���  13360005:����
              ,SUM(YTD_ON_BST_INT_AMT_RCVD)              --����ʵ����Ϣ            
              ,SUM(YTD_OFF_BST_INT_AMT_RCVD)             --����ʵ����Ϣ                       
              ,SUM(TOT_YTD_AMT_OF_INT_INCM)              --��Ϣ����            
              ,SUM(ON_BST_INT_RCVB)                      --����Ӧ��δ����Ϣ        
              ,SUM(OFF_BST_INT_RCVB)                     --����Ӧ��δ����Ϣ
              ,SUM(NBR_LN_DRDWN_AC)                      --�����۷��˻���
              ,SUM(TOT_MTD_LN_DRDWN_AMT)                 --�´����ۼƷ��Ž��
              ,SUM(TOT_MTD_AMT_LN_REPYMT_RCVD)           --���ۼ��ջش�����
              ,SUM(NBR_LN_REPYMT_RCVD_AC)                --���������˻���                     
              ,SUM(TOT_QTD_LN_DRDWN_AMT)                 --���ȴ����ۼƷ��Ž��
              ,SUM(TOT_QTD_AMT_LN_RPYMT_RCVD)            --�����ۼ��ջش�����
              ,SUM(TOT_YTD_LN_DRDWN_AMT)                 --��ȴ����ۼƷ��Ž��
              ,SUM(TOT_YTD_AMT_LN_REPYMT_RCVD)           --����ۼ��ջش�����                  
              ,SUM(CUR_CR_AMT)     			                 --����������      
              ,SUM(CUR_DB_AMT)                           --�跽������      
              ,SUM(TOT_MTD_CR_AMT)                       --���ۼƴ���������
              ,SUM(TOT_MTD_DB_AMT)                       --���ۼƽ跽������
              ,SUM(TOT_QTD_DB_AMT)                       --���ۼƴ���������
              ,SUM(TOT_QTD_CR_AMT)                       --���ۼƽ跽������
              ,SUM(TOT_YTD_CR_AMT)                       --���ۼƴ���������
              ,SUM(TOT_YTD_DB_AMT)                       --���ۼƽ跽������
              ,SUM(OFF_BST_INT_RCVB_WRTOF)               --����Ӧ����Ϣ�������
              ,SUM(OFF_BST_INT_RCVB_RPLC) 	             --����Ӧ����Ϣ�û����
              ,SUM(TOT_YTD_INT_INCM_AMT_DEBT_AST) 		   --��ծ�ʲ���ծ��Ϣ����
					    ,SUM(TOT_YTD_INT_INCM_RTND_WRTOF_LN)       --���������ջ���Ϣ
              ,SUM(TOT_MTD_NBR_LN_DRDWNTXN)              --���ۼƷ��Ŵ������
              ,SUM(TOT_QTD_NBR_LN_DRDWN_TXN)             --���ۼƷ��Ŵ������
              ,SUM(TOT_YTD_NBR_LN_DRDWN_TXN)             --���ۼƷ��Ŵ������
              ,SUM(AMT_LN_REPYMT_RCVD) 
							,CST_Area_LVL1_TP_Id                       --�ͻ���������1
							,CST_Area_LVL2_TP_Id                       --�ͻ���������2
							,CST_Area_LVL3_TP_Id                       --�ͻ���������3
		          ,ENT_IDV_IND	                             --������ҵ��־
		          ,LN_INVST_DIRC_TP_ID                       --��ҵͶ��
		          ,SUM(CUR_AR_FLAG)                          --�˻��Ƿ�����
		          ,SUM(LN_DRDWN_AMT)
							,SUM(MTD_ACML_BAL_AMT)
							,SUM(QTD_ACML_BAL_AMT)
							,SUM(YTD_ACML_BAL_AMT)
		          ,COUNT(distinct TOT_MTD_NBR_NEW_CST) -1    --�����ۼ������ͻ��� 
		          ,SUM(TOT_MTD_NBR_AC_CLS)                   --�����ۼ������˻��� 
		          ,SUM(TOT_MTD_NBR_LN_DRDWN_AC)              --�¶��۷��˻���   
		          ,SUM(TOT_MTD_NBR_LN_REPYMT_RCVD_AC)        --�¶������˻��� 
		          ,SUM(TOT_MTD_NBR_NEW_AC)                   --�¶������˻��� 
		          ,COUNT(distinct TOT_QTD_NBR_NEW_CST) -1    --�����ۼ������ͻ��� 
		          ,SUM(TOT_QTD_NBR_AC_CLS)                   --�����ۼ������˻���
		          ,SUM(TOT_QTD_NBR_LN_DRDWN_AC)              --�����۷��˻���  
		          ,SUM(TOT_QTD_NBR_LN_REPYMT_RCVD_AC)        --���������˻���  
		          ,SUM(TOT_QTD_NBR_NEW_AC)                   --���������˻���   
		          ,COUNT(distinct TOT_YTD_NBR_NEW_CST) -1    --�����ۼ������ͻ���   
		          ,SUM(TOT_YTD_NBR_AC_CLS)                   --�����ۼ������˻���
		          ,SUM(TOT_YTD_NBR_LN_DRDWN_AC)              --����۷��˻���
		          ,SUM(TOT_YTD_NBR_LN_REPYMT_RCVD_AC)        --��������˻���
		          ,SUM(TOT_YTD_NBR_NEW_AC)                   --��������˻���
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

 /** �ռ�������Ϣ */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --3
	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	    --

CREATE INDEX SESSION.TMP_T_CUR ON SESSION.T_CUR(ACG_OU_IP_ID,ACG_SBJ_ID,LN_CTR_TP_ID,LN_GNT_TP_ID,LN_PPS_TP_ID);

		SET SMY_STEPDESC = '����SMY.OU_LN_DLY_SMY �в���������Ϊ���������';     --
		
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
          ,OFF_BST_INT_RCVB_WRTOF            --����Ӧ����Ϣ�������
          ,OFF_BST_INT_RCVB_RPLC	           --����Ӧ����Ϣ�û����
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST		 --��ծ�ʲ���ծ��Ϣ����
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN	   --���������ջ���Ϣ
          ,TOT_MTD_NBR_LN_DRDWNTXN           --���ۼƷ��Ŵ������
          ,TOT_QTD_NBR_LN_DRDWN_TXN          --���ۼƷ��Ŵ������
          ,TOT_YTD_NBR_LN_DRDWN_TXN          --���ۼƷ��Ŵ������
          ,AMT_LN_REPYMT_RCVD
          ,NEW_ACG_SBJ_ID
 					----------------------Start on 2009-1-30-----------------------------------------------
					,CST_Area_LVL1_TP_Id               --�ͻ���������1
					,CST_Area_LVL2_TP_Id               --�ͻ���������2
					,CST_Area_LVL3_TP_Id               --�ͻ���������3
          ----------------------End on 2009-11-30-----------------------------------------------    
					----------------------Start on 2009-1-30-----------------------------------------------
          ,ENT_IDV_IND  										 --������ҵ��־
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
          ,CUR.TOT_MTD_NBR_NEW_CST             --�����ۼ������ͻ���        
          ,CUR.TOT_MTD_NBR_AC_CLS              --�����ۼ������˻���
          ,CUR.MTD_ACML_BAL_AMT		             --���ۼ����
          ,CUR.TOT_MTD_NBR_LN_DRDWN_AC         --�¶��۷��˻���                            
          ,CUR.TOT_MTD_LN_DRDWN_AMT            --�´����ۼƷ��Ž��
          ,CUR.TOT_MTD_AMT_LN_REPYMT_RCVD      --���ۼ��ջش�����          
          ,CUR.TOT_MTD_NBR_LN_REPYMT_RCVD_AC       --�¶������˻���
          ,CUR.TOT_QTD_NBR_NEW_CST          --�����ۼ������ͻ���
          ,CUR.TOT_QTD_NBR_AC_CLS           --�����ۼ������˻���
          ,CUR.QTD_ACML_BAL_AMT             --�����ۼ����
          ,CUR.TOT_QTD_NBR_LN_DRDWN_AC      --�����۷��˻���                            
          ,CUR.TOT_QTD_LN_DRDWN_AMT                                    --���ȴ����ۼƷ��Ž��
          ,CUR.TOT_QTD_AMT_LN_RPYMT_RCVD                               --�����ۼ��ջش�����          
          ,CUR.TOT_QTD_NBR_LN_REPYMT_RCVD_AC   --���������˻���
          ,CUR.TOT_QTD_NBR_NEW_AC              --���������˻���                               
          ,CUR.TOT_YTD_NBR_NEW_CST                  --����ۼ������ͻ���
          ,CUR.TOT_YTD_NBR_AC_CLS                  --����ۼ������˻���
          ---------------------Start on 20100118-----------------------------
          --,COALESCE(PRE.YTD_ACML_BAL_AMT              ,0) + CUR.LN_BAL              --����ۼ����
          ,CUR.YTD_ACML_BAL_AMT                   --����ۼ����
          ---------------------End on 20100118-----------------------------
          ,CUR.TOT_YTD_NBR_LN_DRDWN_AC        --����۷��˻���
          ,CUR.TOT_YTD_LN_DRDWN_AMT                                   --��ȴ����ۼƷ��Ž��
          ,CUR.TOT_YTD_AMT_LN_REPYMT_RCVD                             --����ۼ��ջش�����
          ,CUR.TOT_YTD_NBR_LN_REPYMT_RCVD_AC    --��������˻���
          ,CUR.TOT_YTD_NBR_NEW_AC               --��������˻���
          ,CUR.TOT_MTD_NBR_NEW_AC               --�¶������˻���                  
          ,CUR.CUR_CR_AMT            --����������      
          ,CUR.CUR_DB_AMT            --�跽������      
          ,CUR.TOT_MTD_CR_AMT        --���ۼƴ���������
          ,CUR.TOT_MTD_DB_AMT        --���ۼƽ跽������
          ,CUR.TOT_QTD_DB_AMT        --���ۼƴ���������
          ,CUR.TOT_QTD_CR_AMT        --���ۼƽ跽������
          ,CUR.TOT_YTD_CR_AMT        --���ۼƴ���������
          ,CUR.TOT_YTD_DB_AMT        --���ۼƽ跽������
          ,CUR.OFF_BST_INT_RCVB_WRTOF           --����Ӧ����Ϣ�������   
          ,CUR.OFF_BST_INT_RCVB_RPLC	           --����Ӧ����Ϣ�û����   
          ,CUR.TOT_YTD_INT_INCM_AMT_DEBT_AST		 --��ծ�ʲ���ծ��Ϣ����   
          ,CUR.TOT_YTD_INT_INCM_RTND_WRTOF_LN   --���������ջ���Ϣ
          ,CUR.TOT_MTD_NBR_LN_DRDWNTXN           --���ۼƷ��Ŵ������ 
          ,CUR.TOT_QTD_NBR_LN_DRDWN_TXN          --���ۼƷ��Ŵ������ 
          ,CUR.TOT_YTD_NBR_LN_DRDWN_TXN          --���ۼƷ��Ŵ������        
          ,CUR.AMT_LN_REPYMT_RCVD 
          ,CUR.NEW_ACG_SBJ_ID         --�¿�Ŀ         
 					----------------------Start on 2009-1-30-----------------------------------------------
					,CUR.CST_Area_LVL1_TP_Id               --�ͻ���������1
					,CUR.CST_Area_LVL2_TP_Id               --�ͻ���������2
					,CUR.CST_Area_LVL3_TP_Id               --�ͻ���������3
          ----------------------End on 2009-11-30-----------------------------------------------          
          ----------------------Start on 2009-1-30-----------------------------------------------
          ,CUR.ENT_IDV_IND  									AS ENT_IDV_IND	 --������ҵ��־
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
 /** �ռ�������Ϣ */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	SET SMY_STEPNUM =  SMY_STEPNUM + 1 ;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	    --

--�±�Ĳ���
   IF ACCOUNTING_DATE = MTH_LAST_DAY THEN  		
		  SET SMY_STEPDESC = '����������Ϊ�������һ��,���±�SMY.OU_LN_MTHLY_SMY �в�������';   --  
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
          ,OFF_BST_INT_RCVB_WRTOF            --����Ӧ����Ϣ�������
          ,OFF_BST_INT_RCVB_RPLC	           --����Ӧ����Ϣ�û����
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST		 --��ծ�ʲ���ծ��Ϣ����
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN	   --���������ջ���Ϣ
          ,TOT_MTD_NBR_LN_DRDWNTXN           --���ۼƷ��Ŵ������
          ,TOT_QTD_NBR_LN_DRDWN_TXN          --���ۼƷ��Ŵ������
          ,TOT_YTD_NBR_LN_DRDWN_TXN          --���ۼƷ��Ŵ������
          ,AMT_LN_REPYMT_RCVD
          ,NEW_ACG_SBJ_ID
          ----------------------Start on 2009-1-30-----------------------------------------------
					,CST_Area_LVL1_TP_Id               --�ͻ���������1
					,CST_Area_LVL2_TP_Id               --�ͻ���������2
					,CST_Area_LVL3_TP_Id               --�ͻ���������3
          ----------------------End on 2009-11-30-----------------------------------------------          
					----------------------Start on 2009-1-30-----------------------------------------------
          ,ENT_IDV_IND  									   --������ҵ��־
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
          ,OFF_BST_INT_RCVB_WRTOF            --����Ӧ����Ϣ�������
          ,OFF_BST_INT_RCVB_RPLC	           --����Ӧ����Ϣ�û����
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST		 --��ծ�ʲ���ծ��Ϣ����
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN	   --���������ջ���Ϣ
          ,TOT_MTD_NBR_LN_DRDWNTXN           --���ۼƷ��Ŵ������
          ,TOT_QTD_NBR_LN_DRDWN_TXN          --���ۼƷ��Ŵ������
          ,TOT_YTD_NBR_LN_DRDWN_TXN          --���ۼƷ��Ŵ������
          ,AMT_LN_REPYMT_RCVD
          ,NEW_ACG_SBJ_ID 
          ----------------------Start on 2009-1-30-----------------------------------------------
					,S.CST_Area_LVL1_TP_Id               --�ͻ���������1
					,S.CST_Area_LVL2_TP_Id               --�ͻ���������2
					,S.CST_Area_LVL3_TP_Id               --�ͻ���������3
          ----------------------End on 2009-11-30-----------------------------------------------           
 					----------------------Start on 2009-1-30-----------------------------------------------
          ,S.ENT_IDV_IND  			AS ENT_IDV_IND	 --������ҵ��־
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