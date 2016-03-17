CREATE PROCEDURE SMY.PROC_LN_AR_INT_MTHLY_SMY(IN ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_LN_AR_INT_MTHLY_SMY.sql
-- Procedure name: 			SMY.PROC_LN_AR_INT_MTHLY_SMY
-- Source Table:				SMY.LOAN_AR_SMY, SOR.LN_TXN_DTL_INF,SOR.LN_INT_INF
-- Target Table: 				SMY.LN_AR_INT_MTHLY_SMY
-- Project     :        ZJ RCCB EDW
-- NOTES       :        DEPENDENCY  SMY.LOAN_AR_SMY
-- Purpose     :            
-- PROCESS METHOD      :  Update each day in one period of month, insert in one month.
--=============================================================================
-- Creation Date:       2009.11.09
-- Origin Author:       JAMES SHANG IBM copyright 
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2009-11-09   JAMES SHANG     Create SP File
-- 2009-11-18   Xu Yan          Updated some condition statements
-- 2009-11-23   SHANG						�޸���Ϣ����,����ʵ����Ϣ�Ĺ�������
-- 2009-12-16   Xu Yan          Updated 'NBR_LN_DRDWNTXN','NBR_LN_RCVD_TXN'
-- 2009-12-16   Xu Yan          Fixed the bug for rerunning.
-- 2010-01-06   Xu Yan          Included the related transactions on the account closing day
-- 2010-01-19   Xu Yan          Removed all the conditional statements of the AR life cycle status and modify the calendar days
-- 2011-08-09		Li ShuWen				�����ܽ����Ż����ж��Ƿ��³���������insert����merge
-- 2011-08-11   Li ShenYu       ժҪ�����
-- 2012-02-29   Chen XiaoWen    1���޸�SMY.LN_AR_INT_MTHLY_SMY�Ĳ�ѯ����Ϊʹ��ACG_DT������
--                              2����merge using���Ӳ�ѯ���Ϊ��ʱ��TMP_TMP
--                              3����merge���е�match��not match�߼����Ϊmatch update�͵�����insert��䡣
-------------------------------------------------------------------------------
LANGUAGE SQL

 BEGIN 
    /*�����쳣����ʹ�ñ���*/
		DECLARE SQLCODE INT DEFAULT 0; --
		DECLARE SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
		DECLARE SMY_STEPNUM INT DEFAULT 1;                     --�����ڲ�λ�ñ��
		DECLARE SMY_STEPDESC VARCHAR(100) DEFAULT '';          --�����ڲ�λ������
		DECLARE SMY_DATE DATE;        --��ʱ���ڱ���
		DECLARE SMY_RCOUNT INT;       --DML������ü�¼��
		DECLARE SMY_PROCNM VARCHAR(100);                        --�洢��������
		
/*�����洢����ʹ�ñ���*/
		DECLARE CUR_YEAR SMALLINT;--
		DECLARE CUR_MONTH SMALLINT;--
		DECLARE MON_DAY INTEGER;--
		DECLARE YR_FIRST_DAY DATE;--
		DECLARE QTR_FIRST_DAY DATE;--

		DECLARE MAX_ACG_DT DATE;--
		DECLARE LAST_SMY_DATE DATE;--
		DECLARE MTH_FIRST_DAY DATE;--
		DECLARE EMP_SQL VARCHAR(200);--
		DECLARE LAST_MONTH SMALLINT;--
		DECLARE CUR_QTR SMALLINT;--
		DECLARE V_T SMALLINT;--
		DECLARE C_MON_DAY SMALLINT;--
		DECLARE C_YR_DAY SMALLINT;--
		DECLARE C_QTR_DAY SMALLINT;--
		DECLARE QTR_LAST_DAY DATE;--
		DECLARE MTH_LAST_DAY DATE;
		DECLARE LASTMTH_FIRST_DAY DATE;--���³���

/*
	1.�������SQL�쳣����ľ��(EXIT��ʽ).
  2.������SQL�쳣ʱ�ڴ洢�����е�λ��(SMY_STEPNUM),λ������(SMY_STEPDESC)��SQLCODE(SMY_SQLCODE)�����SMY_LOG����������.
  3.����RESIGNAL���������쳣,�����洢����ִ����,������SQL�쳣֮ǰ�洢������������ɵĲ������лع�.
*/
  
		DECLARE CONTINUE HANDLER FOR NOT FOUND
		  SET V_T=0 ; --
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
    	SET SMY_SQLCODE = SQLCODE;    	--
      ROLLBACK;--
      SET SMY_STEPNUM = SMY_STEPNUM + 1;--
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
      COMMIT;--
      RESIGNAL;--
    END;--
   
   /* 
    DECLARE CONTINUE HANDLER FOR SQLWARNING
    BEGIN
      SET SMY_SQLCODE = SQLCODE;--
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
      COMMIT;--
    END;--
   */
   
   /*������ֵ*/   
    SET SMY_PROCNM  ='PROC_LN_AR_INT_MTHLY_SMY';--
    SET SMY_DATE    =ACCOUNTING_DATE;    --
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --ȡ��ǰ���
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --ȡ��ǰ�·�
    SET MON_DAY     =DAY(ACCOUNTING_DATE);     --ȡ�µڼ���
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- ȡ�����
    ----------------------Start on 20100119---------------------------------
    --SET C_YR_DAY      =DAYOFYEAR(DATE(TRIM(CHAR(YEAR(ACCOUNTING_DATE)))||'-12-31')); --ȡ����ڼ���
    SET C_YR_DAY      =DAYOFYEAR(ACCOUNTING_DATE); --ȡ����ڼ���
    ----------------------End on 20100119---------------------------------
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --��ǰ����
    SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,ISO),1,7)||'-01'); --ȡ���³���
    VALUES(MTH_FIRST_DAY + 1 MONTH - 1 DAY) INTO MTH_LAST_DAY ;
    VALUES(MTH_FIRST_DAY - 1 MONTH) INTO LASTMTH_FIRST_DAY ;
    
    --��������������
    ----------------------Start on 20100119---------------------------------
    --SET C_MON_DAY = DAYS(MTH_FIRST_DAY + 1 MONTH ) - DAYS(MTH_FIRST_DAY);--
    SET C_MON_DAY = MON_DAY;--
    ----------------------End on 20100119---------------------------------
     
    VALUES(ACCOUNTING_DATE - 1 DAY) INTO LAST_SMY_DATE;--
    
    SET LAST_MONTH = MONTH(LAST_SMY_DATE);  --
    
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
    ----------------------Start on 20100119---------------------------------
  	--SET C_QTR_DAY = DAYS(QTR_LAST_DAY) - DAYS(QTR_FIRST_DAY) + 1 ;--
  	SET C_QTR_DAY = DAYS(ACCOUNTING_DATE) - DAYS(QTR_FIRST_DAY) + 1 ;--
  	----------------------End on 20100119---------------------------------
		
		SELECT COALESCE(MAX(ACG_DT),'1900-01-01') INTO MAX_ACG_DT FROM SMY.LN_AR_INT_MTHLY_SMY;--

/*Delete��־��,����SMY_PROCNM=��ǰ�洢��������,SMY_DATE=ACCOUNTING_DATE,�������µ���ʼ��־*/
		DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM ;--
			COMMIT;--
		
		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		
		SET SMY_STEPDESC = 	'�洢���̿�ʼ����';--
		SET SMY_STEPNUM = SMY_STEPNUM + 1;--
		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
				VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);		 --

/*���ݻָ��뱸��*/
    IF MAX_ACG_DT = ACCOUNTING_DATE THEN
       DELETE FROM SMY.LN_AR_INT_MTHLY_SMY WHERE ACG_DT = ACCOUNTING_DATE;--
    /**ÿ�µ�һ�ղ���Ҫ����ʷ���лָ�**/       
       IF MON_DAY <> 1 THEN
      	 INSERT INTO SMY.LN_AR_INT_MTHLY_SMY  SELECT * FROM HIS.LN_AR_INT_MTHLY_SMY hist 
               where  not exists ( select 1 from SMY.LN_AR_INT_MTHLY_SMY cur
                                    where
				    hist.CTR_AR_ID = cur.CTR_AR_ID 
			            and
				    hist.CTR_ITM_ORDR_ID = cur.CTR_ITM_ORDR_ID
				    and
				    hist.CDR_YR = cur.CDR_YR
				    and
			            hist.CDR_MTH = cur.CDR_MTH
				);--
       END IF;--
     ELSE
  		/** ���hist ���ݱ� **/

	    SET EMP_SQL= 'Alter TABLE HIS.LN_AR_INT_MTHLY_SMY ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE' ;		--
		  EXECUTE IMMEDIATE EMP_SQL;             --
      COMMIT;--
      
      /**backup �������� **/
  		INSERT INTO HIS.LN_AR_INT_MTHLY_SMY SELECT * FROM SMY.LN_AR_INT_MTHLY_SMY WHERE ACG_DT = LAST_SMY_DATE;--
      
    END IF;--

SET SMY_STEPDESC = '�����û���ʱ��,�������SMY����';--

	/*�����û���ʱ��*/
	
	DECLARE GLOBAL TEMPORARY TABLE TMP 
		LIKE SMY.LN_AR_INT_MTHLY_SMY
	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(CTR_AR_ID,CTR_ITM_ORDR_ID);--
----add by wuzhansan begin
  CREATE INDEX SESSION.IDX_TMP ON SESSION.TMP(CTR_AR_ID,CTR_ITM_ORDR_ID);
----add by wuzhansan end

----modify by wuzhanshan begin
 /*�������SMY����*/
IF YR_FIRST_DAY <>  ACCOUNTING_DATE  THEN

 IF QTR_FIRST_DAY=ACCOUNTING_DATE THEN 
	INSERT INTO SESSION.TMP 
	(
           CTR_AR_ID                         --��ͬ��            
          ,CTR_ITM_ORDR_ID                   --��ͬ���          
          ,CDR_YR                            --������            
          ,CDR_MTH                           --������            
          ,ACG_DT                            --�������          
          ,LN_AR_ID                          --�˻���            
          ,DNMN_CCY_ID                       --����ID            
          ,YTD_ON_BST_INT_AMT_RCVD           --����ʵ����Ϣ      
          ,YTD_OFF_BST_INT_AMT_RCVD          --����ʵ����Ϣ      
          ,ON_BST_INT_RCVB                   --����Ӧ��δ����Ϣ  
          ,OFF_BST_INT_RCVB                  --����Ӧ��δ����Ϣ  
          ,TOT_YTD_AMT_OF_INT_INCM           --��Ϣ����          
          ,LN_DRDWN_AMT                      --�����۷Ž��      
          ,AMT_LN_REPYMT_RCVD                --�������ս��      
          ,TOT_MTD_LN_DRDWN_AMT              --�´����ۼƷ��Ž��
          ,TOT_QTD_LN_DRDWN_AMT              --�������ۼƷ��Ž��
          ,TOT_YTD_LN_DRDWN_AMT              --������ۼƷ��Ž��
          ,TOT_MTD_AMT_LN_REPYMT_RCVD        --���ۼ��ջش�����
          ,TOT_QTD_AMT_LN_RPYMT_RCVD         --���ۼ��ջش�����
          ,TOT_YTD_AMT_LN_REPYMT_RCVD        --���ۼ��ջش�����
          ,TOT_MTD_NBR_LN_RCVD_TXN           --���ۼ��ջش������
          ,TOT_QTD_NBR_LN_RCVD_TXN           --���ۼ��ջش������
          ,TOT_YTD_NBR_LN_RCVD_TXN           --���ۼ��ջش������
          ,TOT_MTD_NBR_LN_DRDWNTXN           --���ۼƷ��Ŵ������
          ,TOT_QTD_NBR_LN_DRDWN_TXN          --���ۼƷ��Ŵ������
          ,TOT_YTD_NBR_LN_DRDWN_TXN          --���ۼƷ��Ŵ������
          ,CUR_CR_AMT                        --����������        
          ,CUR_DB_AMT                        --�跽������        
          ,TOT_MTD_CR_AMT                    --���ۼƴ���������  
          ,TOT_MTD_DB_AMT                    --���ۼƽ跽������  
          ,TOT_QTD_DB_AMT                    --���ۼƴ���������  
          ,TOT_QTD_CR_AMT                    --���ۼƽ跽������  
          ,TOT_YTD_CR_AMT                    --���ۼƴ���������  
          ,TOT_YTD_DB_AMT                    --���ۼƽ跽������
          ,OFF_BST_INT_RCVB_WRTOF            --����Ӧ����Ϣ�������
          ,OFF_BST_INT_RCVB_RPLC	           --����Ӧ����Ϣ�û����
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST		 --��ծ�ʲ���ծ��Ϣ����
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN	   --���������ջ���Ϣ        
          ,BAL_AMT	            --
          ,MTD_ACML_BAL_AMT		--		���ۼ����
          ,QTD_ACML_BAL_AMT		--		���ۼ����
          ,YTD_ACML_BAL_AMT		--		���ۼ����
          ,NOCLD_In_MTH				--		����������
          ,NOD_In_MTH					--		����Ч����
          ,NOCLD_In_QTR				--		����������
          ,NOD_In_QTR					--		����Ч����
          ,NOCLD_In_Year				--		����������
          ,NOD_In_Year					--		����Ч����
          ,CUR_WRTOF_AMT_RCVD            --�����ջغ������
          ,CUR_AMT_RCVD_Of_AST_RPLC      --�����ջ��û��ʲ����
          ,CUR_WRTOF_AMT                 --����������
          ,TOT_MTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,TOT_MTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��ջ��û��ʲ����
          ,TOT_MTD_WRTOF_AMT             --���ۼƺ������
          ,TOT_QTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,TOT_QTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��ջ��û��ʲ����
          ,TOT_QTD_WRTOF_AMT             --���ۼƺ������
          ,TOT_YTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,TOT_YTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��û��ʲ����
          ,TOT_YTD_WRTOF_AMT             --���ۼƺ������                  
          ) 
    SELECT 
           CTR_AR_ID                         --��ͬ��            
          ,CTR_ITM_ORDR_ID                   --��ͬ���          
          ,CDR_YR                            --������            
          ,CDR_MTH                           --������            
          ,ACG_DT                            --�������          
          ,LN_AR_ID                          --�˻���            
          ,DNMN_CCY_ID                       --����ID            
          ,YTD_ON_BST_INT_AMT_RCVD           --����ʵ����Ϣ      
          ,YTD_OFF_BST_INT_AMT_RCVD          --����ʵ����Ϣ      
          ,ON_BST_INT_RCVB                   --����Ӧ��δ����Ϣ  
          ,OFF_BST_INT_RCVB                  --����Ӧ��δ����Ϣ  
          ,TOT_YTD_AMT_OF_INT_INCM           --��Ϣ����          
          ,LN_DRDWN_AMT                      --�����۷Ž��      
          ,AMT_LN_REPYMT_RCVD                --�������ս��      
          ,0                                 --�´����ۼƷ��Ž��
          ,0                                 --�������ۼƷ��Ž��
          ,TOT_YTD_LN_DRDWN_AMT              --������ۼƷ��Ž��
          ,0                                 --���ۼ��ջش�����
          ,0                                 --���ۼ��ջش�����
          ,TOT_YTD_AMT_LN_REPYMT_RCVD        --���ۼ��ջش�����
          ,0                                 --���ۼ��ջش������
          ,0                                 --���ۼ��ջش������
          ,TOT_YTD_NBR_LN_RCVD_TXN           --���ۼ��ջش������
          ,0                                 --���ۼƷ��Ŵ������
          ,0                                 --���ۼƷ��Ŵ������
          ,TOT_YTD_NBR_LN_DRDWN_TXN          --���ۼƷ��Ŵ������
          ,CUR_CR_AMT                        --����������        
          ,CUR_DB_AMT                        --�跽������        
          ,0                                 --���ۼƴ���������  
          ,0                                 --���ۼƽ跽������  
          ,0                                 --���ۼƴ���������  
          ,0                                 --���ۼƽ跽������  
          ,TOT_YTD_CR_AMT                    --���ۼƴ���������  
          ,TOT_YTD_DB_AMT                    --���ۼƽ跽������
          ,OFF_BST_INT_RCVB_WRTOF            --����Ӧ����Ϣ�������
          ,OFF_BST_INT_RCVB_RPLC	           --����Ӧ����Ϣ�û����
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST		 --��ծ�ʲ���ծ��Ϣ����
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN	   --���������ջ���Ϣ
          ,BAL_AMT	                         --
          ,0		                             --		���ۼ����
          ,0		                             --		���ۼ����
          ,YTD_ACML_BAL_AMT		               --		���ۼ����
          ,NOCLD_In_MTH				               --		����������
          ,0					                       --		����Ч����
          ,NOCLD_In_QTR				               --		����������
          ,0					                       --		����Ч����
          ,NOCLD_In_Year				             --		����������
          ,NOD_In_Year					             --		����Ч����
          ,CUR_WRTOF_AMT_RCVD                --�����ջغ������
          ,CUR_AMT_RCVD_Of_AST_RPLC          --�����ջ��û��ʲ����
          ,CUR_WRTOF_AMT                     --����������
          ,0                                 --���ۼ��ջغ������
          ,0                                 --���ۼ��ջ��û��ʲ����
          ,0                                 --���ۼƺ������
          ,0                                 --���ۼ��ջغ������
          ,0                                 --���ۼ��ջ��û��ʲ����
          ,0                                 --���ۼƺ������
          ,TOT_YTD_WRTOF_AMT_RCVD            --���ۼ��ջغ������
          ,TOT_YTD_AMT_RCVD_Of_AST_RPLC      --���ۼ��û��ʲ����
          ,TOT_YTD_WRTOF_AMT                 --���ۼƺ������              					       
     --FROM SMY.LN_AR_INT_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = LAST_MONTH;--
     FROM SMY.LN_AR_INT_MTHLY_SMY WHERE ACG_DT >= LASTMTH_FIRST_DAY AND ACG_DT < MTH_FIRST_DAY;   --ȡ��������
  ELSEIF ACCOUNTING_DATE=MTH_FIRST_DAY THEN
 	INSERT INTO SESSION.TMP 
	(
           CTR_AR_ID                         --��ͬ��            
          ,CTR_ITM_ORDR_ID                   --��ͬ���          
          ,CDR_YR                            --������            
          ,CDR_MTH                           --������            
          ,ACG_DT                            --�������          
          ,LN_AR_ID                          --�˻���            
          ,DNMN_CCY_ID                       --����ID            
          ,YTD_ON_BST_INT_AMT_RCVD           --����ʵ����Ϣ      
          ,YTD_OFF_BST_INT_AMT_RCVD          --����ʵ����Ϣ      
          ,ON_BST_INT_RCVB                   --����Ӧ��δ����Ϣ  
          ,OFF_BST_INT_RCVB                  --����Ӧ��δ����Ϣ  
          ,TOT_YTD_AMT_OF_INT_INCM           --��Ϣ����          
          ,LN_DRDWN_AMT                      --�����۷Ž��      
          ,AMT_LN_REPYMT_RCVD                --�������ս��      
          ,TOT_MTD_LN_DRDWN_AMT              --�´����ۼƷ��Ž��
          ,TOT_QTD_LN_DRDWN_AMT              --�������ۼƷ��Ž��
          ,TOT_YTD_LN_DRDWN_AMT              --������ۼƷ��Ž��
          ,TOT_MTD_AMT_LN_REPYMT_RCVD        --���ۼ��ջش�����
          ,TOT_QTD_AMT_LN_RPYMT_RCVD         --���ۼ��ջش�����
          ,TOT_YTD_AMT_LN_REPYMT_RCVD        --���ۼ��ջش�����
          ,TOT_MTD_NBR_LN_RCVD_TXN           --���ۼ��ջش������
          ,TOT_QTD_NBR_LN_RCVD_TXN           --���ۼ��ջش������
          ,TOT_YTD_NBR_LN_RCVD_TXN           --���ۼ��ջش������
          ,TOT_MTD_NBR_LN_DRDWNTXN           --���ۼƷ��Ŵ������
          ,TOT_QTD_NBR_LN_DRDWN_TXN          --���ۼƷ��Ŵ������
          ,TOT_YTD_NBR_LN_DRDWN_TXN          --���ۼƷ��Ŵ������
          ,CUR_CR_AMT                        --����������        
          ,CUR_DB_AMT                        --�跽������        
          ,TOT_MTD_CR_AMT                    --���ۼƴ���������  
          ,TOT_MTD_DB_AMT                    --���ۼƽ跽������  
          ,TOT_QTD_DB_AMT                    --���ۼƴ���������  
          ,TOT_QTD_CR_AMT                    --���ۼƽ跽������  
          ,TOT_YTD_CR_AMT                    --���ۼƴ���������  
          ,TOT_YTD_DB_AMT                    --���ۼƽ跽������
          ,OFF_BST_INT_RCVB_WRTOF            --����Ӧ����Ϣ�������
          ,OFF_BST_INT_RCVB_RPLC	           --����Ӧ����Ϣ�û����
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST		 --��ծ�ʲ���ծ��Ϣ����
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN	   --���������ջ���Ϣ        
          ,BAL_AMT	            --
          ,MTD_ACML_BAL_AMT		--		���ۼ����
          ,QTD_ACML_BAL_AMT		--		���ۼ����
          ,YTD_ACML_BAL_AMT		--		���ۼ����
          ,NOCLD_In_MTH				--		����������
          ,NOD_In_MTH					--		����Ч����
          ,NOCLD_In_QTR				--		����������
          ,NOD_In_QTR					--		����Ч����
          ,NOCLD_In_Year				--		����������
          ,NOD_In_Year					--		����Ч����
          ,CUR_WRTOF_AMT_RCVD            --�����ջغ������
          ,CUR_AMT_RCVD_Of_AST_RPLC      --�����ջ��û��ʲ����
          ,CUR_WRTOF_AMT                 --����������
          ,TOT_MTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,TOT_MTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��ջ��û��ʲ����
          ,TOT_MTD_WRTOF_AMT             --���ۼƺ������
          ,TOT_QTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,TOT_QTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��ջ��û��ʲ����
          ,TOT_QTD_WRTOF_AMT             --���ۼƺ������
          ,TOT_YTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,TOT_YTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��û��ʲ����
          ,TOT_YTD_WRTOF_AMT             --���ۼƺ������                  
          ) 
    SELECT 
           CTR_AR_ID                         --��ͬ��            
          ,CTR_ITM_ORDR_ID                   --��ͬ���          
          ,CDR_YR                            --������            
          ,CDR_MTH                           --������            
          ,ACG_DT                            --�������          
          ,LN_AR_ID                          --�˻���            
          ,DNMN_CCY_ID                       --����ID            
          ,YTD_ON_BST_INT_AMT_RCVD           --����ʵ����Ϣ      
          ,YTD_OFF_BST_INT_AMT_RCVD          --����ʵ����Ϣ      
          ,ON_BST_INT_RCVB                   --����Ӧ��δ����Ϣ  
          ,OFF_BST_INT_RCVB                  --����Ӧ��δ����Ϣ  
          ,TOT_YTD_AMT_OF_INT_INCM           --��Ϣ����          
          ,LN_DRDWN_AMT                      --�����۷Ž��      
          ,AMT_LN_REPYMT_RCVD                --�������ս��      
          ,0              --�´����ۼƷ��Ž��
          ,TOT_QTD_LN_DRDWN_AMT              --�������ۼƷ��Ž��
          ,TOT_YTD_LN_DRDWN_AMT              --������ۼƷ��Ž��
          ,0        --���ۼ��ջش�����
          ,TOT_QTD_AMT_LN_RPYMT_RCVD         --���ۼ��ջش�����
          ,TOT_YTD_AMT_LN_REPYMT_RCVD        --���ۼ��ջش�����
          ,0           --���ۼ��ջش������
          ,TOT_QTD_NBR_LN_RCVD_TXN           --���ۼ��ջش������
          ,TOT_YTD_NBR_LN_RCVD_TXN           --���ۼ��ջش������
          ,0           --���ۼƷ��Ŵ������
          ,TOT_QTD_NBR_LN_DRDWN_TXN          --���ۼƷ��Ŵ������
          ,TOT_YTD_NBR_LN_DRDWN_TXN          --���ۼƷ��Ŵ������
          ,CUR_CR_AMT                        --����������        
          ,CUR_DB_AMT                        --�跽������        
          ,0                    --���ۼƴ���������  
          ,0                    --���ۼƽ跽������  
          ,TOT_QTD_DB_AMT                    --���ۼƴ���������  
          ,TOT_QTD_CR_AMT                    --���ۼƽ跽������  
          ,TOT_YTD_CR_AMT                    --���ۼƴ���������  
          ,TOT_YTD_DB_AMT                    --���ۼƽ跽������
          ,OFF_BST_INT_RCVB_WRTOF            --����Ӧ����Ϣ�������
          ,OFF_BST_INT_RCVB_RPLC	           --����Ӧ����Ϣ�û����
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST		 --��ծ�ʲ���ծ��Ϣ����
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN	   --���������ջ���Ϣ
          ,BAL_AMT	            --
          ,0		--		���ۼ����
          ,QTD_ACML_BAL_AMT		--		���ۼ����
          ,YTD_ACML_BAL_AMT		--		���ۼ����
          ,0				--		����������
          ,0					--		����Ч����
          ,NOCLD_In_QTR				--		����������
          ,NOD_In_QTR					--		����Ч����
          ,NOCLD_In_Year				--		����������
          ,NOD_In_Year					--		����Ч����
          ,CUR_WRTOF_AMT_RCVD            --�����ջغ������
          ,CUR_AMT_RCVD_Of_AST_RPLC      --�����ջ��û��ʲ����
          ,CUR_WRTOF_AMT                 --����������
          ,0        --���ۼ��ջغ������
          ,0  --���ۼ��ջ��û��ʲ����
          ,0             --���ۼƺ������
          ,TOT_QTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,TOT_QTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��ջ��û��ʲ����
          ,TOT_QTD_WRTOF_AMT             --���ۼƺ������
          ,TOT_YTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,TOT_YTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��û��ʲ����
          ,TOT_YTD_WRTOF_AMT             --���ۼƺ������              					       
     --FROM SMY.LN_AR_INT_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = LAST_MONTH;
     FROM SMY.LN_AR_INT_MTHLY_SMY WHERE ACG_DT >= LASTMTH_FIRST_DAY AND ACG_DT < MTH_FIRST_DAY;   --ȡ��������
  ELSE
  INSERT INTO SESSION.TMP 
	(
           CTR_AR_ID                         --��ͬ��            
          ,CTR_ITM_ORDR_ID                   --��ͬ���          
          ,CDR_YR                            --������            
          ,CDR_MTH                           --������            
          ,ACG_DT                            --�������          
          ,LN_AR_ID                          --�˻���            
          ,DNMN_CCY_ID                       --����ID            
          ,YTD_ON_BST_INT_AMT_RCVD           --����ʵ����Ϣ      
          ,YTD_OFF_BST_INT_AMT_RCVD          --����ʵ����Ϣ      
          ,ON_BST_INT_RCVB                   --����Ӧ��δ����Ϣ  
          ,OFF_BST_INT_RCVB                  --����Ӧ��δ����Ϣ  
          ,TOT_YTD_AMT_OF_INT_INCM           --��Ϣ����          
          ,LN_DRDWN_AMT                      --�����۷Ž��      
          ,AMT_LN_REPYMT_RCVD                --�������ս��      
          ,TOT_MTD_LN_DRDWN_AMT              --�´����ۼƷ��Ž��
          ,TOT_QTD_LN_DRDWN_AMT              --�������ۼƷ��Ž��
          ,TOT_YTD_LN_DRDWN_AMT              --������ۼƷ��Ž��
          ,TOT_MTD_AMT_LN_REPYMT_RCVD        --���ۼ��ջش�����
          ,TOT_QTD_AMT_LN_RPYMT_RCVD         --���ۼ��ջش�����
          ,TOT_YTD_AMT_LN_REPYMT_RCVD        --���ۼ��ջش�����
          ,TOT_MTD_NBR_LN_RCVD_TXN           --���ۼ��ջش������
          ,TOT_QTD_NBR_LN_RCVD_TXN           --���ۼ��ջش������
          ,TOT_YTD_NBR_LN_RCVD_TXN           --���ۼ��ջش������
          ,TOT_MTD_NBR_LN_DRDWNTXN           --���ۼƷ��Ŵ������
          ,TOT_QTD_NBR_LN_DRDWN_TXN          --���ۼƷ��Ŵ������
          ,TOT_YTD_NBR_LN_DRDWN_TXN          --���ۼƷ��Ŵ������
          ,CUR_CR_AMT                        --����������        
          ,CUR_DB_AMT                        --�跽������        
          ,TOT_MTD_CR_AMT                    --���ۼƴ���������  
          ,TOT_MTD_DB_AMT                    --���ۼƽ跽������  
          ,TOT_QTD_DB_AMT                    --���ۼƴ���������  
          ,TOT_QTD_CR_AMT                    --���ۼƽ跽������  
          ,TOT_YTD_CR_AMT                    --���ۼƴ���������  
          ,TOT_YTD_DB_AMT                    --���ۼƽ跽������
          ,OFF_BST_INT_RCVB_WRTOF            --����Ӧ����Ϣ�������
          ,OFF_BST_INT_RCVB_RPLC	           --����Ӧ����Ϣ�û����
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST		 --��ծ�ʲ���ծ��Ϣ����
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN	   --���������ջ���Ϣ        
          ,BAL_AMT	            --
          ,MTD_ACML_BAL_AMT		--		���ۼ����
          ,QTD_ACML_BAL_AMT		--		���ۼ����
          ,YTD_ACML_BAL_AMT		--		���ۼ����
          ,NOCLD_In_MTH				--		����������
          ,NOD_In_MTH					--		����Ч����
          ,NOCLD_In_QTR				--		����������
          ,NOD_In_QTR					--		����Ч����
          ,NOCLD_In_Year				--		����������
          ,NOD_In_Year					--		����Ч����
          ,CUR_WRTOF_AMT_RCVD            --�����ջغ������
          ,CUR_AMT_RCVD_Of_AST_RPLC      --�����ջ��û��ʲ����
          ,CUR_WRTOF_AMT                 --����������
          ,TOT_MTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,TOT_MTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��ջ��û��ʲ����
          ,TOT_MTD_WRTOF_AMT             --���ۼƺ������
          ,TOT_QTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,TOT_QTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��ջ��û��ʲ����
          ,TOT_QTD_WRTOF_AMT             --���ۼƺ������
          ,TOT_YTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,TOT_YTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��û��ʲ����
          ,TOT_YTD_WRTOF_AMT             --���ۼƺ������                  
          ) 
    SELECT 
           CTR_AR_ID                         --��ͬ��            
          ,CTR_ITM_ORDR_ID                   --��ͬ���          
          ,CDR_YR                            --������            
          ,CDR_MTH                           --������            
          ,ACG_DT                            --�������          
          ,LN_AR_ID                          --�˻���            
          ,DNMN_CCY_ID                       --����ID            
          ,YTD_ON_BST_INT_AMT_RCVD           --����ʵ����Ϣ      
          ,YTD_OFF_BST_INT_AMT_RCVD          --����ʵ����Ϣ      
          ,ON_BST_INT_RCVB                   --����Ӧ��δ����Ϣ  
          ,OFF_BST_INT_RCVB                  --����Ӧ��δ����Ϣ  
          ,TOT_YTD_AMT_OF_INT_INCM           --��Ϣ����          
          ,LN_DRDWN_AMT                      --�����۷Ž��      
          ,AMT_LN_REPYMT_RCVD                --�������ս��      
          ,TOT_MTD_LN_DRDWN_AMT              --�´����ۼƷ��Ž��
          ,TOT_QTD_LN_DRDWN_AMT              --�������ۼƷ��Ž��
          ,TOT_YTD_LN_DRDWN_AMT              --������ۼƷ��Ž��
          ,TOT_MTD_AMT_LN_REPYMT_RCVD        --���ۼ��ջش�����
          ,TOT_QTD_AMT_LN_RPYMT_RCVD         --���ۼ��ջش�����
          ,TOT_YTD_AMT_LN_REPYMT_RCVD        --���ۼ��ջش�����
          ,TOT_MTD_NBR_LN_RCVD_TXN           --���ۼ��ջش������
          ,TOT_QTD_NBR_LN_RCVD_TXN           --���ۼ��ջش������
          ,TOT_YTD_NBR_LN_RCVD_TXN           --���ۼ��ջش������
          ,TOT_MTD_NBR_LN_DRDWNTXN           --���ۼƷ��Ŵ������
          ,TOT_QTD_NBR_LN_DRDWN_TXN          --���ۼƷ��Ŵ������
          ,TOT_YTD_NBR_LN_DRDWN_TXN          --���ۼƷ��Ŵ������
          ,CUR_CR_AMT                        --����������        
          ,CUR_DB_AMT                        --�跽������        
          ,TOT_MTD_CR_AMT                    --���ۼƴ���������  
          ,TOT_MTD_DB_AMT                    --���ۼƽ跽������  
          ,TOT_QTD_DB_AMT                    --���ۼƴ���������  
          ,TOT_QTD_CR_AMT                    --���ۼƽ跽������  
          ,TOT_YTD_CR_AMT                    --���ۼƴ���������  
          ,TOT_YTD_DB_AMT                    --���ۼƽ跽������
          ,OFF_BST_INT_RCVB_WRTOF            --����Ӧ����Ϣ�������
          ,OFF_BST_INT_RCVB_RPLC	           --����Ӧ����Ϣ�û����
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST		 --��ծ�ʲ���ծ��Ϣ����
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN	   --���������ջ���Ϣ
          ,BAL_AMT	            --
          ,MTD_ACML_BAL_AMT		--		���ۼ����
          ,QTD_ACML_BAL_AMT		--		���ۼ����
          ,YTD_ACML_BAL_AMT		--		���ۼ����
          ,NOCLD_In_MTH				--		����������
          ,NOD_In_MTH					--		����Ч����
          ,NOCLD_In_QTR				--		����������
          ,NOD_In_QTR					--		����Ч����
          ,NOCLD_In_Year				--		����������
          ,NOD_In_Year					--		����Ч����
          ,CUR_WRTOF_AMT_RCVD            --�����ջغ������
          ,CUR_AMT_RCVD_Of_AST_RPLC      --�����ջ��û��ʲ����
          ,CUR_WRTOF_AMT                 --����������
          ,TOT_MTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,TOT_MTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��ջ��û��ʲ����
          ,TOT_MTD_WRTOF_AMT             --���ۼƺ������
          ,TOT_QTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,TOT_QTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��ջ��û��ʲ����
          ,TOT_QTD_WRTOF_AMT             --���ۼƺ������
          ,TOT_YTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,TOT_YTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��û��ʲ����
          ,TOT_YTD_WRTOF_AMT             --���ۼƺ������              					       
     --FROM SMY.LN_AR_INT_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = LAST_MONTH;
     FROM SMY.LN_AR_INT_MTHLY_SMY WHERE ACG_DT >= MTH_FIRST_DAY AND ACG_DT <= MTH_LAST_DAY;   --ȡ��������
  END IF;
END IF;
----modify by wuzhanshan end	
       
      
 /** �ռ�������Ϣ */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	SET SMY_STEPNUM = SMY_STEPNUM + 1;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	           --
		
		SET SMY_STEPDESC = '������ʱ��SESSION.TMP_LN_TXN, �������в����ѯSOR.LN_TXN_DTL_INF�л������Ϊ���������'; 		--
  /*����LN_TXN_DTL_INF ��ʱ�� */
   DECLARE GLOBAL TEMPORARY TABLE TMP_LN_TXN AS 
(
  	 	 SELECT  
  	 	 			 CTR_AR_ID   AS CTR_AR_ID        --��ͬ��     
  	 	 			,CTR_SEQ_NBR AS CTR_SEQ_NBR  --��ͬ���
           ,SUM(CASE WHEN INT_BSH >0 AND INT_OFF_BSH + INT_BSH <>0  AND JRL_AC_F=17320001 THEN INT_BSH  WHEN JRL_AC_F=17320003 THEN INT_BSH*-1 ELSE 0 END )  AS ON_BST_INT_AMT_RCVD      --���ձ���ʵ����Ϣ
           ,SUM(CASE WHEN INT_OFF_BSH >0 AND INT_OFF_BSH + INT_BSH <>0 AND JRL_AC_F=17320001 THEN INT_OFF_BSH ELSE 0 END )  AS OFF_BST_INT_AMT_RCVD     --���ձ���ʵ����Ϣ
           , SUM(( CASE WHEN DSC_TP_ID NOT IN (16030161,16030165,16030286,16030287,16030288,16030282,16030283,16030284,16030285) AND JRL_AC_F = 17320001  THEN CMPD_INT_BSH + CMPD_INT_OFF_BSH + CRN_PR_INT_AMT ELSE 0 END )   -- 16030286:�պ������� 16030287:�պ�����Ϣ 16030288:�պ�����Ϣ 16030282:��ծ���� 16030283:��ծ��Ϣ 16030284:��ծ��Ϣ 16030285:��ծ���� 16030161,16030165ժҪ����ͣ��
           - ( CASE WHEN INT_BSH < 0  AND JRL_AC_F=17320001 THEN INT_BSH ELSE 0 END ) 
           + ( CASE WHEN INT_OFF_BSH > 0  AND DSC_TP_ID NOT IN (16030161,16030165,16030286,16030287,16030288,16030282,16030283,16030284,16030285)  AND JRL_AC_F=17320001 THEN INT_OFF_BSH ELSE 0 END )   -- 16030286:�պ������� 16030287:�պ�����Ϣ 16030288:�պ�����Ϣ 16030282:��ծ���� 16030283:��ծ��Ϣ 16030284:��ծ��Ϣ 16030285:��ծ���� 16030161,16030165ժҪ����ͣ��
           - ( CASE WHEN INT_BSH > 0 AND INT_BSH+INT_OFF_BSH=0  AND JRL_AC_F=17320001 THEN INT_BSH ELSE 0 END ) 
           - ( CASE WHEN JRL_AC_F=17320003  THEN INT_BSH ELSE 0 END) )
           	                         AS AMT_OF_INT_INCM        --��Ϣ���� -- ���յ�
           ,SUM(CASE WHEN DSC_TP_ID in (16030158,16030265) AND JRL_AC_F=17320001  THEN TXN_PNP_AMT ELSE 0 END)  AS LN_DRDWN_AMT           --�����۷Ž�� 16030265: ����ſ� 16030158ժҪ����ͣ��
           ,SUM(CASE WHEN DSC_TP_ID IN (16030156,16030006,16030272,16030274,16030269,16030271)  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT ELSE 0 END)       AS AMT_LN_REPYMT_RCVD     --�������ս��   16030272:�Զ��ձ��� 16030274:�Զ��ձ�Ϣ 16030269:�����ձ��� 16030271:�����ձ�Ϣ 16030156,16030006ժҪ����ͣ��
        	 ,SUM(CASE WHEN DSC_TP_ID in (16030158,16030265) THEN 1 ELSE 0 END)            AS NBR_LN_DRDWNTXN        --���շ��Ŵ������ 16030265:����ſ� 16030158ժҪ����ͣ��
        	 ,SUM(CASE WHEN DSC_TP_ID IN (16030156,16030006,16030272,16030274,16030269,16030271) THEN 1 ELSE 0 END)                     AS NBR_LN_RCVD_TXN        --�����ջش������  16030156:����,16030006:���� 16030156,16030006ժҪ����ͣ��
           ,SUM(CASE WHEN CR_DB_IND=14280002  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT ELSE 0 END)         AS CUR_CR_AMT             --���������� 14280002:����
           ,SUM(CASE WHEN CR_DB_IND=14280001  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT ELSE 0 END)         AS CUR_DB_AMT             --�跽������ 14280001,��
           ,SUM(CASE WHEN DSC_TP_ID IN (16030165,16030282,16030283,16030284,16030285)  AND JRL_AC_F=17320001 THEN INT_OFF_BSH+CMPD_INT_BSH + CMPD_INT_OFF_BSH + CRN_PR_INT_AMT ELSE 0 END )  AS INT_INCM_AMT_DEBT_AST		         --��ծ�ʲ���ծ��Ϣ���� 16030282:��ծ���� 16030283:��ծ��Ϣ 16030284:��ծ��Ϣ 16030285:��ծ����  ���� 16030165ժҪ����ͣ��
		       ,SUM(CASE WHEN DSC_TP_ID IN (16030161,16030286,16030287,16030288)  AND JRL_AC_F=17320001 THEN INT_OFF_BSH+CMPD_INT_BSH + CMPD_INT_OFF_BSH + CRN_PR_INT_AMT ELSE 0 END )  AS INT_INCM_RTND_WRTOF_LN	         --���������ջ���Ϣ  16030286:�պ������� 16030287:�պ�����Ϣ 16030288:�պ�����Ϣ	����		 16030161ժҪ����ͣ��
           ,SUM(CASE WHEN DSC_TP_ID IN (16030161,16030105,16030286,16030287,16030288) AND CR_DB_IND= 14280002  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT  ELSE 0 END) AS CUR_WRTOF_AMT_RCVD            --�����ջغ������ -- 16030286:�պ������� 16030287:�պ�����Ϣ 16030288:�պ�����Ϣ  16030105:��ת 16030161,16030105ժҪ����ͣ��
           ,SUM(CASE WHEN ((DSC_TP_ID =16030157 AND  DSC like ('ר��Ʊ���ʲ�%')) OR (DSC_TP_ID IN (16030273,16030274,16030270,16030271))) AND CR_DB_IND= 14280002  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT  ELSE 0 END) AS CUR_AMT_RCVD_Of_AST_RPLC      --�����ջ��û��ʲ����  16030273:�Զ�����Ϣ 16030274:�Զ��ձ�Ϣ 16030270:��������Ϣ 16030271:�����ձ�Ϣ 16030157ժҪ����ͣ��
           ,SUM(CASE WHEN DSC_TP_ID IN (16030063,16030279,16030280,16030281) AND CR_DB_IND= 14280002  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT  ELSE 0 END) AS CUR_WRTOF_AMT                 --���������� 16030279:�������� 16030280:������Ϣ 16030281:������Ϣ 16030063ժҪ����ͣ��
  	 	 FROM  SOR.LN_TXN_DTL_INF
  	 	 GROUP BY 
  	 	 				 CTR_AR_ID,
  	 	 				 CTR_SEQ_NBR
) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K 
     PARTITIONING KEY(CTR_AR_ID,CTR_SEQ_NBR)
   ;--
----add by wuzhansan begin
CREATE UNIQUE INDEX SESSION.IDX_TMP_LN_TXN ON SESSION.TMP_LN_TXN(CTR_AR_ID,CTR_SEQ_NBR);
----add by wuzhansan end

 /*������ʱ��*/
 INSERT INTO  SESSION.TMP_LN_TXN
  	   SELECT  
  	 	 			 CTR_AR_ID   AS CTR_AR_ID        --��ͬ��     
  	 	 			,CTR_SEQ_NBR AS CTR_SEQ_NBR  --��ͬ���
           ,SUM(CASE WHEN INT_BSH >0 AND INT_OFF_BSH + INT_BSH <>0  AND JRL_AC_F=17320001 THEN INT_BSH  WHEN JRL_AC_F=17320003 THEN INT_BSH*-1 ELSE 0 END )  AS ON_BST_INT_AMT_RCVD      --���ձ���ʵ����Ϣ
           ,SUM(CASE WHEN INT_OFF_BSH >0 AND INT_OFF_BSH + INT_BSH <>0 AND JRL_AC_F=17320001 THEN INT_OFF_BSH ELSE 0 END )  AS OFF_BST_INT_AMT_RCVD     --���ձ���ʵ����Ϣ
           -- start of modification on 2011-08-11
           /* , SUM(( CASE WHEN DSC_TP_ID NOT IN (16030161,16030165) AND JRL_AC_F IN (17320001,17320001)  THEN CMPD_INT_BSH + CMPD_INT_OFF_BSH + CRN_PR_INT_AMT ELSE 0 END )  --16030161:�����ջ�5106,16030165:��ծ�ʲ���ծ5110 
           - ( CASE WHEN INT_BSH < 0  AND JRL_AC_F=17320001 THEN INT_BSH ELSE 0 END ) 
           + ( CASE WHEN INT_OFF_BSH > 0  AND DSC_TP_ID NOT IN (16030161,16030165)  AND JRL_AC_F=17320001 THEN INT_OFF_BSH ELSE 0 END )  --16030161:�����ջ�5106,16030165:��ծ�ʲ���ծ5110 
           - ( CASE WHEN INT_BSH > 0 AND INT_BSH+INT_OFF_BSH=0  AND JRL_AC_F=17320001 THEN INT_BSH ELSE 0 END ) 
           - ( CASE WHEN JRL_AC_F=17320003  THEN INT_BSH ELSE 0 END) )
           	                         AS AMT_OF_INT_INCM        --��Ϣ���� -- ���յ� */
           , SUM(( CASE WHEN DSC_TP_ID NOT IN (16030161,16030165,16030286,16030287,16030288,16030282,16030283,16030284,16030285) AND JRL_AC_F = 17320001  THEN CMPD_INT_BSH + CMPD_INT_OFF_BSH + CRN_PR_INT_AMT ELSE 0 END )   -- 16030286:�պ������� 16030287:�պ�����Ϣ 16030288:�պ�����Ϣ 16030282:��ծ���� 16030283:��ծ��Ϣ 16030284:��ծ��Ϣ 16030285:��ծ���� 16030161,16030165ժҪ����ͣ��
           - ( CASE WHEN INT_BSH < 0  AND JRL_AC_F=17320001 THEN INT_BSH ELSE 0 END ) 
           + ( CASE WHEN INT_OFF_BSH > 0  AND DSC_TP_ID NOT IN (16030161,16030165,16030286,16030287,16030288,16030282,16030283,16030284,16030285)  AND JRL_AC_F=17320001 THEN INT_OFF_BSH ELSE 0 END )   -- 16030286:�պ������� 16030287:�պ�����Ϣ 16030288:�պ�����Ϣ 16030282:��ծ���� 16030283:��ծ��Ϣ 16030284:��ծ��Ϣ 16030285:��ծ���� 16030161,16030165ժҪ����ͣ��
           - ( CASE WHEN INT_BSH > 0 AND INT_BSH+INT_OFF_BSH=0  AND JRL_AC_F=17320001 THEN INT_BSH ELSE 0 END ) 
           - ( CASE WHEN JRL_AC_F=17320003  THEN INT_BSH ELSE 0 END) )
           	                         AS AMT_OF_INT_INCM        --��Ϣ���� -- ���յ�
           --------------------------Start of modification on 2009-11-18-------------------------------------------------------------------------------------------------           	                         
           --,SUM(CASE WHEN DSC_TP_ID IN (16030006,16030158) THEN TXN_PNP_AMT ELSE 0 END)  AS LN_DRDWN_AMT           --�����۷Ž��  16030006:����,16030158:���           	
           /* ,SUM(CASE WHEN DSC_TP_ID = 16030158 AND JRL_AC_F=17320001  THEN TXN_PNP_AMT ELSE 0 END)  AS LN_DRDWN_AMT           --�����۷Ž��  16030158:��� */
           ,SUM(CASE WHEN DSC_TP_ID in (16030158,16030265) AND JRL_AC_F=17320001  THEN TXN_PNP_AMT ELSE 0 END)  AS LN_DRDWN_AMT           --�����۷Ž�� 16030265: ����ſ� 16030158ժҪ����ͣ��
           --,SUM(CASE WHEN DSC_TP_ID = 16030156 THEN TXN_PNP_AMT ELSE 0 END)       AS AMT_LN_REPYMT_RCVD     --�������ս��     16030156:����           	
           /* ,SUM(CASE WHEN DSC_TP_ID IN (16030156,16030006)  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT ELSE 0 END)       AS AMT_LN_REPYMT_RCVD     --�������ս��     16030156:����,16030006:���� */
           ,SUM(CASE WHEN DSC_TP_ID IN (16030156,16030006,16030272,16030274,16030269,16030271)  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT ELSE 0 END)       AS AMT_LN_REPYMT_RCVD     --�������ս��   16030272:�Զ��ձ��� 16030274:�Զ��ձ�Ϣ 16030269:�����ձ��� 16030271:�����ձ�Ϣ 16030156,16030006ժҪ����ͣ��
           --,SUM(CASE WHEN DSC_TP_ID IN (16030006,16030158) THEN 1 ELSE 0 END)            AS NBR_LN_RCVD_TXN        --�����ջش������ 16030006:����,16030158:���
       ----------------------------------Start on 2009-12-16----------------------------------------------------------------------------------------------------------------------
       /*    ,SUM(CASE WHEN DSC_TP_ID = 16030158 THEN 1 ELSE 0 END)            AS NBR_LN_RCVD_TXN        --�����ջش������ 16030158:���
           --,SUM(CASE WHEN DSC_TP_ID IN (16030156) THEN 1 ELSE 0 END)                     AS NBR_LN_DRDWNTXN        --���շ��Ŵ������  16030156:����
           ,SUM(CASE WHEN DSC_TP_ID IN (16030156,16030006) THEN 1 ELSE 0 END)                     AS NBR_LN_DRDWNTXN        --���շ��Ŵ������  16030156:����,16030006:����
           --------------------------End of modification on 2009-11-18-------------------------------------------------------------------------------------------------
       */
        	 /* ,SUM(CASE WHEN DSC_TP_ID = 16030158 THEN 1 ELSE 0 END)            AS NBR_LN_DRDWNTXN        --���շ��Ŵ������ 16030158:��� */
        	 ,SUM(CASE WHEN DSC_TP_ID in (16030158,16030265) THEN 1 ELSE 0 END)            AS NBR_LN_DRDWNTXN        --���շ��Ŵ������ 16030265:����ſ� 16030158ժҪ����ͣ��
        	 /* ,SUM(CASE WHEN DSC_TP_ID IN (16030156,16030006) THEN 1 ELSE 0 END)                     AS NBR_LN_RCVD_TXN        --�����ջش������  16030156:����,16030006:���� */
        	 ,SUM(CASE WHEN DSC_TP_ID IN (16030156,16030006,16030272,16030274,16030269,16030271) THEN 1 ELSE 0 END)                     AS NBR_LN_RCVD_TXN        --�����ջش������  16030156:����,16030006:���� 16030156,16030006ժҪ����ͣ��
			 ----------------------------------End on 2009-12-16----------------------------------------------------------------------------------------------------------------------           
           ,SUM(CASE WHEN CR_DB_IND=14280002  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT ELSE 0 END)         AS CUR_CR_AMT             --���������� 14280002:����
           ,SUM(CASE WHEN CR_DB_IND=14280001  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT ELSE 0 END)         AS CUR_DB_AMT             --�跽������ 14280001,��
           /* ,SUM(CASE WHEN DSC_TP_ID  =16030165  AND JRL_AC_F=17320001 THEN INT_OFF_BSH+CMPD_INT_BSH + CMPD_INT_OFF_BSH + CRN_PR_INT_AMT ELSE 0 END )  AS INT_INCM_AMT_DEBT_AST		         --��ծ�ʲ���ծ��Ϣ���� 16030165:��ծ�ʲ���ծ5110  ���� */
           ,SUM(CASE WHEN DSC_TP_ID IN (16030165,16030282,16030283,16030284,16030285)  AND JRL_AC_F=17320001 THEN INT_OFF_BSH+CMPD_INT_BSH + CMPD_INT_OFF_BSH + CRN_PR_INT_AMT ELSE 0 END )  AS INT_INCM_AMT_DEBT_AST		         --��ծ�ʲ���ծ��Ϣ���� 16030282:��ծ���� 16030283:��ծ��Ϣ 16030284:��ծ��Ϣ 16030285:��ծ����  ���� 16030165ժҪ����ͣ��
		       /* ,SUM(CASE WHEN DSC_TP_ID  =16030161  AND JRL_AC_F=17320001 THEN INT_OFF_BSH+CMPD_INT_BSH + CMPD_INT_OFF_BSH + CRN_PR_INT_AMT ELSE 0 END )  AS INT_INCM_RTND_WRTOF_LN	         --���������ջ���Ϣ  16030161:�����ջ�5106	���� */
		       ,SUM(CASE WHEN DSC_TP_ID IN (16030161,16030286,16030287,16030288)  AND JRL_AC_F=17320001 THEN INT_OFF_BSH+CMPD_INT_BSH + CMPD_INT_OFF_BSH + CRN_PR_INT_AMT ELSE 0 END )  AS INT_INCM_RTND_WRTOF_LN	         --���������ջ���Ϣ  16030286:�պ������� 16030287:�պ�����Ϣ 16030288:�պ�����Ϣ	����		 16030161ժҪ����ͣ��
           /* ,SUM(CASE WHEN DSC_TP_ID IN (16030161,16030105 ) AND CR_DB_IND= 14280002  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT  ELSE 0 END) AS CUR_WRTOF_AMT_RCVD            --�����ջغ������ 16030161:�Ѻ��������ջ�  16030105:��ת */
           ,SUM(CASE WHEN DSC_TP_ID IN (16030161,16030105,16030286,16030287,16030288) AND CR_DB_IND= 14280002  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT  ELSE 0 END) AS CUR_WRTOF_AMT_RCVD            --�����ջغ������ -- 16030286:�պ������� 16030287:�պ�����Ϣ 16030288:�պ�����Ϣ  16030105:��ת 16030161,16030105ժҪ����ͣ��
           /* ,SUM(CASE WHEN DSC_TP_ID  =16030157 AND CR_DB_IND= 14280002 AND  DSC like ('ר��Ʊ���ʲ�%')  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT  ELSE 0 END) AS CUR_AMT_RCVD_Of_AST_RPLC      --�����ջ��û��ʲ����  16030157:��Ϣ */
           ,SUM(CASE WHEN ((DSC_TP_ID =16030157 AND  DSC like ('ר��Ʊ���ʲ�%')) OR (DSC_TP_ID IN (16030273,16030274,16030270,16030271))) AND CR_DB_IND= 14280002  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT  ELSE 0 END) AS CUR_AMT_RCVD_Of_AST_RPLC      --�����ջ��û��ʲ����  16030273:�Զ�����Ϣ 16030274:�Զ��ձ�Ϣ 16030270:��������Ϣ 16030271:�����ձ�Ϣ 16030157ժҪ����ͣ��
           /* ,SUM(CASE WHEN DSC_TP_ID  =16030063 AND CR_DB_IND= 14280002  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT  ELSE 0 END) AS CUR_WRTOF_AMT                 --���������� 16030063:���� */
           ,SUM(CASE WHEN DSC_TP_ID IN (16030063,16030279,16030280,16030281) AND CR_DB_IND= 14280002  AND JRL_AC_F=17320001 THEN TXN_PNP_AMT  ELSE 0 END) AS CUR_WRTOF_AMT                 --���������� 16030279:�������� 16030280:������Ϣ 16030281:������Ϣ 16030063ժҪ����ͣ��
  	 	 		-- end of modification on 2011-08-11
  	 	 FROM  SOR.LN_TXN_DTL_INF
  	 	 WHERE    TXN_DT = ACCOUNTING_DATE   -- ���ջ������
            AND DEL_F=0
  	 	 GROUP BY 
  	 	 				 CTR_AR_ID,
  	 	 				 CTR_SEQ_NBR
  	 	;--
 /*  */ 	 	
 		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		SET SMY_STEPNUM = SMY_STEPNUM + 1;--
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	  --
	   	 	
		
		SET SMY_STEPDESC = '������ʱ��SESSION.TMP_LN_INT_INF, �������в��� ��LN_AR_ID �����SOR.LN_INT_INF�е�����'; --
  	 	
DECLARE GLOBAL TEMPORARY TABLE TMP_LN_INT_INF AS 
  (  	 	 	 
  		SELECT 
            LN_AR_ID
           ,SUM(CASE WHEN ON_OFF_BSH_IND=1 AND STL_FLG = 15430007 THEN BAL_AMT ELSE 0 END )  AS ON_BST_INT_RCVB         --����Ӧ��δ����Ϣ 1:����; 15430007,δ����
           ,SUM(CASE WHEN ON_OFF_BSH_IND=0 AND STL_FLG = 15430007 THEN BAL_AMT ELSE 0 END )  AS OFF_BST_INT_RCVB        --����Ӧ��δ����Ϣ 0:����; 15430007:δ����
           ,SUM(CASE WHEN ON_OFF_BSH_IND=0 AND STL_FLG = 15430009 THEN BAL_AMT ELSE 0 END )  AS OFF_BST_INT_RCVB_WRTOF  --����Ӧ����Ϣ������� 0:����;15430009:����
           ,SUM(CASE WHEN ON_OFF_BSH_IND=0 AND STL_FLG = 15430010 THEN BAL_AMT ELSE 0 END )  AS OFF_BST_INT_RCVB_RPLC	  --����Ӧ����Ϣ�û���� 0:����;15430010:�û�  	 	 
  	 	   FROM SOR.LN_INT_INF  
  	 	   WHERE DEL_F=0	 	   
  	 	   GROUP BY 
  	 	   		  LN_AR_ID	 
  ) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K 
     PARTITIONING KEY(LN_AR_ID);--
----add by wuzhansan begin
CREATE UNIQUE INDEX SESSION.IDX_TMP_LN_INT_INF ON SESSION.TMP_LN_INT_INF(LN_AR_ID);
----add by wuzhansan end
/**/

 INSERT INTO SESSION.TMP_LN_INT_INF  	 	
  	 	 	 SELECT 
  	 	 	LN_AR_ID
           ,SUM(CASE WHEN ON_OFF_BSH_IND=1 AND STL_FLG = 15430007 THEN BAL_AMT ELSE 0 END )  AS ON_BST_INT_RCVB         --����Ӧ��δ����Ϣ 1:����; 15430007,δ����
           ,SUM(CASE WHEN ON_OFF_BSH_IND=0 AND STL_FLG = 15430007 THEN BAL_AMT ELSE 0 END )  AS OFF_BST_INT_RCVB        --����Ӧ��δ����Ϣ 0:����; 15430007:δ����
           ,SUM(CASE WHEN ON_OFF_BSH_IND=0 AND STL_FLG = 15430009 THEN BAL_AMT ELSE 0 END )  AS OFF_BST_INT_RCVB_WRTOF  --����Ӧ����Ϣ������� 0:����;15430009:����
           ,SUM(CASE WHEN ON_OFF_BSH_IND=0 AND STL_FLG = 15430010 THEN BAL_AMT ELSE 0 END )  AS OFF_BST_INT_RCVB_RPLC	  --����Ӧ����Ϣ�û���� 0:����;15430010:�û�  	 	 
  	 	   FROM SOR.LN_INT_INF
  	 	   WHERE DEL_F=0  	 	   
  	 	   GROUP BY 
  	 	   	    LN_AR_ID	 
 ;--
 /** Insert the log**/
 		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		SET SMY_STEPNUM = SMY_STEPNUM + 1;--
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	 --
		
		SET SMY_STEPDESC = '������ʱ��SESSION.CUR, ���SMY_LN_AR_INT_SMY ���ջ��ܺ������'; 		--
		
/* ��ǰ���µ�����ͳ��*/
DECLARE GLOBAL TEMPORARY TABLE CUR AS (
  	 	 	 SELECT 
            LOAN_AR_SMY.CTR_AR_ID             AS CTR_AR_ID          --��ͬ��
           ,LOAN_AR_SMY.CTR_ITM_ORDR_ID       AS CTR_ITM_ORDR_ID    --��ͬ���
           ,LOAN_AR_SMY.LN_AR_ID       AS LN_AR_ID                  --�˻���
           ,LOAN_AR_SMY.DNMN_CCY_ID    AS DNMN_CCY_ID               --����ID
           ,COALESCE(LN_TXN.ON_BST_INT_AMT_RCVD   ,0)                   AS ON_BST_INT_AMT_RCVD   --����ʵ����Ϣ
           ,COALESCE(LN_TXN.OFF_BST_INT_AMT_RCVD  ,0)                   AS OFF_BST_INT_AMT_RCVD  --����ʵ����Ϣ
           ,COALESCE(LN_INT_INF.ON_BST_INT_RCVB   ,0)                   AS ON_BST_INT_RCVB           --����Ӧ��δ����Ϣ
           ,COALESCE(LN_INT_INF.OFF_BST_INT_RCVB  ,0)                   AS OFF_BST_INT_RCVB          --����Ӧ��δ����Ϣ
           ,COALESCE(LN_TXN.AMT_OF_INT_INCM      ,0)                    AS AMT_OF_INT_INCM   --��Ϣ����
           ,COALESCE(LN_TXN.LN_DRDWN_AMT         ,0)                    AS LN_DRDWN_AMT              --�����۷Ž��
           ,COALESCE(LN_TXN.AMT_LN_REPYMT_RCVD   ,0)                    AS AMT_LN_REPYMT_RCVD        --�������ս��
           ,COALESCE(LN_TXN.NBR_LN_RCVD_TXN      ,0)                    AS NBR_LN_RCVD_TXN           --���ۼ��ջش������
           ,COALESCE(LN_TXN.NBR_LN_DRDWNTXN      ,0)                    AS NBR_LN_DRDWN_TXN           --���ۼƷ��Ŵ������
           ,COALESCE(LN_TXN.CUR_CR_AMT           ,0)                    AS CUR_CR_AMT                --����������
           ,COALESCE(LN_TXN.CUR_DB_AMT           ,0)                    AS CUR_DB_AMT                --�跽������
           ,COALESCE(LN_INT_INF.OFF_BST_INT_RCVB_WRTOF  ,0)             AS OFF_BST_INT_RCVB_WRTOF    --����Ӧ����Ϣ�������
           ,COALESCE(LN_INT_INF.OFF_BST_INT_RCVB_RPLC	  ,0)            AS OFF_BST_INT_RCVB_RPLC	   --����Ӧ����Ϣ�û����
           ,COALESCE(LN_TXN.INT_INCM_AMT_DEBT_AST	   ,0)               AS INT_INCM_AMT_DEBT_AST		 --��ծ�ʲ���ծ��Ϣ����
					 ,COALESCE(LN_TXN.INT_INCM_RTND_WRTOF_LN   ,0)                AS INT_INCM_RTND_WRTOF_LN	   --���������ջ���Ϣ
					 ,LOAN_AR_SMY.LN_BAL                             AS BAL_AMT
					 ,1   AS CUR_AR_FLAG --�˻��Ƿ�����
           ,COALESCE(LN_TXN.CUR_WRTOF_AMT_RCVD        ,0)               AS CUR_WRTOF_AMT_RCVD            --�����ջغ������
           ,COALESCE(LN_TXN.CUR_AMT_RCVD_Of_AST_RPLC  ,0)               AS CUR_AMT_RCVD_Of_AST_RPLC      --�����ջ��û��ʲ����
           ,COALESCE(LN_TXN.CUR_WRTOF_AMT             ,0)               AS CUR_WRTOF_AMT                 --����������					 
   	   FROM		SMY.LOAN_AR_SMY    AS LOAN_AR_SMY        
        	LEFT OUTER JOIN	SESSION.TMP_LN_TXN AS LN_TXN   ON LOAN_AR_SMY.CTR_AR_ID = LN_TXN.CTR_AR_ID    AND LOAN_AR_SMY.CTR_ITM_ORDR_ID = LN_TXN.CTR_SEQ_NBR
        	LEFT OUTER JOIN SESSION.TMP_LN_INT_INF  AS LN_INT_INF ON LOAN_AR_SMY.LN_AR_ID  = LN_INT_INF.LN_AR_ID   -- LOAN_AR_SMY.LN_AR_ID ����Ϊ��
--				 WHERE LOAN_AR_SMY.AR_LCS_TP_ID= 13360003 -- ����	 	 
  ) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K 
     PARTITIONING KEY(CTR_AR_ID,CTR_ITM_ORDR_ID) ;--
     
----add by wuzhansan begin
CREATE  INDEX SESSION.IDX_CUR ON SESSION.CUR(CTR_AR_ID,CTR_ITM_ORDR_ID);
----add by wuzhansan end

/*����LOAN_AR_SMY ��ʱ�������*/
	DECLARE GLOBAL TEMPORARY TABLE TMP_LOAN_AR_SMY 
		LIKE SMY.LOAN_AR_SMY
	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(CTR_AR_ID,CTR_ITM_ORDR_ID);--
	
	CREATE INDEX SESSION.IDX_LN_AR_ID ON SESSION.TMP_LOAN_AR_SMY(LN_AR_ID);--
----add by wuzhansan begin
	CREATE INDEX SESSION.IDX_CTR_AR_ID ON SESSION.TMP_LOAN_AR_SMY(CTR_AR_ID,CTR_ITM_ORDR_ID);
----add by wuzhansan end
	
	INSERT INTO SESSION.TMP_LOAN_AR_SMY SELECT * FROM SMY.LOAN_AR_SMY;--
     
/**/
INSERT INTO SESSION.CUR
  	 	 	 SELECT 
            LOAN_AR_SMY.CTR_AR_ID             AS CTR_AR_ID          --��ͬ��
           ,LOAN_AR_SMY.CTR_ITM_ORDR_ID       AS CTR_ITM_ORDR_ID    --��ͬ���
           ,LOAN_AR_SMY.LN_AR_ID       AS LN_AR_ID                  --�˻���
           ,LOAN_AR_SMY.DNMN_CCY_ID    AS DNMN_CCY_ID               --����ID
           ,COALESCE(LN_TXN.ON_BST_INT_AMT_RCVD        ,0)             AS ON_BST_INT_AMT_RCVD   --����ʵ����Ϣ
           ,COALESCE(LN_TXN.OFF_BST_INT_AMT_RCVD       ,0)             AS OFF_BST_INT_AMT_RCVD  --����ʵ����Ϣ
           ,COALESCE(LN_INT_INF.ON_BST_INT_RCVB        ,0)             AS ON_BST_INT_RCVB           --����Ӧ��δ����Ϣ
           ,COALESCE(LN_INT_INF.OFF_BST_INT_RCVB       ,0)             AS OFF_BST_INT_RCVB          --����Ӧ��δ����Ϣ
           ,COALESCE(LN_TXN.AMT_OF_INT_INCM            ,0)             AS AMT_OF_INT_INCM   --��Ϣ����
           ,COALESCE(LN_TXN.LN_DRDWN_AMT               ,0)             AS LN_DRDWN_AMT              --�����۷Ž��
           ,COALESCE(LN_TXN.AMT_LN_REPYMT_RCVD         ,0)             AS AMT_LN_REPYMT_RCVD        --�������ս��
           ,COALESCE(LN_TXN.NBR_LN_RCVD_TXN            ,0)             AS NBR_LN_RCVD_TXN           --���ۼ��ջش������
           ,COALESCE(LN_TXN.NBR_LN_DRDWNTXN            ,0)             AS NBR_LN_DRDWN_TXN           --���ۼƷ��Ŵ������
           ,COALESCE(LN_TXN.CUR_CR_AMT                 ,0)             AS CUR_CR_AMT                --����������
           ,COALESCE(LN_TXN.CUR_DB_AMT                 ,0)             AS CUR_DB_AMT                --�跽������
           ,COALESCE(LN_INT_INF.OFF_BST_INT_RCVB_WRTOF ,0)             AS OFF_BST_INT_RCVB_WRTOF    --����Ӧ����Ϣ�������
           ,COALESCE(LN_INT_INF.OFF_BST_INT_RCVB_RPLC	 ,0)            AS OFF_BST_INT_RCVB_RPLC	   --����Ӧ����Ϣ�û����
           ,COALESCE(LN_TXN.INT_INCM_AMT_DEBT_AST	     ,0)            AS INT_INCM_AMT_DEBT_AST		 --��ծ�ʲ���ծ��Ϣ����
					 ,COALESCE(LN_TXN.INT_INCM_RTND_WRTOF_LN     ,0)             AS INT_INCM_RTND_WRTOF_LN	   --���������ջ���Ϣ
					 ,COALESCE(LOAN_AR_SMY.LN_BAL   ,0)                          AS BAL_AMT
					 --------------------------------------Start on 20100106------------------------------------------
					 --,1                                                         AS CUR_AR_FLAG --�˻��Ƿ�����	
					,case when LOAN_AR_SMY.END_DT=SMY_DATE then 0 else 1 end     AS CUR_AR_FLAG --�˻��Ƿ�����	
					 --------------------------------------End on 20100106------------------------------------------
           ,COALESCE(LN_TXN.CUR_WRTOF_AMT_RCVD          ,0)                       AS CUR_WRTOF_AMT_RCVD            --�����ջغ������
           ,COALESCE(LN_TXN.CUR_AMT_RCVD_Of_AST_RPLC    ,0)                       AS CUR_AMT_RCVD_Of_AST_RPLC      --�����ջ��û��ʲ����
           ,COALESCE(LN_TXN.CUR_WRTOF_AMT               ,0)                       AS CUR_WRTOF_AMT                 --����������						 				 
   	   FROM		SESSION.TMP_LOAN_AR_SMY   AS LOAN_AR_SMY        
        	LEFT OUTER JOIN	SESSION.TMP_LN_TXN AS LN_TXN   ON LOAN_AR_SMY.CTR_AR_ID = LN_TXN.CTR_AR_ID    AND LOAN_AR_SMY.CTR_ITM_ORDR_ID = LN_TXN.CTR_SEQ_NBR
        	LEFT OUTER JOIN SESSION.TMP_LN_INT_INF  AS LN_INT_INF ON LOAN_AR_SMY.LN_AR_ID  = LN_INT_INF.LN_AR_ID   -- LOAN_AR_SMY.LN_AR_ID ����Ϊ��
				 -------------------------Start on 20100119----------------------------------------------------------------
				 --Remove all the conditional statements on the AR_LCS_TP_ID
				 /*
				 WHERE LOAN_AR_SMY.AR_LCS_TP_ID= 13360003 -- �˻�״̬���ͣ�����	
				       --------------------------------------Start on 20100106------------------------------------------
				       --�����������ļ�¼Ҳ��������
				       or
				       LOAN_AR_SMY.END_DT = SMY_DATE
				       --------------------------------------End on 20100106------------------------------------------
         */
         -------------------------End on 20100119----------------------------------------------------------------				       
    ;--
  
 /** Insert the log**/

 		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		SET SMY_STEPNUM = SMY_STEPNUM + 1;--
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	     --
	 
DECLARE GLOBAL TEMPORARY TABLE TMP_TMP AS 
(
    SELECT
          CTR_AR_ID
          ,CTR_ITM_ORDR_ID
          ,CDR_YR
          ,CDR_MTH
	        ,ACG_DT
          ,LN_AR_ID
          ,DNMN_CCY_ID
          ,YTD_ON_BST_INT_AMT_RCVD
          ,YTD_OFF_BST_INT_AMT_RCVD
          ,ON_BST_INT_RCVB
          ,OFF_BST_INT_RCVB
          ,TOT_YTD_AMT_OF_INT_INCM
          ,LN_DRDWN_AMT
          ,AMT_LN_REPYMT_RCVD
          ,TOT_MTD_LN_DRDWN_AMT
          ,TOT_QTD_LN_DRDWN_AMT
          ,TOT_YTD_LN_DRDWN_AMT
          ,TOT_MTD_AMT_LN_REPYMT_RCVD
          ,TOT_QTD_AMT_LN_RPYMT_RCVD
          ,TOT_YTD_AMT_LN_REPYMT_RCVD
          ,TOT_MTD_NBR_LN_RCVD_TXN
          ,TOT_QTD_NBR_LN_RCVD_TXN
          ,TOT_YTD_NBR_LN_RCVD_TXN
          ,TOT_MTD_NBR_LN_DRDWNTXN
          ,TOT_QTD_NBR_LN_DRDWN_TXN
          ,TOT_YTD_NBR_LN_DRDWN_TXN
          ,CUR_CR_AMT
          ,CUR_DB_AMT
          ,TOT_MTD_CR_AMT
          ,TOT_MTD_DB_AMT
          ,TOT_QTD_DB_AMT
          ,TOT_QTD_CR_AMT
          ,TOT_YTD_CR_AMT
          ,TOT_YTD_DB_AMT
          ,OFF_BST_INT_RCVB_WRTOF
          ,OFF_BST_INT_RCVB_RPLC
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN
					,BAL_AMT
          ,MTD_ACML_BAL_AMT
          ,QTD_ACML_BAL_AMT
          ,YTD_ACML_BAL_AMT
          ,NOCLD_In_MTH
          ,NOD_IN_MTH
          ,NOCLD_IN_QTR
          ,NOD_IN_QTR
          ,NOCLD_IN_YEAR
          ,NOD_IN_YEAR
          ,CUR_WRTOF_AMT_RCVD
          ,CUR_AMT_RCVD_Of_AST_RPLC
          ,CUR_WRTOF_AMT
          ,TOT_MTD_WRTOF_AMT_RCVD
          ,TOT_MTD_AMT_RCVD_Of_AST_RPLC
          ,TOT_MTD_WRTOF_AMT
          ,TOT_QTD_WRTOF_AMT_RCVD
          ,TOT_QTD_AMT_RCVD_Of_AST_RPLC
          ,TOT_QTD_WRTOF_AMT
          ,TOT_YTD_WRTOF_AMT_RCVD
          ,TOT_YTD_AMT_RCVD_Of_AST_RPLC
          ,TOT_YTD_WRTOF_AMT
    FROM SMY.LN_AR_INT_MTHLY_SMY
)DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(CTR_AR_ID,CTR_ITM_ORDR_ID);

CREATE INDEX SESSION.IDX_TMP_TMP ON SESSION.TMP_TMP(CTR_AR_ID,CTR_ITM_ORDR_ID,CDR_YR,CDR_MTH);

INSERT INTO SESSION.TMP_TMP
SELECT  
           CUR.CTR_AR_ID    AS CTR_AR_ID                     --��ͬ��             
          ,CUR.CTR_ITM_ORDR_ID   AS CTR_ITM_ORDR_ID          --��ͬ���            
          ,CUR_YEAR   AS CDR_YR                              --������              
          ,CUR_MONTH  AS CDR_MTH                             --������              
          ,ACCOUNTING_DATE AS ACG_DT                         --�������            
          ,CUR.LN_AR_ID  AS   LN_AR_ID                       --�˻���              
          ,CUR.DNMN_CCY_ID AS DNMN_CCY_ID                    --����ID              
          ,COALESCE(PRE.YTD_ON_BST_INT_AMT_RCVD,0)  + CUR.ON_BST_INT_AMT_RCVD   AS YTD_ON_BST_INT_AMT_RCVD      --����ʵ����Ϣ        
          ,COALESCE(PRE.YTD_OFF_BST_INT_AMT_RCVD ,0)  + CUR.OFF_BST_INT_AMT_RCVD  AS YTD_OFF_BST_INT_AMT_RCVD   --����ʵ����Ϣ        
          ,CUR.ON_BST_INT_RCVB                   AS ON_BST_INT_RCVB  --����Ӧ��δ����Ϣ    
          ,CUR.OFF_BST_INT_RCVB                  AS OFF_BST_INT_RCVB --����Ӧ��δ����Ϣ    
          ,COALESCE(PRE.TOT_YTD_AMT_OF_INT_INCM,0) + CUR.AMT_OF_INT_INCM   AS TOT_YTD_AMT_OF_INT_INCM       --��Ϣ����            
          ,CUR.LN_DRDWN_AMT                      AS LN_DRDWN_AMT        --�����۷Ž��        
          ,CUR.AMT_LN_REPYMT_RCVD                AS AMT_LN_REPYMT_RCVD  --�������ս��        
          ,COALESCE(PRE.TOT_MTD_LN_DRDWN_AMT  ,0) + CUR.LN_DRDWN_AMT AS TOT_MTD_LN_DRDWN_AMT             --�´����ۼƷ��Ž��  
          ,COALESCE(PRE.TOT_QTD_LN_DRDWN_AMT  ,0) + CUR.LN_DRDWN_AMT AS TOT_QTD_LN_DRDWN_AMT           --�������ۼƷ��Ž��  
          ,COALESCE(PRE.TOT_YTD_LN_DRDWN_AMT  ,0) + CUR.LN_DRDWN_AMT AS TOT_YTD_LN_DRDWN_AMT           --������ۼƷ��Ž��  
          ,COALESCE(PRE.TOT_MTD_AMT_LN_REPYMT_RCVD  ,0) + CUR.AMT_LN_REPYMT_RCVD  AS TOT_MTD_AMT_LN_REPYMT_RCVD    --���ۼ��ջش�����  
          ,COALESCE(PRE.TOT_QTD_AMT_LN_RPYMT_RCVD   ,0) + CUR.AMT_LN_REPYMT_RCVD  AS TOT_QTD_AMT_LN_RPYMT_RCVD    --���ۼ��ջش�����  
          ,COALESCE(PRE.TOT_YTD_AMT_LN_REPYMT_RCVD  ,0) + CUR.AMT_LN_REPYMT_RCVD  AS TOT_YTD_AMT_LN_REPYMT_RCVD   --���ۼ��ջش�����  
          ,COALESCE(PRE.TOT_MTD_NBR_LN_RCVD_TXN  ,0) + CUR.NBR_LN_RCVD_TXN        AS TOT_MTD_NBR_LN_RCVD_TXN       --���ۼ��ջش������  
          ,COALESCE(PRE.TOT_QTD_NBR_LN_RCVD_TXN  ,0) + CUR.NBR_LN_RCVD_TXN        AS TOT_QTD_NBR_LN_RCVD_TXN     --���ۼ��ջش������  
          ,COALESCE(PRE.TOT_YTD_NBR_LN_RCVD_TXN  ,0) + CUR.NBR_LN_RCVD_TXN        AS TOT_YTD_NBR_LN_RCVD_TXN --���ۼ��ջش������  
          ,COALESCE(PRE.TOT_MTD_NBR_LN_DRDWNTXN  ,0) + CUR.NBR_LN_DRDWN_TXN       AS TOT_MTD_NBR_LN_DRDWNTXN   --���ۼƷ��Ŵ������  
          ,COALESCE(PRE.TOT_QTD_NBR_LN_DRDWN_TXN ,0) + CUR.NBR_LN_DRDWN_TXN       AS TOT_QTD_NBR_LN_DRDWN_TXN  --���ۼƷ��Ŵ������  
          ,COALESCE(PRE.TOT_YTD_NBR_LN_DRDWN_TXN ,0) + CUR.NBR_LN_DRDWN_TXN       AS TOT_YTD_NBR_LN_DRDWN_TXN --���ۼƷ��Ŵ������  
          ,CUR.CUR_CR_AMT  AS CUR_CR_AMT                      --����������          
          ,CUR.CUR_DB_AMT  AS CUR_DB_AMT                      --�跽������          
          ,COALESCE(PRE.TOT_MTD_CR_AMT ,0) + CUR.CUR_CR_AMT  AS TOT_MTD_CR_AMT                 --���ۼƴ���������    
          ,COALESCE(PRE.TOT_MTD_DB_AMT ,0) + CUR.CUR_DB_AMT  AS TOT_MTD_DB_AMT                 --���ۼƽ跽������    
          ,COALESCE(PRE.TOT_QTD_DB_AMT ,0) + CUR.CUR_DB_AMT  AS TOT_QTD_DB_AMT                 --���ۼƴ���������    
          ,COALESCE(PRE.TOT_QTD_CR_AMT ,0) + CUR.CUR_CR_AMT  AS TOT_QTD_CR_AMT                 --���ۼƽ跽������    
          ,COALESCE(PRE.TOT_YTD_CR_AMT ,0) + CUR.CUR_CR_AMT  AS TOT_YTD_CR_AMT                 --���ۼƴ���������    
          ,COALESCE(PRE.TOT_YTD_DB_AMT ,0) + CUR.CUR_DB_AMT  AS TOT_YTD_DB_AMT                 --���ۼƽ跽������    
          ,CUR.OFF_BST_INT_RCVB_WRTOF         AS OFF_BST_INT_RCVB_WRTOF   --����Ӧ����Ϣ�������
          ,CUR.OFF_BST_INT_RCVB_RPLC	        AS OFF_BST_INT_RCVB_RPLC   --����Ӧ����Ϣ�û����
          ,COALESCE(TOT_YTD_INT_INCM_AMT_DEBT_AST		,0) + CUR.INT_INCM_AMT_DEBT_AST	AS TOT_YTD_INT_INCM_AMT_DEBT_AST --��ծ�ʲ���ծ��Ϣ����
				  ,COALESCE(TOT_YTD_INT_INCM_RTND_WRTOF_LN	,0) + CUR.INT_INCM_RTND_WRTOF_LN  AS TOT_YTD_INT_INCM_RTND_WRTOF_LN --���������ջ���Ϣ
          ,COALESCE(CUR.BAL_AMT	,0) AS BAL_AMT           --
          ,COALESCE(PRE.MTD_ACML_BAL_AMT	,0) + CUR.BAL_AMT AS MTD_ACML_BAL_AMT	--���ۼ����
          ,COALESCE(PRE.QTD_ACML_BAL_AMT	,0) + CUR.BAL_AMT	AS QTD_ACML_BAL_AMT--���ۼ����
          ,COALESCE(PRE.YTD_ACML_BAL_AMT	,0) + CUR.BAL_AMT	AS YTD_ACML_BAL_AMT--���ۼ����
          ,C_MON_DAY			AS NOCLD_In_MTH	--����������
          ,COALESCE(PRE.NOD_IN_MTH ,0) + CUR.CUR_AR_FLAG AS NOD_In_MTH				--����Ч����
          ,C_QTR_DAY				AS NOCLD_IN_QTR--����������
          ,COALESCE(NOD_IN_QTR ,0)	+ CUR.CUR_AR_FLAG	AS NOD_In_QTR			--����Ч����
          ,C_YR_DAY			AS NOCLD_In_Year--����������
          ,COALESCE(NOD_IN_YEAR,0)	+ CUR.CUR_AR_FLAG	AS NOD_In_Year			--����Ч����
          ,CUR.CUR_WRTOF_AMT_RCVD      AS CUR_WRTOF_AMT_RCVD      --�����ջغ������
          ,CUR.CUR_AMT_RCVD_Of_AST_RPLC    AS CUR_AMT_RCVD_Of_AST_RPLC  --�����ջ��û��ʲ����
          ,CUR.CUR_WRTOF_AMT    AS CUR_WRTOF_AMT             --����������
          ,COALESCE(PRE.TOT_MTD_WRTOF_AMT_RCVD       ,0) + CUR.CUR_WRTOF_AMT_RCVD       AS TOT_MTD_WRTOF_AMT_RCVD--���ۼ��ջغ������
          ,COALESCE(PRE.TOT_MTD_AMT_RCVD_Of_AST_RPLC ,0) + CUR.CUR_AMT_RCVD_Of_AST_RPLC AS TOT_MTD_AMT_RCVD_Of_AST_RPLC--���ۼ��ջ��û��ʲ����
          ,COALESCE(PRE.TOT_MTD_WRTOF_AMT            ,0) + CUR.CUR_WRTOF_AMT            AS TOT_MTD_WRTOF_AMT--���ۼƺ������
          ,COALESCE(PRE.TOT_QTD_WRTOF_AMT_RCVD       ,0) + CUR.CUR_WRTOF_AMT_RCVD       AS TOT_QTD_WRTOF_AMT_RCVD--���ۼ��ջغ������
          ,COALESCE(PRE.TOT_QTD_AMT_RCVD_Of_AST_RPLC ,0) + CUR.CUR_AMT_RCVD_Of_AST_RPLC AS TOT_QTD_AMT_RCVD_Of_AST_RPLC--���ۼ��ջ��û��ʲ����
          ,COALESCE(PRE.TOT_QTD_WRTOF_AMT            ,0) + CUR.CUR_WRTOF_AMT            AS TOT_QTD_WRTOF_AMT--���ۼƺ������
          ,COALESCE(PRE.TOT_YTD_WRTOF_AMT_RCVD       ,0) + CUR.CUR_WRTOF_AMT_RCVD       AS TOT_YTD_WRTOF_AMT_RCVD--���ۼ��ջغ������
          ,COALESCE(PRE.TOT_YTD_AMT_RCVD_Of_AST_RPLC ,0) + CUR.CUR_AMT_RCVD_Of_AST_RPLC AS TOT_YTD_AMT_RCVD_Of_AST_RPLC--���ۼ��û��ʲ����
          ,COALESCE(PRE.TOT_YTD_WRTOF_AMT            ,0) + CUR.CUR_WRTOF_AMT            AS TOT_YTD_WRTOF_AMT--���ۼƺ������              				   
    
	FROM  SESSION.CUR  AS CUR LEFT OUTER JOIN  SESSION.TMP AS PRE ON 
		          CUR.CTR_AR_ID       =  PRE.CTR_AR_ID      
          AND CUR.CTR_ITM_ORDR_ID =  PRE.CTR_ITM_ORDR_ID
;

IF (ACCOUNTING_DATE =MTH_FIRST_DAY AND MAX_ACG_DT <= ACCOUNTING_DATE) THEN

		SET SMY_STEPDESC = '�³�ֱ�Ӳ�SMY ��'; 
		
		INSERT��INTO SMY.LN_AR_INT_MTHLY_SMY 
    (
           CTR_AR_ID                         --��ͬ��            
          ,CTR_ITM_ORDR_ID                   --��ͬ���          
          ,CDR_YR                            --������            
          ,CDR_MTH                           --������        	 
	        ,ACG_DT                        
          ,LN_AR_ID                      
          ,DNMN_CCY_ID                   
          ,YTD_ON_BST_INT_AMT_RCVD       
          ,YTD_OFF_BST_INT_AMT_RCVD      
          ,ON_BST_INT_RCVB               
          ,OFF_BST_INT_RCVB              
          ,TOT_YTD_AMT_OF_INT_INCM       
          ,LN_DRDWN_AMT                  
          ,AMT_LN_REPYMT_RCVD            
          ,TOT_MTD_LN_DRDWN_AMT          
          ,TOT_QTD_LN_DRDWN_AMT          
          ,TOT_YTD_LN_DRDWN_AMT          
          ,TOT_MTD_AMT_LN_REPYMT_RCVD    
          ,TOT_QTD_AMT_LN_RPYMT_RCVD     
          ,TOT_YTD_AMT_LN_REPYMT_RCVD    
          ,TOT_MTD_NBR_LN_RCVD_TXN       
          ,TOT_QTD_NBR_LN_RCVD_TXN       
          ,TOT_YTD_NBR_LN_RCVD_TXN       
          ,TOT_MTD_NBR_LN_DRDWNTXN       
          ,TOT_QTD_NBR_LN_DRDWN_TXN      
          ,TOT_YTD_NBR_LN_DRDWN_TXN      
          ,CUR_CR_AMT                    
          ,CUR_DB_AMT                    
          ,TOT_MTD_CR_AMT                
          ,TOT_MTD_DB_AMT                
          ,TOT_QTD_DB_AMT                
          ,TOT_QTD_CR_AMT                
          ,TOT_YTD_CR_AMT                
          ,TOT_YTD_DB_AMT                
          ,OFF_BST_INT_RCVB_WRTOF        
          ,OFF_BST_INT_RCVB_RPLC	       
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST	
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN 
					,BAL_AMT	        
          ,MTD_ACML_BAL_AMT	
          ,QTD_ACML_BAL_AMT	
          ,YTD_ACML_BAL_AMT	
          ,NOCLD_In_MTH			
          ,NOD_IN_MTH				
          ,NOCLD_IN_QTR			
          ,NOD_IN_QTR				
          ,NOCLD_IN_YEAR		
          ,NOD_IN_YEAR			
          ,CUR_WRTOF_AMT_RCVD            --�����ջغ������
          ,CUR_AMT_RCVD_Of_AST_RPLC      --�����ջ��û��ʲ����
          ,CUR_WRTOF_AMT                 --����������
          ,TOT_MTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,TOT_MTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��ջ��û��ʲ����
          ,TOT_MTD_WRTOF_AMT             --���ۼƺ������
          ,TOT_QTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,TOT_QTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��ջ��û��ʲ����
          ,TOT_QTD_WRTOF_AMT             --���ۼƺ������
          ,TOT_YTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,TOT_YTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��û��ʲ����
          ,TOT_YTD_WRTOF_AMT             --���ۼƺ������  					
    )
    SELECT * FROM SESSION.TMP_TMP
    ;

ELSE

	SET SMY_STEPDESC = 'ʹ��Merge���,����SMY ��'; 			         --
                
 MERGE INTO SMY.LN_AR_INT_MTHLY_SMY AS T
 		USING  SESSION.TMP_TMP AS S 
 	  ON  		  T.CTR_AR_ID       =  S.CTR_AR_ID      
          AND T.CTR_ITM_ORDR_ID =  S.CTR_ITM_ORDR_ID
          AND T.CDR_YR          =  S.CDR_YR         
          AND T.CDR_MTH         =  S.CDR_MTH    
WHEN MATCHED THEN UPDATE SET 
           T.ACG_DT                          = S.ACG_DT                         
          ,T.LN_AR_ID                        = S.LN_AR_ID                       
          ,T.DNMN_CCY_ID                     = S.DNMN_CCY_ID                    
          ,T.YTD_ON_BST_INT_AMT_RCVD         = S.YTD_ON_BST_INT_AMT_RCVD        
          ,T.YTD_OFF_BST_INT_AMT_RCVD        = S.YTD_OFF_BST_INT_AMT_RCVD       
          ,T.ON_BST_INT_RCVB                 = S.ON_BST_INT_RCVB                
          ,T.OFF_BST_INT_RCVB                = S.OFF_BST_INT_RCVB               
          ,T.TOT_YTD_AMT_OF_INT_INCM         = S.TOT_YTD_AMT_OF_INT_INCM        
          ,T.LN_DRDWN_AMT                    = S.LN_DRDWN_AMT                   
          ,T.AMT_LN_REPYMT_RCVD              = S.AMT_LN_REPYMT_RCVD             
          ,T.TOT_MTD_LN_DRDWN_AMT            = S.TOT_MTD_LN_DRDWN_AMT           
          ,T.TOT_QTD_LN_DRDWN_AMT            = S.TOT_QTD_LN_DRDWN_AMT           
          ,T.TOT_YTD_LN_DRDWN_AMT            = S.TOT_YTD_LN_DRDWN_AMT           
          ,T.TOT_MTD_AMT_LN_REPYMT_RCVD      = S.TOT_MTD_AMT_LN_REPYMT_RCVD     
          ,T.TOT_QTD_AMT_LN_RPYMT_RCVD       = S.TOT_QTD_AMT_LN_RPYMT_RCVD      
          ,T.TOT_YTD_AMT_LN_REPYMT_RCVD      = S.TOT_YTD_AMT_LN_REPYMT_RCVD     
          ,T.TOT_MTD_NBR_LN_RCVD_TXN         = S.TOT_MTD_NBR_LN_RCVD_TXN        
          ,T.TOT_QTD_NBR_LN_RCVD_TXN         = S.TOT_QTD_NBR_LN_RCVD_TXN        
          ,T.TOT_YTD_NBR_LN_RCVD_TXN         = S.TOT_YTD_NBR_LN_RCVD_TXN        
          ,T.TOT_MTD_NBR_LN_DRDWNTXN         = S.TOT_MTD_NBR_LN_DRDWNTXN        
          ,T.TOT_QTD_NBR_LN_DRDWN_TXN        = S.TOT_QTD_NBR_LN_DRDWN_TXN       
          ,T.TOT_YTD_NBR_LN_DRDWN_TXN        = S.TOT_YTD_NBR_LN_DRDWN_TXN       
          ,T.CUR_CR_AMT                      = S.CUR_CR_AMT                     
          ,T.CUR_DB_AMT                      = S.CUR_DB_AMT                     
          ,T.TOT_MTD_CR_AMT                  = S.TOT_MTD_CR_AMT                 
          ,T.TOT_MTD_DB_AMT                  = S.TOT_MTD_DB_AMT                 
          ,T.TOT_QTD_DB_AMT                  = S.TOT_QTD_DB_AMT                 
          ,T.TOT_QTD_CR_AMT                  = S.TOT_QTD_CR_AMT                 
          ,T.TOT_YTD_CR_AMT                  = S.TOT_YTD_CR_AMT                 
          ,T.TOT_YTD_DB_AMT                  = S.TOT_YTD_DB_AMT                 
          ,T.OFF_BST_INT_RCVB_WRTOF          = S.OFF_BST_INT_RCVB_WRTOF         
          ,T.OFF_BST_INT_RCVB_RPLC	         = S.OFF_BST_INT_RCVB_RPLC	        
          ,T.TOT_YTD_INT_INCM_AMT_DEBT_AST	 = S.TOT_YTD_INT_INCM_AMT_DEBT_AST
					,T.TOT_YTD_INT_INCM_RTND_WRTOF_LN	 = S.TOT_YTD_INT_INCM_RTND_WRTOF_LN 
					,T.BAL_AMT	                       = S.BAL_AMT	       
          ,T.MTD_ACML_BAL_AMT		             = S.MTD_ACML_BAL_AMT
          ,T.QTD_ACML_BAL_AMT		             = S.QTD_ACML_BAL_AMT
          ,T.YTD_ACML_BAL_AMT		             = S.YTD_ACML_BAL_AMT
          ,T.NOCLD_In_MTH				             = S.NOCLD_In_MTH			
          ,T.NOD_In_MTH					             = S.NOD_In_MTH				
          ,T.NOCLD_In_QTR				             = S.NOCLD_In_QTR			
          ,T.NOD_In_QTR					             = S.NOD_In_QTR				
          ,T.NOCLD_In_Year			             = S.NOCLD_In_Year		
          ,T.NOD_In_Year				             = S.NOD_In_Year			
          ,T.CUR_WRTOF_AMT_RCVD              =S.CUR_WRTOF_AMT_RCVD           --�����ջغ������
          ,T.CUR_AMT_RCVD_Of_AST_RPLC        =S.CUR_AMT_RCVD_Of_AST_RPLC     --�����ջ��û��ʲ����
          ,T.CUR_WRTOF_AMT                   =S.CUR_WRTOF_AMT                --����������
          ,T.TOT_MTD_WRTOF_AMT_RCVD          =S.TOT_MTD_WRTOF_AMT_RCVD       --���ۼ��ջغ������
          ,T.TOT_MTD_AMT_RCVD_Of_AST_RPLC    =S.TOT_MTD_AMT_RCVD_Of_AST_RPLC --���ۼ��ջ��û��ʲ����
          ,T.TOT_MTD_WRTOF_AMT               =S.TOT_MTD_WRTOF_AMT            --���ۼƺ������
          ,T.TOT_QTD_WRTOF_AMT_RCVD          =S.TOT_QTD_WRTOF_AMT_RCVD       --���ۼ��ջغ������
          ,T.TOT_QTD_AMT_RCVD_Of_AST_RPLC    =S.TOT_QTD_AMT_RCVD_Of_AST_RPLC --���ۼ��ջ��û��ʲ����
          ,T.TOT_QTD_WRTOF_AMT               =S.TOT_QTD_WRTOF_AMT            --���ۼƺ������
          ,T.TOT_YTD_WRTOF_AMT_RCVD          =S.TOT_YTD_WRTOF_AMT_RCVD       --���ۼ��ջغ������
          ,T.TOT_YTD_AMT_RCVD_Of_AST_RPLC    =S.TOT_YTD_AMT_RCVD_Of_AST_RPLC --���ۼ��û��ʲ����
          ,T.TOT_YTD_WRTOF_AMT               =S.TOT_YTD_WRTOF_AMT            --���ۼƺ������  
 ;
  INSERT INTO SMY.LN_AR_INT_MTHLY_SMY
  (
          CTR_AR_ID                          --��ͬ��            
          ,CTR_ITM_ORDR_ID                   --��ͬ���          
          ,CDR_YR                            --������            
          ,CDR_MTH                           --������        	 
	        ,ACG_DT                        
          ,LN_AR_ID                      
          ,DNMN_CCY_ID                   
          ,YTD_ON_BST_INT_AMT_RCVD       
          ,YTD_OFF_BST_INT_AMT_RCVD      
          ,ON_BST_INT_RCVB               
          ,OFF_BST_INT_RCVB              
          ,TOT_YTD_AMT_OF_INT_INCM       
          ,LN_DRDWN_AMT                  
          ,AMT_LN_REPYMT_RCVD            
          ,TOT_MTD_LN_DRDWN_AMT          
          ,TOT_QTD_LN_DRDWN_AMT          
          ,TOT_YTD_LN_DRDWN_AMT          
          ,TOT_MTD_AMT_LN_REPYMT_RCVD    
          ,TOT_QTD_AMT_LN_RPYMT_RCVD     
          ,TOT_YTD_AMT_LN_REPYMT_RCVD    
          ,TOT_MTD_NBR_LN_RCVD_TXN       
          ,TOT_QTD_NBR_LN_RCVD_TXN       
          ,TOT_YTD_NBR_LN_RCVD_TXN       
          ,TOT_MTD_NBR_LN_DRDWNTXN       
          ,TOT_QTD_NBR_LN_DRDWN_TXN      
          ,TOT_YTD_NBR_LN_DRDWN_TXN      
          ,CUR_CR_AMT                    
          ,CUR_DB_AMT                    
          ,TOT_MTD_CR_AMT                
          ,TOT_MTD_DB_AMT                
          ,TOT_QTD_DB_AMT                
          ,TOT_QTD_CR_AMT                
          ,TOT_YTD_CR_AMT                
          ,TOT_YTD_DB_AMT                
          ,OFF_BST_INT_RCVB_WRTOF        
          ,OFF_BST_INT_RCVB_RPLC	       
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST	
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN 
					,BAL_AMT	        
          ,MTD_ACML_BAL_AMT	
          ,QTD_ACML_BAL_AMT	
          ,YTD_ACML_BAL_AMT	
          ,NOCLD_In_MTH			
          ,NOD_IN_MTH				
          ,NOCLD_IN_QTR			
          ,NOD_IN_QTR				
          ,NOCLD_IN_YEAR		
          ,NOD_IN_YEAR			
          ,CUR_WRTOF_AMT_RCVD            --�����ջغ������
          ,CUR_AMT_RCVD_Of_AST_RPLC      --�����ջ��û��ʲ����
          ,CUR_WRTOF_AMT                 --����������
          ,TOT_MTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,TOT_MTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��ջ��û��ʲ����
          ,TOT_MTD_WRTOF_AMT             --���ۼƺ������
          ,TOT_QTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,TOT_QTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��ջ��û��ʲ����
          ,TOT_QTD_WRTOF_AMT             --���ۼƺ������
          ,TOT_YTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,TOT_YTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��û��ʲ����
          ,TOT_YTD_WRTOF_AMT             --���ۼƺ������
  )
  SELECT
          S.CTR_AR_ID                          --��ͬ��            
          ,S.CTR_ITM_ORDR_ID                   --��ͬ���          
          ,S.CDR_YR                            --������            
          ,S.CDR_MTH                           --������        
			    ,S.ACG_DT                        
          ,S.LN_AR_ID                      
          ,S.DNMN_CCY_ID                   
          ,S.YTD_ON_BST_INT_AMT_RCVD       
          ,S.YTD_OFF_BST_INT_AMT_RCVD      
          ,S.ON_BST_INT_RCVB               
          ,S.OFF_BST_INT_RCVB              
          ,S.TOT_YTD_AMT_OF_INT_INCM       
          ,S.LN_DRDWN_AMT                  
          ,S.AMT_LN_REPYMT_RCVD            
          ,S.TOT_MTD_LN_DRDWN_AMT          
          ,S.TOT_QTD_LN_DRDWN_AMT          
          ,S.TOT_YTD_LN_DRDWN_AMT          
          ,S.TOT_MTD_AMT_LN_REPYMT_RCVD    
          ,S.TOT_QTD_AMT_LN_RPYMT_RCVD     
          ,S.TOT_YTD_AMT_LN_REPYMT_RCVD    
          ,S.TOT_MTD_NBR_LN_RCVD_TXN       
          ,S.TOT_QTD_NBR_LN_RCVD_TXN       
          ,S.TOT_YTD_NBR_LN_RCVD_TXN       
          ,S.TOT_MTD_NBR_LN_DRDWNTXN       
          ,S.TOT_QTD_NBR_LN_DRDWN_TXN      
          ,S.TOT_YTD_NBR_LN_DRDWN_TXN      
          ,S.CUR_CR_AMT                    
          ,S.CUR_DB_AMT                    
          ,S.TOT_MTD_CR_AMT                
          ,S.TOT_MTD_DB_AMT                
          ,S.TOT_QTD_DB_AMT                
          ,S.TOT_QTD_CR_AMT                
          ,S.TOT_YTD_CR_AMT                
          ,S.TOT_YTD_DB_AMT                
          ,S.OFF_BST_INT_RCVB_WRTOF        
          ,S.OFF_BST_INT_RCVB_RPLC	       
          ,S.TOT_YTD_INT_INCM_AMT_DEBT_AST 
          ,S.TOT_YTD_INT_INCM_RTND_WRTOF_LN
					,S.BAL_AMT	        
          ,S.MTD_ACML_BAL_AMT	
          ,S.QTD_ACML_BAL_AMT	
          ,S.YTD_ACML_BAL_AMT	
          ,S.NOCLD_In_MTH			
          ,S.NOD_In_MTH				
          ,S.NOCLD_In_QTR			
          ,S.NOD_In_QTR				
          ,S.NOCLD_In_Year		
          ,S.NOD_In_Year			
          ,S.CUR_WRTOF_AMT_RCVD            --�����ջغ������
          ,S.CUR_AMT_RCVD_Of_AST_RPLC      --�����ջ��û��ʲ����
          ,S.CUR_WRTOF_AMT                 --����������
          ,S.TOT_MTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,S.TOT_MTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��ջ��û��ʲ����
          ,S.TOT_MTD_WRTOF_AMT             --���ۼƺ������
          ,S.TOT_QTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,S.TOT_QTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��ջ��û��ʲ����
          ,S.TOT_QTD_WRTOF_AMT             --���ۼƺ������
          ,S.TOT_YTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,S.TOT_YTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��û��ʲ����
          ,S.TOT_YTD_WRTOF_AMT
  FROM SESSION.TMP_TMP as S
    WHERE NOT EXISTS (
         SELECT 1 FROM SMY.LN_AR_INT_MTHLY_SMY T
         WHERE T.CTR_AR_ID           =  S.CTR_AR_ID      
               AND T.CTR_ITM_ORDR_ID =  S.CTR_ITM_ORDR_ID
               AND T.CDR_YR          =  S.CDR_YR         
               AND T.CDR_MTH         =  S.CDR_MTH
    )
	;
/*WHEN NOT MATCHED THEN INSERT  
	        
	 (      
           CTR_AR_ID                         --��ͬ��            
          ,CTR_ITM_ORDR_ID                   --��ͬ���          
          ,CDR_YR                            --������            
          ,CDR_MTH                           --������        	 
	        ,ACG_DT                        
          ,LN_AR_ID                      
          ,DNMN_CCY_ID                   
          ,YTD_ON_BST_INT_AMT_RCVD       
          ,YTD_OFF_BST_INT_AMT_RCVD      
          ,ON_BST_INT_RCVB               
          ,OFF_BST_INT_RCVB              
          ,TOT_YTD_AMT_OF_INT_INCM       
          ,LN_DRDWN_AMT                  
          ,AMT_LN_REPYMT_RCVD            
          ,TOT_MTD_LN_DRDWN_AMT          
          ,TOT_QTD_LN_DRDWN_AMT          
          ,TOT_YTD_LN_DRDWN_AMT          
          ,TOT_MTD_AMT_LN_REPYMT_RCVD    
          ,TOT_QTD_AMT_LN_RPYMT_RCVD     
          ,TOT_YTD_AMT_LN_REPYMT_RCVD    
          ,TOT_MTD_NBR_LN_RCVD_TXN       
          ,TOT_QTD_NBR_LN_RCVD_TXN       
          ,TOT_YTD_NBR_LN_RCVD_TXN       
          ,TOT_MTD_NBR_LN_DRDWNTXN       
          ,TOT_QTD_NBR_LN_DRDWN_TXN      
          ,TOT_YTD_NBR_LN_DRDWN_TXN      
          ,CUR_CR_AMT                    
          ,CUR_DB_AMT                    
          ,TOT_MTD_CR_AMT                
          ,TOT_MTD_DB_AMT                
          ,TOT_QTD_DB_AMT                
          ,TOT_QTD_CR_AMT                
          ,TOT_YTD_CR_AMT                
          ,TOT_YTD_DB_AMT                
          ,OFF_BST_INT_RCVB_WRTOF        
          ,OFF_BST_INT_RCVB_RPLC	       
          ,TOT_YTD_INT_INCM_AMT_DEBT_AST	
					,TOT_YTD_INT_INCM_RTND_WRTOF_LN 
					,BAL_AMT	        
          ,MTD_ACML_BAL_AMT	
          ,QTD_ACML_BAL_AMT	
          ,YTD_ACML_BAL_AMT	
          ,NOCLD_In_MTH			
          ,NOD_IN_MTH				
          ,NOCLD_IN_QTR			
          ,NOD_IN_QTR				
          ,NOCLD_IN_YEAR		
          ,NOD_IN_YEAR			
          ,CUR_WRTOF_AMT_RCVD            --�����ջغ������
          ,CUR_AMT_RCVD_Of_AST_RPLC      --�����ջ��û��ʲ����
          ,CUR_WRTOF_AMT                 --����������
          ,TOT_MTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,TOT_MTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��ջ��û��ʲ����
          ,TOT_MTD_WRTOF_AMT             --���ۼƺ������
          ,TOT_QTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,TOT_QTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��ջ��û��ʲ����
          ,TOT_QTD_WRTOF_AMT             --���ۼƺ������
          ,TOT_YTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
          ,TOT_YTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��û��ʲ����
          ,TOT_YTD_WRTOF_AMT             --���ۼƺ������  					
					) 
			VALUES (  
            		S.CTR_AR_ID                         --��ͬ��            
           		 ,S.CTR_ITM_ORDR_ID                   --��ͬ���          
          		 ,S.CDR_YR                            --������            
          		 ,S.CDR_MTH                           --������        
			         ,S.ACG_DT                        
               ,S.LN_AR_ID                      
               ,S.DNMN_CCY_ID                   
               ,S.YTD_ON_BST_INT_AMT_RCVD       
               ,S.YTD_OFF_BST_INT_AMT_RCVD      
               ,S.ON_BST_INT_RCVB               
               ,S.OFF_BST_INT_RCVB              
               ,S.TOT_YTD_AMT_OF_INT_INCM       
               ,S.LN_DRDWN_AMT                  
               ,S.AMT_LN_REPYMT_RCVD            
               ,S.TOT_MTD_LN_DRDWN_AMT          
               ,S.TOT_QTD_LN_DRDWN_AMT          
               ,S.TOT_YTD_LN_DRDWN_AMT          
               ,S.TOT_MTD_AMT_LN_REPYMT_RCVD    
               ,S.TOT_QTD_AMT_LN_RPYMT_RCVD     
               ,S.TOT_YTD_AMT_LN_REPYMT_RCVD    
               ,S.TOT_MTD_NBR_LN_RCVD_TXN       
               ,S.TOT_QTD_NBR_LN_RCVD_TXN       
               ,S.TOT_YTD_NBR_LN_RCVD_TXN       
               ,S.TOT_MTD_NBR_LN_DRDWNTXN       
               ,S.TOT_QTD_NBR_LN_DRDWN_TXN      
               ,S.TOT_YTD_NBR_LN_DRDWN_TXN      
               ,S.CUR_CR_AMT                    
               ,S.CUR_DB_AMT                    
               ,S.TOT_MTD_CR_AMT                
               ,S.TOT_MTD_DB_AMT                
               ,S.TOT_QTD_DB_AMT                
               ,S.TOT_QTD_CR_AMT                
               ,S.TOT_YTD_CR_AMT                
               ,S.TOT_YTD_DB_AMT                
               ,S.OFF_BST_INT_RCVB_WRTOF        
               ,S.OFF_BST_INT_RCVB_RPLC	       
               ,S.TOT_YTD_INT_INCM_AMT_DEBT_AST 
               ,S.TOT_YTD_INT_INCM_RTND_WRTOF_LN
					     ,S.BAL_AMT	        
               ,S.MTD_ACML_BAL_AMT	
               ,S.QTD_ACML_BAL_AMT	
               ,S.YTD_ACML_BAL_AMT	
               ,S.NOCLD_In_MTH			
               ,S.NOD_In_MTH				
               ,S.NOCLD_In_QTR			
               ,S.NOD_In_QTR				
               ,S.NOCLD_In_Year		
               ,S.NOD_In_Year			
               ,S.CUR_WRTOF_AMT_RCVD            --�����ջغ������
               ,S.CUR_AMT_RCVD_Of_AST_RPLC      --�����ջ��û��ʲ����
               ,S.CUR_WRTOF_AMT                 --����������
               ,S.TOT_MTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
               ,S.TOT_MTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��ջ��û��ʲ����
               ,S.TOT_MTD_WRTOF_AMT             --���ۼƺ������
               ,S.TOT_QTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
               ,S.TOT_QTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��ջ��û��ʲ����
               ,S.TOT_QTD_WRTOF_AMT             --���ۼƺ������
               ,S.TOT_YTD_WRTOF_AMT_RCVD        --���ۼ��ջغ������
               ,S.TOT_YTD_AMT_RCVD_Of_AST_RPLC  --���ۼ��û��ʲ����
               ,S.TOT_YTD_WRTOF_AMT             --���ۼƺ������  
               )
   ;--*/

END IF;
   
   
/** Insert the log**/
 		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		SET SMY_STEPNUM = SMY_STEPNUM + 1;--
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
	 
	 COMMIT;   --
	 
END@