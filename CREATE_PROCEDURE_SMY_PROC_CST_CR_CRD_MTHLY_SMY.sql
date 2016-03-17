CREATE PROCEDURE SMY.PROC_CST_CR_CRD_MTHLY_SMY(IN ACCOUNTING_DATE date)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_CST_CR_CRD_MTHLY_SMY.sql
-- Procedure name: 			SMY.PROC_CST_CR_CRD_MTHLY_SMY
-- Source Table:				SMY.CR_CRD_SMY,SMY.CST_INF
-- Target Table: 				SMY.CST_CR_CRD_MTHLY_SMY
-- Project     :        ZJ RCCB EDW
-- NOTES       :        
-- Purpose     :            
-- PROCESS METHOD      :  UPDATE EACH DAY ,INSERT IN THE PERIOD OF ONE MONTH
--=============================================================================
-- Creation Date:       2009.11.23
-- Origin Author:       JAMES SHANG IBM copyright 
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2009-11-23   JAMES SHANG     Create SP File		
-- 2009-12-04   Xu Yan          Rename the history table 
-- 2009-12-16   Xu Yan          Fixed a bug for reruning
-- 2010-08-11   van fuqiao      Fixed a bug for column  'C_QTR_DAY'  'C_MON_DAY' 'C_YR_DAY'
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
		DECLARE EMP_SQL VARCHAR(200);  --

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
  
    SET SMY_PROCNM  ='PROC_CST_CR_CRD_MTHLY_SMY';--
    SET SMY_DATE    =ACCOUNTING_DATE;    --
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --ȡ��ǰ���
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --ȡ��ǰ�·�
    SET CUR_DAY     =DAY(ACCOUNTING_DATE);     --ȡ�µڼ���
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- ȡ�����
    ------------------------Start on 20100811--------------------------
    --SET C_YR_DAY      =DAYOFYEAR(DATE(TRIM(CHAR(YEAR(ACCOUNTING_DATE)))||'-12-31')); --ȡ����ڼ���
    SET C_YR_DAY      =DAYOFYEAR(ACCOUNTING_DATE); --ȡ����ڼ���
    ------------------------End on 20100811--------------------------
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --��ǰ����
    SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,ISO),1,7)||'-01'); --ȡ���³���
    SET MON_DAY     =DAY(ACCOUNTING_DATE);     --ȡ�µڼ���
    
    VALUES(ACCOUNTING_DATE - 1 DAY) INTO LAST_SMY_DATE;--
    SET LAST_MONTH = MONTH(LAST_SMY_DATE);      --
    --��������������
    ------------------------Start on 20100811--------------------------
    --SET C_MON_DAY = DAYS(MTH_FIRST_DAY + 1 MONTH ) - DAYS(MTH_FIRST_DAY);    --
    SET C_MON_DAY = DAYS(ACCOUNTING_DATE) - DAYS(MTH_FIRST_DAY)+1;    --��������������
    ------------------------End on 20100811--------------------------
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
    ------------------------Start on 20100811--------------------------
  	--SET C_QTR_DAY = DAYS(QTR_LAST_DAY) - DAYS(QTR_FIRST_DAY) + 1 ;--
  	SET C_QTR_DAY = DAYS(ACCOUNTING_DATE) - DAYS(QTR_FIRST_DAY) + 1 ;--ȡ������������
  	------------------------End on 20100811--------------------------
		
		SELECT COALESCE(MAX(ACG_DT),'1900-01-01') INTO MAX_ACG_DT FROM SMY.CST_CR_CRD_MTHLY_SMY;--

/*Delete��־��,����SMY_PROCNM=��ǰ�洢��������,SMY_DATE=ACCOUNTING_DATE,�������µ���ʼ��־*/
		DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
			COMMIT;--
		
		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		
		SET SMY_STEPDESC = 	'�洢���̿�ʼ����' ;--
		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
				VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

/*���ݻָ��뱸��*/
    IF MAX_ACG_DT = ACCOUNTING_DATE THEN
       DELETE FROM SMY.CST_CR_CRD_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = CUR_MONTH;--
    /**ÿ�µ�һ�ղ���Ҫ����ʷ���лָ�**/       
       IF MON_DAY <> 1 THEN
      	 INSERT INTO SMY.CST_CR_CRD_MTHLY_SMY SELECT * FROM HIS.CST_CR_CRD_MTHLY_SMY ;--
       END IF;--
     ELSE
  /** ���hist ���ݱ� **/

	    SET EMP_SQL= 'Alter TABLE HIS.CST_CR_CRD_MTHLY_SMY ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE' ;--
		
		  EXECUTE IMMEDIATE EMP_SQL;       		 --
      
      COMMIT;--
		  /**backup �������� **/
		  
		  INSERT INTO HIS.CST_CR_CRD_MTHLY_SMY SELECT * FROM SMY.CST_CR_CRD_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = CUR_MONTH;--
    END IF;--

SET SMY_STEPNUM = 2 ;--
SET SMY_STEPDESC = '�����û���ʱ��,�������SMY����';--

	/*�����û���ʱ��*/
	
	DECLARE GLOBAL TEMPORARY TABLE TMP 
		LIKE SMY.CST_CR_CRD_MTHLY_SMY
	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE   IN TS_USR_TMP32K PARTITIONING KEY(CST_ID);--

 /*��������һ�ղ���Ҫ����*/

 IF YR_FIRST_DAY <>  ACCOUNTING_DATE THEN 
	INSERT INTO SESSION.TMP 
	(
         CST_ID                  --�ͻ�����        
        ,CRD_OU_ID               --���������      
        ,ENT_IDV_IND             --������          
        ,MST_CRD_IND             --��/������־     
        ,LN_FIVE_RTG_STS         --�����弶��̬����
        ,AST_RSK_ASES_RTG_TP_CD  --�ʲ����շ���    
        ,PD_GRP_CD               --��Ʒ��          
        ,PD_SUB_CD               --��Ʒ�Ӵ���      
        ,CCY                     --����            
        ,NOCLD_In_MTH            --������������    
        ,NOD_In_MTH              --������Ч����    
        ,NOCLD_In_QTR            --������������    
        ,NOD_In_QTR              --������Ч����    
        ,NOCLD_In_Year           --������������    
        ,NOD_In_Year             --������Ч����
        ,CDR_YR                  --������
        ,CDR_MTH                 --������         
        ,ACG_DT                  --����YYYY-MM-DD  
        ,DEP_BAL_CRD             --���п�������  
        ,OD_BAL_AMT              --͸֧���        
        ,CR_LMT                  --���Ŷ��        
        ,INT_RCVB                --Ӧ����Ϣ        
        ,LST_DAY_DEP_BAL         --���մ�����    
        ,LST_DAY_OD_BAL          --����͸֧���    
        ,MTD_ACML_DEP_BAL_AMT    --���ۼƴ�����  
        ,QTD_ACML_DEP_BAL_AMT    --���ۼƴ�����  
        ,YTD_ACML_DEP_BAL_AMT    --���ۼƴ�����  
        ,MTD_ACML_OD_BAL_AMT     --���ۼ�͸֧���  
        ,QTD_ACML_OD_BAL_AMT     --���ۼ�͸֧���  
        ,YTD_ACML_OD_BAL_AMT     --���ۼ�͸֧��� 
          ) 
    SELECT
         CST_ID                  --�ͻ�����        
        ,CRD_OU_ID               --���������      
        ,ENT_IDV_IND             --������          
        ,MST_CRD_IND             --��/������־     
        ,LN_FIVE_RTG_STS         --�����弶��̬����
        ,AST_RSK_ASES_RTG_TP_CD  --�ʲ����շ���    
        ,PD_GRP_CD               --��Ʒ��          
        ,PD_SUB_CD               --��Ʒ�Ӵ���      
        ,CCY                     --����            
        ,NOCLD_In_MTH            --������������    
        ,NOD_In_MTH              --������Ч����    
        ,NOCLD_In_QTR            --������������    
        ,NOD_In_QTR              --������Ч����    
        ,NOCLD_In_Year           --������������    
        ,NOD_In_Year             --������Ч����
        ,CDR_YR                  --������
        ,CDR_MTH                 --������              
        ,ACG_DT                  --����YYYY-MM-DD  
        ,DEP_BAL_CRD             --���п�������  
        ,OD_BAL_AMT              --͸֧���        
        ,CR_LMT                  --���Ŷ��        
        ,INT_RCVB                --Ӧ����Ϣ        
        ,LST_DAY_DEP_BAL         --���մ�����    
        ,LST_DAY_OD_BAL          --����͸֧���    
        ,MTD_ACML_DEP_BAL_AMT    --���ۼƴ�����  
        ,QTD_ACML_DEP_BAL_AMT    --���ۼƴ�����  
        ,YTD_ACML_DEP_BAL_AMT    --���ۼƴ�����  
        ,MTD_ACML_OD_BAL_AMT     --���ۼ�͸֧���  
        ,QTD_ACML_OD_BAL_AMT     --���ۼ�͸֧���  
        ,YTD_ACML_OD_BAL_AMT     --���ۼ�͸֧���        
     FROM SMY.CST_CR_CRD_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = LAST_MONTH;--
 END IF ;   --
	
       
      
 /** �ռ�������Ϣ */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	 --


	 IF  ACCOUNTING_DATE IN ( YR_FIRST_DAY )  --�� �� �� ����
	 		THEN 
	 			UPDATE SESSION.TMP 
	 				SET  				
             MTD_ACML_DEP_BAL_AMT =0 --���ۼƴ�����
            ,QTD_ACML_DEP_BAL_AMT =0 --���ۼƴ�����
            ,YTD_ACML_DEP_BAL_AMT =0 --���ۼƴ�����
            ,MTD_ACML_OD_BAL_AMT  =0 --���ۼ�͸֧���
            ,QTD_ACML_OD_BAL_AMT  =0 --���ۼ�͸֧���
            ,YTD_ACML_OD_BAL_AMT  =0 --���ۼ�͸֧��� 
            ,NOD_In_MTH           =0 --������Ч����  
            ,NOD_In_QTR           =0 --������Ч����   
            ,NOD_In_Year          =0 --������Ч���� 
                       
	 		  ;--
	 ELSEIF ACCOUNTING_DATE IN (QTR_FIRST_DAY) --�� �� ����
	 	  THEN
	 			UPDATE SESSION.TMP 
	 				SET 
             MTD_ACML_DEP_BAL_AMT =0 --���ۼƴ�����
            ,QTD_ACML_DEP_BAL_AMT =0 --���ۼƴ�����
            ,MTD_ACML_OD_BAL_AMT  =0 --���ۼ�͸֧���
            ,QTD_ACML_OD_BAL_AMT  =0 --���ۼ�͸֧���
            ,NOD_In_MTH           =0 --������Ч����  
            ,NOD_In_QTR           =0 --������Ч����           
	 			;	 	  --
	 	  	 		
	 ELSEIF ACCOUNTING_DATE IN ( MTH_FIRST_DAY ) --�¹���
	 	  THEN 
	 			UPDATE SESSION.TMP 
	 				SET 
             MTD_ACML_DEP_BAL_AMT =0 --���ۼƴ�����
            ,MTD_ACML_OD_BAL_AMT  =0 --���ۼ�͸֧���
            ,NOD_In_MTH           =0 --������Ч����             
	 			;	 	--
	 END IF;--

 /*��õ���ͳ������*/	
 
		SET SMY_STEPNUM = 3 ;--
		SET SMY_STEPDESC = '������ʱ��SESSION.CUR, ������ÿ����ջ��ܺ������'; 		 --
 
 DECLARE GLOBAL TEMPORARY TABLE CUR AS (
		SELECT 
			   CR_CRD_SMY.CST_ID                  AS CST_ID                --�ͻ�����      
        ,CR_CRD_SMY.OU_ID                   AS CRD_OU_ID             --�˻�����������
        ,CR_CRD_SMY.ENT_IDV_IND             AS ENT_IDV_IND           --������
        ,CR_CRD_SMY.MST_CRD_IND             AS MST_CRD_IND           --��/������־
        ,CR_CRD_SMY.LN_FIVE_RTG_STS         AS LN_FIVE_RTG_STS       --�����弶��̬����
        ,CR_CRD_SMY.AST_RSK_ASES_RTG_TP_CD  AS AST_RSK_ASES_RTG_TP_CD   --�ʲ����շ���
        ,CR_CRD_SMY.PD_GRP_CD               AS PD_GRP_CD                --��Ʒ��      
        ,CR_CRD_SMY.PD_SUB_CD               AS PD_SUB_CD                --��Ʒ�Ӵ���     
        ,CR_CRD_SMY.CCY                     AS CCY                   --����          
        ,1                                  AS NOD_IN_MTH  
        ,1                                  AS NOD_IN_QTR
        ,1                                  AS NOD_IN_YEAR    
        ,SUM(CR_CRD_SMY.DEP_BAL_CRD)        AS DEP_BAL_CRD        --���п�������
        ,SUM(CR_CRD_SMY.OD_BAL_AMT )        AS OD_BAL_AMT         --͸֧���      
        ,SUM(CR_CRD_SMY.CR_LMT     )        AS CR_LMT             --���Ŷ��      
        ,SUM(CR_CRD_SMY.INT_RCVB   )        AS INT_RCVB           --Ӧ����Ϣ      
                                                                    
		FROM            SMY.CR_CRD_SMY  AS CR_CRD_SMY
		GROUP BY 
         CR_CRD_SMY.CST_ID                
        ,CR_CRD_SMY.OU_ID                 
        ,CR_CRD_SMY.ENT_IDV_IND           
        ,CR_CRD_SMY.MST_CRD_IND           
        ,CR_CRD_SMY.LN_FIVE_RTG_STS       
        ,CR_CRD_SMY.AST_RSK_ASES_RTG_TP_CD
        ,CR_CRD_SMY.PD_GRP_CD             
        ,CR_CRD_SMY.PD_SUB_CD             
        ,CR_CRD_SMY.CCY                   
       			    
  ) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K 
     PARTITIONING KEY(CST_ID) ;  			--
     
  INSERT INTO SESSION.CUR 
		SELECT 
			   CR_CRD_SMY.CST_ID                  AS CST_ID                --�ͻ�����      
        ,CR_CRD_SMY.OU_ID                   AS CRD_OU_ID             --�˻�����������
        ,CR_CRD_SMY.ENT_IDV_IND             AS ENT_IDV_IND           --������
        ,CR_CRD_SMY.MST_CRD_IND             AS MST_CRD_IND           --��/������־
        ,CR_CRD_SMY.LN_FIVE_RTG_STS         AS LN_FIVE_RTG_STS       --�����弶��̬����
        ,CR_CRD_SMY.AST_RSK_ASES_RTG_TP_CD  AS AST_RSK_ASES_RTG_TP_CD   --�ʲ����շ���
        ,CR_CRD_SMY.PD_GRP_CD               AS PD_GRP_CD                --��Ʒ��      
        ,CR_CRD_SMY.PD_SUB_CD               AS PD_SUB_CD                --��Ʒ�Ӵ���     
        ,CR_CRD_SMY.CCY                     AS CCY                   --����          
        ,1                                  AS NOD_IN_MTH  
        ,1                                  AS NOD_IN_QTR
        ,1                                  AS NOD_IN_YEAR    
        ,SUM(CR_CRD_SMY.DEP_BAL_CRD)        AS DEP_BAL_CRD        --���п�������
        ,SUM(CR_CRD_SMY.OD_BAL_AMT )        AS OD_BAL_AMT         --͸֧���      
        ,SUM(CR_CRD_SMY.CR_LMT     )        AS CR_LMT             --���Ŷ��      
        ,SUM(CR_CRD_SMY.INT_RCVB   )        AS INT_RCVB           --Ӧ����Ϣ      
                                                                    
		FROM  SMY.CR_CRD_SMY  AS CR_CRD_SMY  WHERE CR_CRD_SMY.CRD_LCS_TP_ID IN (11920001 ,11920002,11920003)  --11920001:����,11920002:�·���δ����,11920003:�»���δ����
		GROUP BY 
         CR_CRD_SMY.CST_ID                
        ,CR_CRD_SMY.OU_ID                 
        ,CR_CRD_SMY.ENT_IDV_IND           
        ,CR_CRD_SMY.MST_CRD_IND           
        ,CR_CRD_SMY.LN_FIVE_RTG_STS       
        ,CR_CRD_SMY.AST_RSK_ASES_RTG_TP_CD
        ,CR_CRD_SMY.PD_GRP_CD             
        ,CR_CRD_SMY.PD_SUB_CD             
        ,CR_CRD_SMY.CCY   
   ;--
 /** �ռ�������Ϣ */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
	 

		SET SMY_STEPNUM = 4 ;--
		SET SMY_STEPDESC = '������ʱ��SESSION.S, ����������ÿ����ܺ�Ҫ���µ�����'; 			 --


/**/
  DECLARE GLOBAL TEMPORARY TABLE S  LIKE SMY.CST_CR_CRD_MTHLY_SMY 
  	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K 
     PARTITIONING KEY(CST_ID);--
  
	INSERT INTO SESSION.S
          (
         CST_ID                  --�ͻ�����        
        ,CRD_OU_ID               --���������      
        ,ENT_IDV_IND             --������          
        ,MST_CRD_IND             --��/������־     
        ,LN_FIVE_RTG_STS         --�����弶��̬����
        ,AST_RSK_ASES_RTG_TP_CD  --�ʲ����շ���    
        ,PD_GRP_CD               --��Ʒ��          
        ,PD_SUB_CD               --��Ʒ�Ӵ���      
        ,CCY                     --����            
        ,NOCLD_In_MTH            --������������    
        ,NOD_In_MTH              --������Ч����    
        ,NOCLD_In_QTR            --������������    
        ,NOD_In_QTR              --������Ч����    
        ,NOCLD_In_Year           --������������    
        ,NOD_In_Year             --������Ч����
        ,CDR_YR                  --������
        ,CDR_MTH                 --������            
        ,ACG_DT                  --����YYYY-MM-DD  
        ,DEP_BAL_CRD             --���п�������  
        ,OD_BAL_AMT              --͸֧���        
        ,CR_LMT                  --���Ŷ��        
        ,INT_RCVB                --Ӧ����Ϣ        
        ,LST_DAY_DEP_BAL         --���մ�����    
        ,LST_DAY_OD_BAL          --����͸֧���    
        ,MTD_ACML_DEP_BAL_AMT    --���ۼƴ�����  
        ,QTD_ACML_DEP_BAL_AMT    --���ۼƴ�����  
        ,YTD_ACML_DEP_BAL_AMT    --���ۼƴ�����  
        ,MTD_ACML_OD_BAL_AMT     --���ۼ�͸֧���  
        ,QTD_ACML_OD_BAL_AMT     --���ۼ�͸֧���  
        ,YTD_ACML_OD_BAL_AMT     --���ۼ�͸֧��� 
            )    	 	  
	SELECT  
			   CUR.CST_ID                  --�ͻ�����      
        ,CUR.CRD_OU_ID                   --�˻�����������
        ,CUR.ENT_IDV_IND             --������
        ,CUR.MST_CRD_IND             --��/������־
        ,CUR.LN_FIVE_RTG_STS         --�����弶��̬����
        ,CUR.AST_RSK_ASES_RTG_TP_CD  --�ʲ����շ���
        ,CUR.PD_GRP_CD               --��Ʒ��      
        ,CUR.PD_SUB_CD               --��Ʒ�Ӵ���     
        ,CUR.CCY                     --����
        ,C_MON_DAY      --������������
        ,COALESCE(PRE.NOD_In_MTH,0) + CUR.NOD_In_MTH        --������Ч����
        ,C_QTR_DAY      --������������  
        ,COALESCE(PRE.NOD_In_QTR,0) + CUR.NOD_In_QTR        --������Ч����  
        ,C_YR_DAY          --������������  
        ,COALESCE(PRE.NOD_In_YEAR,0) + CUR.NOD_In_YEAR       --������Ч����                                   
        ,CUR_YEAR            --���YYYY      
        ,CUR_MONTH           --�·�MM        
        ,ACCOUNTING_DATE            --����YYYY-MM-DD    
        ,CUR.DEP_BAL_CRD             --���п�������  
        ,CUR.OD_BAL_AMT              --͸֧���        
        ,CUR.CR_LMT                  --���Ŷ��        
        ,CUR.INT_RCVB                --Ӧ����Ϣ
        ,COALESCE(PRE.DEP_BAL_CRD,0) --���մ�����  
        ,COALESCE(PRE.OD_BAL_AMT ,0) --����͸֧���
        ,COALESCE(PRE.MTD_ACML_DEP_BAL_AMT,0) + CUR.DEP_BAL_CRD    --���ۼƴ�����  
        ,COALESCE(PRE.QTD_ACML_DEP_BAL_AMT,0) + CUR.DEP_BAL_CRD    --���ۼƴ�����  
        ,COALESCE(PRE.YTD_ACML_DEP_BAL_AMT,0) + CUR.DEP_BAL_CRD    --���ۼƴ�����  
        ,COALESCE(PRE.MTD_ACML_OD_BAL_AMT ,0) + CUR.OD_BAL_AMT    --���ۼ�͸֧���  
        ,COALESCE(PRE.QTD_ACML_OD_BAL_AMT ,0) + CUR.OD_BAL_AMT    --���ۼ�͸֧���  
        ,COALESCE(PRE.YTD_ACML_OD_BAL_AMT ,0) + CUR.OD_BAL_AMT    --���ۼ�͸֧���                         

	FROM  SESSION.CUR  AS CUR LEFT OUTER JOIN  SESSION.TMP AS PRE ON 
       CUR.CST_ID                 =PRE.CST_ID                
   AND CUR.CRD_OU_ID              =PRE.CRD_OU_ID             
   AND CUR.CCY                    =PRE.CCY                   
   AND CUR.LN_FIVE_RTG_STS        =PRE.LN_FIVE_RTG_STS       
   AND CUR.MST_CRD_IND            =PRE.MST_CRD_IND           
   AND CUR.AST_RSK_ASES_RTG_TP_CD =PRE.AST_RSK_ASES_RTG_TP_CD
   AND CUR.PD_GRP_CD              =PRE.PD_GRP_CD             
   AND CUR.PD_SUB_CD              =PRE.PD_SUB_CD      
      ;--
 		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;	          --

 /** Insert the log**/
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	--

		SET SMY_STEPNUM = 5 ;--
		SET SMY_STEPDESC = 'ʹ��Merge���,����SMY ��'; 			 --
	 
MERGE INTO SMY.CST_CR_CRD_MTHLY_SMY AS T
 		USING  SESSION.S AS S 
 	  ON
         T.CST_ID                 =S.CST_ID                
     AND T.CRD_OU_ID              =S.CRD_OU_ID             
     AND T.CCY                    =S.CCY                   
     AND T.LN_FIVE_RTG_STS        =S.LN_FIVE_RTG_STS
     AND T.MST_CRD_IND            =S.MST_CRD_IND           
     AND T.AST_RSK_ASES_RTG_TP_CD =S.AST_RSK_ASES_RTG_TP_CD
     AND T.PD_GRP_CD              =S.PD_GRP_CD             
     AND T.PD_SUB_CD              =S.PD_SUB_CD
     AND T.CDR_YR                 =S.CDR_YR     
     AND T.CDR_MTH                =S.CDR_MTH    
WHEN MATCHED THEN UPDATE SET
    
         NOCLD_In_MTH             =S.NOCLD_In_MTH          --������������    
        ,NOD_In_MTH               =S.NOD_In_MTH            --������Ч����    
        ,NOCLD_In_QTR             =S.NOCLD_In_QTR          --������������    
        ,NOD_In_QTR               =S.NOD_In_QTR            --������Ч����    
        ,NOCLD_In_Year            =S.NOCLD_In_Year         --������������    
        ,NOD_In_Year              =S.NOD_In_Year           --������Ч����         
        ,ACG_DT                   =S.ACG_DT                --����YYYY-MM-DD  
        ,DEP_BAL_CRD              =S.DEP_BAL_CRD           --���п�������  
        ,OD_BAL_AMT               =S.OD_BAL_AMT            --͸֧���        
        ,CR_LMT                   =S.CR_LMT                --���Ŷ��        
        ,INT_RCVB                 =S.INT_RCVB              --Ӧ����Ϣ        
        ,LST_DAY_DEP_BAL          =S.LST_DAY_DEP_BAL       --���մ�����    
        ,LST_DAY_OD_BAL           =S.LST_DAY_OD_BAL        --����͸֧���    
        ,MTD_ACML_DEP_BAL_AMT     =S.MTD_ACML_DEP_BAL_AMT  --���ۼƴ�����  
        ,QTD_ACML_DEP_BAL_AMT     =S.QTD_ACML_DEP_BAL_AMT  --���ۼƴ�����  
        ,YTD_ACML_DEP_BAL_AMT     =S.YTD_ACML_DEP_BAL_AMT  --���ۼƴ�����  
        ,MTD_ACML_OD_BAL_AMT      =S.MTD_ACML_OD_BAL_AMT   --���ۼ�͸֧���  
        ,QTD_ACML_OD_BAL_AMT      =S.QTD_ACML_OD_BAL_AMT   --���ۼ�͸֧���  
        ,YTD_ACML_OD_BAL_AMT      =S.YTD_ACML_OD_BAL_AMT   --���ۼ�͸֧���  
          
WHEN NOT MATCHED THEN INSERT  	        
	 (
         CST_ID                  --�ͻ�����        
        ,CRD_OU_ID               --���������      
        ,ENT_IDV_IND             --������          
        ,MST_CRD_IND             --��/������־     
        ,LN_FIVE_RTG_STS         --�����弶��̬����
        ,AST_RSK_ASES_RTG_TP_CD  --�ʲ����շ���    
        ,PD_GRP_CD               --��Ʒ��          
        ,PD_SUB_CD               --��Ʒ�Ӵ���      
        ,CCY                     --����            
        ,NOCLD_In_MTH            --������������    
        ,NOD_In_MTH              --������Ч����    
        ,NOCLD_In_QTR            --������������    
        ,NOD_In_QTR              --������Ч����    
        ,NOCLD_In_Year           --������������    
        ,NOD_In_Year             --������Ч����
        ,CDR_YR                  --������
        ,CDR_MTH                 --������            
        ,ACG_DT                  --����YYYY-MM-DD  
        ,DEP_BAL_CRD             --���п�������  
        ,OD_BAL_AMT              --͸֧���        
        ,CR_LMT                  --���Ŷ��        
        ,INT_RCVB                --Ӧ����Ϣ        
        ,LST_DAY_DEP_BAL         --���մ�����    
        ,LST_DAY_OD_BAL          --����͸֧���    
        ,MTD_ACML_DEP_BAL_AMT    --���ۼƴ�����  
        ,QTD_ACML_DEP_BAL_AMT    --���ۼƴ�����  
        ,YTD_ACML_DEP_BAL_AMT    --���ۼƴ�����  
        ,MTD_ACML_OD_BAL_AMT     --���ۼ�͸֧���  
        ,QTD_ACML_OD_BAL_AMT     --���ۼ�͸֧���  
        ,YTD_ACML_OD_BAL_AMT     --���ۼ�͸֧���  
        )
    VALUES 
    (
         S.CST_ID                  --�ͻ�����        
        ,S.CRD_OU_ID               --���������      
        ,S.ENT_IDV_IND             --������          
        ,S.MST_CRD_IND             --��/������־     
        ,S.LN_FIVE_RTG_STS         --�����弶��̬����
        ,S.AST_RSK_ASES_RTG_TP_CD  --�ʲ����շ���    
        ,S.PD_GRP_CD               --��Ʒ��          
        ,S.PD_SUB_CD               --��Ʒ�Ӵ���      
        ,S.CCY                     --����            
        ,S.NOCLD_In_MTH            --������������    
        ,S.NOD_In_MTH              --������Ч����    
        ,S.NOCLD_In_QTR            --������������    
        ,S.NOD_In_QTR              --������Ч����    
        ,S.NOCLD_In_Year           --������������    
        ,S.NOD_In_Year             --������Ч����
        ,S.CDR_YR                  --������
        ,S.CDR_MTH                 --������            
        ,S.ACG_DT                  --����YYYY-MM-DD  
        ,S.DEP_BAL_CRD             --���п�������  
        ,S.OD_BAL_AMT              --͸֧���        
        ,S.CR_LMT                  --���Ŷ��        
        ,S.INT_RCVB                --Ӧ����Ϣ        
        ,S.LST_DAY_DEP_BAL         --���մ�����    
        ,S.LST_DAY_OD_BAL          --����͸֧���    
        ,S.MTD_ACML_DEP_BAL_AMT    --���ۼƴ�����  
        ,S.QTD_ACML_DEP_BAL_AMT    --���ۼƴ�����  
        ,S.YTD_ACML_DEP_BAL_AMT    --���ۼƴ�����  
        ,S.MTD_ACML_OD_BAL_AMT     --���ۼ�͸֧���  
        ,S.QTD_ACML_OD_BAL_AMT     --���ۼ�͸֧���  
        ,S.YTD_ACML_OD_BAL_AMT     --���ۼ�͸֧��� 
    )	  	
	;--
	
	 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
	 
	 COMMIT;--
END@