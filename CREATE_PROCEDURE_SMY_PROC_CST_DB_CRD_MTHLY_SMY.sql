CREATE PROCEDURE SMY.PROC_CST_DB_CRD_MTHLY_SMY(IN ACCOUNTING_DATE date)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_CST_DB_CRD_MTHLY_SMY.sql
-- Procedure name: 			SMY.PROC_CST_DB_CRD_MTHLY_SMY
-- Source Table:				SMY.CST_INF,SMY.DB_CRD_SMY
-- Target Table: 				SMY.CST_DB_CRD_MTHLY_SMY
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
-- 2009-12-04   Xu Yan					Rename the history table	
-- 2009-12-16   Xu Yan          Fixed a bug for reruning
-- 2010-08-11   Peng Yi tao     Modify the method of calendar days Calculating
-- 2011-08-03   wu zhan shan    Modify the merge method
-- 2012-02-28   Chen XiaoWen    CST_DB_CRD_MTHLY_SMY��Ĳ�ѯ����ͳһ�����²�ѯ��ΪACG_DT��������ѯ
-- 2012-04-09   Chen XiaoWen    ���ӱ���������DB_CRD_SMY.CRD_LCS_TP_ID in (11920001,11920002,11920003)
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
		DECLARE MTH_LAST_DAY DATE;

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
  
    SET SMY_PROCNM  ='PROC_CST_DB_CRD_MTHLY_SMY';--
    SET SMY_DATE    =ACCOUNTING_DATE;    --
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --ȡ��ǰ���
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --ȡ��ǰ�·�
    SET CUR_DAY     =DAY(ACCOUNTING_DATE);     --ȡ�µڼ���
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- ȡ�����
--------------------------------------------start on 20100811-------------------------------------------------------------    
    --SET C_YR_DAY      =DAYOFYEAR(DATE(TRIM(CHAR(YEAR(ACCOUNTING_DATE)))||'-12-31')); --ȡ����ڼ���
    SET C_YR_DAY      =DAYS(ACCOUNTING_DATE)-DAYS(YR_FIRST_DAY)+1;  --ȡ����ڼ���
--------------------------------------------end on 20100811-------------------------------------------------------------     
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --��ǰ����
    SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,ISO),1,7)||'-01'); --ȡ���³���
    SET MON_DAY     =DAY(ACCOUNTING_DATE);     --ȡ�µڼ���
    VALUES(MTH_FIRST_DAY + 1 MONTH - 1 DAY) INTO MTH_LAST_DAY ;
    
    VALUES(ACCOUNTING_DATE - 1 DAY) INTO LAST_SMY_DATE;--
    SET LAST_MONTH = MONTH(LAST_SMY_DATE);      --
    --��������������
--------------------------------------------start on 20100811-------------------------------------------------------------    
    --SET C_MON_DAY = DAYS(MTH_FIRST_DAY + 1 MONTH ) - DAYS(MTH_FIRST_DAY);    --
      SET C_MON_DAY = DAY(ACCOUNTING_DATE);                 --
--------------------------------------------end on 20100811-------------------------------------------------------------      
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
--------------------------------------------start on 20100811-------------------------------------------------------------
  	--SET C_QTR_DAY = DAYS(QTR_LAST_DAY) - DAYS(QTR_FIRST_DAY) + 1 ;--
  	SET C_QTR_DAY = DAYS(ACCOUNTING_DATE)-DAYS(QTR_FIRST_DAY)+1;--
--------------------------------------------end on 20100811------------------------------------------------------------- 
		
		SELECT COALESCE(MAX(ACG_DT),'1900-01-01') INTO MAX_ACG_DT FROM SMY.CST_DB_CRD_MTHLY_SMY;--

/*Delete��־��,����SMY_PROCNM=��ǰ�洢��������,SMY_DATE=ACCOUNTING_DATE,�������µ���ʼ��־*/
		DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
			COMMIT;--
		
		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
		
		SET SMY_STEPDESC = 	'�洢���̿�ʼ����' ;--
		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
				VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

/*���ݻָ��뱸��*/
    IF MAX_ACG_DT = ACCOUNTING_DATE THEN
       --DELETE FROM SMY.CST_DB_CRD_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = CUR_MONTH;--
       DELETE FROM SMY.CST_DB_CRD_MTHLY_SMY WHERE ACG_DT >= MTH_FIRST_DAY AND ACG_DT <= MTH_LAST_DAY;
    /**ÿ�µ�һ�ղ���Ҫ����ʷ���лָ�**/       
       IF MON_DAY <> 1 THEN
      	 INSERT INTO SMY.CST_DB_CRD_MTHLY_SMY SELECT * FROM HIS.CST_DB_CRD_MTHLY_SMY ;--
       END IF;--
     ELSE
  /** ���hist ���ݱ� **/

	    SET EMP_SQL= 'Alter TABLE HIS.CST_DB_CRD_MTHLY_SMY ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE' ;--
		
		  EXECUTE IMMEDIATE EMP_SQL;       --
      
      COMMIT;--

		  /**backup �������� **/
		  
		  --INSERT INTO HIS.CST_DB_CRD_MTHLY_SMY SELECT * FROM SMY.CST_DB_CRD_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = CUR_MONTH;--
		  INSERT INTO HIS.CST_DB_CRD_MTHLY_SMY SELECT * FROM SMY.CST_DB_CRD_MTHLY_SMY WHERE ACG_DT >= MTH_FIRST_DAY AND ACG_DT <= MTH_LAST_DAY;
    END IF;--

SET SMY_STEPNUM = 2 ;--
SET SMY_STEPDESC = '�����û���ʱ��,�������SMY����';--

	/*�����û���ʱ��*/
	
	DECLARE GLOBAL TEMPORARY TABLE TMP 
		LIKE SMY.CST_DB_CRD_MTHLY_SMY
	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K PARTITIONING KEY(CST_ID);--

 /*��������һ�ղ���Ҫ����*/

 IF YR_FIRST_DAY <>  ACCOUNTING_DATE THEN 
	INSERT INTO SESSION.TMP 
	(
         CST_ID                --�ͻ�����         
        ,AC_OU_ID              --�˻�����������   
        ,CRD_TP_ID             --������           
        ,PSBK_RLTD_F           --������ر�ʶ     
        ,IS_NONGXIN_CRD_F      --���տ�/ũ�ſ���ʶ
        ,CCY                   --����             
        ,CDR_YR                --���YYYY         
        ,CDR_MTH               --�·�MM           
        ,NOCLD_In_MTH          --������������     
        ,NOD_In_MTH            --������Ч����     
        ,NOCLD_In_QTR          --������������     
        ,NOD_In_QTR            --������Ч����     
        ,NOCLD_In_Year         --������������     
        ,NOD_In_Year           --������Ч����     
        ,ACG_DT                --����YYYY-MM-DD   
        ,CST_OU_ID             --�ͻ�������       
        ,CST_TP_ID             --�ͻ�����         
        ,NBR_CRD               --�˻�����         
        ,LST_DAY_BAL           --�������         
        ,BAL                   --���             
        ,MTD_ACML_BAL_AMT      --���ۼ����       
        ,QTD_ACML_BAL_AMT      --���ۼ����       
        ,YTD_ACML_BAL_AMT      --���ۼ���� 
          ) 
    SELECT
         CST_ID                --�ͻ�����         
        ,AC_OU_ID              --�˻�����������   
        ,CRD_TP_ID             --������           
        ,PSBK_RLTD_F           --������ر�ʶ     
        ,IS_NONGXIN_CRD_F      --���տ�/ũ�ſ���ʶ
        ,CCY                   --����             
        ,CDR_YR                --���YYYY         
        ,CDR_MTH               --�·�MM           
        ,NOCLD_In_MTH          --������������     
        ,NOD_In_MTH            --������Ч����     
        ,NOCLD_In_QTR          --������������     
        ,NOD_In_QTR            --������Ч����     
        ,NOCLD_In_Year         --������������     
        ,NOD_In_Year           --������Ч����     
        ,ACG_DT                --����YYYY-MM-DD   
        ,CST_OU_ID             --�ͻ�������       
        ,CST_TP_ID             --�ͻ�����         
        ,NBR_CRD               --�˻�����         
        ,LST_DAY_BAL           --�������         
        ,BAL                   --���             
        ,MTD_ACML_BAL_AMT      --���ۼ����       
        ,QTD_ACML_BAL_AMT      --���ۼ����       
        ,YTD_ACML_BAL_AMT      --���ۼ����       
     FROM SMY.CST_DB_CRD_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = LAST_MONTH;   --�������³�ʱLAST_MONTH��ֵΪ���£�ƽʱ��ֵΪ����
 END IF ;   --
	
       
      
 /** �ռ�������Ϣ */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	 --

----delete by wuzhanshan 20110803 begin
	 --IF  ACCOUNTING_DATE IN ( YR_FIRST_DAY )  --�� �� �� ����
	 --		THEN 
	 --			UPDATE SESSION.TMP 
	 --				SET  				
   --         MTD_ACML_BAL_AMT =0 --���ۼ����
   --        ,QTD_ACML_BAL_AMT =0 --���ۼ����
   --        ,YTD_ACML_BAL_AMT =0 --���ۼ����  
   --        ,NOD_In_MTH       =0 --������Ч����   
   --        ,NOD_In_QTR       =0 --������Ч����  
   --        ,NOD_In_Year      =0 --������Ч���� 
   --                    
	 --		  ;--
	 --ELSE
----delete by wuzhanshan 20110803 end
	 IF ACCOUNTING_DATE IN (QTR_FIRST_DAY) --�� �� ����
	 	  THEN
	 			UPDATE SESSION.TMP 
	 				SET 
            MTD_ACML_BAL_AMT =0 --���ۼ����
           ,QTD_ACML_BAL_AMT =0 --���ۼ���� 
           ,NOD_In_MTH       =0 --������Ч����   
           ,NOD_In_QTR       =0 --������Ч����         
	 			;	 	  --
	 	  	 		
	 ELSEIF ACCOUNTING_DATE IN ( MTH_FIRST_DAY ) --�¹���
	 	  THEN 
	 			UPDATE SESSION.TMP 
	 				SET 			
            MTD_ACML_BAL_AMT =0 --���ۼ����
           ,NOD_In_MTH       =0 --������Ч����             
	 			;	 	--
	 END IF;--

 /*��õ���ͳ������*/	
 
 
		SET SMY_STEPNUM = 3 ;--
		SET SMY_STEPDESC = '������ʱ��SESSION.CUR, ��Ž�ǿ����ջ��ܺ������';--

  DECLARE GLOBAL TEMPORARY TABLE CUR AS (
		SELECT 
			   DB_CRD_SMY.CST_ID   AS CST_ID            --�ͻ�����      
        ,DB_CRD_SMY.AC_OU_ID    AS AC_OU_ID          --�˻�����������
        ,DB_CRD_SMY.DB_CRD_TP_ID               AS CRD_TP_ID                   --������                          
        ,DB_CRD_SMY.PSBK_RLTD_F                AS PSBK_RLTD_F                 --������ر�ʶ 
        ,DB_CRD_SMY.IS_NONGXIN_CRD_F           AS IS_NONGXIN_CRD_F            --���տ�/ũ�ſ���ʶ
        ,DB_CRD_SMY.CCY      AS CCY               --����          
        ,1              AS NOD_IN_MTH
        ,1              AS NOD_IN_QTR
        ,1              AS NOD_IN_YEAR
        ,COALESCE(CST_INF.RPRG_OU_IP_ID,'')                  AS CST_OU_ID             --�ͻ�������         
        ,COALESCE(CST_INF.ENT_IDV_IND,-1)                    AS CST_TP_ID         --�ͻ�����    
        ,COALESCE(COUNT(DISTINCT DB_CRD_SMY.AC_AR_ID),0)     AS NBR_CRD           --�˻�����    
        ,SUM(DB_CRD_SMY.AC_BAL_AMT)              AS BAL               --���                                                                
		FROM            SMY.DB_CRD_SMY  AS DB_CRD_SMY
		LEFT OUTER JOIN SMY.CST_INF     AS CST_INF	ON DB_CRD_SMY.CST_ID=CST_INF.CST_ID
		GROUP BY 
			   DB_CRD_SMY.CST_ID		
        ,DB_CRD_SMY.AC_OU_ID
        ,DB_CRD_SMY.DB_CRD_TP_ID     
        ,DB_CRD_SMY.PSBK_RLTD_F      
        ,DB_CRD_SMY.IS_NONGXIN_CRD_F         
        ,DB_CRD_SMY.CCY
        ,COALESCE(CST_INF.RPRG_OU_IP_ID,'')
        ,COALESCE(CST_INF.ENT_IDV_IND,-1) 
   ) DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K 
     PARTITIONING KEY(CST_ID)
   ;	 		--
		
  INSERT INTO SESSION.CUR 
		SELECT 
			   DB_CRD_SMY.CST_ID   AS CST_ID            --�ͻ�����      
        ,DB_CRD_SMY.AC_OU_ID    AS AC_OU_ID          --�˻�����������
        ,DB_CRD_SMY.DB_CRD_TP_ID               AS CRD_TP_ID                   --������                          
        ,DB_CRD_SMY.PSBK_RLTD_F                AS PSBK_RLTD_F                 --������ر�ʶ 
        ,DB_CRD_SMY.IS_NONGXIN_CRD_F           AS IS_NONGXIN_CRD_F            --���տ�/ũ�ſ���ʶ
        ,DB_CRD_SMY.CCY      AS CCY               --����          
        ,1              AS NOD_IN_MTH
        ,1              AS NOD_IN_QTR
        ,1              AS NOD_IN_YEAR
        ,COALESCE(CST_INF.RPRG_OU_IP_ID,'')                  AS CST_OU_ID             --�ͻ�������         
        ,COALESCE(CST_INF.ENT_IDV_IND,-1)                    AS CST_TP_ID         --�ͻ�����    
        ,COALESCE(COUNT(DISTINCT DB_CRD_SMY.AC_AR_ID),0)     AS NBR_CRD           --�˻�����    
        ,SUM(DB_CRD_SMY.AC_BAL_AMT)              AS BAL               --���                                                                
		FROM            SMY.DB_CRD_SMY  AS DB_CRD_SMY
		LEFT OUTER JOIN SMY.CST_INF     AS CST_INF	ON DB_CRD_SMY.CST_ID=CST_INF.CST_ID
		WHERE DB_CRD_SMY.CRD_LCS_TP_ID in (11920001,11920002,11920003) --11920001:����,11920002:�·���δ����,11920003:�»���δ����
		GROUP BY 
			   DB_CRD_SMY.CST_ID		
        ,DB_CRD_SMY.AC_OU_ID
        ,DB_CRD_SMY.DB_CRD_TP_ID     
        ,DB_CRD_SMY.PSBK_RLTD_F      
        ,DB_CRD_SMY.IS_NONGXIN_CRD_F         
        ,DB_CRD_SMY.CCY
        ,COALESCE(CST_INF.RPRG_OU_IP_ID,'')
        ,COALESCE(CST_INF.ENT_IDV_IND,-1)  
   ;	 --
 /** �ռ�������Ϣ */		                             
 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 	INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	 --

		SET SMY_STEPNUM = 4 ;--
		SET SMY_STEPDESC = '������ʱ��SESSION.S, ������Ž�ǿ����ܺ�Ҫ���µ�����'; 			 --
		
/**/
  DECLARE GLOBAL TEMPORARY TABLE S  LIKE SMY.CST_DB_CRD_MTHLY_SMY 
  	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K 
     PARTITIONING KEY(CST_ID);--
  
	INSERT INTO SESSION.S
          (
         CST_ID                --�ͻ�����         
        ,AC_OU_ID              --�˻�����������   
        ,CRD_TP_ID             --������           
        ,PSBK_RLTD_F           --������ر�ʶ     
        ,IS_NONGXIN_CRD_F      --���տ�/ũ�ſ���ʶ
        ,CCY                   --����             
        ,CDR_YR                --���YYYY         
        ,CDR_MTH               --�·�MM           
        ,NOCLD_In_MTH          --������������     
        ,NOD_In_MTH            --������Ч����     
        ,NOCLD_In_QTR          --������������     
        ,NOD_In_QTR            --������Ч����     
        ,NOCLD_In_Year         --������������     
        ,NOD_In_Year           --������Ч����     
        ,ACG_DT                --����YYYY-MM-DD   
        ,CST_OU_ID             --�ͻ�������       
        ,CST_TP_ID             --�ͻ�����         
        ,NBR_CRD               --�˻�����         
        ,LST_DAY_BAL           --�������         
        ,BAL                   --���             
        ,MTD_ACML_BAL_AMT      --���ۼ����       
        ,QTD_ACML_BAL_AMT      --���ۼ����       
        ,YTD_ACML_BAL_AMT      --���ۼ���� 
            )    	 	  
	SELECT  
        CUR.CST_ID            --�ͻ�����      
       ,CUR.AC_OU_ID          --�˻�����������
       ,CUR.CRD_TP_ID             --������           
       ,CUR.PSBK_RLTD_F           --������ر�ʶ     
       ,CUR.IS_NONGXIN_CRD_F      --���տ�/ũ�ſ���ʶ   
       ,CUR.CCY               --����          
       ,CUR_YEAR            --���YYYY      
       ,CUR_MONTH           --�·�MM        
       ,C_MON_DAY      --������������  
       ,COALESCE(PRE.NOD_In_MTH,0) + CUR.NOD_In_MTH        --������Ч����  
       ,C_QTR_DAY      --������������  
       ,COALESCE(PRE.NOD_In_QTR,0) + CUR.NOD_In_QTR        --������Ч����  
       ,C_YR_DAY          --������������  
       ,COALESCE(PRE.NOD_In_YEAR,0) + CUR.NOD_In_YEAR       --������Ч����  
       ,ACCOUNTING_DATE            --����YYYY-MM-DD
       ,CUR.CST_OU_ID           --�ͻ�������    
       ,CUR.CST_TP_ID        --�ͻ�����      
       ,CUR.NBR_CRD        --�˻�����      
       ,COALESCE(PRE.BAL,0)               --�������      
       ,CUR.BAL             --���          
       ,COALESCE(PRE.MTD_ACML_BAL_AMT,0) + CUR.BAL  --���ۼ����    
       ,COALESCE(PRE.QTD_ACML_BAL_AMT,0) + CUR.BAL  --���ۼ����    
       ,COALESCE(PRE.YTD_ACML_BAL_AMT,0) + CUR.BAL  --���ۼ����  

	FROM  SESSION.CUR  AS CUR LEFT OUTER JOIN  SESSION.TMP AS PRE ON 
        CUR.CST_ID            =PRE.CST_ID          
    AND CUR.AC_OU_ID          =PRE.AC_OU_ID        
    AND CUR.CRD_TP_ID         =PRE.CRD_TP_ID       
    AND CUR.PSBK_RLTD_F       =PRE.PSBK_RLTD_F     
    AND CUR.IS_NONGXIN_CRD_F  =PRE.IS_NONGXIN_CRD_F
    AND CUR.CCY               =PRE.CCY          
      ;--
----add by wuzhanshan 20110803 begin
IF ACCOUNTING_DATE<>MTH_FIRST_DAY THEN
   CREATE INDEX SESSION.IDX_S ON SESSION.S(CST_ID,AC_OU_ID,CRD_TP_ID,PSBK_RLTD_F,IS_NONGXIN_CRD_F,CCY,CDR_YR,CDR_MTH);
END IF;
----add by wuzhanshan 20110803 end      
 		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;	          --

 /** Insert the log**/
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	--

		SET SMY_STEPNUM = 5 ;--
		SET SMY_STEPDESC = 'ʹ��Merge���,����SMY ��'; 			 --

----modify by wuzhanshan 20110803 begin
IF ACCOUNTING_DATE =MTH_FIRST_DAY AND MAX_ACG_DT <= ACCOUNTING_DATE THEN
   INSERT INTO SMY.CST_DB_CRD_MTHLY_SMY
   (     CST_ID                --�ͻ�����         
        ,AC_OU_ID              --�˻�����������   
        ,CRD_TP_ID             --������           
        ,PSBK_RLTD_F           --������ر�ʶ     
        ,IS_NONGXIN_CRD_F      --���տ�/ũ�ſ���ʶ
        ,CCY                   --����             
        ,CDR_YR                --���YYYY         
        ,CDR_MTH               --�·�MM           
        ,NOCLD_In_MTH          --������������     
        ,NOD_In_MTH            --������Ч����     
        ,NOCLD_In_QTR          --������������     
        ,NOD_In_QTR            --������Ч����     
        ,NOCLD_In_Year         --������������     
        ,NOD_In_Year           --������Ч����     
        ,ACG_DT                --����YYYY-MM-DD   
        ,CST_OU_ID             --�ͻ�������       
        ,CST_TP_ID             --�ͻ�����         
        ,NBR_CRD               --�˻�����         
        ,LST_DAY_BAL           --�������         
        ,BAL                   --���             
        ,MTD_ACML_BAL_AMT      --���ۼ����       
        ,QTD_ACML_BAL_AMT      --���ۼ����       
        ,YTD_ACML_BAL_AMT      --���ۼ���� 
    )
   SELECT S.CST_ID                --�ͻ�����         
         ,S.AC_OU_ID              --�˻�����������   
         ,S.CRD_TP_ID             --������           
         ,S.PSBK_RLTD_F           --������ر�ʶ     
         ,S.IS_NONGXIN_CRD_F      --���տ�/ũ�ſ���ʶ
         ,S.CCY                   --����             
         ,S.CDR_YR                --���YYYY         
         ,S.CDR_MTH               --�·�MM           
         ,S.NOCLD_In_MTH          --������������     
         ,S.NOD_In_MTH            --������Ч����     
         ,S.NOCLD_In_QTR          --������������     
         ,S.NOD_In_QTR            --������Ч����     
         ,S.NOCLD_In_Year         --������������     
         ,S.NOD_In_Year           --������Ч����     
         ,S.ACG_DT                --����YYYY-MM-DD   
         ,S.CST_OU_ID             --�ͻ�������       
         ,S.CST_TP_ID             --�ͻ�����         
         ,S.NBR_CRD               --�˻�����         
         ,S.LST_DAY_BAL           --�������         
         ,S.BAL                   --���             
         ,S.MTD_ACML_BAL_AMT      --���ۼ����       
         ,S.QTD_ACML_BAL_AMT      --���ۼ����       
         ,S.YTD_ACML_BAL_AMT      --���ۼ���� 
   FROM SESSION.S S;
ELSE
   MERGE INTO SMY.CST_DB_CRD_MTHLY_SMY AS T
    		USING  SESSION.S AS S 
    	  ON
           T.CST_ID            =S.CST_ID          
       AND T.AC_OU_ID          =S.AC_OU_ID        
       AND T.CRD_TP_ID         =S.CRD_TP_ID       
       AND T.PSBK_RLTD_F       =S.PSBK_RLTD_F     
       AND T.IS_NONGXIN_CRD_F  =S.IS_NONGXIN_CRD_F
       AND T.CCY               =S.CCY         
       AND T.CDR_YR            =S.CDR_YR     
       AND T.CDR_MTH           =S.CDR_MTH    	  	    
   WHEN MATCHED THEN UPDATE SET
           NOCLD_IN_MTH     = S.NOCLD_IN_MTH     --������������  
          ,NOD_IN_MTH       = S.NOD_IN_MTH       --������Ч����  
          ,NOCLD_IN_QTR     = S.NOCLD_IN_QTR     --������������  
          ,NOD_IN_QTR       = S.NOD_IN_QTR       --������Ч����  
          ,NOCLD_IN_YEAR    = S.NOCLD_IN_YEAR    --������������  
          ,NOD_IN_YEAR      = S.NOD_IN_YEAR      --������Ч����  
          ,ACG_DT           = S.ACG_DT           --����YYYY-MM-DD
          ,CST_OU_ID        = S.CST_OU_ID        --�ͻ�������    
          ,CST_TP_ID        = S.CST_TP_ID        --�ͻ�����      
          ,NBR_CRD          = S.NBR_CRD          --�˻�����      
          ,LST_DAY_BAL      = S.LST_DAY_BAL      --�������      
          ,BAL              = S.BAL              --���          
          ,MTD_ACML_BAL_AMT = S.MTD_ACML_BAL_AMT --���ۼ����    
          ,QTD_ACML_BAL_AMT = S.QTD_ACML_BAL_AMT --���ۼ����    
          ,YTD_ACML_BAL_AMT = S.YTD_ACML_BAL_AMT --���ۼ����  
   WHEN NOT MATCHED THEN INSERT  	        
   	 (
            CST_ID                --�ͻ�����         
           ,AC_OU_ID              --�˻�����������   
           ,CRD_TP_ID             --������           
           ,PSBK_RLTD_F           --������ر�ʶ     
           ,IS_NONGXIN_CRD_F      --���տ�/ũ�ſ���ʶ
           ,CCY                   --����             
           ,CDR_YR                --���YYYY         
           ,CDR_MTH               --�·�MM           
           ,NOCLD_In_MTH          --������������     
           ,NOD_In_MTH            --������Ч����     
           ,NOCLD_In_QTR          --������������     
           ,NOD_In_QTR            --������Ч����     
           ,NOCLD_In_Year         --������������     
           ,NOD_In_Year           --������Ч����     
           ,ACG_DT                --����YYYY-MM-DD   
           ,CST_OU_ID             --�ͻ�������       
           ,CST_TP_ID             --�ͻ�����         
           ,NBR_CRD               --�˻�����         
           ,LST_DAY_BAL           --�������         
           ,BAL                   --���             
           ,MTD_ACML_BAL_AMT      --���ۼ����       
           ,QTD_ACML_BAL_AMT      --���ۼ����       
           ,YTD_ACML_BAL_AMT      --���ۼ���� 
           )
       VALUES 
       (
            S.CST_ID                --�ͻ�����         
           ,S.AC_OU_ID              --�˻�����������   
           ,S.CRD_TP_ID             --������           
           ,S.PSBK_RLTD_F           --������ر�ʶ     
           ,S.IS_NONGXIN_CRD_F      --���տ�/ũ�ſ���ʶ
           ,S.CCY                   --����             
           ,S.CDR_YR                --���YYYY         
           ,S.CDR_MTH               --�·�MM           
           ,S.NOCLD_In_MTH          --������������     
           ,S.NOD_In_MTH            --������Ч����     
           ,S.NOCLD_In_QTR          --������������     
           ,S.NOD_In_QTR            --������Ч����     
           ,S.NOCLD_In_Year         --������������     
           ,S.NOD_In_Year           --������Ч����     
           ,S.ACG_DT                --����YYYY-MM-DD   
           ,S.CST_OU_ID             --�ͻ�������       
           ,S.CST_TP_ID             --�ͻ�����         
           ,S.NBR_CRD               --�˻�����         
           ,S.LST_DAY_BAL           --�������         
           ,S.BAL                   --���             
           ,S.MTD_ACML_BAL_AMT      --���ۼ����       
           ,S.QTD_ACML_BAL_AMT      --���ۼ����       
           ,S.YTD_ACML_BAL_AMT      --���ۼ���� 
       )
          ;
END IF;--
----modify by wuzhanshan 20110803 end		
	 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
	 
	 COMMIT;--
END@