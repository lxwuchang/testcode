CREATE PROCEDURE SMY.PROC_CST_CR_MTHLY_SMY(IN ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_CST_CR_MTHLY_SMY.sql
-- Procedure name: 			SMY.PROC_CST_CR_MTHLY_SMY
-- Source Table:				SOR.CST_LMT_DTL_INF,SOR.LMT_USED_INF
-- Target Table: 				SMY.CST_CR_MTHLY_SMY
-- Project     :        ZJ RCCB EDW
-- NOTES       :        
-- Purpose     :            
-- PROCESS METHOD      :  UPDATE EACH DAY ,INSERT IN THE PERIOD OF ONE MONTH
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
-- 2009-12-04   Xu Yan          Rename the history table	
-- 2009-12-16   Xu Yan          Fixed a bug for rerunning
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
		DECLARE CUR_YEAR SMALLINT;
		DECLARE CUR_MONTH SMALLINT;
		DECLARE CUR_DAY INTEGER;
		DECLARE YR_FIRST_DAY DATE;
		DECLARE QTR_FIRST_DAY DATE;
		DECLARE YR_DAY SMALLINT;
		DECLARE QTR_DAY SMALLINT;
		DECLARE MAX_ACG_DT DATE;
		DECLARE LAST_SMY_DATE DATE;
		DECLARE MTH_FIRST_DAY DATE;
		DECLARE V_T SMALLINT;
    DECLARE C_YR_DAY SMALLINT;
		DECLARE C_QTR_DAY SMALLINT;
		DECLARE QTR_LAST_DAY DATE;
		DECLARE C_MON_DAY SMALLINT;
		DECLARE CUR_QTR SMALLINT;
		DECLARE MON_DAY SMALLINT;
		DECLARE LAST_MONTH SMALLINT;
		DECLARE EMP_SQL VARCHAR(200);  

/*
	1.�������SQL�쳣����ľ��(EXIT��ʽ).
  2.������SQL�쳣ʱ�ڴ洢�����е�λ��(SMY_STEPNUM),λ������(SMY_STEPDESC),SQLCODE(SMY_SQLCODE)�����SMY_LOG����������.
  3.����RESIGNAL���������쳣,�����洢����ִ����,������SQL�쳣֮ǰ�洢������������ɵĲ������лع�.
*/

		DECLARE CONTINUE HANDLER FOR NOT FOUND
		  SET V_T=0 ; 
		    
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

   /*������ֵ*/
    SET SMY_PROCNM  ='PROC_CST_CR_MTHLY_SMY';
    SET SMY_DATE    =ACCOUNTING_DATE;    
    SET CUR_YEAR    =YEAR(ACCOUNTING_DATE);  --ȡ��ǰ���
    SET CUR_MONTH   =MONTH(ACCOUNTING_DATE); --ȡ��ǰ�·�
    SET CUR_DAY     =DAY(ACCOUNTING_DATE);     --ȡ�µڼ���
    SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');  -- ȡ�����
    SET C_YR_DAY      =DAYOFYEAR(DATE(TRIM(CHAR(YEAR(ACCOUNTING_DATE)))||'-12-31')); --ȡ����ڼ���
    SET CUR_QTR     =QUARTER(ACCOUNTING_DATE);   --��ǰ����
    SET MTH_FIRST_DAY=DATE(SUBSTR(CHAR(ACCOUNTING_DATE,iso),1,7)||'-01'); --ȡ���³���
    SET MON_DAY     =DAY(ACCOUNTING_DATE);     --ȡ�µڼ���
    
    VALUES(ACCOUNTING_DATE - 1 DAY) INTO LAST_SMY_DATE;
    SET LAST_MONTH = MONTH(LAST_SMY_DATE);      
    --��������������
    SET C_MON_DAY = DAYS(MTH_FIRST_DAY + 1 MONTH ) - DAYS(MTH_FIRST_DAY);    
    
    --���㼾����������
    IF CUR_QTR = 1  
       THEN 
       	SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');
       	SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-04-01') - 1 DAY ;
    ELSEIF CUR_QTR = 2
       THEN 
       	SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-04-01');
       	SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-07-01') - 1 DAY ;       	
    ELSEIF CUR_QTR = 3
       THEN 
       	SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-07-01');
       	SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-10-01') - 1 DAY ;       	
    ELSE
       SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-10-01');
       SET QTR_LAST_DAY =DATE(TRIM(CHAR(CUR_YEAR))||'-12-31');       
    END IF;

  /*ȡ������������*/ 
  	SET C_QTR_DAY = DAYS(QTR_LAST_DAY) - DAYS(QTR_FIRST_DAY) + 1 ;
		
		SELECT COALESCE(MAX(ACG_DT),'1900-01-01') INTO MAX_ACG_DT FROM SMY.CST_CR_MTHLY_SMY;

/*Delete��־��,����SMY_PROCNM=��ǰ�洢��������,SMY_DATE=ACCOUNTING_DATE,�������µ���ʼ��־*/
		DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;
			COMMIT;
		
		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
		
		SET SMY_STEPDESC = 	'�洢���̿�ʼ����' ;
		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
				VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);

/*���ݻָ��뱸��*/
    IF MAX_ACG_DT = ACCOUNTING_DATE THEN
       DELETE FROM SMY.CST_CR_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = CUR_MONTH;
    /**ÿ�µ�һ�ղ���Ҫ����ʷ���лָ�**/       
       IF MON_DAY <> 1 THEN
      	 INSERT INTO SMY.CST_CR_MTHLY_SMY SELECT * FROM HIS.CST_CR_MTHLY_SMY ;
       END IF;
     ELSE
  /** ���hist ���ݱ� **/

	    SET EMP_SQL= 'Alter TABLE HIS.CST_CR_MTHLY_SMY ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE' ;
		
		  EXECUTE IMMEDIATE EMP_SQL;       
      
      COMMIT;
            	
		  /**backup �������� **/
		  
		  INSERT INTO HIS.CST_CR_MTHLY_SMY SELECT * FROM SMY.CST_CR_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = CUR_MONTH;
      
    END IF;

SET SMY_STEPNUM = 2 ;
SET SMY_STEPDESC = '�����û���ʱ��,�������SMY����';

	/*�����û���ʱ��*/
	
	DECLARE GLOBAL TEMPORARY TABLE TMP 
		LIKE SMY.CST_CR_MTHLY_SMY
	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE  IN TS_USR_TMP32K 
	PARTITIONING KEY(CST_ID) ;

 /*��������һ�ղ���Ҫ����*/

 IF YR_FIRST_DAY <>  ACCOUNTING_DATE THEN 
	INSERT INTO SESSION.TMP 
	(
         CST_ID               --�ͻ�����      
        ,OU_ID                --�ͻ�����������
        ,CR_PD_LMT_CGY_TP_Id  --���Ų�Ʒ����  
        ,CCY                  --����          
        ,CDR_YR               --���YYYY      
        ,CDR_MTH              --�·�MM        
        ,ACG_DT               --����YYYY-MM-DD
        ,CR_AMT               --���Ž��      
        ,CR_LMT_AMT_USED      --��ʹ�����Ŷ��
        ,TMP_CRED_LMT         --��ʱ���Ž��  
          ) 
    SELECT
         CST_ID               --�ͻ�����      
        ,OU_ID                --�ͻ�����������
        ,CR_PD_LMT_CGY_TP_Id  --���Ų�Ʒ����  
        ,CCY                  --����          
        ,CDR_YR               --���YYYY      
        ,CDR_MTH              --�·�MM        
        ,ACG_DT               --����YYYY-MM-DD
        ,CR_AMT               --���Ž��      
        ,CR_LMT_AMT_USED      --��ʹ�����Ŷ��
        ,TMP_CRED_LMT         --��ʱ���Ž�� 		       
     FROM SMY.CST_CR_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = LAST_MONTH;
 END IF ;   

 
		SET SMY_STEPNUM = 3 ;
		SET SMY_STEPDESC = '������ʱ��SESSION.S, �������DEP_AR_SMY ��SMY.CST_INF ���ܺ�Ҫ���µ�����'; 			 


/**/
  DECLARE GLOBAL TEMPORARY TABLE S  LIKE SMY.CST_CR_MTHLY_SMY 
  	ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K 
     PARTITIONING KEY(CST_ID);
  
	INSERT INTO SESSION.S
          (
         CST_ID               --�ͻ�����      
        ,OU_ID                --�ͻ�����������
        ,CR_PD_LMT_CGY_TP_Id  --���Ų�Ʒ����  
        ,CCY                  --����          
        ,CDR_YR               --���YYYY      
        ,CDR_MTH              --�·�MM        
        ,ACG_DT               --����YYYY-MM-DD
        ,CR_AMT               --���Ž��      
        ,CR_LMT_AMT_USED      --��ʹ�����Ŷ��
        ,TMP_CRED_LMT         --��ʱ���Ž��  
            )
	WITH TMP_CST_LMT_DTL_INF  AS 
   (
     SELECT 
     	PRIM_CST_ID
     ,RPRG_OU_ID
     ,CRT_PD_LIM_CGY_TP_ID
     ,SUM(CASE WHEN TMP_LMT_F=0	 THEN CRT_LIM  ELSE 0 END) AS CR_AMT
     ,SUM(CASE WHEN TMP_LMT_F=1	 THEN CRT_LIM  ELSE 0 END) AS TMP_CRED_LMT   
     FROM SOR.CST_LMT_DTL_INF
     GROUP BY 
     	PRIM_CST_ID
     ,RPRG_OU_ID
     ,CRT_PD_LIM_CGY_TP_ID     		    
      )              	 	  
	SELECT  
        CST_LMT_DTL_INF.PRIM_CST_ID
       ,CST_LMT_DTL_INF.RPRG_OU_ID
       ,CST_LMT_DTL_INF.CRT_PD_LIM_CGY_TP_ID
       ,COALESCE(LMT_USED_INF.DNMN_CCY_ID,'CNY')	
       ,CUR_YEAR
       ,CUR_MONTH
       ,ACCOUNTING_DATE
       ,SUM(CST_LMT_DTL_INF.CR_AMT)
       ,SUM(LMT_USED_INF.USED_LMT_AMT + LMT_USED_INF.SUB_USED_LMT_AMT)
       ,SUM(CST_LMT_DTL_INF.TMP_CRED_LMT)

		FROM          TMP_CST_LMT_DTL_INF AS CST_LMT_DTL_INF
   LEFT OUTER JOIN SOR.LMT_USED_INF    AS LMT_USED_INF 
    ON  CST_LMT_DTL_INF.PRIM_CST_ID          = LMT_USED_INF.PRIM_CST_ID 
   AND CST_LMT_DTL_INF.CRT_PD_LIM_CGY_TP_ID = LMT_USED_INF.CRT_PD_LIM_CGY_TP_ID
   GROUP BY 
        CST_LMT_DTL_INF.PRIM_CST_ID
        ,CST_LMT_DTL_INF.RPRG_OU_ID
        ,CST_LMT_DTL_INF.CRT_PD_LIM_CGY_TP_ID
        ,LMT_USED_INF.DNMN_CCY_ID	   
      ;
 		GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;	          

 /** Insert the log**/
    SET SMY_RCOUNT=0;
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	

		SET SMY_STEPNUM = 4 ;
		SET SMY_STEPDESC = 'ʹ��Merge���,����SMY ��'; 			 
	 
MERGE INTO SMY.CST_CR_MTHLY_SMY AS T
 		USING  SESSION.S AS S 
 	  ON
          S.CST_ID              =T.CST_ID             
      AND S.OU_ID            =T.OU_ID           
      AND S.CR_PD_LMT_CGY_TP_ID =T.CR_PD_LMT_CGY_TP_ID
      AND S.CCY                 =T.CCY                
      AND S.CDR_YR              =T.CDR_YR             
      AND S.CDR_MTH             =T.CDR_MTH            
WHEN MATCHED THEN UPDATE SET

        T.ACG_DT                 =S.ACG_DT          
       ,T.CR_AMT                 =S.CR_AMT          
       ,T.CR_LMT_AMT_USED        =S.CR_LMT_AMT_USED 
       ,T.TMP_CRED_LMT           =S.TMP_CRED_LMT                
WHEN NOT MATCHED THEN INSERT  	        
	 (
         CST_ID               --�ͻ�����      
        ,OU_ID                --�ͻ�����������
        ,CR_PD_LMT_CGY_TP_Id  --���Ų�Ʒ����  
        ,CCY                  --����          
        ,CDR_YR               --���YYYY      
        ,CDR_MTH              --�·�MM        
        ,ACG_DT               --����YYYY-MM-DD
        ,CR_AMT               --���Ž��      
        ,CR_LMT_AMT_USED      --��ʹ�����Ŷ��
        ,TMP_CRED_LMT         --��ʱ���Ž��  
        )
    VALUES 
    (
         S.CST_ID               --�ͻ�����      
        ,S.OU_ID                --�ͻ�����������
        ,S.CR_PD_LMT_CGY_TP_Id  --���Ų�Ʒ����  
        ,S.CCY                  --����          
        ,S.CDR_YR               --���YYYY      
        ,S.CDR_MTH              --�·�MM        
        ,S.ACG_DT               --����YYYY-MM-DD
        ,S.CR_AMT               --���Ž��      
        ,S.CR_LMT_AMT_USED      --��ʹ�����Ŷ��
        ,S.TMP_CRED_LMT         --��ʱ���Ž��  
    )	  	
	;
	
	 	GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
 		INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
	 VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);	
	 
	 COMMIT;
END@