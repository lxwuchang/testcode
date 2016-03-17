CREATE PROCEDURE SMY.PROC_DLY_HSKEPING(ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.OU_ACG_SBJ_BAL_DLY_SMY.sql
-- Procedure name: 	SMY.OU_ACG_SBJ_BAL_DLY_SMY
-- Source Table:	SMY.OU_ACG_SBJ_BAL_DLY_SMY
--                      
-- Target Table: 	SMY.OU_ACG_SBJ_BAL_DLY_SMY
--                      
-- Project     :        ZJ RCCB EDW
-- NOTES       :        
-- Purpose     :            
-- PROCESS METHOD      :  delete the data which stores over 37days
--=============================================================================
-- Creation Date:       2010.10.05
-- Origin Author:       WangJian IBM copyright 
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2010-10-05   Wang Jian     Create SP File
-- 
-- 
-------------------------------------------------------------------------------
LANGUAGE SQL
BEGIN
/*�����쳣����ʹ�ñ���*/
		DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
		DECLARE SMY_STEPNUM INT DEFAULT 0;                     --�����ڲ�λ�ñ��
		DECLARE SMY_STEPDESC VARCHAR(100) DEFAULT '';          --�����ڲ�λ������
		DECLARE SMY_DATE DATE;                                 --��ʱ���ڱ���
		DECLARE SMY_RCOUNT INT;                                --DML������ü�¼��
		DECLARE SMY_PROCNM VARCHAR(100);                       --�洢��������
/*�쳣����*/
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
    	SET SMY_SQLCODE = SQLCODE;--
      ROLLBACK;--
      SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, 'PROCEDURE START.', SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
      COMMIT;--
      RESIGNAL;--
    END;--

    DECLARE CONTINUE HANDLER FOR SQLWARNING
    BEGIN
      SET SMY_SQLCODE = SQLCODE;--
      SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
      COMMIT;--
    END;--

    /*������ֵ*/		
		SET SMY_PROCNM = 'PROC_DLY_HSKEPING';
		SET SMY_DATE = ACCOUNTING_DATE;    
		
    /*ɾ���洢����37�������*/    
    delete from SMY.OU_ACG_SBJ_BAL_DLY_SMY where ACG_DT <= ACCOUNTING_DATE -  37 days;--    
    GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
    
    SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --
    INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, 'ɾ��SMY.OU_ACG_SBJ_BAL_DLY_SMY����37��ǰ����������', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
    COMMIT;--    
END@