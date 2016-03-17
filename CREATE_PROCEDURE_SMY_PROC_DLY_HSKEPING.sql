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
/*声明异常处理使用变量*/
		DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
		DECLARE SMY_STEPNUM INT DEFAULT 0;                     --过程内部位置标记
		DECLARE SMY_STEPDESC VARCHAR(100) DEFAULT '';          --过程内部位置描述
		DECLARE SMY_DATE DATE;                                 --临时日期变量
		DECLARE SMY_RCOUNT INT;                                --DML语句作用记录数
		DECLARE SMY_PROCNM VARCHAR(100);                       --存储过程名称
/*异常处理*/
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

    /*变量赋值*/		
		SET SMY_PROCNM = 'PROC_DLY_HSKEPING';
		SET SMY_DATE = ACCOUNTING_DATE;    
		
    /*删除存储大于37天的数据*/    
    delete from SMY.OU_ACG_SBJ_BAL_DLY_SMY where ACG_DT <= ACCOUNTING_DATE -  37 days;--    
    GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;
    
    SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --
    INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, '删除SMY.OU_ACG_SBJ_BAL_DLY_SMY表中37天前的数据数据', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
    COMMIT;--    
END@