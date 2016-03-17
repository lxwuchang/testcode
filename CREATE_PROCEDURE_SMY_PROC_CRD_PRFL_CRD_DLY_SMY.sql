CREATE PROCEDURE SMY.PROC_CRD_PRFL_CRD_DLY_SMY(ACCOUNTING_DATE DATE)

-------------------------------------------------------------------------------

-- (C) Copyright ZJRCU and IBM <date>

--

-- File name:           SMY.PROC_CRD_PRFL_CRD_DLY_SMY.sql

-- Procedure name: 			SMY.PROC_CRD_PRFL_CRD_DLY_SMY

-- Source Table:				SOR.CR_CRD,SOR.CRD,SOR.CC_AC_AR,SOR.DB_CRD

-- Target Table: 				SMY.CRD_PRFL_CRD_DLY_SMY

-- Project:             ZJ RCCB EDW

-- Note                 Delete and Insert and Update

-- Purpose:             

--

--=============================================================================

-- Creation Date:       CUR_YEAR.11.09

-- Origin Author:       Peng Jie

--

-- Version: %1.0%

--

-- Modification History

-- --------------------

--

-- Date         ByPerson        Description

-- ----------   --------------  -----------------------------------------------

-- 2009-11-09   Peng Jie     Create SP File		

-- 2009-11-25   Xu Yan       Restructed the script

-- 2009-12-03   Xu Yan       Added a new column 'NBR_CST'

-- 2009-12-08   Xu Yan       Added 3 new columns 'NBR_DRMT_AC' ,'NBR_DRMT_CRD_WITH_LOW_BAL', 'NBR_DRMT_AC_WITH_LOW_BAL'

-- 2009-12-14   Xu Yan       Updated the 'NBR_DRMT_CRD', 'ACT_NBR_AC', 'NBR_DRMT_AC_WITH_LOW_BAL', 'NBR_DRMT_AC', 'NBR_NEW_CST', 'NBR_AC_Opened', 'NBR_AC_CLS'

-- 2009-12-15   Xu Yan       Updated the 'NBR_DRMT_CRD' for the credit cards due to the business rule change

-- 2009-12-22   Xu Yan       Updated the DB_CRD part to take IS_NONGXIN_CRD_F directly 

-- 2009-12-23   Xu Yan       Due to the business request, change the 'NBR_NEW_CRD' definition to describe the number of new delivered cards.

-- 2010-01-14   Xu Yan       Fixed a previous bug for acumulated NBR_AC_CLS and accumulated NBR_CRD_CLECTD

-- 2010-01-14   Xu Yan       Updated the rules for dormant cards

-- 2010-05-21   Xu Yan       Updated the NBR_DRMT_AC for Credit Cards, according to ������'s request.

--                           ȡ�˻������п�Ƭ��Ϊ˯�߿������˻�Ϊ˯�߻���

--                           Updated the NBR_CST for Debit Cards, according to ������'s request.

--                           �Կͻ�����ȥ��
-- 2012-02-27   Chen XiaoWen ������ʱ��TMP������TMP_CRD_PRFL_CRD_DLY_SMY_CR���м���������ٸ�����������group by������

-------------------------------------------------------------------------------

LANGUAGE SQL

BEGIN

	

/*�����쳣����ʹ�ñ���*/

DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE

DECLARE SMY_STEPNUM INT DEFAULT 1;                     --�����ڲ�λ�ñ��

DECLARE SMY_STEPDESC VARCHAR(100) DEFAULT '';          --�����ڲ�λ������

DECLARE SMY_DATE DATE;                                 --��ʱ���ڱ���

DECLARE SMY_RCOUNT INT;                                --DML������ü�¼��

DECLARE SMY_PROCNM VARCHAR(100);    --

DECLARE CUR_YEAR SMALLINT;--

DECLARE CUR_MONTH SMALLINT;--

DECLARE CUR_DAY INTEGER;--

DECLARE MAX_ACG_DT DATE;--

Declare CUR_MTH_YEAR CHAR(6) ; --����



DECLARE EXIT HANDLER FOR SQLEXCEPTION

BEGIN

	SET SMY_SQLCODE = SQLCODE;--

  ROLLBACK;--

  set SMY_STEPNUM = SMY_STEPNUM + 1;--

  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--

  COMMIT;--

  RESIGNAL;--

END;--







/*������ֵ*/

SET SMY_PROCNM = 'PROC_CRD_PRFL_CRD_DLY_SMY';--

SET SMY_DATE=ACCOUNTING_DATE;--

SET CUR_YEAR=YEAR(ACCOUNTING_DATE);  --ȡ��ǰ���

SET CUR_MONTH=MONTH(ACCOUNTING_DATE); --ȡ��ǰ�·�

if CUR_MONTH<10 

	THEN 

		SET CUR_MTH_YEAR = CHAR(CUR_YEAR)||'0'||CHAR(CUR_MONTH) ;--

	ELSE 

		SET CUR_MTH_YEAR = CHAR(CUR_YEAR)||CHAR(CUR_MONTH);--

end if;		--



SET CUR_DAY=DAY(ACCOUNTING_DATE);     --ȡ����

SELECT MAX(ACG_DT) INTO MAX_ACG_DT FROM SMY.MTHLY_CR_CRD_AC_ACML_BAL_AMT;	--



/*Delete��־��,����SMY_PROCNM=��ǰ�洢��������,SMY_DATE=ACCOUNTING_DATE,�������µ���ʼ��־*/

DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--

COMMIT;--

INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, '�洢���̿�ʼ����.', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

COMMIT;--



DELETE FROM SMY.CRD_PRFL_CRD_DLY_SMY WHERE ACG_DT = ACCOUNTING_DATE;--





SET SMY_STEPDESC = '������ʱ��,���ѵ������ݲ���';--



DECLARE GLOBAL TEMPORARY TABLE TMP_CRD_PRFL_CRD_DLY_SMY_CR(

				 OU_ID                                CHARACTER(18)--������

				,CRD_TP_ID                            INTEGER      --������

				,Is_CR_CRD_F                          SMALLINT     --�Ƿ�Ϊ���ǿ�

				,CRD_Brand_TP_Id                      INTEGER      --��Ʒ������

				,CRD_PRVL_TP_ID                       INTEGER      --������

				,PSBK_RLTD_F                          SMALLINT     --������ر�ʶ

				,IS_NONGXIN_CRD_F                     SMALLINT     --���տ�/ũ�ſ���ʶ

				,ENT_IDV_IND                          INTEGER      --������

				,MST_CRD_IND                          INTEGER      --��/������־

				,NGO_CRD_IND                          INTEGER      --Э�鿨����

				,MULT_CCY_F                           SMALLINT     --˫�ҿ���־

				,PD_GRP_CD                            CHARACTER(2) --��Ʒ��

				,PD_SUB_CD                            CHARACTER(3) --��Ʒ�Ӵ���

				,BIZ_CGY_TP_ID                        INTEGER      --ҵ�����

				,CCY                                  CHARACTER(3) --����

				,ACG_DT                               DATE         --����YYYY-MM-DD

				,CDR_YR                               SMALLINT     --���YYYY

				,CDR_MTH                              SMALLINT     --�·�MM

				,ACT_NBR_AC                           INTEGER      --ʵ���˻���

				,NBR_EFF_CRD                          INTEGER      --��������

				,NBR_UNATVD_CR_CRD                    INTEGER      --δ�������ÿ�����

				,NBR_UNATVD_CHGD_CRD                  INTEGER      --�ѻ���δ���ÿ���

				,NBR_EXP_CRD                          INTEGER      --���ڿ���

				,NBR_DRMT_CRD                         INTEGER      --˯�߿�����

				,NBR_CRD_CLECTD                       INTEGER      --���տ���

				,NBR_NEW_CRD                          INTEGER      --�¿�����

				,NBR_CRD_CLD                          INTEGER      --������

				,NBR_NEW_CST                          INTEGER      --�����ͻ���

				,NBR_CST_CLD                          INTEGER      --�ͻ�������

				,NBR_CRD_CHG                          INTEGER      --������

				,NBR_AC_Opened                        INTEGER      --�����˻���

				,NBR_AC_CLS                           INTEGER      --�����˻���

				-----------------------------Start of 20091203---------------------------------

				,NBR_CST                              INTEGER      --�ͻ���

				-----------------------------Start of 20091203---------------------------------

				-----------------------------Start of 20091208---------------------------------

				,NBR_DRMT_CRD_WITH_LOW_BAL            INTEGER      --���С��10��˯�߿�����

				,NBR_DRMT_AC_WITH_LOW_BAL							INTEGER      --���С��10��˯�߻�����

				,NBR_DRMT_AC													INTEGER      --˯�߻�����

				-----------------------------End of 20091208---------------------------------

)			

ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE   IN TS_USR_TMP32K PARTITIONING KEY(OU_ID); 	--



DECLARE GLOBAL TEMPORARY TABLE TMP_CRD_PRFL_CRD_DLY_SMY_DB(

				 OU_ID                                CHARACTER(18)--������

				,CRD_TP_ID                            INTEGER      --������

				,Is_CR_CRD_F                          SMALLINT     --�Ƿ�Ϊ���ǿ�

				,CRD_Brand_TP_Id                      INTEGER      --��Ʒ������

				,CRD_PRVL_TP_ID                       INTEGER      --������

				,PSBK_RLTD_F                          SMALLINT     --������ر�ʶ

				,IS_NONGXIN_CRD_F                     SMALLINT     --���տ�/ũ�ſ���ʶ

				,ENT_IDV_IND                          INTEGER      --������

				,MST_CRD_IND                          INTEGER      --��/������־

				,NGO_CRD_IND                          INTEGER      --Э�鿨����

				,MULT_CCY_F                           SMALLINT     --˫�ҿ���־

				,PD_GRP_CD                            CHARACTER(2) --��Ʒ��

				,PD_SUB_CD                            CHARACTER(3) --��Ʒ�Ӵ���

				,BIZ_CGY_TP_ID                        INTEGER      --ҵ�����

				,CCY                                  CHARACTER(3) --����

				,ACG_DT                               DATE         --����YYYY-MM-DD

				,CDR_YR                               SMALLINT     --���YYYY

				,CDR_MTH                              SMALLINT     --�·�MM

				,ACT_NBR_AC                           INTEGER      --ʵ���˻���

				,NBR_EFF_CRD                          INTEGER      --��������

				,NBR_UNATVD_CR_CRD                    INTEGER      --δ�������ÿ�����

				,NBR_UNATVD_CHGD_CRD                  INTEGER      --�ѻ���δ���ÿ���

				,NBR_EXP_CRD                          INTEGER      --���ڿ���

				,NBR_DRMT_CRD                         INTEGER      --˯�߿�����

				,NBR_CRD_CLECTD                       INTEGER      --���տ���

				,NBR_NEW_CRD                          INTEGER      --�¿�����

				,NBR_CRD_CLD                          INTEGER      --������

				,NBR_NEW_CST                          INTEGER      --�����ͻ���

				,NBR_CST_CLD                          INTEGER      --�ͻ�������

				,NBR_CRD_CHG                          INTEGER      --������

				,NBR_AC_Opened                        INTEGER      --�����˻���

				,NBR_AC_CLS                           INTEGER      --�����˻���

				-----------------------------Start of 20091203---------------------------------

				,NBR_CST                              INTEGER      --�ͻ���

				-----------------------------End of 20091203---------------------------------

				-----------------------------Start of 20091208---------------------------------

				,NBR_DRMT_CRD_WITH_LOW_BAL            INTEGER      --���С��10��˯�߿�����

				,NBR_DRMT_AC_WITH_LOW_BAL							INTEGER      --���С��10��˯�߻�����

				,NBR_DRMT_AC													INTEGER      --˯�߻�����

				-----------------------------End of 20091208---------------------------------

)			

ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE   IN TS_USR_TMP32K PARTITIONING KEY(OU_ID); --



INSERT INTO SESSION.TMP_CRD_PRFL_CRD_DLY_SMY_CR

(	   OU_ID                                --������

		,CRD_TP_ID                            --������

		,Is_CR_CRD_F                          --�Ƿ�Ϊ���ǿ�

		,CRD_Brand_TP_Id                      --��Ʒ������

		,CRD_PRVL_TP_ID                       --������

		,PSBK_RLTD_F                          --������ر�ʶ

		,IS_NONGXIN_CRD_F                     --���տ�/ũ�ſ���ʶ

		,ENT_IDV_IND                          --������

		,MST_CRD_IND                          --��/������־

		,NGO_CRD_IND                          --Э�鿨����

		,MULT_CCY_F                           --˫�ҿ���־

		,PD_GRP_CD                            --��Ʒ��

		,PD_SUB_CD                            --��Ʒ�Ӵ���

		,BIZ_CGY_TP_ID                        --ҵ�����

		,CCY                                  --����

		,ACG_DT                               --����YYYY-MM-DD

		,CDR_YR                               --���YYYY

		,CDR_MTH                              --�·�MM

		,ACT_NBR_AC                           --ʵ���˻���

		,NBR_EFF_CRD                          --��������

		,NBR_UNATVD_CR_CRD                    --δ�������ÿ�����

		,NBR_UNATVD_CHGD_CRD                  --�ѻ���δ���ÿ���

		,NBR_EXP_CRD                          --���ڿ���

		,NBR_DRMT_CRD                         --˯�߿�����

		,NBR_CRD_CLECTD                       --���տ���

		,NBR_NEW_CRD                          --�¿�����

		,NBR_CRD_CLD                          --������

		,NBR_NEW_CST                          --�����ͻ���

		,NBR_CST_CLD                          --�ͻ�������

		,NBR_CRD_CHG                          --������

		,NBR_AC_Opened                        --�����˻���

		,NBR_AC_CLS                           --�����˻��� 

		-----------------------------Start of 20091203---------------------------------

		,NBR_CST                              --�ͻ���

		-----------------------------End of 20091203---------------------------------

		-----------------------------Start of 20091208---------------------------------

		,NBR_DRMT_CRD_WITH_LOW_BAL            --���С��10��˯�߿�����

		,NBR_DRMT_AC_WITH_LOW_BAL					    --���С��10��˯�߻�����

		,NBR_DRMT_AC										      --˯�߻�����

		-----------------------------End of 20091208---------------------------------		

)

----------------------Start on 20100521-------------------------------------------

With T_ACT_CC_AC as (

    select distinct AC_AR_Id as CC_AC_AR_ID                    

    from SMY.CR_CRD_SMY

    where DRMT_CRD_F = 0   --Exists at least one non-dormant card

)

----------------------End on 20100521-------------------------------------------	

SELECT 

  	 CR_CRD.OU_ID      AS OU_ID                           --������              

  	,CR_CRD.CR_CRD_TP_ID               AS CRD_TP_ID                       --������              

  	,1                        AS Is_CR_CRD_F                     --�Ƿ�Ϊ���ǿ�        

  	,CR_CRD.CRD_BRAND_TP_ID         AS CRD_Brand_TP_Id                 --��Ʒ������          

  	,CR_CRD.CRD_PRVL_TP_ID         AS CRD_PRVL_TP_ID                  --������              

  	,0                        AS PSBK_RLTD_F                     --������ر�ʶ        

  	,0                        AS IS_NONGXIN_CRD_F                --���տ�/ũ�ſ���ʶ   

  	,CR_CRD.ENT_IDV_IND        AS ENT_IDV_IND                     --������              

  	,CR_CRD.MST_CRD_IND            AS MST_CRD_IND                     --��/������־         

  	,CR_CRD.NGO_CRD_IND            AS NGO_CRD_IND                     --Э�鿨����          

  	,CR_CRD.MULT_CCY_F            AS MULT_CCY_F                      --˫�ҿ���־          

  	,CR_CRD.PD_GRP_CD              AS PD_GRP_CD                       --��Ʒ��                     

  	,CR_CRD.PD_SUB_CD              AS PD_SUB_CD                       --��Ʒ�Ӵ���          

  	,CR_CRD.BIZ_CGY_TP_ID          AS BIZ_CGY_TP_ID                   --ҵ�����            

  	,CR_CRD.CCY               AS CCY                             --����                                                          

  	,SMY_DATE                 AS ACG_DT                          --����YYYY-MM-DD      

  	,CUR_YEAR                 AS CDR_YR                          --���YYYY            

  	,CUR_MONTH                AS CDR_MTH                         --�·�MM              

  	,SUM(

  				case when CC_AC.AR_LCS_TP_ID = 20370007 --�����˻�  

  									and CR_CRD.CRD_LCS_TP_ID <> 11920006 --���Ͽ�            

  	      then 1 else 0 end

  			) AS ACT_NBR_AC                      --ʵ���˻���          

  	,SUM(

  				case when CRD_LCS_TP_ID = 11920001 --����               

  	      then 1 else 0 end

  	    )   AS NBR_EFF_CRD                     --��������            

  	,SUM(

  			  case when CRD_LCS_TP_ID in (11920002,11920003)

  			  then 1 else 0 end

  			)                     AS NBR_UNATVD_CR_CRD               --δ�������ÿ�����    

  	,SUM(

  			  case when CRD_LCS_TP_ID = 11920003

  			  then 1 else 0 end

  			)                        AS NBR_UNATVD_CHGD_CRD             --�ѻ���δ���ÿ���    

  	,SUM(

  			  case when EXP_MTH_YEAR < CUR_MTH_YEAR AND CRD_LCS_TP_ID = 11920001 --����

  			  then 1 else 0 end

  			)                        AS NBR_EXP_CRD                     --���ڿ���            

  	,SUM(

  	--------------Start on 20100521-----------------------------------------------------------------  	

		--  	----------------------------------------Start on 20100114------------------------------------------

		--  	--			case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) > 180 

		--  	--			----------------------------Start on 20091215------------------------------------------

		--  	--								and DAYS(SMY_DATE) - DAYS(CR_CRD.EFF_DT) > 180

		--  	--								and DAYS(SMY_DATE) - DAYS(CRD_DLVD_DT) > 180

		--  	--			----------------------------End on 20091215------------------------------------------  	

		--  	        case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) >= 180   				

		--  									and DAYS(SMY_DATE) - DAYS(CR_CRD.EFF_DT) >= 180

		--  									and DAYS(SMY_DATE) - DAYS(CRD_DLVD_DT) >= 180

		--     ----------------------------------------End on 20100114------------------------------------------  				

		--  									and CRD_LCS_TP_ID in ( 11920001 --����

		--  																				,11920002 --�·���δ����

		--  																				,11920003 --�»���δ����

		--  																			 )

		--  			  then 1 else 0 end

         CR_CRD.DRMT_CRD_F  			  

    ---------------End on 20100521-----------------------------------------------------------------   			  	

  			)                        AS NBR_DRMT_CRD                    --˯�߿�����          

  	,SUM(

  			  case when CRD_LCS_TP_ID = 11920004 --���տ�

  			  then 1 else 0 end  			

  			)                        AS NBR_CRD_CLECTD                  --���տ���            

  	,SUM(

  	      --case when CR_CRD.EFF_DT = SMY_DATE

  	      case when CR_CRD.CRD_DLVD_DT = SMY_DATE                   

  	    	then 1 else 0 end

  			)                        AS NBR_NEW_CRD                     --�¿����� ->�·�����         

  	,SUM(

  			  case when CR_CRD.END_DT = SMY_DATE AND CRD_LCS_TP_ID = 11920005

  	    	then 1 else 0 end

  			)                        AS NBR_CRD_CLD                     --������              

  	,SUM(

  				case when CST_INF.EFF_CST_DT = SMY_DATE

  									and CR_CRD.CRD_LCS_TP_ID <> 11920006 --���Ͽ�            

  	    	then 1 else 0 end

  			)                        AS NBR_NEW_CST                     --�����ͻ���                     

  	,0                        AS NBR_CST_CLD                     --�ͻ�������          

  	,SUM(

  			  case when CR_CRD.CRD_CHG_DT = SMY_DATE

  	    	then 1 else 0 end

  			)                     AS NBR_CRD_CHG                     --������              

  	,SUM(

  				case when CC_AC.EFF_DT = SMY_DATE

  									and CR_CRD.CRD_LCS_TP_ID <> 11920006 --���Ͽ�            

  	    	then 1 else 0 end

  			)                        AS NBR_AC_Opened                   --�����˻���          

  	,SUM(

  				case when CC_AC.END_DT = SMY_DATE

  									and CR_CRD.CRD_LCS_TP_ID <> 11920006 --���Ͽ�            

  	    	then 1 else 0 end

  			)                        AS NBR_AC_CLS                      --�����˻��� 		    

  	-----------------------------Start of 20091203---------------------------------

		,SUM(

					case when CR_CRD.CRD_LCS_TP_ID = 11920001 --����

  			  then 1 else 0 end

				)

		AS NBR_CST                                --�ͻ���

	  -----------------------------End of 20091203---------------------------------

		

	  -----------------------------Start of 20091208---------------------------------

		,SUM(

					-----------------------------Start of 20100521---------------------------------  		

					--		  -------------------------------------Start on 20100114----------------------------------------------------------

					--		  ----case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) > 180 and CRD_LCS_TP_ID = 11920001 and CR_CRD.AC_BAL_AMT < 10

					--		  --case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) >= 180 and CRD_LCS_TP_ID = 11920001 and CR_CRD.AC_BAL_AMT < 10

					--		  -------------------------------------End on 20100114----------------------------------------------------------

					case when CR_CRD.DRMT_CRD_F = 1 and CR_CRD.AC_BAL_AMT < 10

				  -----------------------------End of 20100521---------------------------------  		

  			  then 1 else 0 end

				)  as NBR_DRMT_CRD_WITH_LOW_BAL            --���С��10��˯�߿�����				

	  --------------------------------Start on 20100521---------------------------------------

	  --���ݲ����������󣬴��ǿ�����Ҫͳ�����ָ�꣬��˲�������������֣�ֻȡ��NBR_DRMT_AC��ͬ��ֵ

		--,SUM(

		--      -------------------------------------Start on 20100114----------------------------------------------------------

		--			--case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) > 180 and CC_AC.AR_LCS_TP_ID = 20370007 and CC_AC.BAL_AMT < 10

		--			case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) >= 180 and CC_AC.AR_LCS_TP_ID = 20370007 and CC_AC.BAL_AMT < 10

		--			-------------------------------------End on 20100114----------------------------------------------------------

		--								and CR_CRD.CRD_LCS_TP_ID <> 11920006 --���Ͽ�            

  	--		  then 1 else 0 end

		--		)  as	NBR_DRMT_AC_WITH_LOW_BAL					    --���С��10��˯�߻�����

		,count(distinct CR_CRD.AC_AR_ID) - count(distinct T_ACT_CC_AC.CC_AC_AR_ID) as	NBR_DRMT_AC_WITH_LOW_BAL					    --���С��10��˯�߻�����

	  --------------------------------End on 20100521---------------------------------------

		--------------------------------Start on 20100521---------------------------------------

		--,SUM(

		--			-------------------------------------Start on 20100114----------------------------------------------------------

		--			--case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) > 180 and CC_AC.AR_LCS_TP_ID = 20370007 

		--			case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) >= 180 and CC_AC.AR_LCS_TP_ID = 20370007 

		--			-------------------------------------End on 20100114----------------------------------------------------------

		--								and CR_CRD.CRD_LCS_TP_ID <> 11920006 --���Ͽ�            

  	--		  then 1 else 0 end

		--		)  as NBR_DRMT_AC										      --˯�߻�����

		,count(distinct CR_CRD.AC_AR_ID) - count(distinct T_ACT_CC_AC.CC_AC_AR_ID) as NBR_DRMT_AC										      --˯�߻�����

	  --------------------------------End on 20100521---------------------------------------

		-----------------------------End of 20091208---------------------------------				

FROM SMY.CR_CRD_SMY AS CR_CRD

LEFT JOIN SMY.CST_INF AS CST_INF on CR_CRD.CST_ID = CST_INF.CST_ID

LEFT JOIN SOR.CC_AC_AR AS CC_AC  on CR_CRD.AC_AR_ID = CC_AC.CC_AC_AR_ID AND CR_CRD.CCY =CC_AC.DNMN_CCY_ID

-----------------------Start on 20100521------------------------

left join T_ACT_CC_AC on CR_CRD.AC_AR_ID = T_ACT_CC_AC.CC_AC_AR_ID

-----------------------End on 20100521------------------------

GROUP BY 

  	 CR_CRD.OU_ID                   

  	,CR_CRD.CR_CRD_TP_ID                            

  	,CR_CRD.CRD_BRAND_TP_ID                      

  	,CR_CRD.CRD_PRVL_TP_ID                      

  	,CR_CRD.ENT_IDV_IND                     

  	,CR_CRD.MST_CRD_IND                         

  	,CR_CRD.NGO_CRD_IND                         

  	,CR_CRD.MULT_CCY_F                         

  	,CR_CRD.PD_GRP_CD                               

  	,CR_CRD.PD_SUB_CD                           

  	,CR_CRD.BIZ_CGY_TP_ID                           

  	,CR_CRD.CCY                                                               

  	,SMY_DATE                              

  	,CUR_YEAR                              

  	,CUR_MONTH

;--

DECLARE GLOBAL TEMPORARY TABLE TMP(

				 OU_ID                                CHARACTER(18)--������

				,CRD_TP_ID                            INTEGER      --������

				,Is_CR_CRD_F                          SMALLINT     --�Ƿ�Ϊ���ǿ�

				,CRD_Brand_TP_Id                      INTEGER      --��Ʒ������

				,CRD_PRVL_TP_ID                       INTEGER      --������

				,PSBK_RLTD_F                          SMALLINT     --������ر�ʶ

				,IS_NONGXIN_CRD_F                     SMALLINT     --���տ�/ũ�ſ���ʶ

				,ENT_IDV_IND                          INTEGER      --������

				,MST_CRD_IND                          INTEGER      --��/������־

				,NGO_CRD_IND                          INTEGER      --Э�鿨����

				,MULT_CCY_F                           SMALLINT     --˫�ҿ���־

				,PD_GRP_CD                            CHARACTER(2) --��Ʒ��

				,PD_SUB_CD                            CHARACTER(3) --��Ʒ�Ӵ���

				,BIZ_CGY_TP_ID                        INTEGER      --ҵ�����

				,CCY                                  CHARACTER(3) --����

				,ACG_DT                               DATE         --����YYYY-MM-DD

				,CDR_YR                               SMALLINT     --���YYYY

				,CDR_MTH                              SMALLINT     --�·�MM

				,ACT_NBR_AC                           INTEGER      --ʵ���˻���

				,NBR_EFF_CRD                          INTEGER      --��������

				,NBR_UNATVD_CR_CRD                    INTEGER      --δ�������ÿ�����

				,NBR_UNATVD_CHGD_CRD                  INTEGER      --�ѻ���δ���ÿ���

				,NBR_EXP_CRD                          INTEGER      --���ڿ���

				,NBR_DRMT_CRD                         INTEGER      --˯�߿�����

				,NBR_CRD_CLECTD                       INTEGER      --���տ���

				,NBR_NEW_CRD                          INTEGER      --�¿�����

				,NBR_CRD_CLD                          INTEGER      --������

				,NBR_NEW_CST                          INTEGER      --�����ͻ���

				,NBR_CST_CLD                          INTEGER      --�ͻ�������

				,NBR_CRD_CHG                          INTEGER      --������

				,NBR_AC_Opened                        INTEGER      --�����˻���

				,NBR_AC_CLS                           INTEGER      --�����˻���

				-----------------------------Start of 20091203---------------------------------

				,CST_ID                              CHARACTER(18)      --�ͻ�ID

				-----------------------------End of 20091203---------------------------------

				-----------------------------Start of 20091208---------------------------------

				,NBR_DRMT_CRD_WITH_LOW_BAL            INTEGER      --���С��10��˯�߿�����

				,NBR_DRMT_AC_WITH_LOW_BAL							INTEGER      --���С��10��˯�߻�����

				,NBR_DRMT_AC													INTEGER      --˯�߻�����

				-----------------------------End of 20091208---------------------------------

)			
ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE   IN TS_USR_TMP32K PARTITIONING KEY(OU_ID); --

CREATE INDEX SESSION.TMP_IDX ON SESSION.TMP(OU_ID,CRD_TP_ID,CRD_BRAND_TP_ID,ENT_IDV_IND,NGO_CRD_IND,PD_GRP_CD,PD_SUB_CD,BIZ_CGY_TP_ID,CCY,ACG_DT,CDR_YR,CDR_MTH,IS_NONGXIN_CRD_F,PSBK_RLTD_F);

INSERT INTO SESSION.TMP

	( OU_ID                                --������

	 ,CRD_TP_ID                            --������

	 ,Is_CR_CRD_F                          --�Ƿ�Ϊ���ǿ�

	 ,CRD_Brand_TP_Id                      --��Ʒ������

	 ,CRD_PRVL_TP_ID                       --������

	 ,PSBK_RLTD_F                          --������ر�ʶ

	 ,IS_NONGXIN_CRD_F                     --���տ�/ũ�ſ���ʶ

	 ,ENT_IDV_IND                          --������

	 ,MST_CRD_IND                          --��/������־

	 ,NGO_CRD_IND                          --Э�鿨����

	 ,MULT_CCY_F                           --˫�ҿ���־

	 ,PD_GRP_CD                            --��Ʒ��

	 ,PD_SUB_CD                            --��Ʒ�Ӵ���

	 ,BIZ_CGY_TP_ID                        --ҵ�����

	 ,CCY                                  --����

	 ,ACG_DT                               --����YYYY-MM-DD

	 ,CDR_YR                               --���YYYY

	 ,CDR_MTH                              --�·�MM

	 ,ACT_NBR_AC                           --ʵ���˻���

	 ,NBR_EFF_CRD                          --��������

	 ,NBR_UNATVD_CR_CRD                    --δ�������ÿ�����

	 ,NBR_UNATVD_CHGD_CRD                  --�ѻ���δ���ÿ���

	 ,NBR_EXP_CRD                          --���ڿ���

	 ,NBR_DRMT_CRD                         --˯�߿�����

	 ,NBR_CRD_CLECTD                       --���տ���

	 ,NBR_NEW_CRD                          --�¿�����

	 ,NBR_CRD_CLD                          --������

	 ,NBR_NEW_CST                          --�����ͻ���

	 ,NBR_CST_CLD                          --�ͻ�������

	 ,NBR_CRD_CHG                          --������

	 ,NBR_AC_Opened                        --�����˻���

	 ,NBR_AC_CLS                            --�����˻���  

  -----------------------------Start of 20091203---------------------------------

	 ,CST_ID                              --�ͻ�ID

	-----------------------------End of 20091203---------------------------------	     

	-----------------------------Start of 20091208---------------------------------

	,NBR_DRMT_CRD_WITH_LOW_BAL            --���С��10��˯�߿�����

	,NBR_DRMT_AC_WITH_LOW_BAL					    --���С��10��˯�߻�����

	,NBR_DRMT_AC										      --˯�߻�����

  -----------------------------End of 20091208---------------------------------			

)

---------------Start on 20100521-------------------------------

with T_ACT_CST as (

  select  CRD_NO

         ,CST_ID

  from SMY.DB_CRD_SMY

  where CRD_LCS_TP_ID = 11920001 --����

)

---------------End on 20100521-------------------------------

SELECT 

  	 DB_CRD.OU_ID      AS OU_ID                           --������              

  	,DB_CRD.DB_CRD_TP_ID               AS CRD_TP_ID       --������              

  	,0                        AS Is_CR_CRD_F              --�Ƿ�Ϊ���ǿ�        

  	,DB_CRD.CRD_BRAND_TP_ID         AS CRD_Brand_TP_Id    --��Ʒ������          

  	,-1         AS CRD_PRVL_TP_ID                         --������              

  	,PSBK_RLTD_F                        AS PSBK_RLTD_F    --������ر�ʶ        

  	,DB_CRD.IS_NONGXIN_CRD_F AS IS_NONGXIN_CRD_F                --���տ�/ũ�ſ���ʶ   

  	,DB_CRD.ENT_IDV_IND        AS ENT_IDV_IND             --������              

  	,-1            AS MST_CRD_IND                          --��/������־         

  	,DB_CRD.NGO_CRD_IND            AS NGO_CRD_IND         --Э�鿨����          

  	,-1            AS MULT_CCY_F                          --˫�ҿ���־          

  	,DB_CRD.PD_GRP_CODE              AS PD_GRP_CD           --��Ʒ��                     

  	,DB_CRD.PD_SUB_CODE              AS PD_SUB_CD           --��Ʒ�Ӵ���          

  	,DB_CRD.BIZ_CGY_TP_ID          AS BIZ_CGY_TP_ID       --ҵ�����            

  	,DB_CRD.CCY               AS CCY                      --����                                                          

  	,SMY_DATE                 AS ACG_DT                   --����YYYY-MM-DD      

  	,CUR_YEAR                 AS CDR_YR                   --���YYYY            

  	,CUR_MONTH                AS CDR_MTH                  --�·�MM              

  	,case when DEP_AC.AR_LCS_TP_ID = 20370007 --�����˻�              

  									and CRD_LCS_TP_ID = 11920001 --����               

  	      then 1 else 0 end

  	 AS ACT_NBR_AC                                   --ʵ���˻���          

  	,case when CRD_LCS_TP_ID = 11920001 --����               

  	      then 1 else 0 end

  	 AS NBR_EFF_CRD                     --��������            

  	,0                     AS NBR_UNATVD_CR_CRD               --δ�������ÿ�����    

  	,0                     AS NBR_UNATVD_CHGD_CRD             --�ѻ���δ���ÿ���    

  	,case when EXP_MTH_YEAR < CUR_MTH_YEAR AND CRD_LCS_TP_ID = 11920001 --����

  			  then 1 else 0 end

  	 AS NBR_EXP_CRD                     --���ڿ���            

  	,case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) >= 365 and CRD_LCS_TP_ID = 11920001

  			  then 1 else 0 end

  	 AS NBR_DRMT_CRD                    --˯�߿�����          

  	,case when CRD_LCS_TP_ID = 11920004 --���տ�

  			  then 1 else 0 end  			

  	 AS NBR_CRD_CLECTD                  --���տ���            

  	,case when DB_CRD.EFF_DT = SMY_DATE

  	    	then 1 else 0 end

  	 AS NBR_NEW_CRD                     --�¿�����            

  	,case when DB_CRD.END_DT = SMY_DATE AND CRD_LCS_TP_ID = 11920005

  	    	then 1 else 0 end

  	 AS NBR_CRD_CLD                     --������              

  	,case when CST_INF.EFF_CST_DT = SMY_DATE

  									and CRD_LCS_TP_ID = 11920001 --����               

  	    	then 1 else 0 end

  	 AS NBR_NEW_CST                     --�����ͻ���                     

  	,0                        AS NBR_CST_CLD                     --�ͻ�������          

  	,case when DB_CRD.CRD_CHG_DT = SMY_DATE

  	    	then 1 else 0 end

  	 AS NBR_CRD_CHG                     --������              

  	,case when DEP_AC.EFF_DT = SMY_DATE

  									and CRD_LCS_TP_ID = 11920001 --����               

  	    	then 1 else 0 end

  	 AS NBR_AC_Opened                   --�����˻���          

  	,case when DEP_AC.END_DT = SMY_DATE

  									and CRD_LCS_TP_ID = 11920001 --����               

  	    	then 1 else 0 end

  	 AS NBR_AC_CLS                      --�����˻��� 

  	-------------------Start on 20100521-------------------------------------

  	-------------------------------Start of 20091203---------------------------------

		--,SUM(

		--			case when DB_CRD.CRD_LCS_TP_ID = 11920001 --����

  	--		  then 1 else 0 end

		--		)

		--AS NBR_CST                                --�ͻ���

		-------------------------------End of 20091203---------------------------------		    

		,T_ACT_CST.CST_ID AS CST_ID                                --�ͻ�ID

		-------------------Start on 20100521-------------------------------------

		-----------------------------Start of 20091208---------------------------------

		,case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) >= 365 and CRD_LCS_TP_ID = 11920001 and DB_CRD.AC_BAL_AMT < 10

					------------------------------------------End on 20100114------------------------------------

  			  then 1 else 0 end

		 as NBR_DRMT_CRD_WITH_LOW_BAL            --���С��10��˯�߿�����

		,case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) >= 365 and DEP_AC.AR_LCS_TP_ID = 20370007 and DB_CRD.AC_BAL_AMT < 10

					------------------------------------------End on 20100114------------------------------------

										and CRD_LCS_TP_ID = 11920001 --����               

  			  then 1 else 0 end

		 as	NBR_DRMT_AC_WITH_LOW_BAL					    --���С��10��˯�߻�����

		,case when DAYS(SMY_DATE) - DAYS(LST_CST_AVY_DT) >= 365 and DEP_AC.AR_LCS_TP_ID = 20370007 

				 ------------------------------------------End on 20100114------------------------------------	

										and CRD_LCS_TP_ID = 11920001 --����               

  			  then 1 else 0 end

		 as NBR_DRMT_AC										      --˯�߻�����

		-----------------------------End of 20091208---------------------------------				

FROM SMY.DB_CRD_SMY AS DB_CRD

LEFT JOIN SMY.CST_INF AS CST_INF on DB_CRD.CST_ID = CST_INF.CST_ID

LEFT JOIN SOR.DMD_DEP_MN_AR AS DEP_AC  on DB_CRD.AC_AR_ID = DEP_AC.DMD_DEP_AR_ID

------------------------Start on 20100521----------------------

left join T_ACT_CST on DB_CRD.CRD_NO = T_ACT_CST.CRD_NO

------------------------End on 20100521----------------------
;

INSERT INTO SESSION.TMP_CRD_PRFL_CRD_DLY_SMY_DB
(
    OU_ID                                --������
	 ,CRD_TP_ID                            --������
	 ,Is_CR_CRD_F                          --�Ƿ�Ϊ���ǿ�
	 ,CRD_Brand_TP_Id                      --��Ʒ������
	 ,CRD_PRVL_TP_ID                       --������
	 ,PSBK_RLTD_F                          --������ر�ʶ
	 ,IS_NONGXIN_CRD_F                     --���տ�/ũ�ſ���ʶ
	 ,ENT_IDV_IND                          --������
	 ,MST_CRD_IND                          --��/������־
	 ,NGO_CRD_IND                          --Э�鿨����
	 ,MULT_CCY_F                           --˫�ҿ���־
	 ,PD_GRP_CD                            --��Ʒ��
	 ,PD_SUB_CD                            --��Ʒ�Ӵ���
	 ,BIZ_CGY_TP_ID                        --ҵ�����
	 ,CCY                                  --����
	 ,ACG_DT                               --����YYYY-MM-DD
	 ,CDR_YR                               --���YYYY
	 ,CDR_MTH                              --�·�MM
	 ,ACT_NBR_AC                           --ʵ���˻���
	 ,NBR_EFF_CRD                          --��������
	 ,NBR_UNATVD_CR_CRD                    --δ�������ÿ�����
	 ,NBR_UNATVD_CHGD_CRD                  --�ѻ���δ���ÿ���
	 ,NBR_EXP_CRD                          --���ڿ���
	 ,NBR_DRMT_CRD                         --˯�߿�����
	 ,NBR_CRD_CLECTD                       --���տ���
	 ,NBR_NEW_CRD                          --�¿�����
	 ,NBR_CRD_CLD                          --������
	 ,NBR_NEW_CST                          --�����ͻ���
	 ,NBR_CST_CLD                          --�ͻ�������
	 ,NBR_CRD_CHG                          --������
	 ,NBR_AC_Opened                        --�����˻���
	 ,NBR_AC_CLS                           --�����˻���  
	 ,NBR_CST                              --�ͻ���
	 ,NBR_DRMT_CRD_WITH_LOW_BAL            --���С��10��˯�߿�����
	 ,NBR_DRMT_AC_WITH_LOW_BAL					   --���С��10��˯�߻�����
	 ,NBR_DRMT_AC										       --˯�߻�����
)
SELECT
  	 T.OU_ID
  	,T.CRD_TP_ID
  	,0 AS Is_CR_CRD_F
  	,T.CRD_BRAND_TP_ID
  	,-1 AS CRD_PRVL_TP_ID
  	,T.PSBK_RLTD_F
  	,T.IS_NONGXIN_CRD_F
  	,T.ENT_IDV_IND
  	,-1 AS MST_CRD_IND
  	,T.NGO_CRD_IND
  	,-1 AS MULT_CCY_F
  	,T.PD_GRP_CD
  	,T.PD_SUB_CD
  	,T.BIZ_CGY_TP_ID
  	,T.CCY
  	,T.ACG_DT
  	,T.CDR_YR
  	,T.CDR_MTH
  	,SUM(T.ACT_NBR_AC)
  	,SUM(T.NBR_EFF_CRD)
  	,0 AS NBR_UNATVD_CR_CRD
  	,0 AS NBR_UNATVD_CHGD_CRD
  	,SUM(T.NBR_EXP_CRD)
  	,SUM(T.NBR_DRMT_CRD)
  	,SUM(T.NBR_CRD_CLECTD)
  	,SUM(T.NBR_NEW_CRD)
  	,SUM(T.NBR_CRD_CLD)
  	,SUM(T.NBR_NEW_CST)
  	,0 AS NBR_CST_CLD
  	,SUM(T.NBR_CRD_CHG)
  	,SUM(T.NBR_AC_Opened)
  	,SUM(T.NBR_AC_CLS)
		,count(distinct T.CST_ID) AS NBR_CST
		,SUM(T.NBR_DRMT_CRD_WITH_LOW_BAL)
		,SUM(T.NBR_DRMT_AC_WITH_LOW_BAL)
		,SUM(T.NBR_DRMT_AC)
FROM SESSION.TMP AS T
GROUP BY 
  	 T.OU_ID
  	,T.CRD_TP_ID
  	,T.CRD_BRAND_TP_ID
  	,T.ENT_IDV_IND
  	,T.NGO_CRD_IND
  	,T.PD_GRP_CD
  	,T.PD_SUB_CD
  	,T.BIZ_CGY_TP_ID
  	,T.CCY
  	,T.ACG_DT
  	,T.CDR_YR
  	,T.CDR_MTH
  	,T.IS_NONGXIN_CRD_F
  	,T.PSBK_RLTD_F
;  	--



GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--

SET SMY_STEPNUM = SMY_STEPNUM+1;--

INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--



SET SMY_STEPDESC = '�������ÿ���������';--



IF CUR_DAY = 1 THEN  

   IF CUR_MONTH = 1 THEN --���

      INSERT INTO SMY.CRD_PRFL_CRD_DLY_SMY

      (  OU_ID                                --������

				,CRD_TP_ID                            --������

				,Is_CR_CRD_F                          --�Ƿ�Ϊ���ǿ�

				,CRD_Brand_TP_Id                      --��Ʒ������

				,CRD_PRVL_TP_ID                       --������

				,PSBK_RLTD_F                          --������ر�ʶ

				,IS_NONGXIN_CRD_F                     --���տ�/ũ�ſ���ʶ

				,ENT_IDV_IND                          --������

				,MST_CRD_IND                          --��/������־

				,NGO_CRD_IND                          --Э�鿨����

				,MULT_CCY_F                           --˫�ҿ���־

				,PD_GRP_CD                            --��Ʒ��

				,PD_SUB_CD                            --��Ʒ�Ӵ���

				,BIZ_CGY_TP_ID                        --ҵ�����

				,CCY                                  --����

				,ACG_DT                               --����YYYY-MM-DD

				,CDR_YR                               --���YYYY

				,CDR_MTH                              --�·�MM

				,ACT_NBR_AC                           --ʵ���˻���

				,NBR_EFF_CRD                          --��������

				,NBR_UNATVD_CR_CRD                    --δ�������ÿ�����

				,NBR_UNATVD_CHGD_CRD                  --�ѻ���δ���ÿ���

				,NBR_EXP_CRD                          --���ڿ���

				,NBR_DRMT_CRD                         --˯�߿�����

				,NBR_CRD_CLECTD                       --���տ���

				,NBR_NEW_CRD                          --�¿�����

				,NBR_CRD_CLD                          --������

				,NBR_NEW_CST                          --�����ͻ���

				,NBR_CST_CLD                          --�ͻ�������

				,NBR_CRD_CHG                          --������

				,NBR_AC_Opened                        --�����˻���

				,NBR_AC_CLS                           --�����˻���

				,TOT_MTD_NBR_NEW_CRD                  --���ۼ��¿�����

				,TOT_MTD_NBR_CRD_CLD                  --���ۼ�������

				,TOT_MTD_NBR_NEW_CST                  --���ۼ������ͻ���

				,TOT_MTD_NBR_CST_CLD                  --���ۼƿͻ�������

				,TOT_MTD_NBR_CRD_CHG                  --���ۼƻ�����

				,TOT_MTD_NBR_AC_Opened                --���ۼƿ����˻���

				,TOT_MTD_NBR_AC_CLS                   --���ۼ������˻���

				,TOT_MTD_NBR_CRD_CLECTD               --���ۼ����տ���

				,TOT_QTD_NBR_NEW_CRD                  --���ۼ��¿�����

				,TOT_QTD_NBR_CRD_CLD                  --���ۼ�������

				,TOT_QTD_NBR_NEW_CST                  --���ۼ������ͻ���

				,TOT_QTD_NBR_CST_CLD                  --���ۼƿͻ�������

				,TOT_QTD_NBR_CRD_CHG                  --���ۼƻ�����

				,TOT_QTD_NBR_AC_Opened                --���ۼƿ����˻���

				,TOT_QTD_NBR_AC_CLS                   --���ۼ������˻���

				,TOT_QTD_NBR_CRD_CLECTD               --���ۼ����տ���

				,TOT_YTD_NBR_NEW_CRD                  --���ۼ��¿�����

				,TOT_YTD_NBR_CRD_CLD                  --���ۼ�������

				,TOT_YTD_NBR_NEW_CST                  --���ۼ������ͻ���

				,TOT_YTD_NBR_CST_CLD                  --���ۼƿͻ�������

				,TOT_YTD_NBR_CRD_CHG                  --���ۼƻ�����

				,TOT_YTD_NBR_AC_Opened                --���ۼƿ����˻���

				,TOT_YTD_NBR_AC_CLS                   --���ۼ������˻���

				,TOT_YTD_NBR_CRD_CLECTD               --���ۼ����տ���

				-----------------------------Start of 20091203---------------------------------

				,NBR_CST                              --�ͻ���

				-----------------------------End of 20091203---------------------------------

				-----------------------------Start of 20091208---------------------------------

				,NBR_DRMT_CRD_WITH_LOW_BAL            --���С��10��˯�߿�����

				,NBR_DRMT_AC_WITH_LOW_BAL					    --���С��10��˯�߻�����

				,NBR_DRMT_AC										      --˯�߻�����

			  -----------------------------End of 20091208---------------------------------			

			)				

      SELECT 

         OU_ID                                --������

				,CRD_TP_ID                            --������

				,Is_CR_CRD_F                          --�Ƿ�Ϊ���ǿ�

				,CRD_Brand_TP_Id                      --��Ʒ������

				,CRD_PRVL_TP_ID                       --������

				,PSBK_RLTD_F                          --������ر�ʶ

				,IS_NONGXIN_CRD_F                     --���տ�/ũ�ſ���ʶ

				,ENT_IDV_IND                          --������

				,MST_CRD_IND                          --��/������־

				,NGO_CRD_IND                          --Э�鿨����

				,MULT_CCY_F                           --˫�ҿ���־

				,PD_GRP_CD                            --��Ʒ��

				,PD_SUB_CD                            --��Ʒ�Ӵ���

				,BIZ_CGY_TP_ID                        --ҵ�����

				,CCY                                  --����

				,ACG_DT                               --����YYYY-MM-DD

				,CDR_YR                               --���YYYY

				,CDR_MTH                              --�·�MM

				,ACT_NBR_AC                           --ʵ���˻���

				,NBR_EFF_CRD                          --��������

				,NBR_UNATVD_CR_CRD                    --δ�������ÿ�����

				,NBR_UNATVD_CHGD_CRD                  --�ѻ���δ���ÿ���

				,NBR_EXP_CRD                          --���ڿ���

				,NBR_DRMT_CRD                         --˯�߿�����

				,NBR_CRD_CLECTD                       --���տ���

				,NBR_NEW_CRD                          --�¿�����

				,NBR_CRD_CLD                          --������

				,NBR_NEW_CST                          --�����ͻ���

				,NBR_CST_CLD                          --�ͻ�������

				,NBR_CRD_CHG                          --������

				,NBR_AC_Opened                        --�����˻���

				,NBR_AC_CLS                           --�����˻���

				,NBR_NEW_CRD                          --���ۼ��¿�����    

				,NBR_CRD_CLD                          --���ۼ�������      

				,NBR_NEW_CST                          --���ۼ������ͻ���  

				,NBR_CST_CLD                          --���ۼƿͻ�������  

				,NBR_CRD_CHG                          --���ۼƻ�����      

				,NBR_AC_Opened                        --���ۼƿ����˻���  

				,NBR_AC_CLS                           --���ۼ������˻���  

				,NBR_CRD_CLECTD                       --���ۼ����տ���    

				,NBR_NEW_CRD                          --���ۼ��¿�����    

				,NBR_CRD_CLD                          --���ۼ�������      

				,NBR_NEW_CST                          --���ۼ������ͻ���  

				,NBR_CST_CLD                          --���ۼƿͻ�������  

				,NBR_CRD_CHG                          --���ۼƻ�����      

				,NBR_AC_Opened                        --���ۼƿ����˻���  

				,NBR_AC_CLS                           --���ۼ������˻���  

				,NBR_CRD_CLECTD                       --���ۼ����տ���    

				,NBR_NEW_CRD                          --���ۼ��¿�����    

				,NBR_CRD_CLD                          --���ۼ�������      

				,NBR_NEW_CST                          --���ۼ������ͻ���  

				,NBR_CST_CLD                          --���ۼƿͻ�������  

				,NBR_CRD_CHG                          --���ۼƻ�����      

				,NBR_AC_Opened                        --���ۼƿ����˻���  

				,NBR_AC_CLS                           --���ۼ������˻���  

				,NBR_CRD_CLECTD                       --���ۼ����տ���    

				-----------------------------Start of 20091203---------------------------------

				,NBR_CST                              --�ͻ���

				-----------------------------End of 20091203---------------------------------						

				-----------------------------Start of 20091208---------------------------------

				,NBR_DRMT_CRD_WITH_LOW_BAL            --���С��10��˯�߿�����

				,NBR_DRMT_AC_WITH_LOW_BAL					    --���С��10��˯�߻�����

				,NBR_DRMT_AC										      --˯�߻�����

			  -----------------------------End of 20091208---------------------------------			

		  FROM SESSION.TMP_CRD_PRFL_CRD_DLY_SMY_CR;--

		  

   ELSEIF CUR_MONTH IN (4, 7 ,10) THEN --����

        INSERT INTO SMY.CRD_PRFL_CRD_DLY_SMY

          ( OU_ID                                --������

				   ,CRD_TP_ID                            --������

				   ,Is_CR_CRD_F                          --�Ƿ�Ϊ���ǿ�

				   ,CRD_Brand_TP_Id                      --��Ʒ������

				   ,CRD_PRVL_TP_ID                       --������

				   ,PSBK_RLTD_F                          --������ر�ʶ

				   ,IS_NONGXIN_CRD_F                     --���տ�/ũ�ſ���ʶ

				   ,ENT_IDV_IND                          --������

				   ,MST_CRD_IND                          --��/������־

				   ,NGO_CRD_IND                          --Э�鿨����

				   ,MULT_CCY_F                           --˫�ҿ���־

				   ,PD_GRP_CD                            --��Ʒ��

				   ,PD_SUB_CD                            --��Ʒ�Ӵ���

				   ,BIZ_CGY_TP_ID                        --ҵ�����

				   ,CCY                                  --����

				   ,ACG_DT                               --����YYYY-MM-DD

				   ,CDR_YR                               --���YYYY

				   ,CDR_MTH                              --�·�MM

				   ,ACT_NBR_AC                           --ʵ���˻���

				   ,NBR_EFF_CRD                          --��������

				   ,NBR_UNATVD_CR_CRD                    --δ�������ÿ�����

				   ,NBR_UNATVD_CHGD_CRD                  --�ѻ���δ���ÿ���

				   ,NBR_EXP_CRD                          --���ڿ���

				   ,NBR_DRMT_CRD                         --˯�߿�����

				   ,NBR_CRD_CLECTD                       --���տ���

				   ,NBR_NEW_CRD                          --�¿�����

				   ,NBR_CRD_CLD                          --������

				   ,NBR_NEW_CST                          --�����ͻ���

				   ,NBR_CST_CLD                          --�ͻ�������

				   ,NBR_CRD_CHG                          --������

				   ,NBR_AC_Opened                        --�����˻���

				   ,NBR_AC_CLS                           --�����˻���

				   ,TOT_MTD_NBR_NEW_CRD                  --���ۼ��¿�����        

				   ,TOT_MTD_NBR_CRD_CLD                  --���ۼ�������          

				   ,TOT_MTD_NBR_NEW_CST                  --���ۼ������ͻ���      

				   ,TOT_MTD_NBR_CST_CLD                  --���ۼƿͻ�������      

				   ,TOT_MTD_NBR_CRD_CHG                  --���ۼƻ�����          

				   ,TOT_MTD_NBR_AC_Opened                --���ۼƿ����˻���      

				   ,TOT_MTD_NBR_AC_CLS                   --���ۼ������˻���      

				   ,TOT_MTD_NBR_CRD_CLECTD               --���ۼ����տ���        

				   ,TOT_QTD_NBR_NEW_CRD                  --���ۼ��¿�����        

				   ,TOT_QTD_NBR_CRD_CLD                  --���ۼ�������          

				   ,TOT_QTD_NBR_NEW_CST                  --���ۼ������ͻ���      

				   ,TOT_QTD_NBR_CST_CLD                  --���ۼƿͻ�������      

				   ,TOT_QTD_NBR_CRD_CHG                  --���ۼƻ�����          

				   ,TOT_QTD_NBR_AC_Opened                --���ۼƿ����˻���      

				   ,TOT_QTD_NBR_AC_CLS                   --���ۼ������˻���      

				   ,TOT_QTD_NBR_CRD_CLECTD               --���ۼ����տ���        

				   ,TOT_YTD_NBR_NEW_CRD                  --���ۼ��¿�����        

				   ,TOT_YTD_NBR_CRD_CLD                  --���ۼ�������          

				   ,TOT_YTD_NBR_NEW_CST                  --���ۼ������ͻ���      

				   ,TOT_YTD_NBR_CST_CLD                  --���ۼƿͻ�������      

				   ,TOT_YTD_NBR_CRD_CHG                  --���ۼƻ�����          

				   ,TOT_YTD_NBR_AC_Opened                --���ۼƿ����˻���      

				   ,TOT_YTD_NBR_AC_CLS                   --���ۼ������˻���      

				   ,TOT_YTD_NBR_CRD_CLECTD               --���ۼ����տ���        

				   -----------------------------Start of 20091203---------------------------------

					 ,NBR_CST                              --�ͻ���

					 -----------------------------End of 20091203---------------------------------

					 -----------------------------Start of 20091208---------------------------------

					 ,NBR_DRMT_CRD_WITH_LOW_BAL            --���С��10��˯�߿�����

					 ,NBR_DRMT_AC_WITH_LOW_BAL					    --���С��10��˯�߻�����

					 ,NBR_DRMT_AC										      --˯�߻�����

				   -----------------------------End of 20091208---------------------------------			

				)

        SELECT 

          a.OU_ID                                --������

				  ,a.CRD_TP_ID                            --������

				  ,a.Is_CR_CRD_F                          --�Ƿ�Ϊ���ǿ�

				  ,a.CRD_Brand_TP_Id                      --��Ʒ������

				  ,a.CRD_PRVL_TP_ID                       --������

				  ,a.PSBK_RLTD_F                          --������ر�ʶ

				  ,a.IS_NONGXIN_CRD_F                     --���տ�/ũ�ſ���ʶ

				  ,a.ENT_IDV_IND                          --������

				  ,a.MST_CRD_IND                          --��/������־

				  ,a.NGO_CRD_IND                          --Э�鿨����

				  ,a.MULT_CCY_F                           --˫�ҿ���־

				  ,a.PD_GRP_CD                            --��Ʒ��

				  ,a.PD_SUB_CD                            --��Ʒ�Ӵ���

				  ,a.BIZ_CGY_TP_ID                        --ҵ�����

				  ,a.CCY                                  --����

				  ,a.ACG_DT                               --����YYYY-MM-DD

				  ,a.CDR_YR                               --���YYYY

				  ,a.CDR_MTH                              --�·�MM

				  ,a.ACT_NBR_AC                           --ʵ���˻���

				  ,a.NBR_EFF_CRD                          --��������

				  ,a.NBR_UNATVD_CR_CRD                    --δ�������ÿ�����

				  ,a.NBR_UNATVD_CHGD_CRD                  --�ѻ���δ���ÿ���

				  ,a.NBR_EXP_CRD                          --���ڿ���

				  ,a.NBR_DRMT_CRD                         --˯�߿�����

				  ,a.NBR_CRD_CLECTD                       --���տ���

				  ,a.NBR_NEW_CRD                          --�¿�����

				  ,a.NBR_CRD_CLD                          --������

				  ,a.NBR_NEW_CST                          --�����ͻ���

				  ,a.NBR_CST_CLD                          --�ͻ�������

				  ,a.NBR_CRD_CHG                          --������

				  ,a.NBR_AC_Opened                        --�����˻���

				  ,a.NBR_AC_CLS                           --�����˻���

				  ,a.NBR_NEW_CRD                          --���ۼ��¿�����    

				  ,a.NBR_CRD_CLD                          --���ۼ�������      

				  ,a.NBR_NEW_CST                          --���ۼ������ͻ���  

				  ,a.NBR_CST_CLD                          --���ۼƿͻ�������  

				  ,a.NBR_CRD_CHG                          --���ۼƻ�����      

				  ,a.NBR_AC_Opened                        --���ۼƿ����˻���  

				  ,a.NBR_AC_CLS                           --���ۼ������˻���  

				  ,a.NBR_CRD_CLECTD                       --���ۼ����տ���    

				  ,a.NBR_NEW_CRD                          --���ۼ��¿�����    

				  ,a.NBR_CRD_CLD                          --���ۼ�������      

				  ,a.NBR_NEW_CST                          --���ۼ������ͻ���  

				  ,a.NBR_CST_CLD                          --���ۼƿͻ�������  

				  ,a.NBR_CRD_CHG                          --���ۼƻ�����      

				  ,a.NBR_AC_Opened                        --���ۼƿ����˻���  

				  ,a.NBR_AC_CLS                           --���ۼ������˻���  

          ,a.NBR_CRD_CLECTD                       --���ۼ����տ���    

				  ,COALESCE(b.TOT_YTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)     		--���ۼ��¿�����    																		        

          ,COALESCE(b.TOT_YTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)     			--���ۼ�������      														

          ,COALESCE(b.TOT_YTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)     			--���ۼ������ͻ���  														

          ,COALESCE(b.TOT_YTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)     			--���ۼƿͻ�������  														

          ,COALESCE(b.TOT_YTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)     			--���ۼƻ�����      														

          ,COALESCE(b.TOT_YTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0) 			--���ۼƿ����˻���

          --------------------------Start on 20100114----------------------------------------------------   														

          --,COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)       			--���ۼ������˻���  														

          --,COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)    --���ۼ����տ���   

          ,COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)       			--���ۼ������˻���  														

          ,COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)    --���ۼ����տ��� 

          --------------------------End on 20100114---------------------------------------------------- 

          -----------------------------Start of 20091203---------------------------------

					,a.NBR_CST                              --�ͻ���

					-----------------------------Start of 20091203--------------------------------- 																										

  			  -----------------------------Start of 20091208---------------------------------

	  			,a.NBR_DRMT_CRD_WITH_LOW_BAL            --���С��10��˯�߿�����

				  ,a.NBR_DRMT_AC_WITH_LOW_BAL					    --���С��10��˯�߻�����

				  ,a.NBR_DRMT_AC										      --˯�߻�����

			    -----------------------------End of 20091208---------------------------------								

        FROM SESSION.TMP_CRD_PRFL_CRD_DLY_SMY_CR a LEFT JOIN SMY.CRD_PRFL_CRD_DLY_SMY b

        ON a.OU_ID = b.OU_ID AND

           a.CRD_TP_ID = b.CRD_TP_ID AND

    			 a.IS_CR_CRD_F      = b.IS_CR_CRD_F AND

    			 a.CRD_BRAND_TP_ID  = b.CRD_BRAND_TP_ID AND

    			 a.CRD_PRVL_TP_ID   = b.CRD_PRVL_TP_ID AND

    			 a.PSBK_RLTD_F      = b.PSBK_RLTD_F AND

    			 a.IS_NONGXIN_CRD_F = b.IS_NONGXIN_CRD_F AND

    			 a.ENT_IDV_IND      = b.ENT_IDV_IND AND

    			 a.MST_CRD_IND      = b.MST_CRD_IND AND

    			 a.NGO_CRD_IND      = b.NGO_CRD_IND AND

    			 a.MULT_CCY_F       = b.MULT_CCY_F AND

    			 a.PD_GRP_CD        = b.PD_GRP_CD AND

    			 a.PD_SUB_CD        = b.PD_SUB_CD AND

    			 a.BIZ_CGY_TP_ID       = b.BIZ_CGY_TP_ID AND

    			 a.CCY              = b.CCY AND

    			 a.ACG_DT - 1 day   = b.ACG_DT  

    			 ;         --

      ELSE     --�³�

        INSERT INTO SMY.CRD_PRFL_CRD_DLY_SMY

          (OU_ID                               --������

				   ,CRD_TP_ID                            --������

				   ,Is_CR_CRD_F                          --�Ƿ�Ϊ���ǿ�

				   ,CRD_Brand_TP_Id                      --��Ʒ������

				   ,CRD_PRVL_TP_ID                       --������

				   ,PSBK_RLTD_F                          --������ر�ʶ

				   ,IS_NONGXIN_CRD_F                     --���տ�/ũ�ſ���ʶ

				   ,ENT_IDV_IND                          --������

				   ,MST_CRD_IND                          --��/������־

				   ,NGO_CRD_IND                          --Э�鿨����

				   ,MULT_CCY_F                           --˫�ҿ���־

				   ,PD_GRP_CD                            --��Ʒ��

				   ,PD_SUB_CD                            --��Ʒ�Ӵ���

				   ,BIZ_CGY_TP_ID                        --ҵ�����

				   ,CCY                                  --����

				   ,ACG_DT                               --����YYYY-MM-DD

				   ,CDR_YR                               --���YYYY

				   ,CDR_MTH                              --�·�MM

				   ,ACT_NBR_AC                           --ʵ���˻���

				   ,NBR_EFF_CRD                          --��������

				   ,NBR_UNATVD_CR_CRD                    --δ�������ÿ�����

				   ,NBR_UNATVD_CHGD_CRD                  --�ѻ���δ���ÿ���

				   ,NBR_EXP_CRD                          --���ڿ���

				   ,NBR_DRMT_CRD                         --˯�߿�����

				   ,NBR_CRD_CLECTD                       --���տ���

				   ,NBR_NEW_CRD                          --�¿�����

				   ,NBR_CRD_CLD                          --������

				   ,NBR_NEW_CST                          --�����ͻ���

				   ,NBR_CST_CLD                          --�ͻ�������

				   ,NBR_CRD_CHG                          --������

				   ,NBR_AC_Opened                        --�����˻���

				   ,NBR_AC_CLS                           --�����˻���

				   ,TOT_MTD_NBR_NEW_CRD                  --���ۼ��¿�����        

				   ,TOT_MTD_NBR_CRD_CLD                  --���ۼ�������          

				   ,TOT_MTD_NBR_NEW_CST                  --���ۼ������ͻ���      

				   ,TOT_MTD_NBR_CST_CLD                  --���ۼƿͻ�������      

				   ,TOT_MTD_NBR_CRD_CHG                  --���ۼƻ�����          

				   ,TOT_MTD_NBR_AC_Opened                --���ۼƿ����˻���      

				   ,TOT_MTD_NBR_AC_CLS                   --���ۼ������˻���      

				   ,TOT_MTD_NBR_CRD_CLECTD               --���ۼ����տ���        

				   ,TOT_QTD_NBR_NEW_CRD                  --���ۼ��¿�����        

				   ,TOT_QTD_NBR_CRD_CLD                  --���ۼ�������          

				   ,TOT_QTD_NBR_NEW_CST                  --���ۼ������ͻ���      

				   ,TOT_QTD_NBR_CST_CLD                  --���ۼƿͻ�������      

				   ,TOT_QTD_NBR_CRD_CHG                  --���ۼƻ�����          

				   ,TOT_QTD_NBR_AC_Opened                --���ۼƿ����˻���      

				   ,TOT_QTD_NBR_AC_CLS                   --���ۼ������˻���      

				   ,TOT_QTD_NBR_CRD_CLECTD               --���ۼ����տ���        

				   ,TOT_YTD_NBR_NEW_CRD                  --���ۼ��¿�����        

				   ,TOT_YTD_NBR_CRD_CLD                  --���ۼ�������          

				   ,TOT_YTD_NBR_NEW_CST                  --���ۼ������ͻ���      

				   ,TOT_YTD_NBR_CST_CLD                  --���ۼƿͻ�������      

				   ,TOT_YTD_NBR_CRD_CHG                  --���ۼƻ�����          

				   ,TOT_YTD_NBR_AC_Opened                --���ۼƿ����˻���      

				   ,TOT_YTD_NBR_AC_CLS                   --���ۼ������˻���      

				   ,TOT_YTD_NBR_CRD_CLECTD               --���ۼ����տ���        

				   -----------------------------Start of 20091203---------------------------------

					 ,NBR_CST                              --�ͻ���

					 -----------------------------End of 20091203--------------------------------- 																										

					 -----------------------------Start of 20091208---------------------------------

					 ,NBR_DRMT_CRD_WITH_LOW_BAL            --���С��10��˯�߿�����

					 ,NBR_DRMT_AC_WITH_LOW_BAL					    --���С��10��˯�߻�����

					 ,NBR_DRMT_AC										      --˯�߻�����

				   -----------------------------End of 20091208---------------------------------			



				)

        SELECT 

           a.OU_ID                                --������

				  ,a.CRD_TP_ID                            --������

				  ,a.Is_CR_CRD_F                          --�Ƿ�Ϊ���ǿ�

				  ,a.CRD_Brand_TP_Id                      --��Ʒ������

				  ,a.CRD_PRVL_TP_ID                       --������

				  ,a.PSBK_RLTD_F                          --������ر�ʶ

				  ,a.IS_NONGXIN_CRD_F                     --���տ�/ũ�ſ���ʶ

				  ,a.ENT_IDV_IND                          --������

				  ,a.MST_CRD_IND                          --��/������־

				  ,a.NGO_CRD_IND                          --Э�鿨����

				  ,a.MULT_CCY_F                           --˫�ҿ���־

				  ,a.PD_GRP_CD                            --��Ʒ��

				  ,a.PD_SUB_CD                            --��Ʒ�Ӵ���

				  ,a.BIZ_CGY_TP_ID                        --ҵ�����

				  ,a.CCY                                  --����

				  ,a.ACG_DT                               --����YYYY-MM-DD

				  ,a.CDR_YR                               --���YYYY

				  ,a.CDR_MTH                              --�·�MM

				  ,a.ACT_NBR_AC                           --ʵ���˻���

				  ,a.NBR_EFF_CRD                          --��������

				  ,a.NBR_UNATVD_CR_CRD                    --δ�������ÿ�����

				  ,a.NBR_UNATVD_CHGD_CRD                  --�ѻ���δ���ÿ���

				  ,a.NBR_EXP_CRD                          --���ڿ���

				  ,a.NBR_DRMT_CRD                         --˯�߿�����

				  ,a.NBR_CRD_CLECTD                       --���տ���

				  ,a.NBR_NEW_CRD                          --�¿�����

				  ,a.NBR_CRD_CLD                          --������

				  ,a.NBR_NEW_CST                          --�����ͻ���

				  ,a.NBR_CST_CLD                          --�ͻ�������

				  ,a.NBR_CRD_CHG                          --������

				  ,a.NBR_AC_Opened                        --�����˻���

				  ,a.NBR_AC_CLS                           --�����˻���

				  ,a.NBR_NEW_CRD                          --���ۼ��¿�����                                           

				  ,a.NBR_CRD_CLD                          --���ۼ�������          

				  ,a.NBR_NEW_CST                          --���ۼ������ͻ���      

				  ,a.NBR_CST_CLD                          --���ۼƿͻ�������      

				  ,a.NBR_CRD_CHG                          --���ۼƻ�����          

				  ,a.NBR_AC_Opened                        --���ۼƿ����˻���      

				  ,a.NBR_AC_CLS                           --���ۼ������˻���      

				  ,a.NBR_CRD_CLECTD                       --���ۼ����տ���        

				  ,COALESCE(b.TOT_QTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)    						--���ۼ��¿�����        																							        

          ,COALESCE(b.TOT_QTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)    						--���ۼ�������          																				

          ,COALESCE(b.TOT_QTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)    						--���ۼ������ͻ���      																				

          ,COALESCE(b.TOT_QTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)    						--���ۼƿͻ�������      																				

          ,COALESCE(b.TOT_QTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)    						--���ۼƻ�����          																				

          ,COALESCE(b.TOT_QTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0)						--���ۼƿ����˻��� 

          --------------------------Start on 20100114----------------------------------------------------     																				

          --,COALESCE(b.TOT_QTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)      						--���ۼ������˻���      																				

          --,COALESCE(b.TOT_QTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)         --���ۼ����տ���  

          ,COALESCE(b.TOT_QTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)      						--���ۼ������˻���      																				

          ,COALESCE(b.TOT_QTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)         --���ۼ����տ���   

          --------------------------End on 20100114----------------------------------------------------    

				  ,COALESCE(b.TOT_YTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)     					--���ۼ��¿�����        																								        

          ,COALESCE(b.TOT_YTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)     						--���ۼ�������          																				

          ,COALESCE(b.TOT_YTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)     						--���ۼ������ͻ���      																				

          ,COALESCE(b.TOT_YTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)     						--���ۼƿͻ�������      																				

          ,COALESCE(b.TOT_YTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)     						--���ۼƻ�����          																				

          ,COALESCE(b.TOT_YTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0) 						--���ۼƿ����˻���   

          --------------------------Start on 20100114----------------------------------------------------   																				

          --,COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)       						--���ۼ������˻���      																				

          --,COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)         --���ۼ����տ��� 

          ,COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)       						--���ۼ������˻���      																				

          ,COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)         --���ۼ����տ���     

          --------------------------End on 20100114----------------------------------------------------   

          -----------------------------Start of 20091203---------------------------------

					,a.NBR_CST                              --�ͻ���

					-----------------------------End of 20091203--------------------------------- 																										

					-----------------------------Start of 20091208---------------------------------

					,a.NBR_DRMT_CRD_WITH_LOW_BAL            --���С��10��˯�߿�����

					,a.NBR_DRMT_AC_WITH_LOW_BAL					    --���С��10��˯�߻�����

					,a.NBR_DRMT_AC										      --˯�߻�����

				  -----------------------------End of 20091208---------------------------------								

        FROM SESSION.TMP_CRD_PRFL_CRD_DLY_SMY_CR a LEFT JOIN SMY.CRD_PRFL_CRD_DLY_SMY b

        ON a.OU_ID = b.OU_ID AND

           a.CRD_TP_ID = b.CRD_TP_ID AND

    			 a.IS_CR_CRD_F      = b.IS_CR_CRD_F AND

    			 a.CRD_BRAND_TP_ID  = b.CRD_BRAND_TP_ID AND

    			 a.CRD_PRVL_TP_ID   = b.CRD_PRVL_TP_ID AND

    			 a.PSBK_RLTD_F      = b.PSBK_RLTD_F AND

    			 a.IS_NONGXIN_CRD_F = b.IS_NONGXIN_CRD_F AND

    			 a.ENT_IDV_IND      = b.ENT_IDV_IND AND

    			 a.MST_CRD_IND      = b.MST_CRD_IND AND

    			 a.NGO_CRD_IND      = b.NGO_CRD_IND AND

    			 a.MULT_CCY_F       = b.MULT_CCY_F AND

    			 a.PD_GRP_CD        = b.PD_GRP_CD AND

    			 a.PD_SUB_CD        = b.PD_SUB_CD AND

    			 a.BIZ_CGY_TP_ID       = b.BIZ_CGY_TP_ID AND

    			 a.CCY              = b.CCY AND

    			 a.ACG_DT - 1 day   = b.ACG_DT   ;               				  				  				      	--

   END IF;--

ELSE  --���³�

	

  INSERT INTO SMY.CRD_PRFL_CRD_DLY_SMY

    (OU_ID                               --������

	   ,CRD_TP_ID                            --������

	   ,Is_CR_CRD_F                          --�Ƿ�Ϊ���ǿ�

	   ,CRD_Brand_TP_Id                      --��Ʒ������

	   ,CRD_PRVL_TP_ID                       --������

	   ,PSBK_RLTD_F                          --������ر�ʶ

	   ,IS_NONGXIN_CRD_F                     --���տ�/ũ�ſ���ʶ

	   ,ENT_IDV_IND                          --������

	   ,MST_CRD_IND                          --��/������־

	   ,NGO_CRD_IND                          --Э�鿨����

	   ,MULT_CCY_F                           --˫�ҿ���־

	   ,PD_GRP_CD                            --��Ʒ��

	   ,PD_SUB_CD                            --��Ʒ�Ӵ���

	   ,BIZ_CGY_TP_ID                        --ҵ�����

	   ,CCY                                  --����

	   ,ACG_DT                               --����YYYY-MM-DD

	   ,CDR_YR                               --���YYYY

	   ,CDR_MTH                              --�·�MM

	   ,ACT_NBR_AC                           --ʵ���˻���

	   ,NBR_EFF_CRD                          --��������

	   ,NBR_UNATVD_CR_CRD                    --δ�������ÿ�����

	   ,NBR_UNATVD_CHGD_CRD                  --�ѻ���δ���ÿ���

	   ,NBR_EXP_CRD                          --���ڿ���

	   ,NBR_DRMT_CRD                         --˯�߿�����

	   ,NBR_CRD_CLECTD                       --���տ���

	   ,NBR_NEW_CRD                          --�¿�����

	   ,NBR_CRD_CLD                          --������

	   ,NBR_NEW_CST                          --�����ͻ���

	   ,NBR_CST_CLD                          --�ͻ�������

	   ,NBR_CRD_CHG                          --������

	   ,NBR_AC_Opened                        --�����˻���

	   ,NBR_AC_CLS                           --�����˻���

	   ,TOT_MTD_NBR_NEW_CRD                  --���ۼ��¿�����        

	   ,TOT_MTD_NBR_CRD_CLD                  --���ۼ�������          

	   ,TOT_MTD_NBR_NEW_CST                  --���ۼ������ͻ���      

	   ,TOT_MTD_NBR_CST_CLD                  --���ۼƿͻ�������      

	   ,TOT_MTD_NBR_CRD_CHG                  --���ۼƻ�����          

	   ,TOT_MTD_NBR_AC_Opened                --���ۼƿ����˻���      

	   ,TOT_MTD_NBR_AC_CLS                   --���ۼ������˻���      

	   ,TOT_MTD_NBR_CRD_CLECTD               --���ۼ����տ���        

	   ,TOT_QTD_NBR_NEW_CRD                  --���ۼ��¿�����        

	   ,TOT_QTD_NBR_CRD_CLD                  --���ۼ�������          

	   ,TOT_QTD_NBR_NEW_CST                  --���ۼ������ͻ���      

	   ,TOT_QTD_NBR_CST_CLD                  --���ۼƿͻ�������      

	   ,TOT_QTD_NBR_CRD_CHG                  --���ۼƻ�����          

	   ,TOT_QTD_NBR_AC_Opened                --���ۼƿ����˻���      

	   ,TOT_QTD_NBR_AC_CLS                   --���ۼ������˻���      

	   ,TOT_QTD_NBR_CRD_CLECTD               --���ۼ����տ���        

	   ,TOT_YTD_NBR_NEW_CRD                  --���ۼ��¿�����        

	   ,TOT_YTD_NBR_CRD_CLD                  --���ۼ�������          

	   ,TOT_YTD_NBR_NEW_CST                  --���ۼ������ͻ���      

	   ,TOT_YTD_NBR_CST_CLD                  --���ۼƿͻ�������      

	   ,TOT_YTD_NBR_CRD_CHG                  --���ۼƻ�����          

	   ,TOT_YTD_NBR_AC_Opened                --���ۼƿ����˻���      

	   ,TOT_YTD_NBR_AC_CLS                   --���ۼ������˻���      

	   ,TOT_YTD_NBR_CRD_CLECTD               --���ۼ����տ���        

	  -----------------------------Start of 20091203---------------------------------

		 ,NBR_CST                              --�ͻ���

		-----------------------------Start of 20091203--------------------------------- 																										

		 -----------------------------Start of 20091208---------------------------------

		 ,NBR_DRMT_CRD_WITH_LOW_BAL            --���С��10��˯�߿�����

		 ,NBR_DRMT_AC_WITH_LOW_BAL					    --���С��10��˯�߻�����

		 ,NBR_DRMT_AC										      --˯�߻�����

	   -----------------------------End of 20091208---------------------------------			

		

	)

  SELECT 

     a.OU_ID                                --������

	  ,a.CRD_TP_ID                            --������

	  ,a.Is_CR_CRD_F                          --�Ƿ�Ϊ���ǿ�

	  ,a.CRD_Brand_TP_Id                      --��Ʒ������

	  ,a.CRD_PRVL_TP_ID                       --������

	  ,a.PSBK_RLTD_F                          --������ر�ʶ

	  ,a.IS_NONGXIN_CRD_F                     --���տ�/ũ�ſ���ʶ

	  ,a.ENT_IDV_IND                          --������

	  ,a.MST_CRD_IND                          --��/������־

	  ,a.NGO_CRD_IND                          --Э�鿨����

	  ,a.MULT_CCY_F                           --˫�ҿ���־

	  ,a.PD_GRP_CD                            --��Ʒ��

	  ,a.PD_SUB_CD                            --��Ʒ�Ӵ���

	  ,a.BIZ_CGY_TP_ID                        --ҵ�����

	  ,a.CCY                                  --����

	  ,a.ACG_DT                               --����YYYY-MM-DD

	  ,a.CDR_YR                               --���YYYY

	  ,a.CDR_MTH                              --�·�MM

	  ,a.ACT_NBR_AC                           --ʵ���˻���

	  ,a.NBR_EFF_CRD                          --��������

	  ,a.NBR_UNATVD_CR_CRD                    --δ�������ÿ�����

	  ,a.NBR_UNATVD_CHGD_CRD                  --�ѻ���δ���ÿ���

	  ,a.NBR_EXP_CRD                          --���ڿ���

	  ,a.NBR_DRMT_CRD                         --˯�߿�����

	  ,a.NBR_CRD_CLECTD                       --���տ���

	  ,a.NBR_NEW_CRD                          --�¿�����

	  ,a.NBR_CRD_CLD                          --������

	  ,a.NBR_NEW_CST                          --�����ͻ���

	  ,a.NBR_CST_CLD                          --�ͻ�������

	  ,a.NBR_CRD_CHG                          --������

	  ,a.NBR_AC_Opened                        --�����˻���

	  ,a.NBR_AC_CLS                           --�����˻���

		,COALESCE(b.TOT_MTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)     	--���ۼ��¿�����        																												        

    ,COALESCE(b.TOT_MTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)     		--���ۼ�������          																								

    ,COALESCE(b.TOT_MTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)     		--���ۼ������ͻ���      																								

    ,COALESCE(b.TOT_MTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)     		--���ۼƿͻ�������      																								

    ,COALESCE(b.TOT_MTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)     		--���ۼƻ�����          																								

    ,COALESCE(b.TOT_MTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0) 		--���ۼƿ����˻��� 

    --------------------------Start on 20100114----------------------------------------------------      																								

    --,COALESCE(b.TOT_MTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)       		--���ۼ������˻���      																								

    --,COALESCE(b.TOT_MTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)  --���ۼ����տ���  

    ,COALESCE(b.TOT_MTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)       		--���ۼ������˻���      																								

    ,COALESCE(b.TOT_MTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)  --���ۼ����տ��� 

    --------------------------End on 20100114----------------------------------------------------        

		,COALESCE(b.TOT_QTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)     	--���ۼ��¿�����        																												        

    ,COALESCE(b.TOT_QTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)     		--���ۼ�������          																								

    ,COALESCE(b.TOT_QTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)     		--���ۼ������ͻ���      																								

    ,COALESCE(b.TOT_QTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)     		--���ۼƿͻ�������      																								

    ,COALESCE(b.TOT_QTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)     		--���ۼƻ�����          																								

    ,COALESCE(b.TOT_QTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0) 		--���ۼƿ����˻���    

    --------------------------Start on 20100114----------------------------------------------------    																								

    --,COALESCE(b.TOT_QTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)       		--���ۼ������˻���      																								

    --,COALESCE(b.TOT_QTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)  --���ۼ����տ���  

    ,COALESCE(b.TOT_QTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)       		--���ۼ������˻���      																								

    ,COALESCE(b.TOT_QTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)  --���ۼ����տ���   

    --------------------------End on 20100114----------------------------------------------------       

		,COALESCE(b.TOT_YTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)     	--���ۼ��¿�����        																												        

    ,COALESCE(b.TOT_YTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)     		--���ۼ�������          																								

    ,COALESCE(b.TOT_YTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)     		--���ۼ������ͻ���      																								

    ,COALESCE(b.TOT_YTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)     		--���ۼƿͻ�������      																								

    ,COALESCE(b.TOT_YTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)     		--���ۼƻ�����          																								

    ,COALESCE(b.TOT_YTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0) 		--���ۼƿ����˻��� 

    --------------------------Start on 20100114----------------------------------------------------       																								

    --,COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)       		--���ۼ������˻���      																								

    --,COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)  --���ۼ����տ��� 

    ,COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)       		--���ۼ������˻���      																								

    ,COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)  --���ۼ����տ���  

    --------------------------End on 20100114----------------------------------------------------        

    -----------------------------Start of 20091203---------------------------------

		,a.NBR_CST                              --�ͻ���

		-----------------------------Start of 20091203--------------------------------- 																										

		 -----------------------------Start of 20091208---------------------------------

		 ,a.NBR_DRMT_CRD_WITH_LOW_BAL            --���С��10��˯�߿�����

		 ,a.NBR_DRMT_AC_WITH_LOW_BAL					    --���С��10��˯�߻�����

		 ,a.NBR_DRMT_AC										      --˯�߻�����

	   -----------------------------End of 20091208---------------------------------					

  FROM SESSION.TMP_CRD_PRFL_CRD_DLY_SMY_CR a LEFT JOIN SMY.CRD_PRFL_CRD_DLY_SMY b

  ON a.OU_ID = b.OU_ID AND

     a.CRD_TP_ID = b.CRD_TP_ID AND

  	 a.IS_CR_CRD_F      = b.IS_CR_CRD_F AND

  	 a.CRD_BRAND_TP_ID  = b.CRD_BRAND_TP_ID AND

  	 a.CRD_PRVL_TP_ID   = b.CRD_PRVL_TP_ID AND

  	 a.PSBK_RLTD_F      = b.PSBK_RLTD_F AND

  	 a.IS_NONGXIN_CRD_F = b.IS_NONGXIN_CRD_F AND

  	 a.ENT_IDV_IND      = b.ENT_IDV_IND AND

  	 a.MST_CRD_IND      = b.MST_CRD_IND AND

  	 a.NGO_CRD_IND      = b.NGO_CRD_IND AND

  	 a.MULT_CCY_F       = b.MULT_CCY_F AND

  	 a.PD_GRP_CD        = b.PD_GRP_CD AND

  	 a.PD_SUB_CD        = b.PD_SUB_CD AND

  	 a.BIZ_CGY_TP_ID       = b.BIZ_CGY_TP_ID AND

  	 a.CCY              = b.CCY AND

  	 a.ACG_DT - 1 day   = b.ACG_DT   ; 	--

	   

END IF  ;            --



GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--

SET SMY_STEPNUM = SMY_STEPNUM+1;--

INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

   

SET SMY_STEPDESC = '�����ǿ���������';--



IF CUR_DAY = 1 THEN  

   IF CUR_MONTH = 1 THEN --���

      INSERT INTO SMY.CRD_PRFL_CRD_DLY_SMY

        ( OU_ID                               --������

				 ,CRD_TP_ID                            --������

				 ,Is_CR_CRD_F                          --�Ƿ�Ϊ���ǿ�

				 ,CRD_Brand_TP_Id                      --��Ʒ������

				 ,CRD_PRVL_TP_ID                       --������

				 ,PSBK_RLTD_F                          --������ر�ʶ

				 ,IS_NONGXIN_CRD_F                     --���տ�/ũ�ſ���ʶ

				 ,ENT_IDV_IND                          --������

				 ,MST_CRD_IND                          --��/������־

				 ,NGO_CRD_IND                          --Э�鿨����

				 ,MULT_CCY_F                           --˫�ҿ���־

				 ,PD_GRP_CD                            --��Ʒ��

				 ,PD_SUB_CD                            --��Ʒ�Ӵ���

				 ,BIZ_CGY_TP_ID                        --ҵ�����

				 ,CCY                                  --����

				 ,ACG_DT                               --����YYYY-MM-DD

				 ,CDR_YR                               --���YYYY

				 ,CDR_MTH                              --�·�MM

				 ,ACT_NBR_AC                           --ʵ���˻���

				 ,NBR_EFF_CRD                          --��������

				 ,NBR_UNATVD_CR_CRD                    --δ�������ÿ�����

				 ,NBR_UNATVD_CHGD_CRD                  --�ѻ���δ���ÿ���

				 ,NBR_EXP_CRD                          --���ڿ���

				 ,NBR_DRMT_CRD                         --˯�߿�����

				 ,NBR_CRD_CLECTD                       --���տ���

				 ,NBR_NEW_CRD                          --�¿�����

				 ,NBR_CRD_CLD                          --������

				 ,NBR_NEW_CST                          --�����ͻ���

				 ,NBR_CST_CLD                          --�ͻ�������

				 ,NBR_CRD_CHG                          --������

				 ,NBR_AC_Opened                        --�����˻���

				 ,NBR_AC_CLS                           --�����˻���

				 ,TOT_MTD_NBR_NEW_CRD                  --���ۼ��¿�����        

				 ,TOT_MTD_NBR_CRD_CLD                  --���ۼ�������          

				 ,TOT_MTD_NBR_NEW_CST                  --���ۼ������ͻ���      

				 ,TOT_MTD_NBR_CST_CLD                  --���ۼƿͻ�������      

				 ,TOT_MTD_NBR_CRD_CHG                  --���ۼƻ�����          

				 ,TOT_MTD_NBR_AC_Opened                --���ۼƿ����˻���      

				 ,TOT_MTD_NBR_AC_CLS                   --���ۼ������˻���      

				 ,TOT_MTD_NBR_CRD_CLECTD               --���ۼ����տ���        

				 ,TOT_QTD_NBR_NEW_CRD                  --���ۼ��¿�����        

				 ,TOT_QTD_NBR_CRD_CLD                  --���ۼ�������          

				 ,TOT_QTD_NBR_NEW_CST                  --���ۼ������ͻ���      

				 ,TOT_QTD_NBR_CST_CLD                  --���ۼƿͻ�������      

				 ,TOT_QTD_NBR_CRD_CHG                  --���ۼƻ�����          

				 ,TOT_QTD_NBR_AC_Opened                --���ۼƿ����˻���      

				 ,TOT_QTD_NBR_AC_CLS                   --���ۼ������˻���      

				 ,TOT_QTD_NBR_CRD_CLECTD               --���ۼ����տ���        

				 ,TOT_YTD_NBR_NEW_CRD                  --���ۼ��¿�����        

				 ,TOT_YTD_NBR_CRD_CLD                  --���ۼ�������          

				 ,TOT_YTD_NBR_NEW_CST                  --���ۼ������ͻ���      

				 ,TOT_YTD_NBR_CST_CLD                  --���ۼƿͻ�������      

				 ,TOT_YTD_NBR_CRD_CHG                  --���ۼƻ�����          

				 ,TOT_YTD_NBR_AC_Opened                --���ۼƿ����˻���      

				 ,TOT_YTD_NBR_AC_CLS                   --���ۼ������˻���      

				 ,TOT_YTD_NBR_CRD_CLECTD               --���ۼ����տ���        

				-----------------------------Start of 20091203---------------------------------

			 	 ,NBR_CST                              --�ͻ���

				-----------------------------End of 20091203--------------------------------- 																														

			 -----------------------------Start of 20091208---------------------------------

			 ,NBR_DRMT_CRD_WITH_LOW_BAL            --���С��10��˯�߿�����

			 ,NBR_DRMT_AC_WITH_LOW_BAL					    --���С��10��˯�߻�����

			 ,NBR_DRMT_AC										      --˯�߻�����

		   -----------------------------End of 20091208---------------------------------							

			)

      SELECT 

        OU_ID                                --������

				,CRD_TP_ID                            --������

				,Is_CR_CRD_F                          --�Ƿ�Ϊ���ǿ�

				,CRD_Brand_TP_Id                      --��Ʒ������

				,CRD_PRVL_TP_ID                       --������

				,PSBK_RLTD_F                          --������ر�ʶ

				,IS_NONGXIN_CRD_F                     --���տ�/ũ�ſ���ʶ

				,ENT_IDV_IND                          --������

				,MST_CRD_IND                          --��/������־

				,NGO_CRD_IND                          --Э�鿨����

				,MULT_CCY_F                           --˫�ҿ���־

				,PD_GRP_CD                            --��Ʒ��

				,PD_SUB_CD                            --��Ʒ�Ӵ���

				,BIZ_CGY_TP_ID                        --ҵ�����

				,CCY                                  --����

				,ACG_DT                               --����YYYY-MM-DD

				,CDR_YR                               --���YYYY

				,CDR_MTH                              --�·�MM

				,ACT_NBR_AC                           --ʵ���˻���

				,NBR_EFF_CRD                          --��������

				,NBR_UNATVD_CR_CRD                    --δ�������ÿ�����

				,NBR_UNATVD_CHGD_CRD                  --�ѻ���δ���ÿ���

				,NBR_EXP_CRD                          --���ڿ���

				,NBR_DRMT_CRD                         --˯�߿�����

				,NBR_CRD_CLECTD                       --���տ���

				,NBR_NEW_CRD                          --�¿�����

				,NBR_CRD_CLD                          --������

				,NBR_NEW_CST                          --�����ͻ���

				,NBR_CST_CLD                          --�ͻ�������

				,NBR_CRD_CHG                          --������

				,NBR_AC_Opened                        --�����˻���

				,NBR_AC_CLS                           --�����˻���

				,NBR_NEW_CRD                          --���ۼ��¿�����        

				,NBR_CRD_CLD                          --���ۼ�������          

				,NBR_NEW_CST                          --���ۼ������ͻ���      

				,NBR_CST_CLD                          --���ۼƿͻ�������      

				,NBR_CRD_CHG                          --���ۼƻ�����          

				,NBR_AC_Opened                        --���ۼƿ����˻���      

				,NBR_AC_CLS                           --���ۼ������˻���      

				,NBR_CRD_CLECTD                       --���ۼ����տ���        

				,NBR_NEW_CRD                          --���ۼ��¿�����        

				,NBR_CRD_CLD                          --���ۼ�������          

				,NBR_NEW_CST                          --���ۼ������ͻ���      

				,NBR_CST_CLD                          --���ۼƿͻ�������      

				,NBR_CRD_CHG                          --���ۼƻ�����          

				,NBR_AC_Opened                        --���ۼƿ����˻���      

				,NBR_AC_CLS                           --���ۼ������˻���      

				,NBR_CRD_CLECTD                       --���ۼ����տ���        

				,NBR_NEW_CRD                          --���ۼ��¿�����        

				,NBR_CRD_CLD                          --���ۼ�������          

				,NBR_NEW_CST                          --���ۼ������ͻ���      

				,NBR_CST_CLD                          --���ۼƿͻ�������      

				,NBR_CRD_CHG                          --���ۼƻ�����          

				,NBR_AC_Opened                        --���ۼƿ����˻���      

				,NBR_AC_CLS                           --���ۼ������˻���      

				,NBR_CRD_CLECTD                       --���ۼ����տ���        

				-----------------------------Start of 20091203---------------------------------

			 	,NBR_CST                              --�ͻ���

				-----------------------------End of 20091203--------------------------------- 																																						

				 -----------------------------Start of 20091208---------------------------------

				 ,NBR_DRMT_CRD_WITH_LOW_BAL            --���С��10��˯�߿�����

				 ,NBR_DRMT_AC_WITH_LOW_BAL					    --���С��10��˯�߻�����

				 ,NBR_DRMT_AC										      --˯�߻�����

			   -----------------------------End of 20091208---------------------------------							

		  FROM SESSION.TMP_CRD_PRFL_CRD_DLY_SMY_DB;--

		  

   ELSEIF CUR_MONTH IN (4, 7 ,10) THEN  --����

        INSERT INTO SMY.CRD_PRFL_CRD_DLY_SMY

          (OU_ID                               --������

				  ,CRD_TP_ID                            --������

				  ,Is_CR_CRD_F                          --�Ƿ�Ϊ���ǿ�

				  ,CRD_Brand_TP_Id                      --��Ʒ������

				  ,CRD_PRVL_TP_ID                       --������

				  ,PSBK_RLTD_F                          --������ر�ʶ

				  ,IS_NONGXIN_CRD_F                     --���տ�/ũ�ſ���ʶ

				  ,ENT_IDV_IND                          --������

				  ,MST_CRD_IND                          --��/������־

				  ,NGO_CRD_IND                          --Э�鿨����

				  ,MULT_CCY_F                           --˫�ҿ���־

				  ,PD_GRP_CD                            --��Ʒ��

				  ,PD_SUB_CD                            --��Ʒ�Ӵ���

				  ,BIZ_CGY_TP_ID                        --ҵ�����

				  ,CCY                                  --����

				  ,ACG_DT                               --����YYYY-MM-DD

				  ,CDR_YR                               --���YYYY

				  ,CDR_MTH                              --�·�MM

				  ,ACT_NBR_AC                           --ʵ���˻���

				  ,NBR_EFF_CRD                          --��������

				  ,NBR_UNATVD_CR_CRD                    --δ�������ÿ�����

				  ,NBR_UNATVD_CHGD_CRD                  --�ѻ���δ���ÿ���

				  ,NBR_EXP_CRD                          --���ڿ���

				  ,NBR_DRMT_CRD                         --˯�߿�����

				  ,NBR_CRD_CLECTD                       --���տ���

				  ,NBR_NEW_CRD                          --�¿�����

				  ,NBR_CRD_CLD                          --������

				  ,NBR_NEW_CST                          --�����ͻ���

				  ,NBR_CST_CLD                          --�ͻ�������

				  ,NBR_CRD_CHG                          --������

				  ,NBR_AC_Opened                        --�����˻���

				  ,NBR_AC_CLS                           --�����˻���

				  ,TOT_MTD_NBR_NEW_CRD                  --���ۼ��¿�����        

				  ,TOT_MTD_NBR_CRD_CLD                  --���ۼ�������          

				  ,TOT_MTD_NBR_NEW_CST                  --���ۼ������ͻ���      

				  ,TOT_MTD_NBR_CST_CLD                  --���ۼƿͻ�������      

				  ,TOT_MTD_NBR_CRD_CHG                  --���ۼƻ�����          

				  ,TOT_MTD_NBR_AC_Opened                --���ۼƿ����˻���      

				  ,TOT_MTD_NBR_AC_CLS                   --���ۼ������˻���      

				  ,TOT_MTD_NBR_CRD_CLECTD               --���ۼ����տ���        

				  ,TOT_QTD_NBR_NEW_CRD                  --���ۼ��¿�����        

				  ,TOT_QTD_NBR_CRD_CLD                  --���ۼ�������          

				  ,TOT_QTD_NBR_NEW_CST                  --���ۼ������ͻ���      

				  ,TOT_QTD_NBR_CST_CLD                  --���ۼƿͻ�������      

				  ,TOT_QTD_NBR_CRD_CHG                  --���ۼƻ�����          

				  ,TOT_QTD_NBR_AC_Opened                --���ۼƿ����˻���      

				  ,TOT_QTD_NBR_AC_CLS                   --���ۼ������˻���      

				  ,TOT_QTD_NBR_CRD_CLECTD               --���ۼ����տ���        

				  ,TOT_YTD_NBR_NEW_CRD                  --���ۼ��¿�����        

				  ,TOT_YTD_NBR_CRD_CLD                  --���ۼ�������          

				  ,TOT_YTD_NBR_NEW_CST                  --���ۼ������ͻ���      

				  ,TOT_YTD_NBR_CST_CLD                  --���ۼƿͻ�������      

				  ,TOT_YTD_NBR_CRD_CHG                  --���ۼƻ�����          

				  ,TOT_YTD_NBR_AC_Opened                --���ۼƿ����˻���      

				  ,TOT_YTD_NBR_AC_CLS                   --���ۼ������˻���      

				  ,TOT_YTD_NBR_CRD_CLECTD               --���ۼ����տ���        

				  -----------------------------Start of 20091203---------------------------------

			 		,NBR_CST                              --�ͻ���

					-----------------------------End of 20091203--------------------------------- 																																						

					 -----------------------------Start of 20091208---------------------------------

					 ,NBR_DRMT_CRD_WITH_LOW_BAL            --���С��10��˯�߿�����

					 ,NBR_DRMT_AC_WITH_LOW_BAL					    --���С��10��˯�߻�����

					 ,NBR_DRMT_AC										      --˯�߻�����

				   -----------------------------End of 20091208---------------------------------			

					

				)

        SELECT 

          a.OU_ID                                --������

				  , a.CRD_TP_ID                            --������

				  , a.Is_CR_CRD_F                          --�Ƿ�Ϊ���ǿ�

				  , a.CRD_Brand_TP_Id                      --��Ʒ������

				  , a.CRD_PRVL_TP_ID                       --������

				  , a.PSBK_RLTD_F                          --������ر�ʶ

				  , a.IS_NONGXIN_CRD_F                     --���տ�/ũ�ſ���ʶ

				  , a.ENT_IDV_IND                          --������

				  , a.MST_CRD_IND                          --��/������־

				  , a.NGO_CRD_IND                          --Э�鿨����

				  , a.MULT_CCY_F                           --˫�ҿ���־

				  , a.PD_GRP_CD                            --��Ʒ��

				  , a.PD_SUB_CD                            --��Ʒ�Ӵ���

				  , a.BIZ_CGY_TP_ID                        --ҵ�����

				  , a.CCY                                  --����

				  , a.ACG_DT                               --����YYYY-MM-DD

				  , a.CDR_YR                               --���YYYY

				  , a.CDR_MTH                              --�·�MM

				  , a.ACT_NBR_AC                           --ʵ���˻���

				  , a.NBR_EFF_CRD                          --��������

				  , a.NBR_UNATVD_CR_CRD                    --δ�������ÿ�����

				  , a.NBR_UNATVD_CHGD_CRD                  --�ѻ���δ���ÿ���

				  , a.NBR_EXP_CRD                          --���ڿ���

				  , a.NBR_DRMT_CRD                         --˯�߿�����

				  , a.NBR_CRD_CLECTD                       --���տ���

				  , a.NBR_NEW_CRD                          --�¿�����

				  , a.NBR_CRD_CLD                          --������

				  , a.NBR_NEW_CST                          --�����ͻ���

				  , a.NBR_CST_CLD                          --�ͻ�������

				  , a.NBR_CRD_CHG                          --������

				  , a.NBR_AC_Opened                        --�����˻���

				  , a.NBR_AC_CLS                           --�����˻���

				  , a.NBR_NEW_CRD                          --���ۼ��¿�����         

				  , a.NBR_CRD_CLD                          --���ۼ�������          

				  , a.NBR_NEW_CST                          --���ۼ������ͻ���      

				  , a.NBR_CST_CLD                          --���ۼƿͻ�������      

				  , a.NBR_CRD_CHG                          --���ۼƻ�����          

				  , a.NBR_AC_Opened                        --���ۼƿ����˻���      

				  , a.NBR_AC_CLS                           --���ۼ������˻���      

				  , a.NBR_CRD_CLECTD                       --���ۼ����տ���        

				  , a.NBR_NEW_CRD                          --���ۼ��¿�����        

				  , a.NBR_CRD_CLD                          --���ۼ�������          

				  , a.NBR_NEW_CST                          --���ۼ������ͻ���      

				  , a.NBR_CST_CLD                          --���ۼƿͻ�������      

				  , a.NBR_CRD_CHG                          --���ۼƻ�����          

				  , a.NBR_AC_Opened                        --���ۼƿ����˻���      

				  , a.NBR_AC_CLS                           --���ۼ������˻���      

          , a.NBR_CRD_CLECTD                       --���ۼ����տ���        

				  , COALESCE(b.TOT_YTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)     					--���ۼ��¿�����        																								        

          , COALESCE(b.TOT_YTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)     					--���ۼ�������          																					

          , COALESCE(b.TOT_YTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)     					--���ۼ������ͻ���      																					

          , COALESCE(b.TOT_YTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)     					--���ۼƿͻ�������      																					

          , COALESCE(b.TOT_YTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)     					--���ۼƻ�����          																					

          , COALESCE(b.TOT_YTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0) 					--���ۼƿ����˻��� 

          --------------------------Start on 20100114----------------------------------------------------     																					

          --, COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)       					--���ۼ������˻���      																					

          --, COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)         --���ۼ����տ���  

          , COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)       					--���ۼ������˻���      																					

          , COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)         --���ۼ����տ���   

          --------------------------End on 20100114----------------------------------------------------   

          -----------------------------Start of 20091203---------------------------------

			 		,a.NBR_CST                              --�ͻ���

					-----------------------------End of 20091203---------------------------------																									

					 -----------------------------Start of 20091208---------------------------------

					 ,a.NBR_DRMT_CRD_WITH_LOW_BAL            --���С��10��˯�߿�����

					 ,a.NBR_DRMT_AC_WITH_LOW_BAL					    --���С��10��˯�߻�����

					 ,a.NBR_DRMT_AC										      --˯�߻�����

				   -----------------------------End of 20091208---------------------------------								

        FROM SESSION.TMP_CRD_PRFL_CRD_DLY_SMY_DB a LEFT JOIN SMY.CRD_PRFL_CRD_DLY_SMY b

        ON a.OU_ID = b.OU_ID AND

           a.CRD_TP_ID = b.CRD_TP_ID AND

    			 a.IS_CR_CRD_F      = b.IS_CR_CRD_F AND

    			 a.CRD_BRAND_TP_ID  = b.CRD_BRAND_TP_ID AND

    			 a.CRD_PRVL_TP_ID   = b.CRD_PRVL_TP_ID AND

    			 a.PSBK_RLTD_F      = b.PSBK_RLTD_F AND

    			 a.IS_NONGXIN_CRD_F = b.IS_NONGXIN_CRD_F AND

    			 a.ENT_IDV_IND      = b.ENT_IDV_IND AND

    			 a.MST_CRD_IND      = b.MST_CRD_IND AND

    			 a.NGO_CRD_IND      = b.NGO_CRD_IND AND

    			 a.MULT_CCY_F       = b.MULT_CCY_F AND

    			 a.PD_GRP_CD        = b.PD_GRP_CD AND

    			 a.PD_SUB_CD        = b.PD_SUB_CD AND

    			 a.BIZ_CGY_TP_ID       = b.BIZ_CGY_TP_ID AND

    			 a.CCY              = b.CCY AND

    			 a.ACG_DT - 1 day   = b.ACG_DT  ;        --

      ELSE              --�³�

        INSERT INTO SMY.CRD_PRFL_CRD_DLY_SMY

          (OU_ID                               --������

				  ,CRD_TP_ID                            --������

				  ,Is_CR_CRD_F                          --�Ƿ�Ϊ���ǿ�

				  ,CRD_Brand_TP_Id                      --��Ʒ������

				  ,CRD_PRVL_TP_ID                       --������

				  ,PSBK_RLTD_F                          --������ر�ʶ

				  ,IS_NONGXIN_CRD_F                     --���տ�/ũ�ſ���ʶ

				  ,ENT_IDV_IND                          --������

				  ,MST_CRD_IND                          --��/������־

				  ,NGO_CRD_IND                          --Э�鿨����

				  ,MULT_CCY_F                           --˫�ҿ���־

				  ,PD_GRP_CD                            --��Ʒ��

				  ,PD_SUB_CD                            --��Ʒ�Ӵ���

				  ,BIZ_CGY_TP_ID                        --ҵ�����

				  ,CCY                                  --����

				  ,ACG_DT                               --����YYYY-MM-DD

				  ,CDR_YR                               --���YYYY

				  ,CDR_MTH                              --�·�MM

				  ,ACT_NBR_AC                           --ʵ���˻���

				  ,NBR_EFF_CRD                          --��������

				  ,NBR_UNATVD_CR_CRD                    --δ�������ÿ�����

				  ,NBR_UNATVD_CHGD_CRD                  --�ѻ���δ���ÿ���

				  ,NBR_EXP_CRD                          --���ڿ���

				  ,NBR_DRMT_CRD                         --˯�߿�����

				  ,NBR_CRD_CLECTD                       --���տ���

				  ,NBR_NEW_CRD                          --�¿�����

				  ,NBR_CRD_CLD                          --������

				  ,NBR_NEW_CST                          --�����ͻ���

				  ,NBR_CST_CLD                          --�ͻ�������

				  ,NBR_CRD_CHG                          --������

				  ,NBR_AC_Opened                        --�����˻���

				  ,NBR_AC_CLS                           --�����˻���

				  ,TOT_MTD_NBR_NEW_CRD                  --���ۼ��¿�����        

				  ,TOT_MTD_NBR_CRD_CLD                  --���ۼ�������          

				  ,TOT_MTD_NBR_NEW_CST                  --���ۼ������ͻ���      

				  ,TOT_MTD_NBR_CST_CLD                  --���ۼƿͻ�������      

				  ,TOT_MTD_NBR_CRD_CHG                  --���ۼƻ�����          

				  ,TOT_MTD_NBR_AC_Opened                --���ۼƿ����˻���      

				  ,TOT_MTD_NBR_AC_CLS                   --���ۼ������˻���      

				  ,TOT_MTD_NBR_CRD_CLECTD               --���ۼ����տ���        

				  ,TOT_QTD_NBR_NEW_CRD                  --���ۼ��¿�����        

				  ,TOT_QTD_NBR_CRD_CLD                  --���ۼ�������          

				  ,TOT_QTD_NBR_NEW_CST                  --���ۼ������ͻ���      

				  ,TOT_QTD_NBR_CST_CLD                  --���ۼƿͻ�������      

				  ,TOT_QTD_NBR_CRD_CHG                  --���ۼƻ�����          

				  ,TOT_QTD_NBR_AC_Opened                --���ۼƿ����˻���      

				  ,TOT_QTD_NBR_AC_CLS                   --���ۼ������˻���      

				  ,TOT_QTD_NBR_CRD_CLECTD               --���ۼ����տ���        

				  ,TOT_YTD_NBR_NEW_CRD                  --���ۼ��¿�����        

				  ,TOT_YTD_NBR_CRD_CLD                  --���ۼ�������          

				  ,TOT_YTD_NBR_NEW_CST                  --���ۼ������ͻ���      

				  ,TOT_YTD_NBR_CST_CLD                  --���ۼƿͻ�������      

				  ,TOT_YTD_NBR_CRD_CHG                  --���ۼƻ�����          

				  ,TOT_YTD_NBR_AC_Opened                --���ۼƿ����˻���      

				  ,TOT_YTD_NBR_AC_CLS                   --���ۼ������˻���      

				  ,TOT_YTD_NBR_CRD_CLECTD               --���ۼ����տ���        

				  -----------------------------Start of 20091203---------------------------------

			 		,NBR_CST                              --�ͻ���

					-----------------------------End of 20091203---------------------------------

					 -----------------------------Start of 20091208---------------------------------

					 ,NBR_DRMT_CRD_WITH_LOW_BAL            --���С��10��˯�߿�����

					 ,NBR_DRMT_AC_WITH_LOW_BAL					    --���С��10��˯�߻�����

					 ,NBR_DRMT_AC										      --˯�߻�����

				   -----------------------------End of 20091208---------------------------------								

				)

        SELECT 

          a.OU_ID                                 --������

				  ,a.CRD_TP_ID                            --������

				  ,a.Is_CR_CRD_F                          --�Ƿ�Ϊ���ǿ�

				  ,a.CRD_Brand_TP_Id                      --��Ʒ������

				  ,a.CRD_PRVL_TP_ID                       --������

				  ,a.PSBK_RLTD_F                          --������ر�ʶ

				  ,a.IS_NONGXIN_CRD_F                     --���տ�/ũ�ſ���ʶ

				  ,a.ENT_IDV_IND                          --������

				  ,a.MST_CRD_IND                          --��/������־

				  ,a.NGO_CRD_IND                          --Э�鿨����

				  ,a.MULT_CCY_F                           --˫�ҿ���־

				  ,a.PD_GRP_CD                            --��Ʒ��

				  ,a.PD_SUB_CD                            --��Ʒ�Ӵ���

				  ,a.BIZ_CGY_TP_ID                        --ҵ�����

				  ,a.CCY                                  --����

				  ,a.ACG_DT                               --����YYYY-MM-DD

				  ,a.CDR_YR                               --���YYYY

				  ,a.CDR_MTH                              --�·�MM

				  ,a.ACT_NBR_AC                           --ʵ���˻���

				  ,a.NBR_EFF_CRD                          --��������

				  ,a.NBR_UNATVD_CR_CRD                    --δ�������ÿ�����

				  ,a.NBR_UNATVD_CHGD_CRD                  --�ѻ���δ���ÿ���

				  ,a.NBR_EXP_CRD                          --���ڿ���

				  ,a.NBR_DRMT_CRD                         --˯�߿�����

				  ,a.NBR_CRD_CLECTD                       --���տ���

				  ,a.NBR_NEW_CRD                          --�¿�����

				  ,a.NBR_CRD_CLD                          --������

				  ,a.NBR_NEW_CST                          --�����ͻ���

				  ,a.NBR_CST_CLD                          --�ͻ�������

				  ,a.NBR_CRD_CHG                          --������

				  ,a.NBR_AC_Opened                        --�����˻���

				  ,a.NBR_AC_CLS                           --�����˻���

				  ,a.NBR_NEW_CRD                          --���ۼ��¿�����                    

				  ,a.NBR_CRD_CLD                          --���ۼ�������          

				  ,a.NBR_NEW_CST                          --���ۼ������ͻ���      

				  ,a.NBR_CST_CLD                          --���ۼƿͻ�������      

				  ,a.NBR_CRD_CHG                          --���ۼƻ�����          

				  ,a.NBR_AC_Opened                        --���ۼƿ����˻���      

				  ,a.NBR_AC_CLS                           --���ۼ������˻���      

				  ,a.NBR_CRD_CLECTD                       --���ۼ����տ���        

				  ,COALESCE(b.TOT_QTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)     			--���ۼ��¿�����        																									        

          ,COALESCE(b.TOT_QTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)     				--���ۼ�������          																						

          ,COALESCE(b.TOT_QTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)     				--���ۼ������ͻ���      																						

          ,COALESCE(b.TOT_QTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)     				--���ۼƿͻ�������      																						

          ,COALESCE(b.TOT_QTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)     				--���ۼƻ�����          																						

          ,COALESCE(b.TOT_QTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0) 				--���ۼƿ����˻���  

          --------------------------Start on 20100114----------------------------------------------------    																						

          --,COALESCE(b.TOT_QTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)       				--���ۼ������˻���      																						

          --,COALESCE(b.TOT_QTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)      --���ۼ����տ��� 

          ,COALESCE(b.TOT_QTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)       				--���ۼ������˻���      																						

          ,COALESCE(b.TOT_QTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)      --���ۼ����տ���      

          --------------------------End on 20100114----------------------------------------------------   

				  ,COALESCE(b.TOT_YTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)     			--���ۼ��¿�����        																									        

          ,COALESCE(b.TOT_YTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)     				--���ۼ�������          																						

          ,COALESCE(b.TOT_YTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)     				--���ۼ������ͻ���      																						

          ,COALESCE(b.TOT_YTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)     				--���ۼƿͻ�������      																						

          ,COALESCE(b.TOT_YTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)     				--���ۼƻ�����          																						

          ,COALESCE(b.TOT_YTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0) 				--���ۼƿ����˻��� 

          --------------------------Start on 20100114----------------------------------------------------     																						

          --,COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)       				--���ۼ������˻���      																						

          --,COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)       --���ۼ����տ��� 

          ,COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)       				--���ۼ������˻���      																						

          ,COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)       --���ۼ����տ���              

          --------------------------End on 20100114----------------------------------------------------    

          -----------------------------Start of 20091203---------------------------------

			 		,a.NBR_CST                              --�ͻ���

					-----------------------------Start of 20091203---------------------------------

					 -----------------------------Start of 20091208---------------------------------

					 ,a.NBR_DRMT_CRD_WITH_LOW_BAL            --���С��10��˯�߿�����

					 ,a.NBR_DRMT_AC_WITH_LOW_BAL					    --���С��10��˯�߻�����

					 ,a.NBR_DRMT_AC										      --˯�߻�����

				   -----------------------------End of 20091208---------------------------------			

					

        FROM SESSION.TMP_CRD_PRFL_CRD_DLY_SMY_DB a LEFT JOIN SMY.CRD_PRFL_CRD_DLY_SMY b

        ON a.OU_ID = b.OU_ID AND

           a.CRD_TP_ID = b.CRD_TP_ID AND

    			 a.IS_CR_CRD_F      = b.IS_CR_CRD_F AND

    			 a.CRD_BRAND_TP_ID  = b.CRD_BRAND_TP_ID AND

    			 a.CRD_PRVL_TP_ID   = b.CRD_PRVL_TP_ID AND

    			 a.PSBK_RLTD_F      = b.PSBK_RLTD_F AND

    			 a.IS_NONGXIN_CRD_F = b.IS_NONGXIN_CRD_F AND

    			 a.ENT_IDV_IND      = b.ENT_IDV_IND AND

    			 a.MST_CRD_IND      = b.MST_CRD_IND AND

    			 a.NGO_CRD_IND      = b.NGO_CRD_IND AND

    			 a.MULT_CCY_F       = b.MULT_CCY_F AND

    			 a.PD_GRP_CD        = b.PD_GRP_CD AND

    			 a.PD_SUB_CD        = b.PD_SUB_CD AND

    			 a.BIZ_CGY_TP_ID       = b.BIZ_CGY_TP_ID AND

    			 a.CCY              = b.CCY AND

    			 a.ACG_DT - 1 day   = b.ACG_DT   ;               				  				  				      	--

   END IF;--

ELSE  --���³�

	

  INSERT INTO SMY.CRD_PRFL_CRD_DLY_SMY

    (OU_ID                                --������

	  ,CRD_TP_ID                            --������

	  ,Is_CR_CRD_F                          --�Ƿ�Ϊ���ǿ�

	  ,CRD_Brand_TP_Id                      --��Ʒ������

	  ,CRD_PRVL_TP_ID                       --������

	  ,PSBK_RLTD_F                          --������ر�ʶ

	  ,IS_NONGXIN_CRD_F                     --���տ�/ũ�ſ���ʶ

	  ,ENT_IDV_IND                          --������

	  ,MST_CRD_IND                          --��/������־

	  ,NGO_CRD_IND                          --Э�鿨����

	  ,MULT_CCY_F                           --˫�ҿ���־

	  ,PD_GRP_CD                            --��Ʒ��

	  ,PD_SUB_CD                            --��Ʒ�Ӵ���

	  ,BIZ_CGY_TP_ID                        --ҵ�����

	  ,CCY                                  --����

	  ,ACG_DT                               --����YYYY-MM-DD

	  ,CDR_YR                               --���YYYY

	  ,CDR_MTH                              --�·�MM

	  ,ACT_NBR_AC                           --ʵ���˻���

	  ,NBR_EFF_CRD                          --��������

	  ,NBR_UNATVD_CR_CRD                    --δ�������ÿ�����

	  ,NBR_UNATVD_CHGD_CRD                  --�ѻ���δ���ÿ���

	  ,NBR_EXP_CRD                          --���ڿ���

	  ,NBR_DRMT_CRD                         --˯�߿�����

	  ,NBR_CRD_CLECTD                       --���տ���

	  ,NBR_NEW_CRD                          --�¿�����

	  ,NBR_CRD_CLD                          --������

	  ,NBR_NEW_CST                          --�����ͻ���

	  ,NBR_CST_CLD                          --�ͻ�������

	  ,NBR_CRD_CHG                          --������

	  ,NBR_AC_Opened                        --�����˻���

	  ,NBR_AC_CLS                           --�����˻���

	  ,TOT_MTD_NBR_NEW_CRD                  --�ۼ��¿�����

	  ,TOT_MTD_NBR_CRD_CLD                  --�ۼ�������

	  ,TOT_MTD_NBR_NEW_CST                  --�ۼ������ͻ���

	  ,TOT_MTD_NBR_CST_CLD                  --�ۼƿͻ�������

	  ,TOT_MTD_NBR_CRD_CHG                  --�ۼƻ�����

	  ,TOT_MTD_NBR_AC_Opened                --�ۼƿ����˻���

	  ,TOT_MTD_NBR_AC_CLS                   --�ۼ������˻���

	  ,TOT_MTD_NBR_CRD_CLECTD               --�ۼ����տ���

	  ,TOT_QTD_NBR_NEW_CRD                  --�ۼ��¿�����

	  ,TOT_QTD_NBR_CRD_CLD                  --�ۼ�������

	  ,TOT_QTD_NBR_NEW_CST                  --�ۼ������ͻ���

	  ,TOT_QTD_NBR_CST_CLD                  --�ۼƿͻ�������

	  ,TOT_QTD_NBR_CRD_CHG                  --�ۼƻ�����

	  ,TOT_QTD_NBR_AC_Opened                --�ۼƿ����˻���

	  ,TOT_QTD_NBR_AC_CLS                   --�ۼ������˻���

	  ,TOT_QTD_NBR_CRD_CLECTD               --�ۼ����տ���

	  ,TOT_YTD_NBR_NEW_CRD                  --�ۼ��¿�����

	  ,TOT_YTD_NBR_CRD_CLD                  --�ۼ�������

	  ,TOT_YTD_NBR_NEW_CST                  --�ۼ������ͻ���

	  ,TOT_YTD_NBR_CST_CLD                  --�ۼƿͻ�������

	  ,TOT_YTD_NBR_CRD_CHG                  --�ۼƻ�����

	  ,TOT_YTD_NBR_AC_Opened                --�ۼƿ����˻���

	  ,TOT_YTD_NBR_AC_CLS                   --�ۼ������˻���

	  ,TOT_YTD_NBR_CRD_CLECTD               --�ۼ����տ���

	  -----------------------------Start of 20091203---------------------------------

 		,NBR_CST                              --�ͻ���

		-----------------------------Start of 20091203---------------------------------

		 -----------------------------Start of 20091208---------------------------------

		 ,NBR_DRMT_CRD_WITH_LOW_BAL            --���С��10��˯�߿�����

		 ,NBR_DRMT_AC_WITH_LOW_BAL					    --���С��10��˯�߻�����

		 ,NBR_DRMT_AC										      --˯�߻�����

	   -----------------------------End of 20091208---------------------------------					

  )

  SELECT 

    a.OU_ID                                --������

	  ,a.CRD_TP_ID                            --������

	  ,a.Is_CR_CRD_F                          --�Ƿ�Ϊ���ǿ�

	  ,a.CRD_Brand_TP_Id                      --��Ʒ������

	  ,a.CRD_PRVL_TP_ID                       --������

	  ,a.PSBK_RLTD_F                          --������ر�ʶ

	  ,a.IS_NONGXIN_CRD_F                     --���տ�/ũ�ſ���ʶ

	  ,a.ENT_IDV_IND                          --������

	  ,a.MST_CRD_IND                          --��/������־

	  ,a.NGO_CRD_IND                          --Э�鿨����

	  ,a.MULT_CCY_F                           --˫�ҿ���־

	  ,a.PD_GRP_CD                            --��Ʒ��

	  ,a.PD_SUB_CD                            --��Ʒ�Ӵ���

	  ,a.BIZ_CGY_TP_ID                        --ҵ�����

	  ,a.CCY                                  --����

	  ,a.ACG_DT                               --����YYYY-MM-DD

	  ,a.CDR_YR                               --���YYYY

	  ,a.CDR_MTH                              --�·�MM

	  ,a.ACT_NBR_AC                           --ʵ���˻���

	  ,a.NBR_EFF_CRD                          --��������

	  ,a.NBR_UNATVD_CR_CRD                    --δ�������ÿ�����

	  ,a.NBR_UNATVD_CHGD_CRD                  --�ѻ���δ���ÿ���

	  ,a.NBR_EXP_CRD                          --���ڿ���

	  ,a.NBR_DRMT_CRD                         --˯�߿�����

	  ,a.NBR_CRD_CLECTD                       --���տ���

	  ,a.NBR_NEW_CRD                          --�¿�����

	  ,a.NBR_CRD_CLD                          --������

	  ,a.NBR_NEW_CST                          --�����ͻ���

	  ,a.NBR_CST_CLD                          --�ͻ�������

	  ,a.NBR_CRD_CHG                          --������

	  ,a.NBR_AC_Opened                        --�����˻���

	  ,a.NBR_AC_CLS                           --�����˻���

		,COALESCE(b.TOT_MTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)    	  --���ۼ��¿�����        																											        

    ,COALESCE(b.TOT_MTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)     		--���ۼ�������          																							

    ,COALESCE(b.TOT_MTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)     		--���ۼ������ͻ���      																							

    ,COALESCE(b.TOT_MTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)     		--���ۼƿͻ�������      																							

    ,COALESCE(b.TOT_MTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)     		--���ۼƻ�����          																							

    ,COALESCE(b.TOT_MTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0) 		--���ۼƿ����˻���  

    --------------------------Start on 20100114----------------------------------------------------    																							

    --,COALESCE(b.TOT_MTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)       		--���ۼ������˻���      																							

    --,COALESCE(b.TOT_MTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)  --���ۼ����տ���  

    ,COALESCE(b.TOT_MTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)       		--���ۼ������˻���      																							

    ,COALESCE(b.TOT_MTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)  --���ۼ����տ���        

    --------------------------End on 20100114----------------------------------------------------

		,COALESCE(b.TOT_QTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)     	--���ۼ��¿�����        																												        

    ,COALESCE(b.TOT_QTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)     		--���ۼ�������          																								

    ,COALESCE(b.TOT_QTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)     		--���ۼ������ͻ���      																								

    ,COALESCE(b.TOT_QTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)     		--���ۼƿͻ�������      																								

    ,COALESCE(b.TOT_QTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)     		--���ۼƻ�����          																								

    ,COALESCE(b.TOT_QTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0) 		--���ۼƿ����˻��� 

    --------------------------Start on 20100114----------------------------------------------------      																								

    --,COALESCE(b.TOT_QTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)       		--���ۼ������˻���      																								

    --,COALESCE(b.TOT_QTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)  --���ۼ����տ��� 

    ,COALESCE(b.TOT_QTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)       		--���ۼ������˻���      																								

    ,COALESCE(b.TOT_QTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)  --���ۼ����տ��� 

    --------------------------End on 20100114----------------------------------------------------        

		,COALESCE(b.TOT_YTD_NBR_NEW_CRD,0) +	COALESCE(a.NBR_NEW_CRD,0)     	--���ۼ��¿�����        																												        

    ,COALESCE(b.TOT_YTD_NBR_CRD_CLD,0) + COALESCE(a.NBR_CRD_CLD,0)     		--���ۼ�������          																								

    ,COALESCE(b.TOT_YTD_NBR_NEW_CST,0) + COALESCE(a.NBR_NEW_CST,0)     		--���ۼ������ͻ���      																								

    ,COALESCE(b.TOT_YTD_NBR_CST_CLD,0) + COALESCE(a.NBR_CST_CLD,0)     		--���ۼƿͻ�������      																								

    ,COALESCE(b.TOT_YTD_NBR_CRD_CHG,0) + COALESCE(a.NBR_CRD_CHG,0)     		--���ۼƻ�����          																								

    ,COALESCE(b.TOT_YTD_NBR_AC_Opened,0) + COALESCE(a.NBR_AC_Opened,0) 		--���ۼƿ����˻���   

    --------------------------Start on 20100114----------------------------------------------------    																								

    --,COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(b.NBR_AC_CLS,0)       		--���ۼ������˻���      																								

    --,COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(b.NBR_CRD_CLECTD,0)  --���ۼ����տ���   

    ,COALESCE(b.TOT_YTD_NBR_AC_CLS,0) + COALESCE(a.NBR_AC_CLS,0)       		--���ۼ������˻���      																								

    ,COALESCE(b.TOT_YTD_NBR_CRD_CLECTD,0) + COALESCE(a.NBR_CRD_CLECTD,0)  --���ۼ����տ���      

    --------------------------End on 20100114----------------------------------------------------       

 	  -----------------------------Start of 20091203---------------------------------

 		,a.NBR_CST                              --�ͻ���

		-----------------------------End of 20091203---------------------------------

		 -----------------------------Start of 20091208---------------------------------

		 ,a.NBR_DRMT_CRD_WITH_LOW_BAL            --���С��10��˯�߿�����

		 ,a.NBR_DRMT_AC_WITH_LOW_BAL					    --���С��10��˯�߻�����

		 ,a.NBR_DRMT_AC										      --˯�߻�����

	   -----------------------------End of 20091208---------------------------------					

  FROM SESSION.TMP_CRD_PRFL_CRD_DLY_SMY_DB a LEFT JOIN SMY.CRD_PRFL_CRD_DLY_SMY b

  ON a.OU_ID = b.OU_ID AND

     a.CRD_TP_ID = b.CRD_TP_ID AND

  	 a.IS_CR_CRD_F      = b.IS_CR_CRD_F AND

  	 a.CRD_BRAND_TP_ID  = b.CRD_BRAND_TP_ID AND

  	 a.CRD_PRVL_TP_ID   = b.CRD_PRVL_TP_ID AND

  	 a.PSBK_RLTD_F      = b.PSBK_RLTD_F AND

  	 a.IS_NONGXIN_CRD_F = b.IS_NONGXIN_CRD_F AND

  	 a.ENT_IDV_IND      = b.ENT_IDV_IND AND

  	 a.MST_CRD_IND      = b.MST_CRD_IND AND

  	 a.NGO_CRD_IND      = b.NGO_CRD_IND AND

  	 a.MULT_CCY_F       = b.MULT_CCY_F AND

  	 a.PD_GRP_CD        = b.PD_GRP_CD AND

  	 a.PD_SUB_CD        = b.PD_SUB_CD AND

  	 a.BIZ_CGY_TP_ID       = b.BIZ_CGY_TP_ID AND

  	 a.CCY              = b.CCY AND

  	 a.ACG_DT - 1 day   = b.ACG_DT   ; 	--

	   

END IF  ;                           --

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--

SET SMY_STEPNUM = SMY_STEPNUM+1;--

INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

 

IF DAY(ACCOUNTING_DATE + 1 DAY ) = 1 THEN	

  SET SMY_STEPDESC = '���µ����ݲ����±�';--

  DELETE FROM SMY.CRD_PRFL_CRD_MTHLY_SMY WHERE CDR_YR = CUR_YEAR AND CDR_MTH = CUR_MONTH;--

	INSERT INTO SMY.CRD_PRFL_CRD_MTHLY_SMY

		(OU_ID

		,CRD_TP_ID

		,IS_CR_CRD_F

		,CRD_BRAND_TP_ID

		,CRD_PRVL_TP_ID

		,PSBK_RLTD_F

		,IS_NONGXIN_CRD_F

		,ENT_IDV_IND

		,MST_CRD_IND

		,NGO_CRD_IND

		,MULT_CCY_F

		,PD_GRP_CD

		,PD_SUB_CD

		,BIZ_CGY_TP_ID

		,CCY

		,CDR_YR

		,CDR_MTH

		,ACG_DT

		,ACT_NBR_AC

		,NBR_EFF_CRD

		,NBR_UNATVD_CR_CRD

		,NBR_UNATVD_CHGD_CRD

		,NBR_EXP_CRD

		,NBR_DRMT_CRD

		,NBR_CRD_CLECTD

		,NBR_NEW_CRD

		,NBR_CRD_CLD

		,NBR_NEW_CST

		,NBR_CST_CLD

		,NBR_CRD_CHG

		,NBR_AC_OPENED

		,NBR_AC_CLS

		,TOT_MTD_NBR_NEW_CRD

		,TOT_MTD_NBR_CRD_CLD

		,TOT_MTD_NBR_NEW_CST

		,TOT_MTD_NBR_CST_CLD

		,TOT_MTD_NBR_CRD_CHG

		,TOT_MTD_NBR_AC_OPENED

		,TOT_MTD_NBR_AC_CLS

		,TOT_MTD_NBR_CRD_CLECTD

		,TOT_QTD_NBR_NEW_CRD

		,TOT_QTD_NBR_CRD_CLD

		,TOT_QTD_NBR_NEW_CST

		,TOT_QTD_NBR_CST_CLD

		,TOT_QTD_NBR_CRD_CHG

		,TOT_QTD_NBR_AC_OPENED

		,TOT_QTD_NBR_AC_CLS

		,TOT_QTD_NBR_CRD_CLECTD

		,TOT_YTD_NBR_NEW_CRD

		,TOT_YTD_NBR_CRD_CLD

		,TOT_YTD_NBR_NEW_CST

		,TOT_YTD_NBR_CST_CLD

		,TOT_YTD_NBR_CRD_CHG

		,TOT_YTD_NBR_AC_OPENED

		,TOT_YTD_NBR_AC_CLS

		,TOT_YTD_NBR_CRD_CLECTD		 

	  -----------------------------Start of 20091203---------------------------------

 		,NBR_CST                              --�ͻ���

		-----------------------------Start of 20091203---------------------------------

		 -----------------------------Start of 20091208---------------------------------

		 ,NBR_DRMT_CRD_WITH_LOW_BAL            --���С��10��˯�߿�����

		 ,NBR_DRMT_AC_WITH_LOW_BAL					    --���С��10��˯�߻�����

		 ,NBR_DRMT_AC										      --˯�߻�����

	   -----------------------------End of 20091208---------------------------------					

  )

	SELECT 

		 OU_ID

		,CRD_TP_ID

		,IS_CR_CRD_F

		,CRD_BRAND_TP_ID

		,CRD_PRVL_TP_ID

		,PSBK_RLTD_F

		,IS_NONGXIN_CRD_F

		,ENT_IDV_IND

		,MST_CRD_IND

		,NGO_CRD_IND

		,MULT_CCY_F

		,PD_GRP_CD

		,PD_SUB_CD

		,BIZ_CGY_TP_ID

		,CCY

		,CDR_YR

		,CDR_MTH

		,ACG_DT

		,ACT_NBR_AC

		,NBR_EFF_CRD

		,NBR_UNATVD_CR_CRD

		,NBR_UNATVD_CHGD_CRD

		,NBR_EXP_CRD

		,NBR_DRMT_CRD

		,NBR_CRD_CLECTD

		,NBR_NEW_CRD

		,NBR_CRD_CLD

		,NBR_NEW_CST

		,NBR_CST_CLD

		,NBR_CRD_CHG

		,NBR_AC_OPENED

		,NBR_AC_CLS

		,TOT_MTD_NBR_NEW_CRD

		,TOT_MTD_NBR_CRD_CLD

		,TOT_MTD_NBR_NEW_CST

		,TOT_MTD_NBR_CST_CLD

		,TOT_MTD_NBR_CRD_CHG

		,TOT_MTD_NBR_AC_OPENED

		,TOT_MTD_NBR_AC_CLS

		,TOT_MTD_NBR_CRD_CLECTD

		,TOT_QTD_NBR_NEW_CRD

		,TOT_QTD_NBR_CRD_CLD

		,TOT_QTD_NBR_NEW_CST

		,TOT_QTD_NBR_CST_CLD

		,TOT_QTD_NBR_CRD_CHG

		,TOT_QTD_NBR_AC_OPENED

		,TOT_QTD_NBR_AC_CLS

		,TOT_QTD_NBR_CRD_CLECTD

		,TOT_YTD_NBR_NEW_CRD

		,TOT_YTD_NBR_CRD_CLD

		,TOT_YTD_NBR_NEW_CST

		,TOT_YTD_NBR_CST_CLD

		,TOT_YTD_NBR_CRD_CHG

		,TOT_YTD_NBR_AC_OPENED

		,TOT_YTD_NBR_AC_CLS

		,TOT_YTD_NBR_CRD_CLECTD		

    -----------------------------Start of 20091203---------------------------------

 		,NBR_CST                              --�ͻ���

		-----------------------------Start of 20091203---------------------------------

		 -----------------------------Start of 20091208---------------------------------

		 ,NBR_DRMT_CRD_WITH_LOW_BAL            --���С��10��˯�߿�����

		 ,NBR_DRMT_AC_WITH_LOW_BAL					    --���С��10��˯�߻�����

		 ,NBR_DRMT_AC										      --˯�߻�����

	   -----------------------------End of 20091208---------------------------------					

	FROM SMY.CRD_PRFL_CRD_DLY_SMY WHERE ACG_DT = SMY_DATE

;--



GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--

SET SMY_STEPNUM = SMY_STEPNUM+1;--

INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--





END IF;--



END
@