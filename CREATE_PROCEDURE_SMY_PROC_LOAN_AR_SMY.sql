CREATE PROCEDURE SMY.PROC_LOAN_AR_SMY (IN ACCOUNTING_DATE DATE)

	Dynamic Result Sets 0 

	Modifies SQL Data   

	Called on Null Input  

	Language SQL 

-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2011-05-31   Chen XiaoWen    1��������ʱ��TEMP_LN_CTR_AR_X_SETL_AC_AR��TMP_LN_CTR_AR_X_SETL_AC_AR��TMP_LN_AR_FNC_ST_HIST��TEMP_LN_AR_LCS��������
--                              2��������ʱ��TMP_DMD_DEP_SUB_AR�ķ�����
--                              3���޸�ȡ����ʱ���BUG
--                              4��������ʱ��TMP_LOAN_AR_SMY��������ʱ�ķ��������ݴ���
-- 2011-09-26   Li Shenyu       5. add column GNT_LN_AR_ID
-------------------------------------------------------------------------------
BEGIN
/*�����쳣����ʹ�ñ���*/
DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
DECLARE SMY_STEPNUM INT DEFAULT 1;                     --�����ڲ�λ�ñ��
DECLARE SMY_STEPDESC VARCHAR(100) DEFAULT '';          --�����ڲ�λ������
DECLARE SMY_DATE DATE;                                 --��ʱ���ڱ���
DECLARE SMY_RCOUNT INT;                                --DML������ü�¼��
DECLARE SMY_PROCNM VARCHAR(100);                        --�洢��������
DECLARE EMP_SQL VARCHAR(200);--
---Modified By Wang Youbing On 2010-09-02 Start----
DECLARE MAT_SEG_ID_1 INT DEFAULT 0;--
DECLARE MAT_SEG_ID_2 INT DEFAULT 0;--
DECLARE CNT INT DEFAULT 0;--
---Modified By Wang Youbing On 2010-09-02 End----

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
	SET SMY_SQLCODE = SQLCODE;--
  ROLLBACK;--
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, 0, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
  COMMIT;--
  RESIGNAL;--
END;--
DECLARE CONTINUE HANDLER FOR SQLWARNING
BEGIN
  SET SMY_SQLCODE = SQLCODE;--
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, NULL, CURRENT TIMESTAMP);--
  COMMIT;--
  SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
END;--
/*������ֵ*/

---Modified By Wang Youbing On 2010-09-02 Start----
SELECT MAT_SEG_ID INTO MAT_SEG_ID_1 FROM SMY.MAT_SEG WHERE LOW_VAL=-99999;--
SELECT MAT_SEG_ID INTO MAT_SEG_ID_2 FROM SMY.MAT_SEG WHERE MAX_VAL=99999;--
SELECT MAX(LOW_VAL)-1 INTO CNT FROM SMY.MAT_SEG;--
---Modified By Wang Youbing On 2010-09-02 End----

SET SMY_PROCNM = 'PROC_LOAN_AR_SMY';--
SET SMY_DATE=ACCOUNTING_DATE;--

/*Delete��־��,����SMY_PROCNM=��ǰ�洢��������,SMY_DATE=ACCOUNTING_DATE,�������µ���ʼ��־*/
DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
COMMIT;--

INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, 'PROCEDURE START.', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--


     SET SMY_STEPNUM = SMY_STEPNUM + 1 ; --2
     SET SMY_STEPDESC = 'ɾ��SMY.LOAN_AR_SMY��������' ;--

	   SET EMP_SQL= 'Alter TABLE SMY.LOAN_AR_SMY ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE' ;--
		
		  EXECUTE IMMEDIATE EMP_SQL;       --
      
      COMMIT;--
GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--

INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE ,SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

--------------------Modified By Wang Youbing On 2010-09-17 Start---------
SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
SET SMY_STEPDESC = '������ʱ��TMP_CRD';--
  DECLARE GLOBAL TEMPORARY TABLE TMP_CRD(DMD_DEP_AC_AR_ID char(20),
      AC_AR_ID   char(20) 
      )ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE
      IN TS_USR_TMP32K PARTITIONING KEY(DMD_DEP_AC_AR_ID);--
  INSERT INTO SESSION.TMP_CRD
  SELECT CRD_NO||' ',
         AC_AR_ID
  FROM SOR.CRD;--

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--

INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE ,SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

CREATE INDEX SESSION.IDX_TMP_CRD ON SESSION.TMP_CRD(DMD_DEP_AC_AR_ID);--

SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
SET SMY_STEPDESC = '������ʱ��TMP_DMD_DEP_SUB_AR';--

	DECLARE GLOBAL TEMPORARY TABLE TEMP_LN_CTR_AR_X_SETL_AC_AR
  AS (SELECT a.DMD_DEP_AC_AR_ID,
             a.LN_CTR_AR_ID,
             a.CTR_SEQ_NO
      FROM SOR.LN_CTR_AR_X_SETL_AC_AR A 
      WHERE A.ACG_CGY_TP_ID=15820002 AND A.AR_INO_DRTN_RNG_NBR=1 AND A.DEL_F = 0
      )DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE
      IN TS_USR_TMP32K PARTITIONING KEY(DMD_DEP_AC_AR_ID);--
  INSERT INTO SESSION.TEMP_LN_CTR_AR_X_SETL_AC_AR
  SELECT a.DMD_DEP_AC_AR_ID,
             a.LN_CTR_AR_ID,
             a.CTR_SEQ_NO
  FROM SOR.LN_CTR_AR_X_SETL_AC_AR A 
  WHERE A.ACG_CGY_TP_ID=15820002 AND A.AR_INO_DRTN_RNG_NBR=1 AND A.DEL_F = 0;
  
  CREATE INDEX SESSION.IDX_TMP_SETL_AC_AR ON SESSION.TEMP_LN_CTR_AR_X_SETL_AC_AR(DMD_DEP_AC_AR_ID,LN_CTR_AR_ID,CTR_SEQ_NO);
  
  -- modified by Li Shenyu on 20110926 start --
  DECLARE GLOBAL TEMPORARY TABLE TEMP_LN_CTR_AR_X_SETL_AC_AR_2
  AS (SELECT a.DMD_DEP_AC_AR_ID,
             a.LN_CTR_AR_ID,
             a.CTR_SEQ_NO
      FROM SOR.LN_CTR_AR_X_SETL_AC_AR A 
      WHERE A.ACG_CGY_TP_ID=15820001 AND A.DEL_F = 0
      )DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE
      IN TS_USR_TMP32K PARTITIONING KEY(DMD_DEP_AC_AR_ID);--
  INSERT INTO SESSION.TEMP_LN_CTR_AR_X_SETL_AC_AR_2
  SELECT a.DMD_DEP_AC_AR_ID,
             a.LN_CTR_AR_ID,
             a.CTR_SEQ_NO
  FROM SOR.LN_CTR_AR_X_SETL_AC_AR A 
  WHERE A.ACG_CGY_TP_ID=15820001 AND A.DEL_F = 0;
  
  CREATE INDEX SESSION.IDX_TMP_SETL_AC_AR_2 ON SESSION.TEMP_LN_CTR_AR_X_SETL_AC_AR_2(DMD_DEP_AC_AR_ID,LN_CTR_AR_ID,CTR_SEQ_NO);
  -- modified by Li Shenyu on 20110926 end --
  
	DECLARE GLOBAL TEMPORARY TABLE TMP_DMD_DEP_SUB_AR
  AS (SELECT a.DMD_DEP_AC_AR_ID,
             a.LN_CTR_AR_ID,
             a.CTR_SEQ_NO,
             value(b.DNMN_CCY_ID,'') as DNMN_CCY_ID,
             SUM(coalesce(BAL_AMT, 0.00)) AS BAL_AMT 
      FROM sor.LN_CTR_AR_X_SETL_AC_AR A 
      LEFT JOIN sor.DMD_DEP_SUB_AR b 
      ON a.DMD_DEP_AC_AR_ID = b.DMD_DEP_AR_ID 
      WHERE A.ACG_CGY_TP_ID=15820002 AND A.AR_INO_DRTN_RNG_NBR=1 AND A.DEL_F = 0 
      AND a.DMD_DEP_AC_AR_ID NOT LIKE '622858%' AND a.DMD_DEP_AC_AR_ID NOT LIKE '621058%' 
      GROUP BY  a.DMD_DEP_AC_AR_ID,a.LN_CTR_AR_ID,a.CTR_SEQ_NO,value(b.DNMN_CCY_ID,'')		     
      )DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE
      IN TS_USR_TMP32K PARTITIONING KEY(LN_CTR_AR_ID);--
  
      INSERT INTO SESSION.TMP_DMD_DEP_SUB_AR
      SELECT a.DMD_DEP_AC_AR_ID,
             a.LN_CTR_AR_ID,
             a.CTR_SEQ_NO,
             value(b.DNMN_CCY_ID,'') as DNMN_CCY_ID,
             SUM(coalesce(BAL_AMT, 0.00)) AS BAL_AMT 
      FROM SESSION.TEMP_LN_CTR_AR_X_SETL_AC_AR A 
      LEFT JOIN sor.DMD_DEP_SUB_AR b 
      ON a.DMD_DEP_AC_AR_ID = b.DMD_DEP_AR_ID 
      WHERE a.DMD_DEP_AC_AR_ID NOT LIKE '622858%' AND a.DMD_DEP_AC_AR_ID NOT LIKE '621058%' 
      GROUP BY a.DMD_DEP_AC_AR_ID,a.LN_CTR_AR_ID,a.CTR_SEQ_NO,value(b.DNMN_CCY_ID,'')
      UNION ALL
      SELECT A.DMD_DEP_AC_AR_ID,
             a.LN_CTR_AR_ID,
             a.CTR_SEQ_NO,
             value(b.DNMN_CCY_ID,'') as DNMN_CCY_ID,
             SUM(coalesce(BAL_AMT, 0.00)) AS BAL_AMT 
      FROM SESSION.TEMP_LN_CTR_AR_X_SETL_AC_AR A 
      LEFT JOIN SESSION.TMP_CRD C
      ON A.DMD_DEP_AC_AR_ID=C.DMD_DEP_AC_AR_ID
      LEFT JOIN sor.DMD_DEP_SUB_AR b 
      ON C.AC_AR_ID = b.DMD_DEP_AR_ID
      WHERE a.DMD_DEP_AC_AR_ID LIKE '622858%' OR a.DMD_DEP_AC_AR_ID LIKE '621058%'
      GROUP BY A.DMD_DEP_AC_AR_ID,a.LN_CTR_AR_ID,a.CTR_SEQ_NO,value(b.DNMN_CCY_ID,'');--

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--

INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE ,SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

CREATE INDEX SESSION.IDX_TMP_DMD_DEP_SUB_AR ON SESSION.TMP_DMD_DEP_SUB_AR(LN_CTR_AR_ID,CTR_SEQ_NO,DNMN_CCY_ID);--

--------------------Modified By Wang Youbing On 2010-09-17 End---------

--������������
SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
SET SMY_STEPDESC = '��SOR.DCN_CTR_AR���в�����������';--

DECLARE GLOBAL TEMPORARY TABLE TMP_LN_CTR_AR_X_SETL_AC_AR as
     (SELECT distinct DMD_DEP_AC_AR_ID, LN_CTR_AR_ID ,CTR_SEQ_NO 
      FROM SESSION.TMP_DMD_DEP_SUB_AR
      )DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE
      IN TS_USR_TMP32K PARTITIONING KEY(LN_CTR_AR_ID);
INSERT INTO SESSION.TMP_LN_CTR_AR_X_SETL_AC_AR
SELECT distinct DMD_DEP_AC_AR_ID, LN_CTR_AR_ID ,CTR_SEQ_NO 
      FROM SESSION.TMP_DMD_DEP_SUB_AR;

INSERT INTO SMY.LOAN_AR_SMY
	(LN_AR_ID                      --�����˻�
	 ,CTR_AR_ID                    --�����ͬ
	 ,CTR_ITM_ORDR_ID              --��ͬ���
	 ,LN_CGY_TP_ID                 --ҵ��Ʒ��
	 ,DNMN_CCY_ID                  --����ID
	 ,PRIM_CST_ID                  --�ͻ�����
	 ,CST_NO                       --�ͻ���
	 ,CST_NM                       --�ͻ�����
	 ,CST_CR_RSK_RTG_ID            --�ͻ���������
	 ,ENT_IDV_IND                  --�ͻ�����
	 ,RPRG_OU_IP_ID                --����ID
	 ,ALT_TP_ID                    --���������
	 ,CLT_TP_ID                    --������ʽ
	 ,FND_SRC_DST_TP_ID            --�ʽ���Դ
	 ,LN_TERM_TP_ID                --��������
	 ,ACG_SBJ_ID                   --��Ŀ��
	 ,NML_ACG_SBJ_ID               --�ñʴ����ʱ�Ŀ�Ŀ��
	 ,ORG_Scale_TP_Id              --��ҵ��ģ����
	 ,CNRL_BNK_IDY_CL_ID           --��ҵ���
	 ,LN_PPS_TP_ID                 --������;
	 ,PD_GRP_CODE                  --��Ʒ�����
	 ,PD_SUB_CODE                  --��Ʒ�Ӵ���
	 ,AR_LCS_TP_ID                 --�˻�״̬
	 ,CTR_CGY_ID                   --��ͬ����
	 ,CTR_ST_TP_ID                 --��ͬ״̬
	 ,LN_DRDWN_DT                  --��������
	 ,EST_END_DT                   --Ԥ�Ƶ�����
	 ,TM_MAT_SEG_ID                --������������
	 ,LN_BAL                       --�������
	 ,LN_DRDWN_AMT                 --���Ž��
	 ,AR_FNC_ST_TP_ID              --�ļ���̬
	 ,LN_FR_RSLT_TP_ID             --�弶��̬
	 ,NPERF_FNC_STS_CHG_DT         --�ļ�ת��������
	 ,NPERF_FR_RSLT_CHG_DT         --�弶ת��������
	 ,GNL_LN_AR_ID                 --������������˻�
	 ,DEP_BAL_Of_REPYMT_AC         --������
	 -- modified by Li Shenyu on 20110926 start -- 
	 ,GNT_LN_AR_ID                 --��������ſ��˻�
	 -- modified by Li Shenyu on 20110926 end --
	 ,LN_MGT_RSPL_TP_ID            --���δ���
	 ,FRST_RSPL_IP                 --��һ������
	 ,Area_LVL1_TP_Id              --��������1
	 ,Area_LVL2_TP_Id              --��������2
	 ,Area_LVL3_TP_Id              --��������3
	 ,NEW_ACG_SBJ_ID               --�¿�Ŀ
	 ,LN_INVST_DIRC_TP_ID          --��ҵͶ��
	 ,IDV_EMPR_RGST_TP_ID         --�ͻ�ע������
	 ,ORG_RGST_TP_ID              --�ͻ��Ǽ�ע������
	 ,EFF_CST_DT                  --�ͻ���Ч����
	 ,LN_AR_TP_ID                 --��������
	 ,END_DT                      --��������
	 ,ACG_DT                      --����ʱ��
	 ,AR_OPN_INT_RATE             --ִ������
	 ,PD_UN_CODE            			--��Ʒ�������
	 )

with TMP_DCN_CTR_AR_FNC_ST_HIST_FIVE as
     ( SELECT coalesce(min(EFF_DT),'9999-12-31') as EFF_DT,  DIS_LOAN_AR_ID 
       FROM SOR.DCN_CTR_AR_FNC_ST_HIST 
       WHERE DCN_CTR_AR_FNC_ST_SCM_ID = 13300000 and DCN_CTR_AR_FNC_ST_TP_ID > 13300002
       GROUP BY DIS_LOAN_AR_ID  ),

--------------------Modified By Wang Youbing On 2010-09-17 Start---------
/*
     TMP_LN_CTR_AR_X_SETL_AC_AR as
     ( SELECT max(DMD_DEP_AC_AR_ID) as DMD_DEP_AC_AR_ID, LN_CTR_AR_ID ,CTR_SEQ_NO 
       FROM SOR.LN_CTR_AR_X_SETL_AC_AR
       WHERE ACG_CGY_TP_ID=15820002  AND DEL_F=0
       GROUP BY LN_CTR_AR_ID ,CTR_SEQ_NO ),
     TMP_DMD_DEP_SUB_AR as
     ------------------------------Start on 20100116-------------------------------------------------
     --( SELECT a.LN_CTR_AR_ID,a.CTR_SEQ_NO, SUM(coalesce(BAL_AMT, 0.00)) AS BAL_AMT FROM (
     ( SELECT a.LN_CTR_AR_ID,a.CTR_SEQ_NO,value(b.DNMN_CCY_ID,'') as DNMN_CCY_ID ,SUM(coalesce(BAL_AMT, 0.00)) AS BAL_AMT FROM (
          SELECT max(DMD_DEP_AC_AR_ID) AS DMD_DEP_AC_AR_ID, LN_CTR_AR_ID ,CTR_SEQ_NO
          FROM sor.LN_CTR_AR_X_SETL_AC_AR
          WHERE ACG_CGY_TP_ID=15820002 AND DEL_F = 0
          GROUP BY LN_CTR_AR_ID ,CTR_SEQ_NO ) AS a 
       LEFT JOIN sor.DMD_DEP_SUB_AR b ON a.DMD_DEP_AC_AR_ID = b.DMD_DEP_AR_ID 
       --GROUP BY a.LN_CTR_AR_ID,a.CTR_SEQ_NO)
       GROUP BY a.LN_CTR_AR_ID,a.CTR_SEQ_NO,value(b.DNMN_CCY_ID,'')),       
     ------------------------------End on 20100116-------------------------------------------------       
*/
--------------------Modified By Wang Youbing On 2010-09-17 End---------
     TMP_LN_AR_LCS as (
       select DIS_LOAN_AR_ID , EFF_DT
       from SOR.DCN_CTR_AR_LCS_HIST
       where END_DT = '9999-12-31' 
            and
            ------------Start on 20100108----------------
            --AR_LCS_TP_ID = 13360005  --����
            AR_LCS_TP_ID in ( 13360005  --����
                             ,13360004  --����
                            )
            ------------End on 20100108----------------
     )       
SELECT
	 a.DIS_LOAN_AR_ID                                        --�����˻�                         
	,a.DCN_CTR_AR_ID                                         --�����ͬ                      
	,a.CTR_SEQ_NBR                                           --��ͬ���                      
	,VALUE(d.LN_CGY_TP_ID,-1)                                --ҵ��Ʒ��                      
	,a.DNMN_CCY_ID                    							         --����ID                        
	,a.PRIM_CST_ID                    							         --�ͻ�����                      
	,c.CST_NO                         							         --�ͻ���                        
	,c.CST_NM                         							         --�ͻ�����                      
	,coalesce(c.CST_CR_RSK_RTG_ID, -1)              			   --�ͻ���������                  
	,coalesce(c.ENT_IDV_IND, -1)                             --�ͻ�����                      
	,a.RPRG_OU_IP_ID                                         --����ID                        
	,a.ALT_TP_ID                                             --���������                    
	,coalesce(h.CLT_TP_ID, -1)                               --������ʽ                      
	,coalesce(h.FND_SRC_DST_TP_ID, -1)                       --�ʽ���Դ                      
	-----------------------Start on 20100104-------------------------------------------------
	--,coalesce(h.LOAN_TP_ID, -1)                            --��������                    
	,value(a.LN_TM_TP_ID,-1)                                 --��������   
	-----------------------End on 20100104-------------------------------------------------  
	,a.ACG_SBJ_ID                                            --��Ŀ��                        
	,a.ACG_SBJ_ID                                            --�ñʴ����ʱ�Ŀ�Ŀ��        
	,coalesce(c.ORG_Scale_TP_Id, -1)                         --��ҵ��ģ����                  
	,coalesce(c.CNRL_BNK_IDY_CL_ID, -1)                      --��ҵ���                      
	--,coalesce(h.LN_PPS_TP_ID, -1)                          --������;                      
	,coalesce(a.LN_PPS_TP_ID, -1)                            --������;                      
	,a.PD_GRP_CODE                                           --��Ʒ�����                    
	,a.PD_SUB_CODE                                           --��Ʒ�Ӵ���                    
	,a.AR_LCS_TP_ID                                          --�˻�״̬                      
	,coalesce(d.CTR_CGY_ID, -1)                              --��ͬ����                      
	,coalesce(d.CTR_ST_TP_ID, -1)                            --��ͬ״̬                      
	,a.DCN_DT                                                --��������                      
	,a.DCN_END_DT                                            --Ԥ�Ƶ�����                    
---Modified By Wang Youbing On 2010-09-02 Start----
---    ,VALUE((select MAT_SEG_ID from SMY.MAT_SEG 
---	        where DAYS(a.DCN_END_DT)- DAYS(SMY_DATE) >=LOW_VAL 
---	        and DAYS(a.DCN_END_DT)- DAYS(SMY_DATE) < MAX_VAL ),-1)                    --������������              
  ,(CASE WHEN a.DCN_END_DT<=SMY_DATE THEN MAT_SEG_ID_1
         WHEN a.DCN_END_DT>=SMY_DATE+CNT DAYS THEN MAT_SEG_ID_2
         ELSE z.MAT_SEG_ID END)
---Modified By Wang Youbing On 2010-09-02 End----
	,a.BAL_AMT                                               --�������                      
	,a.BILL_NOTE_TOT_DNMN_AMT                                --���Ž��                      
	,13290001                                                --�ļ���̬                      
	,a.LN_FR_RSLT_TP_ID                                      --�弶��̬                      
	,'9999-12-31'                                            --�ļ�ת��������                
	,b.EFF_DT                                                --�弶ת��������                
	,f.DMD_DEP_AC_AR_ID                                      --������������˻�              
	,j.BAL_AMT                                               --������  
	-- modified by Li Shenyu on 20110926 start --
	,f2.DMD_DEP_AC_AR_ID                                     --��������ſ��˻�
	-- modified by Li Shenyu on 20110926 end --                              
	,coalesce(g.LN_MGT_RSPL_TP_ID, -1)                       --���δ���                      
	,g.FRST_RSPL_IP                                          --��һ������                    
	,a.CST_AREA_LVL1_TP_ID                                   --��������1
	,a.CST_AREA_LVL2_TP_ID                                   --��������2
	,a.CST_AREA_LVL3_TP_ID                                   --��������3
	,VALUE(ACG_MAP.NEW_ACG_SBJ_ID,'')								         --�¿�Ŀ
	,-1                                                      --��ҵͶ��
	,c.IDV_EMPR_RGST_TP_ID                                   --�ͻ�ע������
	,c.ORG_RGST_TP_ID                                        --�ͻ��Ǽ�ע������
	,c.EFF_CST_DT                                            --�ͻ���Ч����
	,-1					                														 --LN_AR_TP_ID ��������
	,value(TMP_LN_AR_LCS.EFF_DT,'9999-12-31')                --��������
  ,(CASE WHEN VALUE(c.ACG_DT,'1900-01-01')>=VALUE(a.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(c.ACG_DT,'1900-01-01')>=VALUE(d.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(c.ACG_DT,'1900-01-01')>=VALUE(g.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(c.ACG_DT,'1900-01-01')>=VALUE(h.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(c.ACG_DT,'1900-01-01')>=VALUE(ACG_MAP.EFF_DT,'1900-01-01')
     AND VALUE(c.ACG_DT,'1900-01-01')>=VALUE(TMP_LN_AR_LCS.EFF_DT,'1900-01-01')
     THEN VALUE(c.ACG_DT,'1900-01-01')
     WHEN VALUE(d.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(a.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(d.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(c.ACG_DT,'1900-01-01')
     AND VALUE(d.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(g.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(d.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(h.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(d.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(ACG_MAP.EFF_DT,'1900-01-01')
     AND VALUE(d.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(TMP_LN_AR_LCS.EFF_DT,'1900-01-01')
     THEN VALUE(d.LAST_ETL_ACG_DT,'1900-01-01')
     WHEN VALUE(g.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(a.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(g.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(c.ACG_DT,'1900-01-01')
     AND VALUE(g.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(d.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(g.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(h.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(g.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(ACG_MAP.EFF_DT,'1900-01-01')
     AND VALUE(g.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(TMP_LN_AR_LCS.EFF_DT,'1900-01-01')
     THEN VALUE(g.LAST_ETL_ACG_DT,'1900-01-01')
     WHEN VALUE(h.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(a.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(h.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(c.ACG_DT,'1900-01-01')
     AND VALUE(h.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(d.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(h.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(g.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(h.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(ACG_MAP.EFF_DT,'1900-01-01')
     AND VALUE(h.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(TMP_LN_AR_LCS.EFF_DT,'1900-01-01')
     THEN VALUE(h.LAST_ETL_ACG_DT,'1900-01-01')
     WHEN VALUE(ACG_MAP.EFF_DT,'1900-01-01')>=VALUE(a.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(ACG_MAP.EFF_DT,'1900-01-01')>=VALUE(c.ACG_DT,'1900-01-01')
     AND VALUE(ACG_MAP.EFF_DT,'1900-01-01')>=VALUE(d.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(ACG_MAP.EFF_DT,'1900-01-01')>=VALUE(g.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(ACG_MAP.EFF_DT,'1900-01-01')>=VALUE(h.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(ACG_MAP.EFF_DT,'1900-01-01')>=VALUE(TMP_LN_AR_LCS.EFF_DT,'1900-01-01')
     THEN VALUE(ACG_MAP.EFF_DT,'1900-01-01')
     WHEN VALUE(TMP_LN_AR_LCS.EFF_DT,'1900-01-01')>=VALUE(a.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(TMP_LN_AR_LCS.EFF_DT,'1900-01-01')>=VALUE(c.ACG_DT,'1900-01-01')
     AND VALUE(TMP_LN_AR_LCS.EFF_DT,'1900-01-01')>=VALUE(d.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(TMP_LN_AR_LCS.EFF_DT,'1900-01-01')>=VALUE(g.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(TMP_LN_AR_LCS.EFF_DT,'1900-01-01')>=VALUE(h.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(TMP_LN_AR_LCS.EFF_DT,'1900-01-01')>=VALUE(ACG_MAP.EFF_DT,'1900-01-01')
     THEN VALUE(TMP_LN_AR_LCS.EFF_DT,'1900-01-01')
     ELSE VALUE(a.LAST_ETL_ACG_DT,'1900-01-01') END)
  ,a.AR_OPN_INT_RATE
  ,a.PD_UN_CODE
FROM SOR.DCN_CTR_AR a LEFT JOIN TMP_DCN_CTR_AR_FNC_ST_HIST_FIVE b ON A.DIS_LOAN_AR_ID = b.DIS_LOAN_AR_ID 
    LEFT JOIN SMY.CST_INF c ON a.PRIM_CST_ID = c.CST_ID 
    left join SOR.CTR_AR d on a.DCN_CTR_AR_ID = d.CTR_AR_ID    
    left join SESSION.TMP_LN_CTR_AR_X_SETL_AC_AR f on a.DCN_CTR_AR_ID = f.LN_CTR_AR_ID and a.CTR_SEQ_NBR = f.CTR_SEQ_NO     
    -- modified by Li Shenyu on 20110926 start --
    left join SESSION.TEMP_LN_CTR_AR_X_SETL_AC_AR_2 f2 on a.DCN_CTR_AR_ID = f2.LN_CTR_AR_ID and a.CTR_SEQ_NBR = f2.CTR_SEQ_NO
    -- modified by Li Shenyu on 20110926 end --
   --------------Start on 2011-01-20----------------------------------------------------  
    --left join SOR.LN_MGT_RSPL g on a.DCN_CTR_AR_ID = g.LN_CTR_NO and g.LN_MGT_RSPL_TP_ID=44460001

-----------Modified By Shi LongChuan On 2011-07-20 Start-------------
    --left join SOR.LN_MGT_RSPL g on a.DCN_CTR_AR_ID = g.LN_CTR_NO and g.LN_MGT_RSPL_TP_ID=44460001 and g.DEL_F = 0
    left join SOR.LN_MGT_RSPL g on a.DCN_CTR_AR_ID = g.LN_CTR_NO and g.DEL_F = 0
-----------Modified By Shi LongChuan On 2011-07-20 End-------------

    --------------Start on 2011-01-20----------------------------------------------------
    left join SOR.LN_CTR_AR h on a.DCN_CTR_AR_ID = h.LN_CTR_AR_ID
    --left join SOR.ORG i on a.PRIM_CST_ID=i.ORG_ID
    ------------------------------------------Start on 20100116----------------------------------------------------
    --left join TMP_DMD_DEP_SUB_AR j on a.DCN_CTR_AR_ID = j.LN_CTR_AR_ID and a.CTR_SEQ_NBR = j.CTR_SEQ_NO
--------------------Modified By Wang Youbing On 2010-09-17 Start---------
    --left join TMP_DMD_DEP_SUB_AR j on a.DCN_CTR_AR_ID = j.LN_CTR_AR_ID and a.CTR_SEQ_NBR = j.CTR_SEQ_NO and a.DNMN_CCY_ID = j.DNMN_CCY_ID
    left join SESSION.TMP_DMD_DEP_SUB_AR j on a.DCN_CTR_AR_ID = j.LN_CTR_AR_ID and a.CTR_SEQ_NBR = j.CTR_SEQ_NO and a.DNMN_CCY_ID = j.DNMN_CCY_ID
--------------------Modified By Wang Youbing On 2010-09-17 End---------
    ------------------------------------------End on 20100116----------------------------------------------------
    left join SOR.ACG_SBJ_CODE_MAPPING ACG_MAP on ACG_MAP.ACG_SBJ_ID = a.ACG_SBJ_ID AND ACG_MAP.END_DT = '9999-12-31'
    left join TMP_LN_AR_LCS on a.DIS_LOAN_AR_ID = TMP_LN_AR_LCS.DIS_LOAN_AR_ID
---Modified By Wang Youbing On 2010-09-02 Start----
    left join SMY.SMY_DT z ON a.DCN_END_DT=z.SMY_DT
---Modified By Wang Youbing On 2010-09-02 End----    
WHERE a.DEL_F <> 1    
;--
GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC , SMY_SQLCODE ,SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
SET SMY_STEPDESC = '��SOR.LOAN_AR�в����������';--

DECLARE GLOBAL TEMPORARY TABLE TMP_LN_AR_FNC_ST_HIST AS (
  SELECT EFF_DT,LN_AR_ID,LN_AR_FNC_ST_TP_ID,LN_AR_FNC_ST_SCM_ID FROM SOR.LN_AR_FNC_ST_HIST
)DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(LN_AR_ID);

INSERT INTO SESSION.TMP_LN_AR_FNC_ST_HIST
SELECT EFF_DT,LN_AR_ID,LN_AR_FNC_ST_TP_ID,LN_AR_FNC_ST_SCM_ID FROM SOR.LN_AR_FNC_ST_HIST WHERE LN_AR_FNC_ST_SCM_ID = 13290000 or LN_AR_FNC_ST_SCM_ID = 13300000;

CREATE INDEX SESSION.ST_HIST_LN_AR_ID ON SESSION.TMP_LN_AR_FNC_ST_HIST(LN_AR_ID);

DECLARE GLOBAL TEMPORARY TABLE TEMP_LN_AR_LCS AS(
  SELECT LN_AR_ID , EFF_DT
  FROM SOR.LN_AR_LCS_HIST
)DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(LN_AR_ID);

INSERT INTO SESSION.TEMP_LN_AR_LCS
SELECT LN_AR_ID , EFF_DT
       FROM SOR.LN_AR_LCS_HIST
       WHERE END_DT = '9999-12-31'
       			 and 
            AR_LCS_TP_ID in ( 13360005  --����
                             ,13360004  --����
                            );
CREATE INDEX SESSION.LN_AR_LCS_LN_AR_ID ON SESSION.TEMP_LN_AR_LCS(LN_AR_ID);

DECLARE GLOBAL TEMPORARY TABLE TMP_LOAN_AR_SMY AS(
  SELECT 
	 a.LN_AR_ID                                             --�����˻�                  
	,a.CTR_AR_ID                                            --�����ͬ                  
	,a.CTR_ITM_ORDR_ID                                      --��ͬ���                  
	,a.DNMN_CCY_ID                                          --����ID                    
	,a.PRIM_CST_ID                                          --�ͻ�����                  
	,a.RPRG_OU_IP_ID                                        --����ID                    
	,a.ALT_TP_ID                                            --���������               
	,coalesce(a.FND_SRC_DST_TP_ID, -1) AS FND_SRC_DST_TP_ID --�ʽ���Դ                  
	,value(a.LN_TM_TP_ID,-1) AS LN_TM_TP_ID                 --��������                  
	,a.ACG_SBJ_ID                                           --��Ŀ��                    
	,a.NML_ACG_SBJ_ID                                       --�ñʴ����ʱ�Ŀ�Ŀ��    
	,coalesce(a.LN_PPS_TP_ID, -1) AS LN_PPS_TP_ID           --������;                  
	,a.PD_GRP_CODE                                          --��Ʒ�����                
	,a.PD_SUB_CODE                                          --��Ʒ�Ӵ���                
	,a.AR_LCS_TP_ID                                         --�˻�״̬                  
	,a.EFF_DT AS LN_DRDWN_DT                                --��������                  
	,a.EST_END_DT                                           --Ԥ�Ƶ�����
  ,a.CTR_ITM_ORDR_ID AS TM_MAT_SEG_ID                     --������������
	,a.BAL_AMT                                              --�������                  
	,a.LN_DRDWN_AMT                                         --���Ž��                  
	,a.AR_FNC_ST_TP_ID                                      --�ļ���̬                  
	,a.LN_FR_RSLT_TP_ID                                     --�弶��̬                  
	,a.EFF_DT AS FOUR_EFF_DT                                --�ļ�ת��������            
	,a.EFF_DT AS FIVE_EFF_DT                                --�弶ת��������            
	,a.CST_AREA_LVL1_TP_ID                                  --��������1                                                                                                                                
	,a.CST_AREA_LVL2_TP_ID                                  --��������2                                                                                                                                
	,a.CST_AREA_LVL3_TP_ID                                  --��������3   
	,a.LN_INVS_DIR_TP_ID                                    --��ҵͶ��
  ,a.LN_TP_ID                                             --��������
  ,value(TMP_LN_AR_LCS.EFF_DT,'9999-12-31') AS EFF_DT     --��������
  ,a.EST_END_DT AS ACG_DT                                 --����ʱ��
  ,a.AR_OPN_INT_RATE                                      --ִ������
  ,a.PD_UN_CODE                                           --��Ʒ�������
FROM 	
  SOR.LOAN_AR a 
  left join SESSION.TEMP_LN_AR_LCS TMP_LN_AR_LCS on a.LN_AR_ID = TMP_LN_AR_LCS.LN_AR_ID
  WHERE a.DEL_F <> 1
)DEFINITION ONLY ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE IN TS_USR_TMP32K PARTITIONING KEY(CTR_AR_ID);

INSERT INTO SESSION.TMP_LOAN_AR_SMY
WITH TMP_LN_AR_FNC_ST_HIST_FOUR AS 
    (SELECT coalesce(min(EFF_DT), '9999-12-31') as FOUR_EFF_DT, LN_AR_ID 
     FROM SESSION.TMP_LN_AR_FNC_ST_HIST
     WHERE LN_AR_FNC_ST_SCM_ID = 13290000 and LN_AR_FNC_ST_TP_ID <> 13290001 
     GROUP BY LN_AR_ID),
TMP_LN_AR_FNC_ST_HIST_FIVE AS
    (SELECT coalesce(min(EFF_DT), '9999-12-31') as FIVE_EFF_DT, LN_AR_ID 
     FROM SESSION.TMP_LN_AR_FNC_ST_HIST
     WHERE LN_AR_FNC_ST_SCM_ID = 13300000 and LN_AR_FNC_ST_TP_ID > 13300002
     GROUP BY LN_AR_ID)
SELECT 
	 a.LN_AR_ID                                   --�����˻�                  
	,a.CTR_AR_ID                                  --�����ͬ                  
	,a.CTR_ITM_ORDR_ID                            --��ͬ���                          
	,a.DNMN_CCY_ID                                --����ID                    
	,a.PRIM_CST_ID                                --�ͻ�����                     
	,a.RPRG_OU_IP_ID                              --����ID                    
	,a.ALT_TP_ID                                  --���������               
	,coalesce(a.FND_SRC_DST_TP_ID, -1)            --�ʽ���Դ                  
	,value(a.LN_TM_TP_ID,-1)                      --��������                  
	,a.ACG_SBJ_ID                                 --��Ŀ��                    
	,a.NML_ACG_SBJ_ID                             --�ñʴ����ʱ�Ŀ�Ŀ��    
	,coalesce(a.LN_PPS_TP_ID, -1)                 --������;                  
	,a.PD_GRP_CODE                                --��Ʒ�����                
	,a.PD_SUB_CODE                                --��Ʒ�Ӵ���                
	,a.AR_LCS_TP_ID                               --�˻�״̬                  
	,a.EFF_DT                                     --��������                  
	,a.EST_END_DT                                 --Ԥ�Ƶ�����                
  ,(CASE WHEN a.EST_END_DT<=SMY_DATE THEN MAT_SEG_ID_1
         WHEN a.EST_END_DT>=SMY_DATE+CNT DAYS THEN MAT_SEG_ID_2
         ELSE z.MAT_SEG_ID END)
	,a.BAL_AMT                                    --�������                  
	,a.LN_DRDWN_AMT                               --���Ž��                  
	,a.AR_FNC_ST_TP_ID                            --�ļ���̬                  
	,a.LN_FR_RSLT_TP_ID                           --�弶��̬                  
	,e.FOUR_EFF_DT                                --�ļ�ת��������            
	,f.FIVE_EFF_DT                                --�弶ת��������            
	,a.CST_AREA_LVL1_TP_ID                        --��������1                                                                                                                                
	,a.CST_AREA_LVL2_TP_ID                        --��������2                                                                                                                                
	,a.CST_AREA_LVL3_TP_ID                        --��������3   
	,a.LN_INVS_DIR_TP_ID                          --��ҵͶ��
  ,a.LN_TP_ID                                   --��������
  ,value(TMP_LN_AR_LCS.EFF_DT,'9999-12-31')     --��������
  ,(CASE WHEN VALUE(a.LAST_ETL_ACG_DT,'1900-01-01')>VALUE(TMP_LN_AR_LCS.EFF_DT,'1900-01-01')
     THEN VALUE(a.LAST_ETL_ACG_DT,'1900-01-01')
     ELSE VALUE(TMP_LN_AR_LCS.EFF_DT,'1900-01-01') END)
  ,a.AR_OPN_INT_RATE
  ,a.PD_UN_CODE
FROM 	
  SOR.LOAN_AR a 
 -- left join sor.IP d on a.PRIM_CST_ID = d.IP_ID
  left join TMP_LN_AR_FNC_ST_HIST_FOUR e on a.LN_AR_ID = e.LN_AR_ID 
  left join TMP_LN_AR_FNC_ST_HIST_FIVE f on a.LN_AR_ID = f.LN_AR_ID   
  left join SESSION.TEMP_LN_AR_LCS TMP_LN_AR_LCS on a.LN_AR_ID = TMP_LN_AR_LCS.LN_AR_ID
  left join SMY.SMY_DT z ON a.EST_END_DT=z.SMY_DT
  WHERE a.DEL_F <> 1
;

INSERT INTO SMY.LOAN_AR_SMY
	(LN_AR_ID                    --�����˻�
	,CTR_AR_ID                    --�����ͬ
	,CTR_ITM_ORDR_ID              --��ͬ���
	,LN_CGY_TP_ID                 --ҵ��Ʒ��
	,DNMN_CCY_ID                  --����ID
	,PRIM_CST_ID                  --�ͻ�����
	,CST_NO                       --�ͻ���
	,CST_NM                       --�ͻ�����
	,CST_CR_RSK_RTG_ID            --�ͻ���������
	,ENT_IDV_IND                  --�ͻ�����
	,RPRG_OU_IP_ID                --����ID
	,ALT_TP_ID                    --���������
	,CLT_TP_ID                    --������ʽ
	,FND_SRC_DST_TP_ID            --�ʽ���Դ
	,LN_TERM_TP_ID                --��������
	,ACG_SBJ_ID                   --��Ŀ��
	,NML_ACG_SBJ_ID               --�ñʴ����ʱ�Ŀ�Ŀ��
	,ORG_Scale_TP_Id              --��ҵ��ģ����
	,CNRL_BNK_IDY_CL_ID           --��ҵ���
	,LN_PPS_TP_ID                 --������;
	,PD_GRP_CODE                  --��Ʒ�����
	,PD_SUB_CODE                  --��Ʒ�Ӵ���
	,AR_LCS_TP_ID                 --�˻�״̬
	,CTR_CGY_ID                   --��ͬ����
	,CTR_ST_TP_ID                 --��ͬ״̬
	,LN_DRDWN_DT                  --��������
	,EST_END_DT                   --Ԥ�Ƶ�����
	,TM_MAT_SEG_ID                --������������
	,LN_BAL                       --�������
	,LN_DRDWN_AMT                 --���Ž��
	,AR_FNC_ST_TP_ID              --�ļ���̬
	,LN_FR_RSLT_TP_ID             --�弶��̬
	,NPERF_FNC_STS_CHG_DT         --�ļ�ת��������
	,NPERF_FR_RSLT_CHG_DT         --�弶ת��������
	,GNL_LN_AR_ID                 --������������˻�
	,DEP_BAL_Of_REPYMT_AC         --������ 
  -- modified by Li Shenyu on 20110926 start -- 
	,GNT_LN_AR_ID                 --��������ſ��˻�
	-- modified by Li Shenyu on 20110926 end --  
	,LN_MGT_RSPL_TP_ID            --���δ���   
	,FRST_RSPL_IP                 --��һ������ 
	,Area_LVL1_TP_Id              --��������1  
	,Area_LVL2_TP_Id              --��������2  
	,Area_LVL3_TP_Id              --��������3  
	,NEW_ACG_SBJ_ID               --�¿�Ŀ
	,LN_INVST_DIRC_TP_ID          --��ҵͶ��    
	,IDV_EMPR_RGST_TP_ID         --�ͻ�ע������
	,ORG_RGST_TP_ID              --�ͻ��Ǽ�ע������
	,EFF_CST_DT                  --�ͻ���Ч����
	,LN_AR_TP_ID                 --��������
	,END_DT                      --��������
	,ACG_DT                      --����ʱ��
	,AR_OPN_INT_RATE             --ִ������
	,PD_UN_CODE            			 --��Ʒ�������
)
SELECT 
	 a.LN_AR_ID                                   --�����˻�                  
	,a.CTR_AR_ID                                  --�����ͬ                  
	,a.CTR_ITM_ORDR_ID                            --��ͬ���                  
	,value(c.LN_CGY_TP_ID,-1)                     --ҵ��Ʒ��                  
	,a.DNMN_CCY_ID                                --����ID                    
	,a.PRIM_CST_ID                                --�ͻ�����                  
	,b.CST_NO                                     --�ͻ���                    
	,b.CST_NM                                     --�ͻ�����                  
	,coalesce(b.CST_CR_RSK_RTG_ID, -1)            --�ͻ���������              
	,coalesce(b.ENT_IDV_IND, -1)                  --�ͻ�����                  
	,a.RPRG_OU_IP_ID                              --����ID                    
	,a.ALT_TP_ID                                  --���������               
	,coalesce(g.CLT_TP_ID, -1)                    --������ʽ                  
	----------------------------Start on 20100107------------------------------------------------
	--,coalesce(g.FND_SRC_DST_TP_ID, -1)          --�ʽ���Դ                  
	,a.FND_SRC_DST_TP_ID            --�ʽ���Դ                  
	----------------------------End on 20100107------------------------------------------------
	----------------------------Start on 20100104------------------------------------------------
	--,coalesce(g.LOAN_TP_ID, -1)                 --��������                  
	,a.LN_TM_TP_ID                                --��������                  
	----------------------------End on 20100104------------------------------------------------
	,a.ACG_SBJ_ID                                 --��Ŀ��                    
	,a.NML_ACG_SBJ_ID                             --�ñʴ����ʱ�Ŀ�Ŀ��    
	,coalesce(b.ORG_Scale_TP_Id, -1)              --��ҵ��ģ����              
	,coalesce(b.CNRL_BNK_IDY_CL_ID, -1)           --��ҵ���                  
	--,coalesce(g.LN_PPS_TP_ID, -1)               --������;                  
	,a.LN_PPS_TP_ID                               --������;                  
	,a.PD_GRP_CODE                                --��Ʒ�����                
	,a.PD_SUB_CODE                                --��Ʒ�Ӵ���                
	,a.AR_LCS_TP_ID                               --�˻�״̬                  
	,coalesce(c.CTR_CGY_ID, -1)                   --��ͬ����                  
	,coalesce(c.CTR_ST_TP_ID, -1)                 --��ͬ״̬                  
	,a.LN_DRDWN_DT                                --��������                  
	,a.EST_END_DT                                 --Ԥ�Ƶ�����                
---Modified By Wang Youbing On 2010-09-02 Start----
--	,VALUE((select MAT_SEG_ID from SMY.MAT_SEG where DAYS(a.EST_END_DT) - DAYS(SMY_DATE) >=LOW_VAL and DAYS(a.EST_END_DT) - DAYS(SMY_DATE) < MAX_VAL ),-1)   --������������                                    
  ,a.TM_MAT_SEG_ID                              --������������
---Modified By Wang Youbing On 2010-09-02 End----
	,a.BAL_AMT                                    --�������                  
	,a.LN_DRDWN_AMT                               --���Ž��                  
	,a.AR_FNC_ST_TP_ID                            --�ļ���̬                  
	,a.LN_FR_RSLT_TP_ID                           --�弶��̬                  
	,a.FOUR_EFF_DT                                --�ļ�ת��������            
	,a.FIVE_EFF_DT                                --�弶ת��������            
	,h.DMD_DEP_AC_AR_ID                           --������������˻�          
	,k.BAL_AMT                                    --������  
  -- modified by Li Shenyu on 20110926 start --
	,h2.DMD_DEP_AC_AR_ID                          --��������ſ��˻�
	-- modified by Li Shenyu on 20110926 end --                              
	,coalesce(i.LN_MGT_RSPL_TP_ID, -1)            --���δ���                    
	,i.FRST_RSPL_IP                               --��һ������                              
	,a.CST_AREA_LVL1_TP_ID                        --��������1                                                                                                                                
	,a.CST_AREA_LVL2_TP_ID                        --��������2                                                                                                                                
	,a.CST_AREA_LVL3_TP_ID                        --��������3   
	,VALUE(ACG_MAP.NEW_ACG_SBJ_ID,'')             --�¿�Ŀ    
	,a.LN_INVS_DIR_TP_ID                          --��ҵͶ��
	,b.IDV_EMPR_RGST_TP_ID                        --�ͻ�ע������
	,b.ORG_RGST_TP_ID                             --�ͻ��Ǽ�ע������
	,b.EFF_CST_DT                                 --�ͻ���Ч����
  ,a.LN_TP_ID                                   --��������
  ,a.EFF_DT                                     --��������
  ,(CASE WHEN VALUE(a.ACG_DT,'1900-01-01')>=VALUE(b.ACG_DT,'1900-01-01')
     AND VALUE(a.ACG_DT,'1900-01-01')>=VALUE(c.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(a.ACG_DT,'1900-01-01')>=VALUE(g.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(a.ACG_DT,'1900-01-01')>=VALUE(i.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(a.ACG_DT,'1900-01-01')>=VALUE(ACG_MAP.EFF_DT,'1900-01-01')
     THEN VALUE(a.ACG_DT,'1900-01-01')
     WHEN VALUE(b.ACG_DT,'1900-01-01')>=VALUE(a.ACG_DT,'1900-01-01')
     AND VALUE(b.ACG_DT,'1900-01-01')>=VALUE(c.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(b.ACG_DT,'1900-01-01')>=VALUE(g.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(b.ACG_DT,'1900-01-01')>=VALUE(i.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(b.ACG_DT,'1900-01-01')>=VALUE(ACG_MAP.EFF_DT,'1900-01-01')
     THEN VALUE(b.ACG_DT,'1900-01-01')
     WHEN VALUE(c.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(a.ACG_DT,'1900-01-01')
     AND VALUE(c.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(b.ACG_DT,'1900-01-01')
     AND VALUE(c.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(g.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(c.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(i.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(c.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(ACG_MAP.EFF_DT,'1900-01-01')
     THEN VALUE(c.LAST_ETL_ACG_DT,'1900-01-01')
     WHEN VALUE(g.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(a.ACG_DT,'1900-01-01')
     AND VALUE(g.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(b.ACG_DT,'1900-01-01')
     AND VALUE(g.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(c.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(g.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(i.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(g.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(ACG_MAP.EFF_DT,'1900-01-01')
     THEN VALUE(g.LAST_ETL_ACG_DT,'1900-01-01')
     WHEN VALUE(i.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(a.ACG_DT,'1900-01-01')
     AND VALUE(i.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(b.ACG_DT,'1900-01-01')
     AND VALUE(i.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(c.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(i.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(g.LAST_ETL_ACG_DT,'1900-01-01')
     AND VALUE(i.LAST_ETL_ACG_DT,'1900-01-01')>=VALUE(ACG_MAP.EFF_DT,'1900-01-01')
     THEN VALUE(i.LAST_ETL_ACG_DT,'1900-01-01')
     ELSE VALUE(ACG_MAP.EFF_DT,'1900-01-01') END)
  ,a.AR_OPN_INT_RATE
  ,a.PD_UN_CODE
FROM 	
  SESSION.TMP_LOAN_AR_SMY a
  left join sor.CTR_AR c on a.CTR_AR_ID = c.CTR_AR_ID 
  left join sor.LN_CTR_AR g on a.CTR_AR_ID = g.LN_CTR_AR_ID
  left join SESSION.TMP_LN_CTR_AR_X_SETL_AC_AR h on a.CTR_AR_ID = h.LN_CTR_AR_ID and a.CTR_ITM_ORDR_ID = h.CTR_SEQ_NO
  -- modified by Li Shenyu on 20110926 start --
  left join SESSION.TEMP_LN_CTR_AR_X_SETL_AC_AR_2 h2 on a.CTR_AR_ID = h2.LN_CTR_AR_ID and a.CTR_ITM_ORDR_ID = h2.CTR_SEQ_NO
  -- modified by Li Shenyu on 20110926 end --
  left join SESSION.TMP_DMD_DEP_SUB_AR k on a.CTR_AR_ID = k.LN_CTR_AR_ID and a.CTR_ITM_ORDR_ID = k.CTR_SEQ_NO and a.DNMN_CCY_ID =k.DNMN_CCY_ID
  ---need to be modified by xuyan on 20091230. A temporary method. 

  --------------Start on 2011-01-20----------------------------------------------------
  --left join sor.LN_MGT_RSPL i on a.CTR_AR_ID = i.LN_CTR_NO and i.LN_MGT_RSPL_TP_ID=44460001 

-----------Modified By Shi LongChuan On 2011-07-20 Start-------------
  --left join sor.LN_MGT_RSPL i on a.CTR_AR_ID = i.LN_CTR_NO and i.LN_MGT_RSPL_TP_ID=44460001 and i.DEL_F = 0
  left join sor.LN_MGT_RSPL i on a.CTR_AR_ID = i.LN_CTR_NO and i.DEL_F = 0
-----------Modified By Shi LongChuan On 2011-07-20 End-------------  

  --------------End on 2011-01-20----------------------------------------------------
 -- left join sor.ORG j on a.PRIM_CST_ID =  j.ORG_ID
  -------------------------------------------Start on 20100116------------------------------------------
  --left join TMP_DMD_DEP_SUB_AR k on a.CTR_AR_ID = k.LN_CTR_AR_ID and a.CTR_ITM_ORDR_ID = k.CTR_SEQ_NO
--------------------Modified By Wang Youbing On 2010-09-17 Start---------
  --left join TMP_DMD_DEP_SUB_AR k on a.CTR_AR_ID = k.LN_CTR_AR_ID and a.CTR_ITM_ORDR_ID = k.CTR_SEQ_NO and a.DNMN_CCY_ID =k.DNMN_CCY_ID
  --left join SESSION.TMP_DMD_DEP_SUB_AR k on a.CTR_AR_ID = k.LN_CTR_AR_ID and a.CTR_ITM_ORDR_ID = k.CTR_SEQ_NO and a.DNMN_CCY_ID =k.DNMN_CCY_ID
--------------------Modified By Wang Youbing On 2010-09-17 Start---------
  -------------------------------------------End on 20100116------------------------------------------
  left join SOR.ACG_SBJ_CODE_MAPPING ACG_MAP on ACG_MAP.ACG_SBJ_ID = a.ACG_SBJ_ID AND ACG_MAP.END_DT = '9999-12-31'
  left join smy.CST_INF b on a.PRIM_CST_ID = b.CST_ID 
;--
GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE ,SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--
END@