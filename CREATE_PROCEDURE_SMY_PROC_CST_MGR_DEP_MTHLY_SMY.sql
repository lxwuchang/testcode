DROP PROCEDURE SMY.PROC_CST_MGR_DEP_MTHLY_SMY@
CREATE PROCEDURE SMY.PROC_CST_MGR_DEP_MTHLY_SMY(ACCOUNTING_DATE DATE)
-------------------------------------------------------------------------------
-- (C) Copyright ZJRCU and IBM <date>
--
-- File name:           SMY.PROC_CST_MGR_DEP_MTHLY_SMY.sql
-- Procedure name: 			SMY.PROC_CST_MGR_DEP_MTHLY_SMY
-- Source Table:				SOR.CST_MGR_PRFT_PCT_INF,SMY.DEP_AR_SMY,SMY.CST_INF,SOR.DB_CRD
-- Target Table: 				SMY.CST_MGR_DEP_MTHLY_SMY
-- Project:             ZJ RCCB EDW
--
-- Purpose:             
--
--=============================================================================
-- Creation Date:       2009.11.13
-- Origin Author:       Wang Youbing
--
-- Version: %1.0%
--
-- Modification History
-- --------------------
--
-- Date         ByPerson        Description
-- ----------   --------------  -----------------------------------------------
-- 2009-10-28   Wang Youbing     Create SP File		
-- 2009-12-04   Xu Yan           Rename the history table
-- 2010-01-15   Xu Yan           Added new columns RSPL_DEP_BAL, RSPL_MTD_ACML_DEP_BAL, RSPL_QTD_ACML_DEP_BAL, RSPL_YTD_ACML_DEP_BAL
-- 2010-08-11   van fuqiao       Fixed a bug for column  'QTR_DAY'  'MONTH_DAY' 'YR_DAY'
-- 2012-07-09   Chen XiaoWen     ���Ӷ���ͨ����ӳ�����
-------------------------------------------------------------------------------
LANGUAGE SQL
BEGIN
/*�����쳣����ʹ�ñ���*/
DECLARE SQLCODE, SMY_SQLCODE INT DEFAULT 0;            --SQLCODE
DECLARE SMY_STEPNUM INT DEFAULT 1;                     --�����ڲ�λ�ñ��
DECLARE SMY_STEPDESC VARCHAR(100) DEFAULT '';          --�����ڲ�λ������
DECLARE SMY_DATE DATE;                                 --��ʱ���ڱ���
DECLARE SMY_RCOUNT INT;                                --DML������ü�¼��
DECLARE SMY_PROCNM VARCHAR(100);                        --�洢��������
DECLARE at_end SMALLINT DEFAULT 0;--
/*�����洢����ʹ�ñ���*/
DECLARE CUR_YEAR SMALLINT;                             --��
DECLARE CUR_MONTH SMALLINT;                            --��
DECLARE CUR_DAY INTEGER;                               --��
DECLARE YR_FIRST_DAY DATE;                             --�����1��1��
DECLARE QTR_FIRST_DAY DATE;                            --�����ȵ�1��
DECLARE MONTH_FIRST_DAY DATE;                          --���µ�1��
DECLARE NEXT_YR_FIRST_DAY DATE;                        --����1��1��
DECLARE NEXT_QTR_FIRST_DAY DATE;                       --�¼��ȵ�1��
DECLARE NEXT_MONTH_FIRST_DAY DATE;                     --���µ�1��
DECLARE MONTH_DAY SMALLINT;                            --��������
DECLARE YR_DAY SMALLINT;                               --��������
DECLARE QTR_DAY SMALLINT;                              --����������
DECLARE MAX_ACG_DT DATE;                               --���������
DECLARE DELETE_SQL VARCHAR(200);                       --ɾ����ʷ��̬SQL

/*1.�������SQL�쳣����ľ��(EXIT��ʽ).
  2.������SQL�쳣ʱ�ڴ洢�����е�λ��(SMY_STEPNUM),λ������(SMY_STEPDESC)��SQLCODE(SMY_SQLCODE)�����SMY_LOG����������.
  3.����RESIGNAL���������쳣,�����洢����ִ����,������SQL�쳣֮ǰ�洢������������ɵĲ������лع�.*/
DECLARE CONTINUE HANDLER FOR NOT FOUND
SET at_end=1;--
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
  SET SMY_STEPNUM = SMY_STEPNUM+1;--
END;--
/*������ֵ*/
SET SMY_PROCNM = 'PROC_CST_MGR_DEP_MTHLY_SMY';--
SET SMY_DATE=ACCOUNTING_DATE;--
SET CUR_YEAR=YEAR(ACCOUNTING_DATE);  --ȡ��ǰ���
SET CUR_MONTH=MONTH(ACCOUNTING_DATE); --ȡ��ǰ�·�
SET CUR_DAY=DAY(ACCOUNTING_DATE);     --ȡ����
SET YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');--
SET NEXT_YR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR+1))||'-01-01');--
       ------------------------Start on 20100811--------------------------
--SET YR_DAY=DAYS(NEXT_YR_FIRST_DAY)-DAYS(YR_FIRST_DAY);--
SET YR_DAY=DAYS(ACCOUNTING_DATE)-DAYS(YR_FIRST_DAY)+1;--����������
       ------------------------End on 20100811--------------------------
IF CUR_MONTH IN (1,2,3) THEN 
   SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-01-01');--
   SET NEXT_QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-04-01');--
ELSEIF CUR_MONTH IN (4,5,6) THEN 
       SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-04-01');--
       SET NEXT_QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-07-01');--
    ELSEIF CUR_MONTH IN (7,8,9) THEN 
           SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-07-01');--
           SET NEXT_QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-10-01');--
        ELSE
            SET QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR))||'-10-01');--
            SET NEXT_QTR_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR+1))||'-01-01');--
END IF;--
       ------------------------Start on 20100811--------------------------
--SET QTR_DAY=DAYS(NEXT_QTR_FIRST_DAY)-DAYS(QTR_FIRST_DAY);--
SET QTR_DAY=DAYS(ACCOUNTING_DATE)-DAYS(QTR_FIRST_DAY)+1;--������������
       ------------------------end on 20100811--------------------------
SET MONTH_FIRST_DAY=DATE(TRIM(CHAR(TRIM(CHAR(CUR_YEAR))||'-'||RIGHT('0'||TRIM(CHAR(CUR_MONTH)),2)||'-01')));--
IF CUR_MONTH=12 THEN
   SET NEXT_MONTH_FIRST_DAY=DATE(TRIM(CHAR(CUR_YEAR+1))||'-01-01');--
ELSE
   SET NEXT_MONTH_FIRST_DAY=DATE(TRIM(CHAR(TRIM(CHAR(CUR_YEAR))||'-'||RIGHT('0'||TRIM(CHAR(CUR_MONTH+1)),2)||'-01')));--
END IF;--
       ------------------------Start on 20100811--------------------------
--SET MONTH_DAY=DAYS(NEXT_MONTH_FIRST_DAY)-DAYS(MONTH_FIRST_DAY);--
SET MONTH_DAY=DAYS(ACCOUNTING_DATE)-DAYS(MONTH_FIRST_DAY)+1;--����������
       ------------------------end on 20100811--------------------------
       
       
SELECT MAX(ACG_DT) INTO MAX_ACG_DT FROM SMY.CST_MGR_DEP_MTHLY_SMY;--
SET DELETE_SQL='ALTER TABLE HIS.CST_MGR_DEP_MTHLY_SMY ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE';--

/*Delete��־��,����SMY_PROCNM=��ǰ�洢��������,SMY_DATE=ACCOUNTING_DATE,�������µ���ʼ��־*/
DELETE FROM SMY.SMY_LOG WHERE SMY_ACT_DT = SMY_DATE AND SMY_PROC_NM = SMY_PROCNM;--
COMMIT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, 'PROCEDURE START.', SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

/*���ݻָ��뱸��*/
IF MAX_ACG_DT=ACCOUNTING_DATE THEN
   DELETE FROM SMY.CST_MGR_DEP_MTHLY_SMY WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;--
   COMMIT;--
   IF CUR_DAY<>1 THEN
      INSERT INTO SMY.CST_MGR_DEP_MTHLY_SMY SELECT * FROM HIS.CST_MGR_DEP_MTHLY_SMY;--
      COMMIT;--
   END IF;--
ELSE
   EXECUTE IMMEDIATE DELETE_SQL;--
   INSERT INTO HIS.CST_MGR_DEP_MTHLY_SMY SELECT * FROM SMY.CST_MGR_DEP_MTHLY_SMY WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;--
   COMMIT;--
END IF;--

SET SMY_STEPNUM = SMY_STEPNUM+1;--
SET SMY_STEPDESC = '����ϵͳ��ʱ��,����ά�Ȼ���,��ʱ��ŵ��ս�������.';--

DECLARE GLOBAL TEMPORARY TABLE TMP(CST_MGR_ID CHAR(18),
                                   ENT_IDV_IND INTEGER,
                                   DEP_TP_ID INTEGER,
                                   AC_OU_ID CHAR(18),
                                   CCY CHAR(3),
                                   CDR_YR SMALLINT,
                                   CDR_MTH SMALLINT,
                                   NOCLD_In_MTH SMALLINT,
                                   NOCLD_In_QTR SMALLINT,
                                   NOCLD_In_Year SMALLINT,
                                   NOD_CUR_DAY SMALLINT,
                                   ACG_DT DATE,
                                   OU_ID CHAR(18),
                                   NBR_CST INTEGER,
                                   NBR_AC INTEGER,
                                   DEP_BAL DECIMAL(17,2),
                                   NBR_NEW_CST INTEGER,
                                   NBR_NEW_AC INTEGER
                                   -------------Start on 20100115-----------
                                   ,RSPL_DEP_BAL DECIMAL(17,2)
                                   -------------End on 20100115-----------
                                   ) 
ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE PARTITIONING KEY(CST_MGR_ID);--

INSERT INTO SESSION.TMP
with tmp0 as(select A.CR_MGR_ID as CST_MGR_ID,
                    coalesce(B.ENT_IDV_IND,-1) as ENT_IDV_IND,
                    coalesce(B.DEP_TP_ID,-1) as DEP_TP_ID,
                    coalesce(B.RPRG_OU_IP_ID,' ') as AC_OU_ID,
                    coalesce(B.DNMN_CCY_ID,' ') as CCY,
                    CUR_YEAR as CDR_YR,
                    CUR_MONTH as CDR_MTH,
                    MONTH_DAY as NOCLD_In_MTH,
                    QTR_DAY as NOCLD_In_QTR,
                    YR_DAY as NOCLD_In_Year,
                    1 as NOD_CUR_DAY,              --������Ч
                    ACCOUNTING_DATE as ACG_DT,
                    coalesce(D.RPT_OU_IP_ID,' ') as OU_ID,
                    B.CST_ID,
                    B.DEP_AR_ID,
                    coalesce(B.BAL_AMT,0) as BAL_AMT,
                    C.EFF_CST_DT
                    -------------Start on 20100115--------------------------------------
                   ,coalesce(B.BAL_AMT,0)*value(PCT_EACH_PCP_SHR,0)/100 as RSPL_DEP_BAL
                   -------------End on 20100115--------------------------------------
             from SOR.CST_MGR_PRFT_PCT_INF A
             left join SMY.DEP_AR_SMY B
             on A.AC_NO=B.DEP_AR_ID
             left join SMY.CST_INF C
             on B.CST_ID=C.CST_ID
             left join SOR.TELLER D
             on A.CR_MGR_ID=D.TELLER_ID
             where A.DEL_F=0 
             and AC_AR_TP_ID=15840002),
tmp1 as (select CST_MGR_ID,
                ENT_IDV_IND,
                DEP_TP_ID,
                AC_OU_ID,
                CCY,
                CDR_YR,
                CDR_MTH,
                NOCLD_In_MTH,
                NOCLD_In_QTR,
                NOCLD_In_Year,
                NOD_CUR_DAY,
                ACG_DT,
                OU_ID,
                sum(NBR_CST) NBR_CST,                    --�ͻ���
                sum(NBR_AC) NBR_AC,                      --�˻���
                sum(DEP_BAL) DEP_BAL,                    --������
                sum(NBR_NEW_CST) NBR_NEW_CST,            --�����ͻ���
                sum(NBR_NEW_AC)  NBR_NEW_AC              --�����˻���
                ,sum(RSPL_DEP_BAL) RSPL_DEP_BAL     --���������α�
         from ((select CST_MGR_ID,
                       ENT_IDV_IND,
                       DEP_TP_ID,
                       AC_OU_ID,
                       CCY,
                       CDR_YR,
                       CDR_MTH,
                       NOCLD_In_MTH,
                       NOCLD_In_QTR,
                       NOCLD_In_Year,
                       NOD_CUR_DAY,
                       ACG_DT,
                       OU_ID,
                       count(distinct CST_ID) NBR_CST,     --�ͻ���
                       count(distinct DEP_AR_ID)  NBR_AC,  --�˻���
                       sum(BAL_AMT)  DEP_BAL,              --������
                       0 NBR_NEW_CST,                      --�����ͻ���
                       0 NBR_NEW_AC                        --�����˻���
                       -------------Start on 20100115--------------------------------------
                       ,sum(RSPL_DEP_BAL) RSPL_DEP_BAL     --���������α�
                       -------------End on 20100115--------------------------------------
                from tmp0
                group by CST_MGR_ID,
                         ENT_IDV_IND,
                         DEP_TP_ID,
                         AC_OU_ID,
                         CCY,
                         CDR_YR,
                         CDR_MTH,
                         NOCLD_In_MTH,
                         NOCLD_In_QTR,
                         NOCLD_In_Year,
                         NOD_CUR_DAY,
                         ACG_DT,
                         OU_ID)
              union all
              (select CST_MGR_ID,
                      ENT_IDV_IND,
                      DEP_TP_ID,
                      AC_OU_ID,
                      CCY,
                      CDR_YR,
                      CDR_MTH,
                      NOCLD_In_MTH,
                      NOCLD_In_QTR,
                      NOCLD_In_Year,
                      NOD_CUR_DAY,
                      ACG_DT,
                      OU_ID,
                      0 NBR_CST,                                   --�ͻ���
                      0 NBR_AC,                                    --�˻���
                      0 DEP_BAL,                                   --������
                      count(distinct CST_ID) NBR_NEW_CST,          --�����ͻ���
                      count(distinct DEP_AR_ID) NBR_NEW_AC         --�����˻���
                      -------------Start on 20100115--------------------------------------
                      ,0 RSPL_DEP_BAL     --���������α�
                      -------------End on 20100115-------------------------------------- 
              from tmp0
              where EFF_CST_DT=ACCOUNTING_DATE
              group by CST_MGR_ID,
                       ENT_IDV_IND,
                       DEP_TP_ID,
                       AC_OU_ID,
                       CCY,
                       CDR_YR,
                       CDR_MTH,
                       NOCLD_In_MTH,
                       NOCLD_In_QTR,
                       NOCLD_In_Year,
                       NOD_CUR_DAY,
                       ACG_DT,
                       OU_ID)) M
              group by CST_MGR_ID,
                       ENT_IDV_IND,
                       DEP_TP_ID,
                       AC_OU_ID,
                       CCY,
                       CDR_YR,
                       CDR_MTH,
                       NOCLD_In_MTH,
                       NOCLD_In_QTR,
                       NOCLD_In_Year,
                       NOD_CUR_DAY,
                       ACG_DT,
                       OU_ID),
tmp2 as(select A.CR_MGR_ID as CST_MGR_ID,
               coalesce(B.ENT_IDV_IND,-1) as ENT_IDV_IND,
               coalesce(B.DEP_TP_ID,-1) as DEP_TP_ID,
               coalesce(B.RPRG_OU_IP_ID,' ') as AC_OU_ID,
               coalesce(B.DNMN_CCY_ID,' ') as CCY,
               CUR_YEAR as CDR_YR,
               CUR_MONTH as CDR_MTH,
               MONTH_DAY as NOCLD_In_MTH,
               QTR_DAY as NOCLD_In_QTR,
               YR_DAY as NOCLD_In_Year,
               1 as NOD_CUR_DAY,              --������Ч
               ACCOUNTING_DATE as ACG_DT,
               coalesce(D.RPT_OU_IP_ID,' ') as OU_ID,
               B.CST_ID,
               B.DEP_AR_ID,
               coalesce(B.BAL_AMT,0) BAL_AMT,
               C.EFF_CST_DT
               -------------Start on 20100115--------------------------------------
               ,coalesce(B.BAL_AMT,0)*value(A.PCT_EACH_PCP_SHR,0)/100 RSPL_DEP_BAL     --���������α�
               -------------End on 20100115--------------------------------------
        from SOR.CST_MGR_PRFT_PCT_INF A
        left join (SELECT T.DB_CRD_NO,S.ENT_IDV_IND,S.DEP_TP_ID,S.RPRG_OU_IP_ID, S.DNMN_CCY_ID,S.CST_ID,S.DEP_AR_ID,S.BAL_AMT
                   FROM SMY.DEP_AR_SMY S
                   INNER JOIN SOR.DB_CRD T 
                   ON S.DEP_AR_ID=T.DB_CRD_AC_AR_ID AND S.DNMN_CCY_ID=T.PRIM_CCY_ID)B
        on A.AC_NO=B.DB_CRD_NO
        left join SMY.CST_INF C
        on B.CST_ID=C.CST_ID
        left join SOR.TELLER D
        on A.CR_MGR_ID=D.TELLER_ID
        where A.DEL_F=0
        and AC_AR_TP_ID=15840003),
tmp3 as (select CST_MGR_ID,
                ENT_IDV_IND,
                DEP_TP_ID,
                AC_OU_ID,
                CCY,
                CDR_YR,
                CDR_MTH,
                NOCLD_In_MTH,
                NOCLD_In_QTR,
                NOCLD_In_Year,
                NOD_CUR_DAY,
                ACG_DT,
                OU_ID,
                sum(NBR_CST) NBR_CST,                           --�ͻ���
                sum(NBR_AC) NBR_AC,                             --�˻���
                sum(DEP_BAL) DEP_BAL,                           --������
                sum(NBR_NEW_CST) NBR_NEW_CST,                   --�����ͻ���
                sum(NBR_NEW_AC)  NBR_NEW_AC                     --�����˻���
                ,sum(RSPL_DEP_BAL) RSPL_DEP_BAL     --���������α�
         from ((select CST_MGR_ID,
                       ENT_IDV_IND,
                       DEP_TP_ID,
                       AC_OU_ID,
                       CCY,
                       CDR_YR,
                       CDR_MTH,
                       NOCLD_In_MTH,
                       NOCLD_In_QTR,
                       NOCLD_In_Year,
                       NOD_CUR_DAY,
                       ACG_DT,
                       OU_ID,
                       count(distinct CST_ID) NBR_CST,     --�ͻ���
                       count(distinct DEP_AR_ID)  NBR_AC,  --�˻���
                       sum(BAL_AMT)  DEP_BAL,              --������
                       0 NBR_NEW_CST,                      --�����ͻ���
                       0 NBR_NEW_AC                        --�����˻���
                       -------------Start on 20100115--------------------------------------
                       ,sum(RSPL_DEP_BAL) RSPL_DEP_BAL     --���������α�
                       -------------End on 20100115--------------------------------------                       
                from tmp2
                group by CST_MGR_ID,
                         ENT_IDV_IND,
                         DEP_TP_ID,
                         AC_OU_ID,
                         CCY,
                         CDR_YR,
                         CDR_MTH,
                         NOCLD_In_MTH,
                         NOCLD_In_QTR,
                         NOCLD_In_Year,
                         NOD_CUR_DAY,
                         ACG_DT,
                         OU_ID)
               union all
                (select CST_MGR_ID,
                        ENT_IDV_IND,
                        DEP_TP_ID,
                        AC_OU_ID,
                        CCY,
                        CDR_YR,
                        CDR_MTH,
                        NOCLD_In_MTH,
                        NOCLD_In_QTR,
                        NOCLD_In_Year,
                        NOD_CUR_DAY,
                        ACG_DT,
                        OU_ID,
                        0 NBR_CST,                                    --�ͻ���
                        0 NBR_AC,                                     --�˻���
                        0 DEP_BAL,                                    --������
                        count(distinct CST_ID) NBR_NEW_CST,           --�����ͻ���
                        count(distinct DEP_AR_ID) NBR_NEW_AC          --�����˻���
                       -------------Start on 20100115--------------------------------------
                       ,0 RSPL_DEP_BAL     --���������α�
                       -------------End on 20100115--------------------------------------                        
                from tmp2
                where EFF_CST_DT=ACCOUNTING_DATE
                group by CST_MGR_ID,
                         ENT_IDV_IND,
                         DEP_TP_ID,
                         AC_OU_ID,
                         CCY,
                         CDR_YR,
                         CDR_MTH,
                         NOCLD_In_MTH,
                         NOCLD_In_QTR,
                         NOCLD_In_Year,
                         NOD_CUR_DAY,
                         ACG_DT,
                         OU_ID)) M
                group by CST_MGR_ID,
                         ENT_IDV_IND,
                         DEP_TP_ID,
                         AC_OU_ID,
                         CCY,
                         CDR_YR,
                         CDR_MTH,
                         NOCLD_In_MTH,
                         NOCLD_In_QTR,
                         NOCLD_In_Year,
                         NOD_CUR_DAY,
                         ACG_DT,
                         OU_ID),
tmp4 as (
    select 
        A.CR_MGR_ID as CST_MGR_ID,
        coalesce(B.ENT_IDV_IND,-1) as ENT_IDV_IND,
        21200015 as DEP_TP_ID,
        coalesce(B.RPRG_OU_IP_ID,' ') as AC_OU_ID,
        B.DNMN_CCY_ID as CCY,
        CUR_YEAR as CDR_YR,
        CUR_MONTH as CDR_MTH,
        MONTH_DAY as NOCLD_In_MTH,
        QTR_DAY as NOCLD_In_QTR,
        YR_DAY as NOCLD_In_Year,
        1 as NOD_CUR_DAY,              --������Ч
        ACCOUNTING_DATE as ACG_DT,
        coalesce(D.RPT_OU_IP_ID,' ') as OU_ID,
        B.CST_ID,
        FIX.AC_AR_ID as DEP_AR_ID,
        (COALESCE(VIRT.BAL_AMT,0) - COALESCE(B.BAL_AMT,0)) as BAL_AMT,
        C.EFF_CST_DT,
        (COALESCE(VIRT.BAL_AMT,0) - COALESCE(B.BAL_AMT,0)) * value(A.PCT_EACH_PCP_SHR,0)/100 as RSPL_DEP_BAL
    from SOR.CST_MGR_PRFT_PCT_INF A
    inner join SMY.DEP_AR_SMY B on A.AC_NO=B.DEP_AR_ID
    inner join SOR.FIX_SUB_CTR_INFO FIX ON B.DEP_AR_ID=FIX.AC_AR_ID AND B.DNMN_CCY_ID=FIX.CCY_ID AND B.CSH_RMIT_IND_TP_ID=FIX.CSH_RMIT_IND_TP_ID 
    inner join SOR.VIRT_MST_AR_BAL_INF VIRT ON FIX.VIRT_MST_AR_ID=VIRT.VIRT_MST_AR_ID AND FIX.CCY_ID=VIRT.DNMN_CCY_ID AND FIX.CSH_RMIT_IND_TP_ID=VIRT.CSH_RMIT_IND_TP_ID
    left join SMY.CST_INF C on B.CST_ID=C.CST_ID
    left join SOR.TELLER D on A.CR_MGR_ID = D.TELLER_ID
    WHERE A.DEL_F=0 and A.AC_AR_TP_ID=15840002 and B.DEP_TP_ID=21200001 AND FIX.CTR_ST_TP_ID<>17720001 AND FIX.DEL_F=0 AND VIRT.DEL_F=0),
tmp5 as (
    select
        CST_MGR_ID,
        ENT_IDV_IND,
        DEP_TP_ID,
        AC_OU_ID,
        CCY,
        CDR_YR,
        CDR_MTH,
        NOCLD_In_MTH,
        NOCLD_In_QTR,
        NOCLD_In_Year,
        NOD_CUR_DAY,
        ACG_DT,
        OU_ID,
        sum(NBR_CST) NBR_CST,                    --�ͻ���
        sum(NBR_AC) NBR_AC,                      --�˻���
        sum(DEP_BAL) DEP_BAL,                    --������
        sum(NBR_NEW_CST) NBR_NEW_CST,            --�����ͻ���
        sum(NBR_NEW_AC)  NBR_NEW_AC,             --�����˻���
        sum(RSPL_DEP_BAL) RSPL_DEP_BAL           --���������α�
    from (
        (select 
            CST_MGR_ID,
            ENT_IDV_IND,
            DEP_TP_ID,
            AC_OU_ID,
            CCY,
            CDR_YR,
            CDR_MTH,
            NOCLD_In_MTH,
            NOCLD_In_QTR,
            NOCLD_In_Year,
            NOD_CUR_DAY,
            ACG_DT,
            OU_ID,
            count(distinct CST_ID) NBR_CST,     --�ͻ���
            count(distinct DEP_AR_ID)  NBR_AC,  --�˻���
            sum(BAL_AMT)  DEP_BAL,              --������
            0 NBR_NEW_CST,                      --�����ͻ���
            0 NBR_NEW_AC,                       --�����˻���
            sum(RSPL_DEP_BAL) RSPL_DEP_BAL      --���������α�
        from tmp4
        group by 
            CST_MGR_ID,
            ENT_IDV_IND,
            DEP_TP_ID,
            AC_OU_ID,
            CCY,
            CDR_YR,
            CDR_MTH,
            NOCLD_In_MTH,
            NOCLD_In_QTR,
            NOCLD_In_Year,
            NOD_CUR_DAY,
            ACG_DT,
            OU_ID)
        union all
        (select 
            CST_MGR_ID,
            ENT_IDV_IND,
            DEP_TP_ID,
            AC_OU_ID,
            CCY,
            CDR_YR,
            CDR_MTH,
            NOCLD_In_MTH,
            NOCLD_In_QTR,
            NOCLD_In_Year,
            NOD_CUR_DAY,
            ACG_DT,
            OU_ID,
            0 NBR_CST,                                   --�ͻ���
            0 NBR_AC,                                    --�˻���
            0 DEP_BAL,                                   --������
            count(distinct CST_ID) NBR_NEW_CST,          --�����ͻ���
            count(distinct DEP_AR_ID) NBR_NEW_AC,        --�����˻���
            0 RSPL_DEP_BAL                               --���������α�
        from tmp4
        where EFF_CST_DT=ACCOUNTING_DATE
        group by 
            CST_MGR_ID,
            ENT_IDV_IND,
            DEP_TP_ID,
            AC_OU_ID,
            CCY,
            CDR_YR,
            CDR_MTH,
            NOCLD_In_MTH,
            NOCLD_In_QTR,
            NOCLD_In_Year,
            NOD_CUR_DAY,
            ACG_DT,
            OU_ID)
    ) M
    group by 
        CST_MGR_ID,
        ENT_IDV_IND,
        DEP_TP_ID,
        AC_OU_ID,
        CCY,
        CDR_YR,
        CDR_MTH,
        NOCLD_In_MTH,
        NOCLD_In_QTR,
        NOCLD_In_Year,
        NOD_CUR_DAY,
        ACG_DT,
        OU_ID),
tmp6 as (
    select
        A.CR_MGR_ID as CST_MGR_ID,
        coalesce(B.ENT_IDV_IND,-1) as ENT_IDV_IND,
        21200015 as DEP_TP_ID,
        coalesce(B.RPRG_OU_IP_ID,' ') as AC_OU_ID,
        B.DNMN_CCY_ID as CCY,
        CUR_YEAR as CDR_YR,
        CUR_MONTH as CDR_MTH,
        MONTH_DAY as NOCLD_In_MTH,
        QTR_DAY as NOCLD_In_QTR,
        YR_DAY as NOCLD_In_Year,
        1 as NOD_CUR_DAY,              --������Ч
        ACCOUNTING_DATE as ACG_DT,
        coalesce(D.RPT_OU_IP_ID,' ') as OU_ID,
        B.CST_ID,
        FIX.AC_AR_ID as DEP_AR_ID,
        (COALESCE(VIRT.BAL_AMT,0) - COALESCE(B.BAL_AMT,0)) as BAL_AMT,
        C.EFF_CST_DT,
        (COALESCE(VIRT.BAL_AMT,0) - COALESCE(B.BAL_AMT,0)) * value(A.PCT_EACH_PCP_SHR,0)/100 as RSPL_DEP_BAL
    from SOR.CST_MGR_PRFT_PCT_INF A
    inner join SOR.DB_CRD DB_CRD on A.AC_NO=DB_CRD.DB_CRD_NO 
    inner join SMY.DEP_AR_SMY B on B.DEP_AR_ID=DB_CRD.DB_CRD_AC_AR_ID and B.DNMN_CCY_ID=DB_CRD.PRIM_CCY_ID
    inner join SOR.FIX_SUB_CTR_INFO FIX ON B.DEP_AR_ID=FIX.AC_AR_ID AND B.DNMN_CCY_ID=FIX.CCY_ID AND B.CSH_RMIT_IND_TP_ID=FIX.CSH_RMIT_IND_TP_ID 
    inner join SOR.VIRT_MST_AR_BAL_INF VIRT ON FIX.VIRT_MST_AR_ID=VIRT.VIRT_MST_AR_ID AND FIX.CCY_ID=VIRT.DNMN_CCY_ID AND FIX.CSH_RMIT_IND_TP_ID=VIRT.CSH_RMIT_IND_TP_ID
    left join SMY.CST_INF C on A.PRIM_CST_ID=C.CST_ID 
    left join SOR.TELLER D on A.CR_MGR_ID = D.TELLER_ID 
    WHERE A.DEL_F=0 and A.AC_AR_TP_ID=15840003 and B.DEP_TP_ID=21200001 AND FIX.CTR_ST_TP_ID<>17720001 AND FIX.DEL_F=0 AND VIRT.DEL_F=0),
tmp7 as (
    select
        CST_MGR_ID,
        ENT_IDV_IND,
        DEP_TP_ID,
        AC_OU_ID,
        CCY,
        CDR_YR,
        CDR_MTH,
        NOCLD_In_MTH,
        NOCLD_In_QTR,
        NOCLD_In_Year,
        NOD_CUR_DAY,
        ACG_DT,
        OU_ID,
        sum(NBR_CST) NBR_CST,                           --�ͻ���
        sum(NBR_AC) NBR_AC,                             --�˻���
        sum(DEP_BAL) DEP_BAL,                           --������
        sum(NBR_NEW_CST) NBR_NEW_CST,                   --�����ͻ���
        sum(NBR_NEW_AC)  NBR_NEW_AC,                    --�����˻���
        sum(RSPL_DEP_BAL) RSPL_DEP_BAL                  --���������α�
    from (
        (select 
            CST_MGR_ID,
            ENT_IDV_IND,
            DEP_TP_ID,
            AC_OU_ID,
            CCY,
            CDR_YR,
            CDR_MTH,
            NOCLD_In_MTH,
            NOCLD_In_QTR,
            NOCLD_In_Year,
            NOD_CUR_DAY,
            ACG_DT,
            OU_ID,
            count(distinct CST_ID) NBR_CST,     --�ͻ���
            count(distinct DEP_AR_ID)  NBR_AC,  --�˻���
            sum(BAL_AMT)  DEP_BAL,              --������
            0 NBR_NEW_CST,                      --�����ͻ���
            0 NBR_NEW_AC,                       --�����˻���
            sum(RSPL_DEP_BAL) RSPL_DEP_BAL      --���������α�
        from tmp6
        group by 
            CST_MGR_ID,
            ENT_IDV_IND,
            DEP_TP_ID,
            AC_OU_ID,
            CCY,
            CDR_YR,
            CDR_MTH,
            NOCLD_In_MTH,
            NOCLD_In_QTR,
            NOCLD_In_Year,
            NOD_CUR_DAY,
            ACG_DT,
            OU_ID)
        union all
        (select 
            CST_MGR_ID,
            ENT_IDV_IND,
            DEP_TP_ID,
            AC_OU_ID,
            CCY,
            CDR_YR,
            CDR_MTH,
            NOCLD_In_MTH,
            NOCLD_In_QTR,
            NOCLD_In_Year,
            NOD_CUR_DAY,
            ACG_DT,
            OU_ID,
            0 NBR_CST,                                    --�ͻ���
            0 NBR_AC,                                     --�˻���
            0 DEP_BAL,                                    --������
            count(distinct CST_ID) NBR_NEW_CST,           --�����ͻ���
            count(distinct DEP_AR_ID) NBR_NEW_AC,         --�����˻���
            0 RSPL_DEP_BAL                                --���������α�
        from tmp6
        where EFF_CST_DT=ACCOUNTING_DATE
        group by 
            CST_MGR_ID,
            ENT_IDV_IND,
            DEP_TP_ID,
            AC_OU_ID,
            CCY,
            CDR_YR,
            CDR_MTH,
            NOCLD_In_MTH,
            NOCLD_In_QTR,
            NOCLD_In_Year,
            NOD_CUR_DAY,
            ACG_DT,
            OU_ID)
        ) M
    group by 
        CST_MGR_ID,
        ENT_IDV_IND,
        DEP_TP_ID,
        AC_OU_ID,
        CCY,
        CDR_YR,
        CDR_MTH,
        NOCLD_In_MTH,
        NOCLD_In_QTR,
        NOCLD_In_Year,
        NOD_CUR_DAY,
        ACG_DT,
        OU_ID)
select 
    CST_MGR_ID,
    ENT_IDV_IND,
    DEP_TP_ID,
    AC_OU_ID,
    CCY,
    CDR_YR,
    CDR_MTH,
    NOCLD_In_MTH,
    NOCLD_In_QTR,
    NOCLD_In_Year,
    NOD_CUR_DAY,
    ACG_DT,
    OU_ID,
    sum(NBR_CST) NBR_CST,              --�ͻ���
    sum(NBR_AC) NBR_AC,                --�˻���
    sum(DEP_BAL) DEP_BAL,              --������
    sum(NBR_NEW_CST) NBR_NEW_CST,      --�����ͻ���
    sum(NBR_NEW_AC)  NBR_NEW_AC,       --�����˻���
    sum(RSPL_DEP_BAL) RSPL_DEP_BAL     --���������α�
from (
    select * from tmp1
    union all
    select * from tmp3
    union all
    select * from tmp5
    union all
    select * from tmp7) N
group by 
    CST_MGR_ID,
    ENT_IDV_IND,
    DEP_TP_ID,
    AC_OU_ID,
    CCY,
    CDR_YR,
    CDR_MTH,
    NOCLD_In_MTH,
    NOCLD_In_QTR,
    NOCLD_In_Year,
    NOD_CUR_DAY,
    ACG_DT,
    OU_ID;

GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
COMMIT;--

SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
SET SMY_STEPDESC = 'ʹ�õ��ս������ݸ��»��ܱ�.';--

IF CUR_DAY=1 THEN                                                                     --�³�
   IF CUR_MONTH IN (4,7,10) THEN                                                      --���������
      INSERT INTO SMY.CST_MGR_DEP_MTHLY_SMY(CST_MGR_ID,
                                            ENT_IDV_IND,
                                            DEP_TP_ID,
                                            AC_OU_ID, 
                                            CCY,
                                            CDR_YR,
                                            CDR_MTH,
                                            NOCLD_IN_MTH,
                                            NOD_IN_MTH,
                                            NOCLD_IN_QTR,
                                            NOD_IN_QTR,
                                            NOCLD_IN_YEAR,
                                            NOD_IN_YEAR,
                                            ACG_DT,
                                            OU_ID,
                                            NBR_CST,
                                            NBR_AC,
                                            DEP_BAL,
                                            NBR_NEW_CST,
                                            NBR_NEW_AC,
                                            MTD_ACML_DEP_BAL,
                                            TOT_MTD_NBR_NEW_CST,
                                            TOT_MTD_NBR_NEW_AC,
                                            QTD_ACML_DEP_BAL,
                                            TOT_QTD_NBR_NEW_CST,
                                            TOT_QTD_NBR_NEW_AC,
                                            YTD_ACML_DEP_BAL,
                                            TOT_YTD_NBR_NEW_CST,
                                            TOT_YTD_NBR_NEW_AC
                                            ------------Start on 20100115---------------
                                            ,RSPL_DEP_BAL
                                            ,RSPL_MTD_ACML_DEP_BAL
                                            ,RSPL_QTD_ACML_DEP_BAL
                                            ,RSPL_YTD_ACML_DEP_BAL
                                            ------------End on 20100115---------------
                                            )
      SELECT S.CST_MGR_ID,
             S.ENT_IDV_IND,
             S.DEP_TP_ID,
             S.AC_OU_ID, 
             S.CCY,
             S.CDR_YR,
             S.CDR_MTH,
             S.NOCLD_IN_MTH,
             S.NOD_CUR_DAY AS NOD_IN_MTH,
             S.NOCLD_IN_QTR,
             S.NOD_CUR_DAY AS NOD_IN_QTR,
             S.NOCLD_IN_YEAR,
             COALESCE(T.NOD_IN_YEAR+S.NOD_CUR_DAY,S.NOD_CUR_DAY) AS NOD_IN_YEAR,
             S.ACG_DT,
             S.OU_ID,
             S.NBR_CST,
             S.NBR_AC,
             S.DEP_BAL,
             S.NBR_NEW_CST,
             S.NBR_NEW_AC,
             S.DEP_BAL AS MTD_ACML_DEP_BAL,
             S.NBR_NEW_CST AS TOT_MTD_NBR_NEW_CST,
             S.NBR_NEW_AC AS TOT_MTD_NBR_NEW_AC,
             S.DEP_BAL AS QTD_ACML_DEP_BAL,
             S.NBR_NEW_CST AS TOT_QTD_NBR_NEW_CST,
             S.NBR_NEW_AC AS TOT_QTD_NBR_NEW_AC,
             COALESCE(T.YTD_ACML_DEP_BAL+S.DEP_BAL,S.DEP_BAL) AS YTD_ACML_DEP_BAL,
             COALESCE(T.TOT_YTD_NBR_NEW_CST+S.NBR_NEW_CST,S.NBR_NEW_CST) AS TOT_YTD_NBR_NEW_CST,
             COALESCE(T.TOT_YTD_NBR_NEW_AC+S.NBR_NEW_AC,S.NBR_NEW_AC) AS TOT_YTD_NBR_NEW_AC
            ------------Start on 20100115---------------
            ,S.RSPL_DEP_BAL
            ,S.RSPL_DEP_BAL AS RSPL_MTD_ACML_DEP_BAL
            ,S.RSPL_DEP_BAL AS RSPL_QTD_ACML_DEP_BAL
            ,value(T.RSPL_YTD_ACML_DEP_BAL + S.RSPL_DEP_BAL,S.RSPL_DEP_BAL) AS RSPL_YTD_ACML_DEP_BAL
            ------------End on 20100115---------------             
      FROM SESSION.TMP S
      LEFT JOIN SMY.CST_MGR_DEP_MTHLY_SMY T
      ON S.CST_MGR_ID=T.CST_MGR_ID
      AND S.ENT_IDV_IND=T.ENT_IDV_IND
      AND S.DEP_TP_ID=T.DEP_TP_ID
      AND S.AC_OU_ID=T.AC_OU_ID
      AND S.CCY=T.CCY
      AND S.CDR_YR=T.CDR_YR
      AND S.CDR_MTH-1=T.CDR_MTH;--
            
      GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
      INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
      COMMIT;--
   ELSEIF CUR_MONTH=1 THEN                                                              --���
      INSERT INTO SMY.CST_MGR_DEP_MTHLY_SMY(CST_MGR_ID,
                                            ENT_IDV_IND,
                                            DEP_TP_ID,
                                            AC_OU_ID, 
                                            CCY,
                                            CDR_YR,
                                            CDR_MTH,
                                            NOCLD_IN_MTH,
                                            NOD_IN_MTH,
                                            NOCLD_IN_QTR,
                                            NOD_IN_QTR,
                                            NOCLD_IN_YEAR,
                                            NOD_IN_YEAR,
                                            ACG_DT,
                                            OU_ID,
                                            NBR_CST,
                                            NBR_AC,
                                            DEP_BAL,
                                            NBR_NEW_CST,
                                            NBR_NEW_AC,
                                            MTD_ACML_DEP_BAL,
                                            TOT_MTD_NBR_NEW_CST,
                                            TOT_MTD_NBR_NEW_AC,
                                            QTD_ACML_DEP_BAL,
                                            TOT_QTD_NBR_NEW_CST,
                                            TOT_QTD_NBR_NEW_AC,
                                            YTD_ACML_DEP_BAL,
                                            TOT_YTD_NBR_NEW_CST,
                                            TOT_YTD_NBR_NEW_AC
                                            ------------Start on 20100115---------------
                                            ,RSPL_DEP_BAL
                                            ,RSPL_MTD_ACML_DEP_BAL
                                            ,RSPL_QTD_ACML_DEP_BAL
                                            ,RSPL_YTD_ACML_DEP_BAL
                                            ------------End on 20100115---------------
                                            )
      SELECT S.CST_MGR_ID,
             S.ENT_IDV_IND,
             S.DEP_TP_ID,
             S.AC_OU_ID, 
             S.CCY,
             S.CDR_YR,
             S.CDR_MTH,
             S.NOCLD_IN_MTH,
             S.NOD_CUR_DAY AS NOD_IN_MTH,
             S.NOCLD_IN_QTR,
             S.NOD_CUR_DAY AS NOD_IN_QTR,
             S.NOCLD_IN_YEAR,
             S.NOD_CUR_DAY AS NOD_IN_YEAR,
             S.ACG_DT,
             S.OU_ID,
             S.NBR_CST,
             S.NBR_AC,
             S.DEP_BAL,
             S.NBR_NEW_CST,
             S.NBR_NEW_AC,
             S.DEP_BAL AS MTD_ACML_DEP_BAL,
             S.NBR_NEW_CST AS TOT_MTD_NBR_NEW_CST,
             S.NBR_NEW_AC AS TOT_MTD_NBR_NEW_AC,
             S.DEP_BAL AS QTD_ACML_DEP_BAL,
             S.NBR_NEW_CST AS TOT_QTD_NBR_NEW_CST,
             S.NBR_NEW_AC AS TOT_QTD_NBR_NEW_AC,
             S.DEP_BAL AS YTD_ACML_DEP_BAL,
             S.NBR_NEW_CST AS TOT_YTD_NBR_NEW_CST,
             S.NBR_NEW_AC AS TOT_YTD_NBR_NEW_AC
            ------------Start on 20100115---------------
            ,S.RSPL_DEP_BAL
            ,S.RSPL_DEP_BAL AS RSPL_MTD_ACML_DEP_BAL
            ,S.RSPL_DEP_BAL AS RSPL_QTD_ACML_DEP_BAL
            ,S.RSPL_DEP_BAL AS RSPL_YTD_ACML_DEP_BAL
            ------------End on 20100115---------------              
      FROM SESSION.TMP S;--
                
          GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
          INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
          COMMIT;--
        ELSE                                                                             --�³��Ǽ��������
      INSERT INTO SMY.CST_MGR_DEP_MTHLY_SMY(CST_MGR_ID,
                                            ENT_IDV_IND,
                                            DEP_TP_ID,
                                            AC_OU_ID, 
                                            CCY,
                                            CDR_YR,
                                            CDR_MTH,
                                            NOCLD_IN_MTH,
                                            NOD_IN_MTH,
                                            NOCLD_IN_QTR,
                                            NOD_IN_QTR,
                                            NOCLD_IN_YEAR,
                                            NOD_IN_YEAR,
                                            ACG_DT,
                                            OU_ID,
                                            NBR_CST,
                                            NBR_AC,
                                            DEP_BAL,
                                            NBR_NEW_CST,
                                            NBR_NEW_AC,
                                            MTD_ACML_DEP_BAL,
                                            TOT_MTD_NBR_NEW_CST,
                                            TOT_MTD_NBR_NEW_AC,
                                            QTD_ACML_DEP_BAL,
                                            TOT_QTD_NBR_NEW_CST,
                                            TOT_QTD_NBR_NEW_AC,
                                            YTD_ACML_DEP_BAL,
                                            TOT_YTD_NBR_NEW_CST,
                                            TOT_YTD_NBR_NEW_AC
                                            ------------Start on 20100115---------------
                                            ,RSPL_DEP_BAL
                                            ,RSPL_MTD_ACML_DEP_BAL
                                            ,RSPL_QTD_ACML_DEP_BAL
                                            ,RSPL_YTD_ACML_DEP_BAL
                                            ------------End on 20100115---------------
                                            )
      SELECT S.CST_MGR_ID,
             S.ENT_IDV_IND,
             S.DEP_TP_ID,
             S.AC_OU_ID, 
             S.CCY,
             S.CDR_YR,
             S.CDR_MTH,
             S.NOCLD_IN_MTH,
             S.NOD_CUR_DAY AS NOD_IN_MTH,
             S.NOCLD_IN_QTR,
             COALESCE(T.NOD_IN_QTR+S.NOD_CUR_DAY,S.NOD_CUR_DAY) AS NOD_CUR_DAY,
             S.NOCLD_IN_YEAR,
             COALESCE(T.NOD_IN_YEAR+S.NOD_CUR_DAY,S.NOD_CUR_DAY) AS NOD_IN_YEAR,
             S.ACG_DT,
             S.OU_ID,
             S.NBR_CST,
             S.NBR_AC,
             S.DEP_BAL,
             S.NBR_NEW_CST,
             S.NBR_NEW_AC,
             S.DEP_BAL AS MTD_ACML_DEP_BAL,
             S.NBR_NEW_CST AS TOT_MTD_NBR_NEW_CST,
             S.NBR_NEW_AC AS TOT_MTD_NBR_NEW_AC,
             COALESCE(T.QTD_ACML_DEP_BAL+S.DEP_BAL,S.DEP_BAL) AS QTD_ACML_DEP_BAL,
             COALESCE(T.TOT_QTD_NBR_NEW_CST+S.NBR_NEW_CST,S.NBR_NEW_CST) AS TOT_QTD_NBR_NEW_CST,
             COALESCE(T.TOT_QTD_NBR_NEW_AC+S.NBR_NEW_AC,S.NBR_NEW_AC) AS TOT_QTD_NBR_NEW_AC,
             COALESCE(T.YTD_ACML_DEP_BAL+S.DEP_BAL,S.DEP_BAL) AS YTD_ACML_DEP_BAL,
             COALESCE(T.TOT_YTD_NBR_NEW_CST+S.NBR_NEW_CST,S.NBR_NEW_CST) AS TOT_YTD_NBR_NEW_CST,
             COALESCE(T.TOT_YTD_NBR_NEW_AC+S.NBR_NEW_AC,S.NBR_NEW_AC) AS TOT_YTD_NBR_NEW_AC
            ------------Start on 20100115---------------
            ,S.RSPL_DEP_BAL
            ,S.RSPL_DEP_BAL AS RSPL_MTD_ACML_DEP_BAL
            ,value(T.RSPL_QTD_ACML_DEP_BAL + S.RSPL_DEP_BAL,S.RSPL_DEP_BAL)  AS RSPL_QTD_ACML_DEP_BAL
            ,value(T.RSPL_YTD_ACML_DEP_BAL + S.RSPL_DEP_BAL,S.RSPL_DEP_BAL) AS RSPL_YTD_ACML_DEP_BAL
            ------------End on 20100115---------------                
      FROM SESSION.TMP S
      LEFT JOIN SMY.CST_MGR_DEP_MTHLY_SMY T
      ON S.CST_MGR_ID=T.CST_MGR_ID
      AND S.ENT_IDV_IND=T.ENT_IDV_IND
      AND S.DEP_TP_ID=T.DEP_TP_ID
      AND S.AC_OU_ID=T.AC_OU_ID
      AND S.CCY=T.CCY
      AND S.CDR_YR=T.CDR_YR
      AND S.CDR_MTH-1=T.CDR_MTH;--
                
          GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
          INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP); --
          COMMIT;                     --
   END IF;--
ELSE                                                                                          ---���³�
  MERGE INTO SMY.CST_MGR_DEP_MTHLY_SMY S
  USING SESSION.TMP T
  ON S.CST_MGR_ID=T.CST_MGR_ID
  AND S.ENT_IDV_IND=T.ENT_IDV_IND
  AND S.DEP_TP_ID=T.DEP_TP_ID
  AND S.AC_OU_ID=T.AC_OU_ID
  AND S.CCY=T.CCY
  AND S.CDR_YR=T.CDR_YR
  AND S.CDR_MTH=T.CDR_MTH
  WHEN MATCHED 
  THEN UPDATE SET(CST_MGR_ID,
                  ENT_IDV_IND,
                  DEP_TP_ID,
                  AC_OU_ID, 
                  CCY,
                  CDR_YR,
                  CDR_MTH,
                  NOCLD_IN_MTH,
                  NOD_IN_MTH,
                  NOCLD_IN_QTR,
                  NOD_IN_QTR,
                  NOCLD_IN_YEAR,
                  NOD_IN_YEAR,
                  ACG_DT,
                  OU_ID,
                  NBR_CST,
                  NBR_AC,
                  DEP_BAL,
                  NBR_NEW_CST,
                  NBR_NEW_AC,
                  MTD_ACML_DEP_BAL,
                  TOT_MTD_NBR_NEW_CST,
                  TOT_MTD_NBR_NEW_AC,
                  QTD_ACML_DEP_BAL,
                  TOT_QTD_NBR_NEW_CST,
                  TOT_QTD_NBR_NEW_AC,
                  YTD_ACML_DEP_BAL,
                  TOT_YTD_NBR_NEW_CST,
                  TOT_YTD_NBR_NEW_AC
                  ------------Start on 20100115---------------
                  ,RSPL_DEP_BAL
	                ,RSPL_MTD_ACML_DEP_BAL
	                ,RSPL_QTD_ACML_DEP_BAL
	                ,RSPL_YTD_ACML_DEP_BAL
	                ------------End on 20100115---------------
                  )
                =(T.CST_MGR_ID,
                  T.ENT_IDV_IND,
                  T.DEP_TP_ID,
                  T.AC_OU_ID, 
                  T.CCY,
                  T.CDR_YR,
                  T.CDR_MTH,
                  T.NOCLD_IN_MTH,
                  S.NOD_IN_MTH+T.NOD_CUR_DAY,
                  T.NOCLD_IN_QTR,
                  S.NOD_IN_QTR+T.NOD_CUR_DAY,
                  T.NOCLD_IN_YEAR,
                  S.NOD_IN_YEAR+T.NOD_CUR_DAY,
                  T.ACG_DT,
                  T.OU_ID,
                  T.NBR_CST,
                  T.NBR_AC,
                  T.DEP_BAL,
                  T.NBR_NEW_CST,
                  T.NBR_NEW_AC,
                  S.MTD_ACML_DEP_BAL+T.DEP_BAL,
                  S.TOT_MTD_NBR_NEW_CST+T.NBR_NEW_CST,
                  S.TOT_MTD_NBR_NEW_AC+T.NBR_NEW_AC,
                  S.QTD_ACML_DEP_BAL+T.DEP_BAL,
                  S.TOT_QTD_NBR_NEW_CST+T.NBR_NEW_CST,
                  S.TOT_QTD_NBR_NEW_AC,
                  S.YTD_ACML_DEP_BAL+T.DEP_BAL,
                  S.TOT_YTD_NBR_NEW_CST+T.NBR_NEW_CST,
                  S.TOT_YTD_NBR_NEW_AC+T.NBR_NEW_AC
                  ------------Start on 20100115---------------
                  ,T.RSPL_DEP_BAL
			            ,S.RSPL_MTD_ACML_DEP_BAL + T.RSPL_DEP_BAL 
			            ,S.RSPL_QTD_ACML_DEP_BAL + T.RSPL_DEP_BAL 
			            ,S.RSPL_YTD_ACML_DEP_BAL + T.RSPL_DEP_BAL
			            ------------End on 20100115--------------- 
                  )
  WHEN NOT MATCHED
  THEN INSERT(CST_MGR_ID,
              ENT_IDV_IND,
              DEP_TP_ID,
              AC_OU_ID, 
              CCY,
              CDR_YR,
              CDR_MTH,
              NOCLD_IN_MTH,
              NOD_IN_MTH,
              NOCLD_IN_QTR,
              NOD_IN_QTR,
              NOCLD_IN_YEAR,
              NOD_IN_YEAR,
              ACG_DT,
              OU_ID,
              NBR_CST,
              NBR_AC,
              DEP_BAL,
              NBR_NEW_CST,
              NBR_NEW_AC,
              MTD_ACML_DEP_BAL,
              TOT_MTD_NBR_NEW_CST,
              TOT_MTD_NBR_NEW_AC,
              QTD_ACML_DEP_BAL,
              TOT_QTD_NBR_NEW_CST,
              TOT_QTD_NBR_NEW_AC,
              YTD_ACML_DEP_BAL,
              TOT_YTD_NBR_NEW_CST,
              TOT_YTD_NBR_NEW_AC
              ------------Start on 20100115---------------
              ,RSPL_DEP_BAL
              ,RSPL_MTD_ACML_DEP_BAL
              ,RSPL_QTD_ACML_DEP_BAL
              ,RSPL_YTD_ACML_DEP_BAL
              ------------End on 20100115---------------
              )
       VALUES(T.CST_MGR_ID,
              T.ENT_IDV_IND,
              T.DEP_TP_ID,
              T.AC_OU_ID, 
              T.CCY,
              T.CDR_YR,
              T.CDR_MTH,
              T.NOCLD_IN_MTH,
              T.NOD_CUR_DAY,
              T.NOCLD_IN_QTR,
              T.NOD_CUR_DAY,
              T.NOCLD_IN_YEAR,
              T.NOD_CUR_DAY,
              T.ACG_DT,
              T.OU_ID,
              T.NBR_CST,
              T.NBR_AC,
              T.DEP_BAL,
              T.NBR_NEW_CST,
              T.NBR_NEW_AC,
              T.DEP_BAL,
              T.NBR_NEW_CST,
              T.NBR_NEW_AC,
              T.DEP_BAL,
              T.NBR_NEW_CST,
              T.NBR_NEW_AC,
              T.DEP_BAL,
              T.NBR_NEW_CST,
              T.NBR_NEW_AC
              ------------Start on 20100115---------------
              ,T.RSPL_DEP_BAL
              ,T.RSPL_DEP_BAL
              ,T.RSPL_DEP_BAL
              ,T.RSPL_DEP_BAL
              ------------End on 20100115---------------              
              );--
              
  GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
  INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--
  COMMIT;--
END IF;--

/*SET SMY_STEPNUM = SMY_STEPNUM + 1 ;--
SET SMY_STEPDESC = 'UPDATE ACG_DT.';--
UPDATE SMY.CST_MGR_DEP_MTHLY_SMY SET ACG_DT=ACCOUNTING_DATE WHERE CDR_YR=CUR_YEAR AND CDR_MTH=CUR_MONTH;--
GET DIAGNOSTICS SMY_RCOUNT = ROW_COUNT;--
INSERT INTO SMY.SMY_LOG VALUES(SMY_PROCNM, SMY_DATE, SMY_STEPNUM, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);*/

SET SMY_STEPNUM=6 ;--
SET SMY_STEPDESC = '�洢���̽���!';--

INSERT INTO SMY.SMY_LOG (SMY_PROC_NM,SMY_ACT_DT,SMY_STEPNUM,SMY_STEPDESC,SMY_SQLCODE,SMY_RCOUNT,CUR_TS)
VALUES(SMY_PROCNM, SMY_DATE, 0, SMY_STEPDESC, SMY_SQLCODE, SMY_RCOUNT, CURRENT TIMESTAMP);--

END@

