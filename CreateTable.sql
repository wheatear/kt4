-- Create table
create table KTCLIENT_RESULT
(
  caseno        NUMBER(15),
  casename     VARCHAR2(64),
  billId       VARCHAR2(64),
  createDate   DATE,
  comparestatus  NUMBER(2),
  regioncode_kt3  VARCHAR2(6),
  regioncode_kt4  VARCHAR2(6),
  psId_kt3      NUMBER(15),
  psId_kt4       NUMBER(15),
  psStatus_kt3   NUMBER(2),
  psStatus_kt4   NUMBER(2),
  failReason_kt3 VARCHAR2(2000),
  failReason_kt4 VARCHAR2(2000),
  tradId_kt3     NUMBER(15),
  tradId_kt4     NUMBER(15),
  errCode_kt3    NUMBER(2),
  errCode_kt4    NUMBER(2),
  errDesc_kt3    VARCHAR2(2000),
  errDesc_kt4    VARCHAR2(2000),
  resp_kt3        VARCHAR2(4000),
  resp_kt4        VARCHAR2(4000)
);

