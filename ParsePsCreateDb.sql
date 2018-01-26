DROP TABLE PS_TYPE_COUNT;

create table PS_MODEL_SUMMARY
(
  ps_table        VARCHAR2(100) not null,
  ps_model          VARCHAR2(4000) not null,
  ps_model_name   VARCHAR2(400),
  ps_service_code VARCHAR2(200),
  ps_service_type VARCHAR2(100) not null,
  action_id       NUMBER(4) not null,
  ps_param        VARCHAR2(4000) not null,
  ps_count             NUMBER(15),
  ps_id           NUMBER(15),
  bill_id         VARCHAR2(64),
  sub_bill_id    VARCHAR2(64),
  CREATE_DATE    DATE,
  REGION_CODE    VARCHAR2(6),
  notes           VARCHAR2(2000)
)
;
