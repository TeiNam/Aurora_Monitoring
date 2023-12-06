# AWS Aurora for MySQL Monitoring
Aurora for MySQL Metric & Slow Query Monitoring
- 그라파나를 위한 모니터링 api
- 그라파나 플러그인 JSON API를 이용한 모니터링 시각화
- 저장소를 MongoDB로 한 이유:
  - MongoDB는 데이터 입출력 작업에 있어 타 오픈소스 RDBMS 보다 뛰어나고, 데이터 압축률이 좋으며, TTL 인덱스를 통한 데이터 관리가 용이하며, 저장된 데이터를 재활용하기에 편리함
- CloudWatch의 지표만 가지고는 모니터링이 힘든 부분이 있어, 성능 최적화 도구가 하는 역할을 performance_schema, sys 및 information_schema의 휘발성 정보들을 필요한 부분만 직접 수집하고 저장하여 활용

## [apis.py](apis.py)
- FastAPI를 이용한 MongoDB에 저장된 메트릭 및 쿼리 데이터 호출 api
- 모니터링 대상 instance 목록 관리 api
### api
  - /api/instance_setup : 모니터링 대상 인스턴스 목록 관리 api, rds_instances.json 
  - /api/aws_rds : aws rds instance status 호출 api
  - /api/mysql_status : MySQL 누적 Command status 호출 api
  - /api/aurora_status : Cloudwatch에서 수집한 Metric 호출 api
  - /api/mysql_slow_query : 수집한 Slow query 호출 api
  - /api/mysql_explain : 수집한 Slow query에서 플랜 저장 및 파일로 다운로드 api

## [collector_app.py](collector_app.py)
- 데이터 수집기
- Cloudwatch에서 메트릭 수집
- MySQL 슬로우 쿼리 수집 및 플랜 저장
- MySQL Command 누적 데이터 수집


### .env
#### 환경 변수 내용
```
## MongoDB Connect
MONGODB_URI=
MONGODB_DB_NAME=
MONGODB_SLOWLOG_COLLECTION_NAME=rdsSlowQueries
MONGODB_PLAN_COLLECTION_NAME=rdsQueriesPlan
MONGODB_STATUS_COLLECTION_NAME=rdsCommandStatus

## encrypt key
AES_KEY=
AES_IV=

## AWS Access Key
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
```