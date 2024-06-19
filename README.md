# AWS Aurora for MySQL Monitoring
## Aurora for MySQL Metric & Slow Query Monitoring
- 그라파나를 위한 모니터링 api
- 그라파나 플러그인 [JSON API](https://grafana.com/grafana/plugins/marcusolsson-json-datasource/)를 이용한 모니터링 시각화
- 저장소를 MongoDB로 한 이유:
  - MongoDB는 데이터 입출력 작업에 있어 타 오픈소스 RDBMS 보다 뛰어나고, 데이터 압축률이 좋으며, TTL 인덱스를 통한 데이터 관리가 용이하며, 저장된 데이터를 재활용하기에 편리함
  - Primary와 Secondary간의 차이에 따른 저장 필드가 다른 경우가 있음
- CloudWatch의 지표만 가지고는 모니터링이 힘든 부분이 있어, 성능 최적화 도구가 하는 역할을 performance_schema, sys 및 information_schema의 휘발성 정보들을 필요한 부분만 직접 수집하고 저장하여 활용
### 주요 수집 지표
- Aurora 클러스터 스테이터스만 별도로 수집
- MySQL Slow Query
- Slow Query의 실행 계획을 선택하여 EXPLAIN JSON으로 저장
- COMMAND Status 저장 - select, insert, update, delete, commit, rollback 비중 등
- Binlog 캐시 사용 여부 - Binlog_cache_use, Binlog_cache_disk_use 수집
- TempTable 사용 여부 - Created_tmp_tables, Created_tmp_files, Created_tmp_disk_tables 값 수집

## [apis.py](apis.py)
- FastAPI를 이용한 MongoDB에 저장된 메트릭 및 쿼리 데이터 호출 api
- 모니터링 대상 instance 목록 관리 api
### api
  - /api/instance_setup/add_instance: 모니터링 대상 인스턴스 추가
  - /api/instance_setup/list_instances: 모니터링 대상 인스턴스 목록 출력
  - /api/instance_setup/delete_instance: 모니터링 대상 인스턴스 목록 삭제
  - /api/rds/aurora_cluster: Aurora 클러스터 수집 정보를 가져오기
  - /api/mysql_status/status/?instance_name=\<변수\> : MySQL 누적 스탯을 가져오기
  - /api/mysql_explain/items: 슬로우 쿼리 목록 가져오기
  - /api/mysql_explain/download: 슬로우 쿼리 저장된 플랜을 Markdown으로 내려받기
  - /api/mysql_explain/plans: 플랜이 저장된 리스트 가져오기
  - /api/slow_query/statistics: 슬로우 쿼리의 통계를 보여주기
  - /api/mysql_io/status/?instance_name=: 디스크 사용량 가져오기

## [collector_app.py](collector_app.py)
- collector 디렉토리 밑의 수집기를 정해진 시간 단위로 구동
- MySQL 슬로우 쿼리 수집 및 플랜 저장 - 실시간
- MySQL Command 누적 데이터 수집 - 15분 주기
- Binlog 캐시, TempTable 사용 여부 - 10분 주기

## Slack Noti 
- 슬랙 노티 모듈을 통해 개인 사용자가 슬로우 쿼리를 던졌을 때 Slack으로 알림을 보낼 수 있음

## Grafana
- default.ini 파일안에 그라파나 text 패널에서 iframe을 사용할 수 있도록 세팅이 되어 있음

## metadata
- MongoDB에 미리 AWS RDS 인스턴스 스펙 데이터를 업로드 해야함

### .env
#### 환경 변수 내용
```
## MongoDB Connection
MONGODB_URI=
MONGODB_DB_NAME=mgmt_db
MONGODB_SLOWLOG_COLLECTION_NAME=mysql_slowquery
MONGODB_PLAN_COLLECTION_NAME=mysql_slowquery_plan
MONGODB_STATUS_COLLECTION_NAME=mysql_command_status
MONGODB_RDS_INSTANCE_LIST_COLLATION_NAME=instance_list
MONGODB_AURORA_INFO_COLLECTION_NAME=aurora_cluster_info
MONGODB_HISTORY_COLLECTION_NAME=mysql_event_stat_hist
MONGODB_DIGEST_COLLECTION_NAME=mysql_event_sum_digest
MONGODB_DISK_USAGE_COLLECTION_NAME=mysql_disk_usage

## Slack Noti
SLACK_API_TOKEN=
SLACK_WEBHOOK_URL=
```

