import datetime
import logging
import pytz
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.models import XCom
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessApplicationSensor, EmrServerlessJobSensor
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessStartJobOperator,
    EmrServerlessStopApplicationOperator,
)
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
from io import StringIO


# 24시간 -> ms 
day_time = 86400000

# airflow connetion 에서 미리 연결해 둔 aws_conn_id를 통해서 S3 접근 / 버킷 경로 지정
hook = S3Hook(aws_conn_id='aws_s3')
bucket ='taehun-s3-bucket-230717'

# Emrserverless 의 생성 / 시작 / 중지 / 삭제 등의 권한을 부여한, 사전에 만들어 둔 iam role
JOB_ROLE_ARN =  "arn:aws:iam::796630047050:role/emr_serverless_exe_role"

# Spark Job 제출 후, log들을 저장 할 S3 경로 지정( 선택사항입니다. app 삭제를 하지 않으면, Spark UI 를 통해서 쉽게 볼 수 있습니다. )
S3_LOGS_BUCKET = bucket
DEFAULT_MONITORING_CONFIG = {
    "monitoringConfiguration": {
        "s3MonitoringConfiguration": {"logUri": "s3://taehun-s3-bucket-230717/spark_logs/"}
    },
}


# S3hook 의 list_keys 메서드 사용 , 파라미터로 bucket/prefix를 받아서 해당 경로의 파일들에 접근할 수 있음
def list_keys(**context):
    # hook = S3Hook(aws_conn_id='aws_s3')

    # 해당 경로 파일들의 총 크기를 보기 위해 paginator 생성
    paginator = hook.get_conn().get_paginator('list_objects_v2')
    # slack 에 전송 할 때 보낼 현재 시간 생성
    current_time = datetime.now().strftime('%m월 %d일 %A %H시 %M분')
    #  airflow Variable 을 통해서 폴더명을 가져와서 경로로 지정 ( ex) 1684454400000 ) 
    current_folder = Variable.get('current_date_folder')
    prefix = f'{current_folder}/'
  
    file_size = 0
    # logging 은 airflow에서 로그를 보기 위해 테스트 차원에서 사용, 테스트 후 제거
    # logging.info(f"Listing Keys from {bucket}/{prefix}")
    keys = hook.list_keys(bucket_name=bucket, prefix=prefix)

    # 생성한 paginator와 반복문을 통해서 , slack 에 보낼 파일 사이즈 반환
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
      for obj in page.get('Contents', []):
        file_size += obj['Size']
    # byte 단위로 출력돼서, (x.xx kb) 형식으로 나오도록 변경
    file_size = round(file_size / 1024, 2)
        
    # 파일의 총 개수    
    len_keys = len(keys)
    # logging.info(f"len_key chk {len_keys}, {keys}")

    # xcom 을 사용해서 , 다른 Task 에서 Pull을 통해 사용하도록 push
    context['task_instance'].xcom_push(key='file_size',value=file_size)
    context['task_instance'].xcom_push(key='current_time',value=current_time)
    context['task_instance'].xcom_push(key='key_val', value=len_keys)
    context['task_instance'].xcom_push(key='prefix_val', value=prefix) 

    # 만약 파일이 없다면 실패 메세지와 함께 airflow가 종료되도록, 성공한다면 계속 진행하도록 의존성 추가( BranchPythonOperator 를 통해 어느 방향으로 Dag가 진행할지 여기서 파악 )
    if len_keys == 0:
      return 'send_slack_fail'
    else:
      return 'send_slack_success'
    
# Data 정합성 체크를 위한 check_Data 함수 생성
def check_data(**context):
    # hook = S3Hook(aws_conn_id='aws_s3')
    current_folder = Variable.get('current_date_folder')
    # Spark에서 정합성 체크 결과를 저장한 경로
    prefix = f'tempdir/{current_folder}/checkData'
    keys = hook.list_keys(bucket_name=bucket, prefix=prefix)
    check_message = """
    <<segment_log table>>
    S3 원본 데이터의 MessageId 
    중복 제거 후 MessageId 
    S3 원본 데이터의 총 row 
    
    Redshift 적재 데이터의 MessagId 
    중복 제거 후 MessageId 
    Redshift 적재 데이터의 총 row
    
    <<segment_log_session table>>
    S3 원본 데이터의 session_id 
    S3 원본 데이터의 총 row
    Redshift 적재 데이터의 session_id
    Redshift 적재 데이터의 총 row
    """
    # Spark에서 정합성 체크 결과를 저장한 경로의 파일들을 pandas를 통해 읽어와서, slack에 전송하기 위한 check_message 생성
    for key in keys:
      if key.endswith('.json'):
        body = hook.read_key(key=key, bucket_name=bucket)
        data = StringIO(body)
        df = pd.read_json(data,lines=True)
        check_message += f"\ncheck:\n{df.head().to_string()}\n"

    # xcom 을 통해 slack 메세지를 보내는 task로 전송
    context['task_instance'].xcom_push(key='check', value=check_message)

    # 작업들이 다 정상종료된다면, 현재 1684454400000 으로 저장된 variable을 내일 날짜의 폴더명으로 업데이트 
    new_current_folder = int(current_folder) + day_time
    Variable.set('current_date_folder', new_current_folder)
        
    





with DAG( 
    dag_id="medistream_job3",
    # 실제 자동화 시, schedule_interval 를 아래와 같이 설정하면, KST로 오전 10시 30분마다 Dag 실행 , Test 환경에선 None으로 두고 사용
    # schedule_interval="30 1 * * *"
    schedule_interval=None,
    start_date=datetime(2023, 12, 6),
    tags=["Medistream"],
    # catchup 을 true로 설정하게 되면, 시작 일 ~ 현재까지의 모든 작업 수행 
    catchup=False,
) as dag:
    # 미리 variable에 등록한 slack_token / 노출에 민감한 정보들을 .env 파일에 등록해서 사용하는 것과 동일
    token = Variable.get("slack_token")
    # list_keys 함수를 콜백함
    check_task = BranchPythonOperator(
      task_id='list_keys',
      python_callable=list_keys,
    )
    
    send_slack_fail = SlackAPIPostOperator(
      task_id='send_slack_fail',
      token = token,
      channel = '#일반',
      text = '적재된 로그 파일이 없습니다 (실패).',
    )
    
    send_slack_success = SlackAPIPostOperator(
      task_id='send_slack_success',
      token = token,
      channel = '#일반',
      text = """
        시작 시간:{{ task_instance.xcom_pull(task_ids='list_keys', key='current_time') }}\n
        파일 경로: {{ task_instance.xcom_pull(task_ids='list_keys', key='prefix_val') }}
        file_size(total): {{ task_instance.xcom_pull(task_ids='list_keys', key='file_size') }} KB
        {{ task_instance.xcom_pull(task_ids='list_keys', key='key_val') }}개의 파일이 적재되었습니다.
    """,
    )

    ### emr - app도 airflow로 생성할 수 있음 
    # create_app = EmrServerlessCreateApplicationOperator(
    #     task_id="create_spark_app",
    #     job_type="SPARK",
    #     release_label="emr-6.15.0",
    #     config={"name": "airflow-test"},
    #     aws_conn_id='aws_s3'
    # )
    ###

    # 미리 만들어 둔 Spark 를 설치한 app 이름
    application_id = "00ffc1vgng0gms2p"
    job1 = EmrServerlessStartJobOperator(
        task_id="start_job_1",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
            #    spark job에 제출되는 컴파일 된 scala 코드
                "entryPoint": f"s3://{bucket}/scripts/medi_project.jar",
                # scala 코드 내에서 --path 값을 받아서 경로를 지정
                "entryPointArguments" :["--path","{{ task_instance.xcom_pull(task_ids='list_keys', key='prefix_val') }}","--token",token],
                # 사전 용량 지정 -> 지정하지 않으면, runtime 이 늘어서, 비용이 증가함 ( 사전 초기 용량을 emrserverless에서 자체적으로 계산하는 시간이 늘기 때문에)
                "sparkSubmitParameters": "--conf spark.executor.cores=1 --conf spark.executor.memory=4g\
            --conf spark.driver.cores=1 --conf spark.driver.memory=4g --conf spark.executor.instances=1"
              
            
            }
        },
        # log 를 저장하는 경로와, aws CLI 에 접근할 수 있는 aws_conn_id 설정
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
        aws_conn_id='aws_s3'
    )
    
    # delete_app = EmrServerlessDeleteApplicationOperator(
    #     task_id="delete_app",
    #     application_id=application_id,
    #     trigger_rule="all_done",
    #     aws_conn_id='aws_s3'
    # )
    
    #spark job submit 후, 정지
    stop_app = EmrServerlessStopApplicationOperator(
      task_id="stop_app",
      application_id = application_id,
      trigger_rule="all_done",
      force_stop=True,
      aws_conn_id='aws_s3'
    )
    # 모니터링을 위한 메세지
    spark_slack = SlackAPIPostOperator(
        task_id='spark_success',
        token = token,
        channel = '#일반',
        text ="spark 작업이 완료되었습니다."
    )
    # 정합성 체크 ( pandas 사용 )
    check_data_task = PythonOperator(
        task_id = 'check_data',
        python_callable=check_data
    )
    # 모니터링을 위한 메세지
    test_data_slack = SlackAPIPostOperator(
        task_id='check_data_slack',
        token = token,
        channel = '#일반',
        text ="{{ task_instance.xcom_pull(task_ids='check_data', key='check') }}\n airflow 작업 완료"
    )
    # 모니터링을 위한 메세지
    spark_slack_fail_emr = SlackAPIPostOperator(
        task_id='spark_fail_emr',
        token = token,
        channel = '#일반',
        text ="Spark Job 이 실패하였습니다. Log 를 확인하세요 ",
        trigger_rule=TriggerRule.ALL_FAILED
    )
    
    check_task >> [send_slack_fail, send_slack_success]
    send_slack_success >> job1
    job1 >> stop_app >> spark_slack
    spark_slack >> check_data_task >> test_data_slack
    job1 >> spark_slack_fail_emr