## 프로젝트 개요

기업에선 매일 진행되는 **Log 데이터 ETL 작업**에서 발생하는 **Redshift의 부하 문제와 비용 증가**를 해결하기 위해

**AWS EMR Serverless 환경에서 Spark**를 활용한 **Migration** 프로젝트를 진행하게 되었습니다.
![현재 파이프라인](https://github.com/hctaehoon/emrserverless-etl-cicd-pipeline/assets/113021892/8183583c-6c6f-4d18-83ed-d45af8db8618)


## 프로젝트 진행(AWS 환경 설정)
프로젝트에서 저는 팀원들의 **PySpark 코드를 Scala로 변환하고 컴파일**하는 역할을 수행하였으며, **자동화된 파이프라인 구축**을 통해 프로젝트를 고도화 하는 데 중점을 두었습니다.

기업측에서는 비용 문제를 고려하여 목적과는 다르게 Serverless가 아닌 EC2 환경을 제공해주셨기 때문에, 

데이터 정합성 검사 및 자동화 파이프라인 테스트 등 권한이 필요한 작업들을 제한 없이 수행할 수 없었습니다.

이 문제를 해결하기 위해 **개인 AWS 계정에서 직접 파이프라인을 구축하여 프로젝트를  진행**하기로 하였습니다. 

(EMR serverless 구축 단계의 더 자세한 내용은 아래 링크에서 확인하실 수 있습니다.)

[EMR serverless 구축(wiki 문서)](https://github.com/hctaehoon/emrserverless-etl-cicd-pipeline/wiki/Airflow-CICD-%ED%8C%8C%EC%9D%B4%ED%94%84%EB%9D%BC%EC%9D%B8#emr-serverless-operator-%EB%A5%BC-%ED%86%B5%ED%95%9C-spark-job-%EC%9E%90%EB%8F%99%ED%99%94-%EA%B3%BC%EC%A0%95)


## 프로젝트 진행(Airflow를 통한 자동화 구성)

기업의 S3에 Log 데이터들을 확인하다보니, 매일 **새로운 폴더에 접근하여 데이터를 추출**한다면 ETL 과정을 자동화 할 수 있다고 생각했습니다.

기업이 현재 사용 중인 AWS Glue 서비스는 유료 자동화 파이프라인을 제공하지만, 이를 **오픈 소스인 Airflow**만으로 대체하면

비용을 절감할 수 있을 것이라고 판단하였습니다.

![파티셔닝규칙](https://github.com/hctaehoon/emrserverless-etl-cicd-pipeline/assets/113021892/2c5673e1-3f59-459f-8514-55d5c073e8e6)

Airflow에서는 **LocalExecutor**를 사용하였으며, 기존에 사용했던 2.2 버전에서는 EMR Serverless Operator가 Airflow Providers에 포함되지 않았고

2.7 버전에서는 SlackOperator 내에서 Slack API 토큰을 인식하지 못하는 버그가 있어

**Airflow 2.6.0** 버전을 사용하였습니다. 또한 배치 작업 특성 상, Airflow Job 이 실행될 때만 리소스를 사용하는 이점이 있는

**K8s Executor**를 사용하는 K8s로 Migration을 고려해볼 수 있을 것 같습니다.

사용한 도커 이미지 및 환경 설정은 다음 문서에서 확인하실 수 있습니다.

[Airflow 환경 설정](https://github.com/hctaehoon/emrserverless-etl-cicd-pipeline/wiki/Airflow-CICD-%ED%8C%8C%EC%9D%B4%ED%94%84%EB%9D%BC%EC%9D%B8#3-airflowawss3hook)


## 프로젝트 진행(Airflow Dag)

![Airflow 기능 활용](https://github.com/hctaehoon/emrserverless-etl-cicd-pipeline/assets/113021892/d47945e5-86ff-4de5-aa72-fc302ccd49a0)

위와 같이 Airflow의 Variable 기능을 사용하여 폴더명을 저장해두고

오늘 ETL 작업이 성공적으로 끝나면 내일 Log 데이터가 적재 될 폴더명으로 변경하여 다음 ETL 작업을 할 수 있도록 설정하였습니다.

또한 **데이터의 생명주기**와 추후에 진행 될 Spark Job의 **확장성**을 고려하여,

Variable로 받은 폴더명은 Xcom 을 통해 Spark Job에 전달하도록 하였습니다.

* 다음과 같이 각 Task 마다, **Slack Notification** 을 통해서 작업의 성공 여부를 파악하고  

  실패 시 불필요한 리소스의 낭비를 줄이도록 DAG를 구성하였습니다.

![Airflow Dag](https://github.com/hctaehoon/emrserverless-etl-cicd-pipeline/assets/113021892/0980d126-91fd-4785-a3f2-e7be3918677c)

![DAg2](https://github.com/hctaehoon/emrserverless-etl-cicd-pipeline/assets/113021892/4ec5dbcd-d96b-4d58-b136-8d330380d15c)



* 또한 EMR Operator 를 통해 Spark Job 을 제출 시, 초기 사전 용량을 설정할 수 있는데 이를 설정해주어 

**비용 및 시간을 약 40%** 절감할 수 있었습니다.또한 최대 용량도 설정하여, EMR 환경에 사용 할 최대 리소스도 제한할 수 있습니다.

![dag3](https://github.com/hctaehoon/emrserverless-etl-cicd-pipeline/assets/113021892/4c875f98-4c09-4f2a-85d2-be5f9fe34849)


![sparkjob종료](https://github.com/hctaehoon/emrserverless-etl-cicd-pipeline/assets/113021892/41a6eec5-4b34-44d6-82c7-0acb72b5a2b9)

* 데이터의 정합성 체크 결과도 Slack을 통해 확인할 수 있도록 구성하였습니다.
![정합성체크](https://github.com/hctaehoon/emrserverless-etl-cicd-pipeline/assets/113021892/a3cab3c1-4b1a-4db7-9e79-d4b0a56eba19)


## 프로젝트-전체 아키텍처





## 프로젝트 사용 스택

<h3 align="left">Languages and Tools:</h3>
<p align="left"> <a href="https://www.docker.com/" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/docker/docker-original-wordmark.svg" alt="docker" width="40" height="40"/> </a> <a href="https://www.postgresql.org" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/postgresql/postgresql-original-wordmark.svg" alt="postgresql" width="40" height="40"/> </a> <a href="https://www.python.org" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" alt="python" width="40" height="40"/> </a> <a href="https://redis.io" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/redis/redis-original-wordmark.svg" alt="redis" width="40" height="40"/> </a> <a href="https://www.scala-lang.org" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/scala/scala-original.svg" alt="scala" width="40" height="40"/> 
</a> </p>

![Apache Airflow](https://img.shields.io/badge/Airflow-blue) 
![AWS EMR (Serverless)](https://img.shields.io/badge/AWS_EMR(serverless)-yellow)
![GitHub Actions](https://img.shields.io/badge/GithubAction-black)
![Spark](https://img.shields.io/badge/Spark-green)

