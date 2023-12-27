## 프로젝트 개요

기존에 매일 진행되는 Log 데이터 ETL 작업에서 발생하는 Redshift의 부하 문제와 비용 증가를 해결하기 위해, AWS의 EMR Serverless 환경의 Spark를 활용한 분산 처리 Migration 프로젝트를 진행하게 되었습니다.
![현재 파이프라인](https://github.com/hctaehoon/emrserverless-etl-cicd-pipeline/assets/113021892/8183583c-6c6f-4d18-83ed-d45af8db8618)


## 프로젝트 진행
프로젝트에서 저는 팀원들의 PySpark 코드를 Scala로 변환하고 컴파일하는 역할을 맡았으며, 자동화된 파이프라인 구축을 통한 프로젝트의 고도화를 추진하였습니다. 하지만, 기업의 비용 문제로 인해 제공받은 EMR 환경이 Serverless가 아닌 EC2였기에, 데이터 정합성 확인 및 자동화 파이프라인 테스트와 같은 권한이 필요한 작업을 자유롭게 진행할 수 없었습니다. 이에 대한 해결책으로, 개인 AWS 계정에서 직접 파이프라인을 구축하여 프로젝트를 진행하였습니다

[EMR serverless 구축](https://github.com/hctaehoon/emrserverless-etl-cicd-pipeline/wiki/Airflow-CICD-%ED%8C%8C%EC%9D%B4%ED%94%84%EB%9D%BC%EC%9D%B8#emr-serverless-operator-%EB%A5%BC-%ED%86%B5%ED%95%9C-spark-job-%EC%9E%90%EB%8F%99%ED%99%94-%EA%B3%BC%EC%A0%95)


## 프로젝트 사용 스택

<h3 align="left">Languages and Tools:</h3>
<p align="left"> <a href="https://www.docker.com/" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/docker/docker-original-wordmark.svg" alt="docker" width="40" height="40"/> </a> <a href="https://www.postgresql.org" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/postgresql/postgresql-original-wordmark.svg" alt="postgresql" width="40" height="40"/> </a> <a href="https://www.python.org" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" alt="python" width="40" height="40"/> </a> <a href="https://redis.io" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/redis/redis-original-wordmark.svg" alt="redis" width="40" height="40"/> </a> <a href="https://www.scala-lang.org" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/scala/scala-original.svg" alt="scala" width="40" height="40"/> 
</a> </p>

![Apache Airflow](https://img.shields.io/badge/Airflow-blue) 
![AWS EMR (Serverless)](https://img.shields.io/badge/AWS_EMR(serverless)-yellow)
![GitHub Actions](https://img.shields.io/badge/GithubAction-black)
![Spark](https://img.shields.io/badge/Spark-green)

