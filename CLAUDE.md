# Payment Sync Outbox

결제 상태 동기화 아키텍처 고도화 프로젝트 (Transactional Outbox + Redis Streams).

## 기술 스택
- Java 17
- Spring Boot 3.5
- Spring Data JPA
- MySQL 8.0
- Redis 7 (Streams)
- Docker / Docker Compose (로컬 인프라)
- Gradle (wrapper 사용: `./gradlew`)

## 아키텍처 원칙
- 단일 인스턴스 환경을 기본으로 하되, 동시 접근/중복 처리를 방어하는 멱등성 제어를 적용한다.
- 과도한 추상화를 지양한다. 불필요한 인터페이스를 만들지 않는다 (구현체가 하나뿐이면 클래스만 둔다).

## 작업 규칙
- 한 번에 한 단계씩만 구현한다. 사용자가 지시한 단계의 작업만 수행하고, 다음 단계를 미리 구현하지 않는다.
- 각 단계마다 검증 가능한 테스트 코드 또는 실행 방법을 함께 제시한다.
- 사용자가 각 단계의 코드를 직접 검토하고 Git 커밋한다. Claude가 임의로 커밋하지 않는다.

## 로컬 포트 참고
- 개발 PC에 로컬 MySQL(3306)과 Jenkins(8080)가 상시 실행 중이라, 이 프로젝트는 MySQL 3307, 앱 8081을 사용한다.

## 로컬 실행
```bash
docker compose up -d      # MySQL 8.0 (호스트 3307 -> 컨테이너 3306), Redis 7 (6379)
./gradlew bootRun         # 앱 포트 8081, 확인: curl localhost:8081/actuator/health
./gradlew test
```
