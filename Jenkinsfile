pipeline {
    agent any

    tools {
        // 名称需与 Jenkins 全局工具配置一致
        jdk 'JDK-17'
        maven 'Maven-3.6.3'
    }

    environment {
        JAVA_HOME = "${tool 'JDK-17'}"
        MAVEN_HOME = "${tool 'Maven-3.6.3'}"
        PATH = "${JAVA_HOME}/bin:${MAVEN_HOME}/bin:${env.PATH}"

        // Harbor 配置
        HARBOR_REGISTRY = "10.3.80.25:30003"
        IMAGE_NAMESPACE = "middleware"
        IMAGE_NAME = "knowledge-base"
        IMAGE_TAG = "${env.GIT_COMMIT ? env.GIT_COMMIT.take(7) : env.BUILD_NUMBER}"
    }

    // 推送后由 Webhook 触发；若未配置 Webhook，可保留轮询作为兜底
    triggers {
        githubPush()
        pollSCM('H/5 * * * *')
    }

    options {
        timestamps()
        disableConcurrentBuilds()
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Env Check') {
            steps {
                sh '''
                  echo "JAVA_HOME=$JAVA_HOME"
                  echo "MAVEN_HOME=$MAVEN_HOME"
                  which java || true
                  java -version
                  if [ -f "$JAVA_HOME/conf/security/java.security" ]; then
                    ls -l "$JAVA_HOME/conf/security/java.security"
                  else
                    echo "java.security not found under $JAVA_HOME"
                  fi
                '''
            }
        }

        stage('Build & Test') {
            steps {
                script {
                    def mvnCmd = fileExists('settings.xml') ? 'mvn -s settings.xml' : 'mvn'
                    sh "${mvnCmd} -B clean verify"
                }
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/target/surefire-reports/*.xml'
                }
            }
        }

        stage('Package') {
            steps {
                script {
                    def mvnCmd = fileExists('settings.xml') ? 'mvn -s settings.xml' : 'mvn'
                    sh "${mvnCmd} -B package -DskipTests"
                }
            }
            post {
                success {
                    archiveArtifacts artifacts: 'target/*.jar', fingerprint: true
                }
            }
        }

        stage('Build & Push Base Images') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'harbor-admin', usernameVariable: 'HARBOR_USER', passwordVariable: 'HARBOR_PASS')]) {
                    sh '''
                      set -e
                      echo "$HARBOR_PASS" | docker login $HARBOR_REGISTRY -u "$HARBOR_USER" --password-stdin

                      # 构建并推送 maven 构建基础镜像（Dockerfile.base 第一阶段）
                      docker build -f Dockerfile.base --target maven-builder -t $HARBOR_REGISTRY/$IMAGE_NAMESPACE/$IMAGE_NAME:maven .
                      docker push $HARBOR_REGISTRY/$IMAGE_NAMESPACE/$IMAGE_NAME:maven

                      # 构建并推送运行时基础镜像（Dockerfile.base 最终阶段）
                      docker build -f Dockerfile.base -t $HARBOR_REGISTRY/$IMAGE_NAMESPACE/$IMAGE_NAME:runtime .
                      docker push $HARBOR_REGISTRY/$IMAGE_NAMESPACE/$IMAGE_NAME:runtime
                    '''
                }
            }
        }

        stage('Build & Push App Image') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'harbor-admin', usernameVariable: 'HARBOR_USER', passwordVariable: 'HARBOR_PASS')]) {
                    sh '''
                      set -e
                      echo "$HARBOR_PASS" | docker login $HARBOR_REGISTRY -u "$HARBOR_USER" --password-stdin

                      APP_IMAGE=$HARBOR_REGISTRY/$IMAGE_NAMESPACE/$IMAGE_NAME
                      docker build -f Dockerfile -t $APP_IMAGE:$IMAGE_TAG -t $APP_IMAGE:latest .
                      docker push $APP_IMAGE:$IMAGE_TAG
                      docker push $APP_IMAGE:latest
                    '''
                }
            }
        }
    }

    post {
        success {
            echo 'Build succeeded. Artifact archived.'
        }
        failure {
            echo 'Build failed. Please check the logs.'
        }
    }
}
