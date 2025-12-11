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
