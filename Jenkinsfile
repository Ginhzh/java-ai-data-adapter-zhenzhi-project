pipeline {
    agent any

    tools {
        // 名称需与 Jenkins 全局工具配置一致
        jdk 'JDK-17'
        maven 'Maven-3.6.3'
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
