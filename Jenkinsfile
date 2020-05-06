pipeline {

    agent any

    stages {

        stage("Build") {
            steps {
                sh "./gradlew clean buildDockerImage --info"
            }
        }

        stage('Publish') {
            steps {
                script {
                    sh "./gradlew pushDockerImage --info"
                }
            }

        }
    }

}