# Data Engineering Design Patterns 
## Code snippets

Welcome to the Github repository of the "Data Engineering Design Patterns" book. The organization follows the book chapters. 

Each pattern has a dedicated `README.md` that explains how to set it up on your local machine. Besides, you will often find the implementations available in 2 programming languages: Scala or Java, and Python. If there is only one implementation, it means another one was not possible.

The code snippets were ran and tested against the following configuration:

* JDK 11
* Python 3.8
* Minikube 1.22.0
* Docker 23.0.2
* docker-compose 2.17.0
* OS: Ubuntu, MacOS

**Disclaimer**: although the examples tend to be the most realistic possible, their goal is to focus on one problem at a time. For that reason they'll often be simplified version of the code you should deploy on production. For that reason you might find hardcoded credentials or batch pipelines exposing the processed data without any data quality guards. Hopefully, thanks to the patterns presented in the book you'll be able to identify and apply all the best practices to your workloads. 
