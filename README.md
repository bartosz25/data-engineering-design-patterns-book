# Data Engineering Design Patterns 
## Code snippets

<img src="assets/cover_color.jpg" align="left" width="300" />

Welcome to the Github repository of the [Data Engineering Design Patterns](https://www.oreilly.com/library/view/data-engineering-design/9781098165826/). The organization follows the book chapters. 

Each pattern has a dedicated `README.md` that explains how to set it up on your local machine. The requirements for running the snippets are:

* JDK 11
* Python 3.8
* Minikube 1.22.0
* Docker 23.0.2
* docker-compose 2.17.0

If you want to know more about the data generation part used in most of the examples, you can check my another repo: [data-generator-blogging-platform project](https://github.com/bartosz25/data-generator-blogging-platform).

**Disclaimer**: although the examples tend to be the most realistic possible, their goal is to focus on one problem at a time. For that reason they'll often be simplified version of the code you should deploy on production. For that reason you might find hardcoded credentials or batch pipelines exposing the processed data without any data quality guards. Hopefully, thanks to the patterns presented in the book you'll be able to identify and apply all the best practices to your workloads. 
