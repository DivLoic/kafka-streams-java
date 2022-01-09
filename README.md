# Kafka Streams Java
###### Hands on exercise from the [Kafka Streams](#) course v2

## Setup
[jenv](https://www.jenv.be/)
```shell
brew install jenv
```

[Java 11](https://docs.aws.amazon.com/corretto/latest/corretto-11-ug/downloads-list.html)
```shell
brew tap homebrew/cask-versions
brew install --cask corretto11
jenv add /Library/Java/JavaVirtualMachines/amazon-corretto-11.jdk/Contents/Home/
```

[Maven](https://maven.apache.org/download.cgi)
```shell
brew install maven
```

[Confluent](https://www.confluent.io/installation/)
```shell
curl -O http://packages.confluent.io/archive/6.2/confluent-6.2.0.tar.gz
tar zxf confluent-6.2.0.tar.gz
# Add to you profile
export PATH=/Users/loicd/Personal/bin/confluent-6.2.0/bin:$PATH
```

## Organisation