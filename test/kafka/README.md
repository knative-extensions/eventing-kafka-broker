# kafka

## Upgrading to a newer version of the Strimzi Kafka operator

In order to upgrade to a newer version of the Strimzi Kafka operator you would need to create a new version of the `strimzi-cluster-operator.yaml` file in this directory. To do so simply follow these steps:

```bash
cd $HOME
git clone https://github.com/strimzi/strimzi-kafka-operator.git
cd strimzi-kafka-operator
git checkout release-0.29.x # checkout the release branch that you're interested in
cd install/cluster-operator
for i in $(ls | grep ".yaml"); do echo "---" >> $HOME/strimzi-cluster-operator.yaml && cat $i >> $HOME/strimzi-cluster-operator.yaml; done
sed -i "s/myproject/kafka/g" $HOME/strimzi-cluster-operator.yaml
```

Once the new file `$HOME/strimzi-cluster-operator.yaml` has been created you can copy it to your local clone of the knative/eventing-kafka-broker git repository and make it part of your Strimzi Kafka update PR, following the project's contribution guidelines.

Depending on the actual Strimzi Kafka operator version you're upgrading to chances are that you need to modify the file `kafka-ephemeral.yaml` in this directory as well as it contains references to specific Kafka versions and configuration settings.
